# BSOS Third-Party Client Developer Guide

This guide provides a comprehensive reference for software engineers and systems developers integrating with **BSOS** (Bare Space Object Storage). It covers client architecture, the official Go SDK, the Dual-Path & FastCDC chunking subsystem, cross-language gRPC integration, streaming invariants, collision management, and command-line automation.

---

## 1. Quickstart (5 Minutes)

### Go Application

Add the client package to your Go module:

```bash
go get github.com/echo983/BSOS/pkg/client
```

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/echo983/BSOS/pkg/client"
)

func main() {
	// Initialize a single shared client instance
	c, err := client.New("127.0.0.1:9090")
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	ctx := context.Background()

	// Store an object (auto-routed: atomic single-pass for <=16MB, FastCDC for >16MB)
	data := []byte("Hello, BSOS!")
	res, err := c.PutWithJumpRetry(ctx, data)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Stored object: FID=0x%016x (jumped=%d)\n", res.FID, res.JumpsTaken)

	// Read it back (GetAuto transparently handles atomic objects and CDC manifests)
	content, err := c.GetBytes(ctx, res.FID)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Retrieved: %s\n", string(content))
}
```

### Command-Line Interface (CLI)

Install the CLI tool:

```bash
go install github.com/echo983/BSOS/cmd/bsos@latest
```

Store and retrieve data:

```bash
# Store small/medium file (auto atomic)
echo "Hello Bare Space" | bsos put -
# Output: 0x5a1811e74a584061

# Store large file (auto FastCDC chunked)
bsos put huge_dataset.tar
# Output: 0x39b755dc5d19105d (manifest: 16 chunks, 0 deduped, 16 uploaded)

# Inspect CDC chunk topology
bsos manifest inspect 0x39b755dc5d19105d

# Transparent retrieve (works for atomic and CDC files alike)
bsos get 0x39b755dc5d19105d recovered.tar

# Sparse range read (fetches only overlapping chunks)
bsos get -range 10485760-26214400 0x39b755dc5d19105d slice.bin
```

---

## 2. Core Architecture & Mental Model

### Content Addressing (`FID = xxh3_64`)
BSOS is a content-addressed storage system. Every object is uniquely identified by the 64-bit XXH3 hash of its exact payload bytes:
$$\text{FID} = \text{xxh3\_64}(\text{payload})$$

Clients calculate the FID before uploading. The server trusts this FID and maps it deterministically to physical disk space without maintaining centralized metadata or B-trees.

### Dual-Path Upload Architecture

BSOS implements an optimized dual-path storage pipeline:

```
                         [Payload Upload]
                                 │
                 ┌───────────────┴───────────────┐
                 ▼ (<= 16 MB)                    ▼ (> 16 MB)
         [Track 1: PutAtomic]             [Track 2: PutCDC]
                 │                               │
        ┌────────┴────────┐             ┌────────┴────────┐
   os.ReadFile (1-Pass I/O)             FastCDC Chunker (4~32MB)
   xxh3 内存秒算 FID                     Worker 并发并发上传 Chunks
   直接 gRPC 流式提交 (无二次读盘)          提交 FileManifest 元数据对象
```

1. **Track 1 (`<= 16MB`): `PutAtomic` / `PutFileAtomic`**
   - Single-pass memory loading via `os.ReadFile`.
   - Computes XXH3 in RAM and streams directly, completely eliminating the two-pass disk read bottleneck.
2. **Track 2 (`> 16MB`): `PutCDC` / `PutFileCDC`**
   - FastCDC content-defined chunking (Min 4MB, Target 16MB, Max 32MB).
   - High-throughput parallel upload across a configurable worker pool (default 8 workers).
   - Generates a self-contained, content-addressed `FileManifest` object with `BSMN\x01` magic header.
   - Provides fine-grained deduplication: modifying 64KB in a 100MB file only re-uploads the 1 mutated chunk (~95% bandwidth savings).

### Critical Invariant: Pre-Declared `total_size`
Unlike conventional filesystems that accept unbounded chunk streams, **BSOS requires `total_size` to be declared in the first message of the gRPC Put stream** (`PutHeader`).

The daemon uses `(fid, total_size)` to deterministically calculate the exact disk slot, verify allocation boundaries, and reserve raw disk extents in memory before reading data chunks.

### One-Hop Jump Collision Handling
Because object addresses are derived directly from hashes, two distinct objects of similar size could theoretically map to overlapping raw disk sectors (an extent collision).

BSOS resolves extent collisions with a **one-hop alias jump protocol**:
1. The client attempts direct placement under $\text{FID} = \text{xxh3\_64}(\text{payload})$.
2. If the daemon returns `ALREADY_EXISTS` (extent collision with a different object), the client probes jump codes $j \in [1, 255]$:
   $$\text{jumpData} = \text{payload} \mathbin{\Vert} \text{byte}(j)$$
   $$\text{jumpFID} = \text{xxh3\_64}(\text{jumpData})$$
3. The client uploads `jumpData` specifying `header.alias_for = FID`.
4. The daemon stores `jumpData` at `jumpFID` and writes an alias pointer mapping $\text{FID} \to \text{jumpFID}$.
5. Subsequent read requests to $\text{FID}$ automatically resolve through the alias, strip the salt byte, and return original data seamlessly.

---

## 3. Public Go SDK Packages (`pkg/`)

All reusable libraries are organized under public Go packages for easy third-party import:

| Package | Import Path | Description |
| :--- | :--- | :--- |
| **`client`** | `github.com/echo983/BSOS/pkg/client` | Official BSOS client SDK (Dual-Path, CDC, Transparent Reads, Jump Retries). |
| **`cdc`** | `github.com/echo983/BSOS/pkg/cdc` | Standalone FastCDC content-defined chunker engine with 4KB sector alignment. |
| **`manifest`** | `github.com/echo983/BSOS/pkg/manifest` | `FileManifest` wire protocol encoder/decoder and `BSMN\x01` magic sniffer. |
| **`bsospb`** | `github.com/echo983/BSOS/pkg/bsospb` | Public gRPC stubs and protobuf structs (`FileManifest`, `ChunkDescriptor`, etc.). |

### Go Client API Reference (`pkg/client`)

#### Initializing the Client

```go
// Connect to a BSOS daemon using default settings
c, err := client.New("127.0.0.1:9090")

// Connect with custom options
c, err := client.New("127.0.0.1:9090",
    client.WithChunkSize(2 << 20), // 2 MiB streaming buffer
    client.WithDialOptions(grpc.WithBlock()),
)
defer c.Close()
```

> [!NOTE]
> `*client.Client` is **safe for concurrent use across multiple goroutines**. Create a single instance at application startup and share it across the process lifecycle.

#### Upload Methods

| Method | Signature | Description |
| :--- | :--- | :--- |
| `PutAtomic` | `PutAtomic(ctx, data, ...JumpOptions) (JumpResult, error)` | Single-pass atomic write for payloads `<= 16MB`. Returns `ErrPayloadTooLarge` if `> 16MB`. |
| `PutFileAtomic` | `PutFileAtomic(ctx, path, ...JumpOptions) (JumpResult, error)` | Single-pass file upload for `<= 16MB` files without dual-read disk penalty. |
| `PutCDC` | `PutCDC(ctx, r, totalSize, ...CDCOptions) (CDCResult, error)` | Slices input stream via FastCDC and uploads chunks in parallel. Commits `FileManifest`. |
| `PutFileCDC` | `PutFileCDC(ctx, path, ...CDCOptions) (CDCResult, error)` | Streams and chunks large on-disk files via FastCDC with concurrent upload. |
| `PutWithJumpRetry` | `PutWithJumpRetry(ctx, data, ...JumpOptions) (JumpResult, error)` | In-memory atomic write with automatic one-hop jump collision retry. |
| `PutFileWithJumpRetry` | `PutFileWithJumpRetry(ctx, path, ...JumpOptions) (JumpResult, error)` | Auto-routing file upload with collision jump retry. |

#### Download & Inspection Methods

| Method | Signature | Description |
| :--- | :--- | :--- |
| `GetAuto` | `GetAuto(ctx, fid, w, ...Range) error` | **Recommended**: Transparently sniffs manifest magic header; reassembles CDC files or streams atomic objects automatically. |
| `GetBytes` | `GetBytes(ctx, fid, ...Range) ([]byte, error)` | Reads entire object or byte range into memory (transparently handles CDC and atomic). |
| `GetCDC` | `GetCDC(ctx, manifestFID, w, ...Range) error` | Reassembles a CDC file or executes sparse range reads across chunks. |
| `InspectManifest` | `InspectManifest(ctx, manifestFID) (*bsospb.FileManifest, error)` | Reads and decodes a `FileManifest`, returning chunk count, offsets, and hashes. |
| `Head` | `Head(ctx, fid) (uint64, error)` | Returns object byte size without downloading payload. |
| `VerifyContent` | `VerifyContent(ctx, fid, expected, ...VerifyOptions) (bool, error)` | Validates stored content against expected data with retry backoff. |

#### Cluster Status Methods

| Method | Signature | Description |
| :--- | :--- | :--- |
| `Health` | `Health(ctx) (bool, error)` | Checks daemon reachability and NVMe/zram pool health. |
| `Bonnie` | `Bonnie(ctx) (uint32, error)` | Queries largest contiguous power-of-2 placement size currently allocatable. |

---

## 4. Standalone FastCDC Package (`pkg/cdc`)

Third-party projects requiring content-defined chunking independent of BSOS can import `pkg/cdc` directly:

```go
package main

import (
	"fmt"
	"io"
	"os"

	"github.com/echo983/BSOS/pkg/cdc"
)

func main() {
	f, _ := os.Open("data.tar")
	defer f.Close()

	// Initialize FastCDC chunker with default 4MB/16MB/32MB bounds
	chunker, _ := cdc.NewChunker(f, cdc.DefaultOptions())

	for {
		chunk, err := chunker.Next()
		if err == io.EOF {
			break
		}
		fmt.Printf("Chunk FID: 0x%016x | Offset: %d | Size: %d bytes\n",
			chunk.FID, chunk.Offset, chunk.Size)
	}
}
```

---

## 5. Cross-Language Integration (Python, Rust, C++)

BSOS uses standard gRPC (`proto/bsos.proto`). Protobuf stubs can be generated for Python, Rust, C++, and other languages:

```
Client                                      BSOS Daemon
  │                                              │
  │── 1. PutRequest(PutHeader{fid, size, ...}) ─>│ (Checks extent, checks alias)
  │                                              │ (If error: closes stream immediately)
  │── 2. PutRequest(chunk 1) ───────────────────>│ (Writes chunk to raw disk extent)
  │── 3. PutRequest(chunk 2) ───────────────────>│
  │   ...                                        │
  │── 4. CloseSend() ───────────────────────────>│ (Commits index entry to ZRAM/disk)
  │<─ 5. PutResponse() ──────────────────────────│ (Success)
```

- **Python SDK**: See [`examples/python/client.py`](../examples/python/client.py)
- **XXH3 Implementations**:
  - Python: `xxhash.xxh3_64_intdigest(data)`
  - Rust: `xxhash_rust::xxh3::xxh3_64(data)`
  - C/C++: `XXH3_64bits(data, len)` from official `xxhash.h`

---

## 6. Runnable Examples Directory

Ready-to-run examples are provided in the repository:

- [`examples/go/cdc_chunking/main.go`](../examples/go/cdc_chunking/main.go): FastCDC chunking, `PutCDC`, manifest inspection, `GetAuto`, and sparse range reads.
- [`examples/go/basic/main.go`](../examples/go/basic/main.go): Basic Put/Get/Head/Bonnie operations.
- [`examples/go/file_streaming/main.go`](../examples/go/file_streaming/main.go): Streaming multi-megabyte files with single-pass memory speed.
- [`examples/python/client.py`](../examples/python/client.py): Python gRPC client with stubs.
- [`examples/bash/pipeline.sh`](../examples/bash/pipeline.sh): Unix pipeline scripts with tarball streaming.
