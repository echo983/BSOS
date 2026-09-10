# BSOS Third-Party Client Developer Guide

This guide provides a comprehensive reference for software engineers and systems developers integrating with **BSOS** (Bare Space Object Storage). It covers client architecture, the Go SDK, cross-language gRPC integration, streaming invariants, collision management, and command-line automation.

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

	// Store an object
	data := []byte("Hello, BSOS!")
	res, err := c.PutWithJumpRetry(ctx, data)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Stored object: FID=0x%016x (jumped=%d)\n", res.FID, res.JumpsTaken)

	// Read it back
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
# Store data from file or pipe
echo "Hello Bare Space" | bsos put -
# Output: 0x5a1811e74a584061

# Query metadata
bsos head 0x5a1811e74a584061
# Output: fid: 0x5a1811e74a584061 size: 17 bytes

# Read back
bsos get 0x5a1811e74a584061
# Output: Hello Bare Space
```

---

## 2. Core Architecture & Mental Model

### Content Addressing (`FID = xxh3_64`)
BSOS is a content-addressed storage system. Every object is uniquely identified by the 64-bit XXH3 hash of its exact payload bytes:
$$\text{FID} = \text{xxh3\_64}(\text{payload})$$

Clients calculate the FID before uploading. The server trusts this FID and maps it deterministically to physical disk space without maintaining centralized metadata or B-trees.

### Critical Invariant: Pre-Declared `total_size`
Unlike conventional filesystems or object stores that accept unbounded chunk streams, **BSOS requires `total_size` to be declared in the first message of the gRPC Put stream** (`PutHeader`).

> [!IMPORTANT]
> **Why `total_size` must be known upfront:**
> The daemon uses `(fid, total_size)` to deterministically calculate the exact disk slot, verify allocation boundaries, and reserve raw disk extents in memory before reading a single data chunk.
> If you are streaming dynamic output of unknown length (e.g. real-time video or on-the-fly compression), you must buffer the data or write it to a temporary file first to determine its exact byte length.

### One-Hop Jump Collision Handling
Because object addresses are derived directly from hashes, two distinct objects of similar size could theoretically map to overlapping raw disk sectors (an extent collision).

BSOS resolves extent collisions with a **one-hop alias jump protocol**:
1. The client attempts direct placement under $\text{FID} = \text{xxh3\_64}(\text{payload})$.
2. If the daemon returns `ALREADY_EXISTS` (extent collision with a different object), the client probes jump codes $j \in [1, 255]$:
   $$\text{jumpData} = \text{payload} \mathbin{\Vert} \text{byte}(j)$$
   $$\text{jumpFID} = \text{xxh3\_64}(\text{jumpData})$$
3. The client uploads `jumpData` specifying `header.alias_for = FID`.
4. The daemon stores `jumpData` at `jumpFID` and writes a compact 16-byte alias record on disk mapping $\text{FID} \to \text{jumpFID}$.
5. Subsequent read requests to $\text{FID}$ automatically resolve through the alias, strip the salt byte, and return the original data seamlessly.

The client library (`PutWithJumpRetry`, `PutFileWithJumpRetry`, and `bsos put`) performs this jump procedure automatically.

### Thread Safety & Concurrency
- `*client.Client` is **safe for concurrent use by multiple goroutines**.
- Applications should create **one persistent `Client` instance** at startup and reuse it across all requests and workers.
- Avoid creating and destroying `Client` instances per request.

---

## 3. Go Client Reference (`pkg/client`)

### Initializing the Client

```go
// Connect to a BSOS daemon using default settings (1 MiB streaming chunks)
c, err := client.New("127.0.0.1:9090")

// Connect with custom options
c, err := client.New("127.0.0.1:9090",
    client.WithChunkSize(2 << 20), // 2 MiB chunk size
    client.WithDialOptions(grpc.WithBlock()),
)

// Wrap an existing grpc.ClientConn
c := client.NewFromConn(conn)
```

### In-Memory Operations

| Method | Signature | Description |
| :--- | :--- | :--- |
| `ComputeFID` | `ComputeFID(data []byte) uint64` | Computes 64-bit XXH3 hash of content. |
| `PutBytes` | `PutBytes(ctx, data) (uint64, error)` | Direct write without collision retry. |
| `PutWithJumpRetry` | `PutWithJumpRetry(ctx, data, ...JumpOptions) (JumpResult, error)` | Recommended: writes object with automatic jump retry. |
| `GetBytes` | `GetBytes(ctx, fid, ...Range) ([]byte, error)` | Reads entire object or specified byte range. |
| `Head` | `Head(ctx, fid) (uint64, error)` | Returns object size in bytes without retrieving payload. |
| `VerifyContent` | `VerifyContent(ctx, fid, expected, ...VerifyOptions) (bool, error)` | Verifies stored content matches expected payload. |

### Streaming & File Operations (Zero-RAM Overhead)

For large files (gigabytes to tens of gigabytes), use file-oriented helpers to stream directly to/from disk without loading objects into memory:

```go
// Precompute FID and size of an on-disk file
fid, size, err := client.ComputeFileFID("/path/to/archive.tar")

// Upload file directly from disk with automatic collision retry
res, err := c.PutFileWithJumpRetry(ctx, "/path/to/archive.tar")

// Download object directly to local disk
err := c.GetFile(ctx, res.FID, "/path/to/downloaded.tar")

// Download partial slice [start, end) directly to local disk
err := c.GetFile(ctx, res.FID, "/path/to/slice.bin", client.Range{Start: 1024, End: 2048})
```

### Cluster Metrics & Status

```go
// Check daemon and storage pool health
ok, err := c.Health(ctx)

// Query Bonnie target capacity metric (returns ch_d_pow2)
// Recommended max single-object size is (1 << chd) bytes
chdPow2, err := c.Bonnie(ctx)
maxObjectBytes := uint64(1) << chdPow2
```

### Error Classification Helpers

Use built-in error inspectors instead of parsing gRPC status strings:

```go
if client.IsConflict(err) {
    // gRPC codes.AlreadyExists (object FID exists or extent is occupied)
}
if client.IsNotFound(err) {
    // gRPC codes.NotFound (requested FID does not exist)
}
if client.IsResourceExhausted(err) {
    // gRPC codes.ResourceExhausted (disk or pool full)
}
if client.IsInvalidArgument(err) {
    // gRPC codes.InvalidArgument (total_size=0, self-alias, etc.)
}
```

---

## 4. Cross-Language Integration (Python, Rust, C++)

BSOS uses standard gRPC (`proto/bsos.proto`). Clients in any language follow this protocol sequence:

### Put Request Protocol Flow

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

> [!CAUTION]
> **Check errors after EVERY chunk send:**
> Per the BSOS transport specification, clients must check the stream status after every chunk sent. If the daemon rejects an upload (e.g. disk full, invalid header, unroutable extent), it terminates the stream immediately. Halting uploads on error saves network bandwidth and avoids wasted disk I/O.

### XXH3 Reference Implementations
- **Python**: `xxhash.xxh3_64_intdigest(data)` ([`examples/python/`](../examples/python/))
- **Rust**: `xxhash_rust::xxh3::xxh3_64(data)`
- **C/C++**: `XXH3_64bits(data, len)` from official `xxhash.h`

---

## 5. Command-Line Automation Recipes

### JSON Output & Scripting with `jq`

`bsos put`, `bsos head`, and `bsos bonnie` support `-json` for scripting:

```bash
# Upload a file and extract the FID into a variable
FID=$(bsos put -json /path/to/file.img | jq -r .fid)
echo "Uploaded to FID: ${FID}"

# Inspect object size via JSON
SIZE=$(bsos head -json "${FID}" | jq -r .size)
echo "Size: ${SIZE} bytes"
```

### Shell Pipe Streaming

Stream stdin directly to BSOS, and retrieve via stdout:

```bash
# Backup a folder straight to BSOS
tar -czf - ./my-directory | bsos put -json - > backup.json
BACKUP_FID=$(jq -r .fid < backup.json)

# Restore folder straight from BSOS
bsos get "${BACKUP_FID}" - | tar -xzf -
```

### Media / Range Streaming

BSOS supports partial byte reads via `-range`:

```bash
# Read bytes 0 through 1024
bsos get -range 0-1024 0x5a1811e74a584061 partial.bin

# Read from byte 1048576 (1 MiB) to the end of the object
bsos get -range 1048576- 0x5a1811e74a584061 tail.bin
```

---

## 6. Runnable Examples Directory

Ready-to-run examples are provided in the repository:

- [`examples/go/basic/main.go`](../examples/go/basic/main.go): Basic Put/Get/Head/Bonnie operations.
- [`examples/go/file_streaming/main.go`](../examples/go/file_streaming/main.go): Streaming multi-megabyte files without RAM buffering.
- [`examples/python/client.py`](../examples/python/client.py): Standalone Python client with gRPC stubs.
- [`examples/bash/pipeline.sh`](../examples/bash/pipeline.sh): Unix pipeline scripts with tarball streaming.
