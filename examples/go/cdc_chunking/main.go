// Package main demonstrates FastCDC chunking, FileManifest inspection,
// transparent auto-retrieval, and sparse range reads using the BSOS Go SDK.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/echo983/BSOS/pkg/cdc"
	"github.com/echo983/BSOS/pkg/client"
)

func main() {
	addr := "127.0.0.1:9090"
	if env := os.Getenv("BSOS_ADDR"); env != "" {
		addr = env
	}

	// 1. Initialize BSOS client
	c, err := client.New(addr)
	if err != nil {
		log.Fatalf("Failed to connect to BSOS: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// 2. Generate sample 32MB dataset for demonstration
	const dataSize = 32 * 1024 * 1024 // 32 MB
	fmt.Printf("[1] Generating %d MB pseudo-random dataset...\n", dataSize/(1024*1024))
	data := make([]byte, dataSize)
	if _, err := rand.Read(data); err != nil {
		log.Fatalf("Failed to generate test data: %v", err)
	}

	// 3. Upload using Track 2: FastCDC Content-Defined Chunking
	fmt.Println("[2] Uploading large object via FastCDC (PutCDC)...")
	opts := client.CDCOptions{
		Workers:         8,
		MinChunkSize:    cdc.DefaultMinSize,    // 4 MB
		TargetChunkSize: cdc.DefaultTargetSize, // 16 MB
		MaxChunkSize:    cdc.DefaultMaxSize,    // 32 MB
		Filename:        "example_large_blob.bin",
		ProgressCallback: func(completedChunks, totalChunks int, bytesUploaded uint64) {
			fmt.Printf("    -> Progress: %d/%d chunks processed\n", completedChunks, totalChunks)
		},
	}

	res, err := c.PutCDC(ctx, bytes.NewReader(data), uint64(len(data)), opts)
	if err != nil {
		log.Fatalf("PutCDC failed: %v", err)
	}

	fmt.Printf("    [+] Upload complete!\n")
	fmt.Printf("        Manifest FID:      0x%016x\n", res.ManifestFID)
	fmt.Printf("        Full Content Hash: 0x%016x\n", res.FullContentHash)
	fmt.Printf("        Total Chunks:      %d (Uploaded: %d, Deduplicated: %d)\n",
		res.ChunkCount, res.UploadedCount, res.DeduplicatedCount)

	// 4. Inspect Manifest metadata & chunk topology
	fmt.Println("\n[3] Inspecting FileManifest metadata...")
	manifest, err := c.InspectManifest(ctx, res.ManifestFID)
	if err != nil {
		log.Fatalf("InspectManifest failed: %v", err)
	}

	fmt.Printf("    Manifest Version: %d, File: %q, Total Size: %d bytes\n",
		manifest.Version, manifest.Filename, manifest.TotalSize)
	for i, chunk := range manifest.Chunks {
		fmt.Printf("      Chunk [%02d] -> FID: 0x%016x | Offset: %8d | Size: %8d\n",
			i, chunk.Fid, chunk.Offset, chunk.Size)
	}

	// 5. Transparent Retrieval via GetAuto
	fmt.Println("\n[4] Downloading and reassembling full file via GetAuto...")
	var fullBuf bytes.Buffer
	if err := c.GetAuto(ctx, res.ManifestFID, &fullBuf); err != nil {
		log.Fatalf("GetAuto failed: %v", err)
	}
	if bytes.Equal(fullBuf.Bytes(), data) {
		fmt.Printf("    [+] Reassembly verified! Retrieved %d bytes matching original bit-for-bit.\n", fullBuf.Len())
	} else {
		log.Fatalf("    [-] Reassembly content mismatch!")
	}

	// 6. Sparse Range Read (reading 5MB from offset 10MB)
	fmt.Println("\n[5] Executing sparse range read [10MB, 15MB)...")
	const rStart = 10 * 1024 * 1024
	const rEnd = 15 * 1024 * 1024
	var rangeBuf bytes.Buffer
	if err := c.GetCDC(ctx, res.ManifestFID, &rangeBuf, client.Range{Start: rStart, End: rEnd}); err != nil {
		log.Fatalf("GetCDC range read failed: %v", err)
	}
	if bytes.Equal(rangeBuf.Bytes(), data[rStart:rEnd]) {
		fmt.Printf("    [+] Sparse Range Read verified! Fetched exact %d bytes window.\n", rangeBuf.Len())
	} else {
		log.Fatalf("    [-] Sparse range read content mismatch!")
	}
}
