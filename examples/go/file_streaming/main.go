// Package main demonstrates streaming large files to and from BSOS
// without loading them entirely into memory.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/echo983/BSOS/pkg/client"
)

func main() {
	addr := "127.0.0.1:9090"
	if env := os.Getenv("BSOS_ADDR"); env != "" {
		addr = env
	}

	c, err := client.New(addr)
	if err != nil {
		log.Fatalf("Failed to create BSOS client: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create a temporary workspace
	tmpDir, err := os.MkdirTemp("", "bsos-streaming-example-*")
	if err != nil {
		log.Fatalf("MkdirTemp failed: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// 1. Generate a sample 2 MiB file
	srcPath := filepath.Join(tmpDir, "upload_sample.bin")
	srcFile, err := os.Create(srcPath)
	if err != nil {
		log.Fatalf("Create sample file: %v", err)
	}
	const fileSize = 2 * 1024 * 1024 // 2 MiB
	if _, err := io.CopyN(srcFile, rand.Reader, fileSize); err != nil {
		log.Fatalf("Write random data: %v", err)
	}
	srcFile.Close()

	// 2. Compute FID without loading the file into RAM
	fid, size, err := client.ComputeFileFID(srcPath)
	if err != nil {
		log.Fatalf("ComputeFileFID failed: %v", err)
	}
	fmt.Printf("[+] Precomputed FID for %s: 0x%016x (size: %d bytes)\n", filepath.Base(srcPath), fid, size)

	// 3. Upload file with automatic collision handling (zero RAM buffering)
	startTime := time.Now()
	res, err := c.PutFileWithJumpRetry(ctx, srcPath)
	if err != nil {
		log.Fatalf("PutFileWithJumpRetry failed: %v", err)
	}
	fmt.Printf("[+] Uploaded in %v: FID=0x%016x (jumped=%d)\n",
		time.Since(startTime), res.FID, res.JumpsTaken)

	// 4. Download file directly to local disk (zero RAM buffering)
	dstPath := filepath.Join(tmpDir, "downloaded.bin")
	startTime = time.Now()
	if err := c.GetFile(ctx, res.FID, dstPath); err != nil {
		log.Fatalf("GetFile failed: %v", err)
	}
	fmt.Printf("[+] Downloaded in %v to %s\n", time.Since(startTime), dstPath)

	// 5. Verify SHA/content equality
	srcBytes, _ := os.ReadFile(srcPath)
	dstBytes, _ := os.ReadFile(dstPath)
	if !bytes.Equal(srcBytes, dstBytes) {
		log.Fatalf("Integrity verification failed: uploaded and downloaded files do not match!")
	}
	fmt.Println("[+] Integrity verified: downloaded file matches source exactly")

	// 6. Range download a 64 KiB slice directly to disk
	slicePath := filepath.Join(tmpDir, "slice_64k.bin")
	const sliceOffset = 1024 * 1024 // 1 MiB offset
	const sliceLen = 64 * 1024      // 64 KiB
	if err := c.GetFile(ctx, res.FID, slicePath, client.Range{
		Start: sliceOffset,
		End:   sliceOffset + sliceLen,
	}); err != nil {
		log.Fatalf("GetFile with range failed: %v", err)
	}

	sliceBytes, _ := os.ReadFile(slicePath)
	if !bytes.Equal(sliceBytes, srcBytes[sliceOffset:sliceOffset+sliceLen]) {
		log.Fatalf("Range slice integrity verification failed!")
	}
	fmt.Printf("[+] Range slice verified: retrieved 64 KiB from offset 1 MiB\n")
}
