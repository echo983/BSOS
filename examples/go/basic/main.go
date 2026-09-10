// Package main demonstrates basic CRUD, range read, and cluster query operations
// using the official BSOS Go client library.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/echo983/BSOS/pkg/client"
)

func main() {
	addr := "127.0.0.1:9090"
	if env := os.Getenv("BSOS_ADDR"); env != "" {
		addr = env
	}

	// 1. Initialize client
	// Third-party applications should create a single Client instance and share it
	// across goroutines throughout the application lifecycle.
	c, err := client.New(addr)
	if err != nil {
		log.Fatalf("Failed to create BSOS client: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 2. Health check
	ok, err := c.Health(ctx)
	if err != nil || !ok {
		log.Fatalf("Daemon health check failed: ok=%v, err=%v", ok, err)
	}
	fmt.Printf("[+] Connected to BSOS daemon at %s (status: OK)\n", addr)

	// 3. Query Bonnie single placement capacity metric
	chdPow2, err := c.Bonnie(ctx)
	if err != nil {
		log.Printf("[-] Failed to query Bonnie: %v", err)
	} else {
		maxBytes := uint64(1) << chdPow2
		fmt.Printf("[+] Bonnie ch_d_pow2 = %d (recommended max object size: %d bytes / %d KiB)\n",
			chdPow2, maxBytes, maxBytes/1024)
	}

	// 4. Put an object with collision jump retry
	payload := []byte("Hello, Bare Space Object Storage! Content-addressed with xxh3_64.")
	res, err := c.PutWithJumpRetry(ctx, payload)
	if err != nil {
		if client.IsConflict(err) {
			log.Printf("[!] Object already registered on daemon (FID: 0x%016x)\n", res.FID)
		} else {
			log.Fatalf("Put failed: %v", err)
		}
	} else {
		if res.JumpsTaken > 0 {
			fmt.Printf("[+] Stored object (jumped): logical FID=0x%016x, physical TargetFID=0x%016x, jumpCode=%d\n",
				res.FID, res.TargetFID, res.JumpsTaken)
		} else {
			fmt.Printf("[+] Stored object (direct): FID=0x%016x (%d bytes)\n", res.FID, len(payload))
		}
	}

	fid := client.ComputeFID(payload)

	// 5. Query object metadata (Head)
	size, err := c.Head(ctx, fid)
	if err != nil {
		log.Fatalf("Head failed: %v", err)
	}
	fmt.Printf("[+] Head check: FID=0x%016x, size=%d bytes\n", fid, size)

	// 6. Retrieve full object (GetBytes)
	retrieved, err := c.GetBytes(ctx, fid)
	if err != nil {
		log.Fatalf("GetBytes failed: %v", err)
	}
	fmt.Printf("[+] GetBytes retrieved: %q\n", string(retrieved))

	// 7. Range read (GetBytes with Range)
	part, err := c.GetBytes(ctx, fid, client.Range{Start: 7, End: 33})
	if err != nil {
		log.Fatalf("Range read failed: %v", err)
	}
	fmt.Printf("[+] Range read [7..33): %q\n", string(part))
}
