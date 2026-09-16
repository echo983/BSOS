package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/echo983/BSOS/pkg/cdc"
	"github.com/echo983/BSOS/pkg/client"
)

func defaultAddr() string {
	if addr := os.Getenv("BSOS_ADDR"); addr != "" {
		return addr
	}
	return "127.0.0.1:9090"
}

func runPut(args []string) int {
	fs := flag.NewFlagSet("bsos put", flag.ContinueOnError)
	addr := fs.String("addr", defaultAddr(), "BSOS daemon address (host:port)")
	asJSON := fs.Bool("json", false, "output JSON metadata")
	noJump := fs.Bool("no-jump", false, "disable automatic one-hop jump collision retry")
	maxJumps := fs.Int("max-jumps", client.MaxJumpCode, "maximum jump attempts (1..255)")
	atomicMode := fs.Bool("atomic", false, "force atomic single-object upload (payload must be <= 16MB)")
	cdcMode := fs.Bool("cdc", false, "force FastCDC content-defined chunking upload")
	workers := fs.Int("workers", client.DefaultCDCWorkers, "number of concurrent CDC upload workers")
	minChunk := fs.Int("min-chunk", cdc.DefaultMinSize, "FastCDC minimum chunk size in bytes")
	targetChunk := fs.Int("target-chunk", cdc.DefaultTargetSize, "FastCDC target chunk size in bytes")
	maxChunk := fs.Int("max-chunk", cdc.DefaultMaxSize, "FastCDC maximum chunk size in bytes")
	timeout := fs.Duration("timeout", 10*time.Minute, "timeout for Put operation")

	fs.Usage = func() {
		fmt.Println("Usage:")
		fmt.Println("  bsos put [options] [file|-]")
		fmt.Println()
		fmt.Println("Description:")
		fmt.Println("  Writes an object into BSOS content-addressed storage.")
		fmt.Println("  By default, BSOS automatically dual-path routes based on payload size:")
		fmt.Println("    - Small/Medium files (<= 16MB): Track 1 single-pass atomic upload directly into memory/NVMe.")
		fmt.Println("    - Large files (> 16MB): Track 2 FastCDC content-defined chunking (4~32MB) with concurrent workers.")
		fmt.Println()
		fmt.Println("Options:")
		fs.PrintDefaults()
		fmt.Println()
		fmt.Println("Examples:")
		fmt.Println("  bsos put photo.jpg")
		fmt.Println("  bsos put --cdc large_archive.tar")
		fmt.Println("  bsos put --atomic --json config.json")
		fmt.Println("  cat dataset.bin | bsos put -")
	}

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}

	if *atomicMode && *cdcMode {
		fmt.Fprintf(os.Stderr, "E_INVALID_ARGUMENT: cannot specify both --atomic and --cdc\n")
		return 2
	}

	c, err := client.New(*addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "E_CONNECTION: %v\n", err)
		return 1
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	cdcOpts := client.CDCOptions{
		Workers:         *workers,
		MinChunkSize:    *minChunk,
		TargetChunkSize: *targetChunk,
		MaxChunkSize:    *maxChunk,
	}

	if fs.NArg() > 0 && fs.Arg(0) != "-" {
		filePath := fs.Arg(0)
		stat, err := os.Stat(filePath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "E_IO: stat input file %s: %v\n", filePath, err)
			return 1
		}
		if stat.IsDir() {
			fmt.Fprintf(os.Stderr, "E_INVALID_ARGUMENT: %s is a directory\n", filePath)
			return 1
		}
		totalSize := uint64(stat.Size())
		if totalSize == 0 {
			fmt.Fprintf(os.Stderr, "E_INVALID_ARGUMENT: empty payload not allowed (total_size must be > 0)\n")
			return 1
		}

		if *atomicMode && totalSize > client.MaxAtomicPayloadSize {
			fmt.Fprintf(os.Stderr, "E_PAYLOAD_TOO_LARGE: file size (%d bytes) exceeds atomic %d byte limit\n", totalSize, client.MaxAtomicPayloadSize)
			return 1
		}

		useCDC := *cdcMode || (!*atomicMode && totalSize > client.MaxAtomicPayloadSize)
		if useCDC {
			res, err := c.PutFileCDC(ctx, filePath, cdcOpts)
			if err != nil {
				fmt.Fprintf(os.Stderr, "E_CDC_PUT_FAILED: %v\n", err)
				return 1
			}

			if *asJSON {
				_ = json.NewEncoder(os.Stdout).Encode(map[string]any{
					"manifest_fid": fmt.Sprintf("0x%016x", res.ManifestFID),
					"content_hash": fmt.Sprintf("0x%016x", res.FullContentHash),
					"size":         res.TotalSize,
					"type":         "cdc",
					"chunks":       res.ChunkCount,
					"deduplicated": res.DeduplicatedCount,
					"uploaded":     res.UploadedCount,
				})
				return 0
			}

			fmt.Printf("0x%016x (manifest: %d chunks, %d deduped, %d uploaded)\n", res.ManifestFID, res.ChunkCount, res.DeduplicatedCount, res.UploadedCount)
			return 0
		}

		// Atomic upload path
		var result client.JumpResult
		if *noJump {
			fid, err := c.PutFile(ctx, filePath)
			if err != nil {
				if client.IsConflict(err) {
					fmt.Fprintf(os.Stderr, "E_CONFLICT: fid 0x%016x already registered\n", fid)
				} else {
					fmt.Fprintf(os.Stderr, "E_PUT_FAILED: %v\n", err)
				}
				return 1
			}
			result = client.JumpResult{FID: fid, TargetFID: fid, JumpsTaken: 0}
		} else {
			res, err := c.PutFileWithJumpRetry(ctx, filePath, client.JumpOptions{MaxJumps: *maxJumps})
			if err != nil {
				if errors.Is(err, client.ErrJumpExhausted) {
					fmt.Fprintf(os.Stderr, "E_JUMP_EXHAUSTED: collision on fid 0x%016x, all %d jump retries failed\n", res.FID, *maxJumps)
				} else if client.IsConflict(err) {
					fmt.Fprintf(os.Stderr, "E_CONFLICT: fid 0x%016x conflict: %v\n", res.FID, err)
				} else {
					fmt.Fprintf(os.Stderr, "E_PUT_FAILED: %v\n", err)
				}
				return 1
			}
			result = res
		}

		if *asJSON {
			_ = json.NewEncoder(os.Stdout).Encode(map[string]any{
				"fid":        fmt.Sprintf("0x%016x", result.FID),
				"target_fid": fmt.Sprintf("0x%016x", result.TargetFID),
				"size":       totalSize,
				"type":       "atomic",
				"jumps":      result.JumpsTaken,
			})
			return 0
		}

		if result.JumpsTaken > 0 {
			fmt.Printf("0x%016x (jumped to 0x%016x via jump code %d)\n", result.FID, result.TargetFID, result.JumpsTaken)
		} else {
			fmt.Printf("0x%016x\n", result.FID)
		}
		return 0
	}

	// Reading from stdin
	data, err := io.ReadAll(os.Stdin)
	if err != nil {
		fmt.Fprintf(os.Stderr, "E_IO: read stdin: %v\n", err)
		return 1
	}
	totalSize := uint64(len(data))
	if totalSize == 0 {
		fmt.Fprintf(os.Stderr, "E_INVALID_ARGUMENT: empty payload not allowed (total_size must be > 0)\n")
		return 1
	}

	if *atomicMode && totalSize > client.MaxAtomicPayloadSize {
		fmt.Fprintf(os.Stderr, "E_PAYLOAD_TOO_LARGE: stdin payload size (%d bytes) exceeds atomic %d byte limit\n", totalSize, client.MaxAtomicPayloadSize)
		return 1
	}

	useCDC := *cdcMode || (!*atomicMode && totalSize > client.MaxAtomicPayloadSize)
	if useCDC {
		res, err := c.PutCDC(ctx, bytes.NewReader(data), totalSize, cdcOpts)
		if err != nil {
			fmt.Fprintf(os.Stderr, "E_CDC_PUT_FAILED: %v\n", err)
			return 1
		}

		if *asJSON {
			_ = json.NewEncoder(os.Stdout).Encode(map[string]any{
				"manifest_fid": fmt.Sprintf("0x%016x", res.ManifestFID),
				"content_hash": fmt.Sprintf("0x%016x", res.FullContentHash),
				"size":         res.TotalSize,
				"type":         "cdc",
				"chunks":       res.ChunkCount,
				"deduplicated": res.DeduplicatedCount,
				"uploaded":     res.UploadedCount,
			})
			return 0
		}

		fmt.Printf("0x%016x (manifest: %d chunks, %d deduped, %d uploaded)\n", res.ManifestFID, res.ChunkCount, res.DeduplicatedCount, res.UploadedCount)
		return 0
	}

	var result client.JumpResult
	if *noJump {
		fid, err := c.PutBytes(ctx, data)
		if err != nil {
			if client.IsConflict(err) {
				fmt.Fprintf(os.Stderr, "E_CONFLICT: fid 0x%016x already registered\n", fid)
			} else {
				fmt.Fprintf(os.Stderr, "E_PUT_FAILED: %v\n", err)
			}
			return 1
		}
		result = client.JumpResult{FID: fid, TargetFID: fid, JumpsTaken: 0}
	} else {
		res, err := c.PutWithJumpRetry(ctx, data, client.JumpOptions{MaxJumps: *maxJumps})
		if err != nil {
			if errors.Is(err, client.ErrJumpExhausted) {
				fmt.Fprintf(os.Stderr, "E_JUMP_EXHAUSTED: collision on fid 0x%016x, all %d jump retries failed\n", res.FID, *maxJumps)
			} else if client.IsConflict(err) {
				fmt.Fprintf(os.Stderr, "E_CONFLICT: fid 0x%016x conflict: %v\n", res.FID, err)
			} else {
				fmt.Fprintf(os.Stderr, "E_PUT_FAILED: %v\n", err)
			}
			return 1
		}
		result = res
	}

	if *asJSON {
		_ = json.NewEncoder(os.Stdout).Encode(map[string]any{
			"fid":        fmt.Sprintf("0x%016x", result.FID),
			"target_fid": fmt.Sprintf("0x%016x", result.TargetFID),
			"size":       totalSize,
			"type":       "atomic",
			"jumps":      result.JumpsTaken,
		})
		return 0
	}

	if result.JumpsTaken > 0 {
		fmt.Printf("0x%016x (jumped to 0x%016x via jump code %d)\n", result.FID, result.TargetFID, result.JumpsTaken)
	} else {
		fmt.Printf("0x%016x\n", result.FID)
	}
	return 0
}

func runGet(args []string) int {
	fs := flag.NewFlagSet("bsos get", flag.ContinueOnError)
	addr := fs.String("addr", defaultAddr(), "BSOS daemon address (host:port)")
	rangeStr := fs.String("range", "", "optional byte range start-end (e.g. 0-1024, or 10-)")
	timeout := fs.Duration("timeout", 5*time.Minute, "timeout for Get operation")

	fs.Usage = func() {
		fmt.Println("Usage:")
		fmt.Println("  bsos get [options] <fid> [output-file|-]")
		fmt.Println()
		fmt.Println("Description:")
		fmt.Println("  Retrieves an object from BSOS content-addressed storage.")
		fmt.Println("  Automatically detects whether the FID is an atomic object or a FastCDC manifest,")
		fmt.Println("  transparently streaming or reassembling chunks on the fly.")
		fmt.Println()
		fmt.Println("Options:")
		fs.PrintDefaults()
		fmt.Println()
		fmt.Println("Examples:")
		fmt.Println("  bsos get 0x1234567890abcdef output.bin")
		fmt.Println("  bsos get -range 0-1048576 0x1234567890abcdef slice.bin")
		fmt.Println("  bsos get 0x1234567890abcdef - | tar -xvf -")
	}

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if fs.NArg() < 1 {
		fs.Usage()
		return 2
	}

	fidStr := fs.Arg(0)
	fid, err := strconv.ParseUint(fidStr, 0, 64)
	if err != nil {
		fmt.Fprintf(os.Stderr, "E_INVALID_ARGUMENT: invalid fid %q: %v\n", fidStr, err)
		return 2
	}

	var optRange []client.Range
	if *rangeStr != "" {
		parts := strings.Split(*rangeStr, "-")
		if len(parts) != 2 {
			fmt.Fprintf(os.Stderr, "E_INVALID_ARGUMENT: invalid range format %q (expected start-end)\n", *rangeStr)
			return 2
		}
		start, err := strconv.ParseUint(strings.TrimSpace(parts[0]), 10, 64)
		if err != nil {
			fmt.Fprintf(os.Stderr, "E_INVALID_ARGUMENT: invalid range start: %v\n", err)
			return 2
		}
		end := uint64(0)
		if strings.TrimSpace(parts[1]) != "" {
			end, err = strconv.ParseUint(strings.TrimSpace(parts[1]), 10, 64)
			if err != nil {
				fmt.Fprintf(os.Stderr, "E_INVALID_ARGUMENT: invalid range end: %v\n", err)
				return 2
			}
		}
		optRange = append(optRange, client.Range{Start: start, End: end})
	}

	var out io.Writer = os.Stdout
	if fs.NArg() > 1 && fs.Arg(1) != "-" {
		outFile, err := os.Create(fs.Arg(1))
		if err != nil {
			fmt.Fprintf(os.Stderr, "E_IO: create output file: %v\n", err)
			return 1
		}
		defer outFile.Close()
		out = outFile
	}

	c, err := client.New(*addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "E_CONNECTION: %v\n", err)
		return 1
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	err = c.GetAuto(ctx, fid, out, optRange...)
	if err != nil {
		if client.IsNotFound(err) {
			fmt.Fprintf(os.Stderr, "E_NOT_FOUND: fid 0x%016x not found\n", fid)
		} else {
			fmt.Fprintf(os.Stderr, "E_GET_FAILED: %v\n", err)
		}
		return 1
	}
	return 0
}

func runHead(args []string) int {
	fs := flag.NewFlagSet("bsos head", flag.ContinueOnError)
	addr := fs.String("addr", defaultAddr(), "BSOS daemon address (host:port)")
	asJSON := fs.Bool("json", false, "output JSON metadata")
	timeout := fs.Duration("timeout", 10*time.Second, "timeout for Head operation")

	fs.Usage = func() {
		fmt.Println("Usage:")
		fmt.Println("  bsos head [options] <fid>")
		fmt.Println()
		fmt.Println("Description:")
		fmt.Println("  Queries object existence and byte size without downloading payload.")
		fmt.Println()
		fmt.Println("Options:")
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if fs.NArg() < 1 {
		fs.Usage()
		return 2
	}

	fidStr := fs.Arg(0)
	fid, err := strconv.ParseUint(fidStr, 0, 64)
	if err != nil {
		fmt.Fprintf(os.Stderr, "E_INVALID_ARGUMENT: invalid fid %q: %v\n", fidStr, err)
		return 2
	}

	c, err := client.New(*addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "E_CONNECTION: %v\n", err)
		return 1
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	size, err := c.Head(ctx, fid)
	if err != nil {
		if client.IsNotFound(err) {
			fmt.Fprintf(os.Stderr, "E_NOT_FOUND: fid 0x%016x not found\n", fid)
		} else {
			fmt.Fprintf(os.Stderr, "E_HEAD_FAILED: %v\n", err)
		}
		return 1
	}

	if *asJSON {
		_ = json.NewEncoder(os.Stdout).Encode(map[string]any{
			"fid":  fmt.Sprintf("0x%016x", fid),
			"size": size,
		})
	} else {
		fmt.Printf("fid: 0x%016x size: %d bytes\n", fid, size)
	}
	return 0
}

func runBonnie(args []string) int {
	fs := flag.NewFlagSet("bsos bonnie", flag.ContinueOnError)
	addr := fs.String("addr", defaultAddr(), "BSOS daemon address (host:port)")
	asJSON := fs.Bool("json", false, "output JSON metadata")
	timeout := fs.Duration("timeout", 10*time.Second, "timeout for Bonnie operation")

	fs.Usage = func() {
		fmt.Println("Usage:")
		fmt.Println("  bsos bonnie [options]")
		fmt.Println()
		fmt.Println("Description:")
		fmt.Println("  Queries the largest contiguous power-of-2 placement size currently allocatable in the active pool.")
		fmt.Println()
		fmt.Println("Options:")
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}

	c, err := client.New(*addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "E_CONNECTION: %v\n", err)
		return 1
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	chdPow2, err := c.Bonnie(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "E_BONNIE_FAILED: %v\n", err)
		return 1
	}

	maxBytes := uint64(1) << chdPow2
	if *asJSON {
		_ = json.NewEncoder(os.Stdout).Encode(map[string]any{
			"ch_d_pow2": chdPow2,
			"max_bytes": maxBytes,
		})
	} else {
		fmt.Printf("ch_d_pow2: %d (max single placement estimate: %d bytes)\n", chdPow2, maxBytes)
	}
	return 0
}

func runHealth(args []string) int {
	fs := flag.NewFlagSet("bsos health", flag.ContinueOnError)
	addr := fs.String("addr", defaultAddr(), "BSOS daemon address (host:port)")
	quiet := fs.Bool("quiet", false, "quiet mode (exit code only)")
	timeout := fs.Duration("timeout", 5*time.Second, "timeout for Health check")

	fs.Usage = func() {
		fmt.Println("Usage:")
		fmt.Println("  bsos health [options]")
		fmt.Println()
		fmt.Println("Description:")
		fmt.Println("  Checks daemon reachability and underlying block/zram storage pool health.")
		fmt.Println()
		fmt.Println("Options:")
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}

	c, err := client.New(*addr)
	if err != nil {
		if !*quiet {
			fmt.Fprintf(os.Stderr, "E_CONNECTION: %v\n", err)
		}
		return 1
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	ok, err := c.Health(ctx)
	if err != nil {
		if !*quiet {
			fmt.Fprintf(os.Stderr, "E_HEALTH_CHECK_FAILED: %v\n", err)
		}
		return 1
	}

	if !ok {
		if !*quiet {
			fmt.Fprintf(os.Stderr, "DEGRADED\n")
		}
		return 1
	}

	if !*quiet {
		fmt.Println("OK")
	}
	return 0
}

func runManifest(args []string) int {
	if len(args) < 1 {
		printManifestUsage()
		return 2
	}

	switch args[0] {
	case "inspect":
		return runManifestInspect(args[1:])
	case "-h", "--help", "help":
		printManifestUsage()
		return 0
	default:
		fmt.Fprintf(os.Stderr, "E_BAD_COMMAND: unknown manifest command: %s\n", args[0])
		printManifestUsage()
		return 2
	}
}

func printManifestUsage() {
	fmt.Println("Usage:")
	fmt.Println("  bsos manifest inspect [options] <manifest-fid>")
	fmt.Println()
	fmt.Println("Commands:")
	fmt.Println("  inspect   Decode and inspect FastCDC FileManifest metadata and chunk topology")
}

func runManifestInspect(args []string) int {
	fs := flag.NewFlagSet("bsos manifest inspect", flag.ContinueOnError)
	addr := fs.String("addr", defaultAddr(), "BSOS daemon address (host:port)")
	asJSON := fs.Bool("json", false, "output JSON metadata")
	timeout := fs.Duration("timeout", 10*time.Second, "timeout for inspect operation")

	fs.Usage = func() {
		fmt.Println("Usage:")
		fmt.Println("  bsos manifest inspect [options] <manifest-fid>")
		fmt.Println()
		fmt.Println("Description:")
		fmt.Println("  Decodes and displays the FileManifest recorded under manifest-fid,")
		fmt.Println("  listing original size, content hash, filename, and all chunk descriptors.")
		fmt.Println()
		fmt.Println("Options:")
		fs.PrintDefaults()
		fmt.Println()
		fmt.Println("Examples:")
		fmt.Println("  bsos manifest inspect 0x1234567890abcdef")
		fmt.Println("  bsos manifest inspect -json 0x1234567890abcdef")
	}

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if fs.NArg() < 1 {
		fs.Usage()
		return 2
	}

	fidStr := fs.Arg(0)
	fid, err := strconv.ParseUint(fidStr, 0, 64)
	if err != nil {
		fmt.Fprintf(os.Stderr, "E_INVALID_ARGUMENT: invalid fid %q: %v\n", fidStr, err)
		return 2
	}

	c, err := client.New(*addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "E_CONNECTION: %v\n", err)
		return 1
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	m, err := c.InspectManifest(ctx, fid)
	if err != nil {
		if client.IsNotFound(err) {
			fmt.Fprintf(os.Stderr, "E_NOT_FOUND: fid 0x%016x not found\n", fid)
		} else {
			fmt.Fprintf(os.Stderr, "E_MANIFEST_FAILED: %v\n", err)
		}
		return 1
	}

	if *asJSON {
		type chunkInfo struct {
			Index  int    `json:"index"`
			FID    string `json:"fid"`
			Offset uint64 `json:"offset"`
			Size   uint64 `json:"size"`
		}
		chunks := make([]chunkInfo, len(m.Chunks))
		for i, ch := range m.Chunks {
			chunks[i] = chunkInfo{
				Index:  i,
				FID:    fmt.Sprintf("0x%016x", ch.Fid),
				Offset: ch.Offset,
				Size:   ch.Size,
			}
		}
		_ = json.NewEncoder(os.Stdout).Encode(map[string]any{
			"manifest_fid":      fmt.Sprintf("0x%016x", fid),
			"version":           m.Version,
			"total_size":        m.TotalSize,
			"full_content_hash": fmt.Sprintf("0x%016x", m.FullContentHash),
			"filename":          m.Filename,
			"chunk_target_size": m.ChunkTargetSize,
			"chunk_count":       len(m.Chunks),
			"chunks":            chunks,
		})
		return 0
	}

	fmt.Printf("File Manifest: 0x%016x\n", fid)
	fmt.Printf("  Version:           %d\n", m.Version)
	fmt.Printf("  Total Size:        %d bytes (%.2f MB)\n", m.TotalSize, float64(m.TotalSize)/(1024*1024))
	fmt.Printf("  Full Content Hash: 0x%016x\n", m.FullContentHash)
	if m.Filename != "" {
		fmt.Printf("  Original Filename: %s\n", m.Filename)
	}
	fmt.Printf("  Chunk Target Size: %d bytes (%.2f MB)\n", m.ChunkTargetSize, float64(m.ChunkTargetSize)/(1024*1024))
	fmt.Printf("  Total Chunks:      %d\n", len(m.Chunks))
	fmt.Println("  Chunks:")
	for i, ch := range m.Chunks {
		fmt.Printf("    [%3d] FID: 0x%016x | Offset: %10d | Size: %8d bytes\n", i, ch.Fid, ch.Offset, ch.Size)
	}
	return 0
}
