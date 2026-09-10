package main

import (
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
	addr := fs.String("addr", defaultAddr(), "BSOS daemon address")
	asJSON := fs.Bool("json", false, "output JSON")
	noJump := fs.Bool("no-jump", false, "disable automatic one-hop jump collision retry")
	maxJumps := fs.Int("max-jumps", client.MaxJumpCode, "maximum jump attempts (1..255)")
	timeout := fs.Duration("timeout", 2*time.Minute, "timeout for Put operation")
	if err := fs.Parse(args); err != nil {
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

	var result client.JumpResult
	var totalSize uint64

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
		totalSize = uint64(stat.Size())
		if totalSize == 0 {
			fmt.Fprintf(os.Stderr, "E_INVALID_ARGUMENT: empty payload not allowed (total_size must be > 0)\n")
			return 1
		}

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
	} else {
		// Reading from stdin
		data, err := io.ReadAll(os.Stdin)
		if err != nil {
			fmt.Fprintf(os.Stderr, "E_IO: read stdin: %v\n", err)
			return 1
		}
		totalSize = uint64(len(data))
		if totalSize == 0 {
			fmt.Fprintf(os.Stderr, "E_INVALID_ARGUMENT: empty payload not allowed (total_size must be > 0)\n")
			return 1
		}

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
	}

	if *asJSON {
		_ = json.NewEncoder(os.Stdout).Encode(map[string]any{
			"fid":        fmt.Sprintf("0x%016x", result.FID),
			"target_fid": fmt.Sprintf("0x%016x", result.TargetFID),
			"size":       totalSize,
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
	addr := fs.String("addr", defaultAddr(), "BSOS daemon address")
	rangeStr := fs.String("range", "", "optional byte range start-end (e.g. 0-1024, or 10-)")
	timeout := fs.Duration("timeout", 2*time.Minute, "timeout for Get operation")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() < 1 {
		fmt.Fprintf(os.Stderr, "Usage: bsos get [flags] <fid> [output-file]\n")
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

	_, _, err = c.Get(ctx, fid, out, optRange...)
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
	addr := fs.String("addr", defaultAddr(), "BSOS daemon address")
	asJSON := fs.Bool("json", false, "output JSON")
	timeout := fs.Duration("timeout", 10*time.Second, "timeout for Head operation")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() < 1 {
		fmt.Fprintf(os.Stderr, "Usage: bsos head [flags] <fid>\n")
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
	addr := fs.String("addr", defaultAddr(), "BSOS daemon address")
	asJSON := fs.Bool("json", false, "output JSON")
	timeout := fs.Duration("timeout", 10*time.Second, "timeout for Bonnie operation")
	if err := fs.Parse(args); err != nil {
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
	addr := fs.String("addr", defaultAddr(), "BSOS daemon address")
	quiet := fs.Bool("quiet", false, "quiet mode (exit code only)")
	timeout := fs.Duration("timeout", 5*time.Second, "timeout for Health check")
	if err := fs.Parse(args); err != nil {
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
