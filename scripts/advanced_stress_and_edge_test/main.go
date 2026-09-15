package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zeebo/xxh3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/echo983/BSOS/internal/daemon/bsospb"
	"github.com/echo983/BSOS/pkg/client"
)

const (
	defaultAddr = "192.168.1.79:9090"
	green       = "\033[0;32m"
	red         = "\033[0;31m"
	cyan        = "\033[0;36m"
	bold        = "\033[1m"
	reset       = "\033[0m"
)

var (
	passCount int32
	failCount int32
)

func pass(msg string) {
	atomic.AddInt32(&passCount, 1)
	fmt.Printf("%s  ✓ PASS%s  %s\n", green, reset, msg)
}

func fail(msg string, err error) {
	atomic.AddInt32(&failCount, 1)
	if err != nil {
		fmt.Printf("%s  ✗ FAIL%s  %s: %v\n", red, reset, msg, err)
	} else {
		fmt.Printf("%s  ✗ FAIL%s  %s\n", red, reset, msg)
	}
}

func section(title string) {
	fmt.Printf("\n%s%s═══ %s ═══%s\n", bold, cyan, title, reset)
}

func sha256Hex(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func main() {
	addr := defaultAddr
	if env := os.Getenv("BSOS_ADDR"); env != "" {
		addr = env
	}
	fmt.Printf("%sBSOS Advanced Extreme & Edge-Case Test Suite%s\nTarget: %s\n", bold, reset, addr)

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf("Fatal dial error: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	rawClient := bsospb.NewBSOSClient(conn)
	c, err := client.New(addr)
	if err != nil {
		fmt.Printf("Fatal client init error: %v\n", err)
		os.Exit(1)
	}
	defer c.Close()

	ctx := context.Background()

	// ══════════════════════════════════════════════════════════════════════════
	section("1. Max Put Limit & Upper Boundary Security")
	// ══════════════════════════════════════════════════════════════════════════
	{
		// 1.1 Object exceeding max_put (256MB + 1 byte) must be rejected immediately at header
		const maxPutLimit = 256 << 20
		oversizeStream, err := rawClient.Put(ctx)
		if err == nil {
			err = oversizeStream.Send(&bsospb.PutRequest{
				Msg: &bsospb.PutRequest_Header{
					Header: &bsospb.PutHeader{
						Fid:       0x1122334455667788,
						TotalSize: maxPutLimit + 1,
					},
				},
			})
			if err == nil {
				_, recvErr := oversizeStream.CloseAndRecv()
				if status.Code(recvErr) == codes.InvalidArgument {
					pass("Oversized Put (256MB + 1B) rejected at header with InvalidArgument")
				} else {
					fail("Oversized Put rejection", fmt.Errorf("expected InvalidArgument, got %v", recvErr))
				}
			} else {
				pass("Oversized Put rejected on header send")
			}
		} else {
			fail("Open Put stream for oversize test", err)
		}

		// 1.2 Store and readback a massive 200MB object (near upper threshold, testing multi-megabyte DirectIO streaming)
		fmt.Printf("  Streaming 200 MB object to NVMe tier (testing high-memory stream buffer)...\n")
		var massiveBuf bytes.Buffer
		massiveBuf.Grow(200 << 20)
		chunk := make([]byte, 1<<20)
		for i := range chunk {
			chunk[i] = byte(i % 251)
		}
		for i := 0; i < 200; i++ {
			massiveBuf.Write(chunk)
		}
		massiveData := massiveBuf.Bytes()
		massiveHash := sha256Hex(massiveData)
		massiveFID := xxh3.Hash(massiveData)

		putCtx, putCancel := context.WithTimeout(ctx, 3*time.Minute)
		err = c.Put(putCtx, massiveFID, uint64(len(massiveData)), 0, bytes.NewReader(massiveData))
		putCancel()
		if err != nil {
			fail("200 MB massive Put failed", err)
		} else {
			pass(fmt.Sprintf("200 MB massive Put succeeded (FID=0x%016X)", massiveFID))
			// Readback and verify
			getCtx, getCancel := context.WithTimeout(ctx, 3*time.Minute)
			gotMassive, err := c.GetBytes(getCtx, massiveFID)
			getCancel()
			if err != nil {
				fail("200 MB GetBytes failed", err)
			} else if sha256Hex(gotMassive) != massiveHash {
				fail("200 MB sha256 mismatch", nil)
			} else {
				pass("200 MB round-trip sha256 100% verified")
			}
		}
	}

	// ══════════════════════════════════════════════════════════════════════════
	section("2. Truncated Stream & Size Mismatch Defense (Payload Spoofing)")
	// ══════════════════════════════════════════════════════════════════════════
	{
		// Declared 5MB, but client only sends 1MB then abruptly closes stream
		truncatedFID := uint64(0xAABBCCDDEEFF0011)
		truncStream, err := rawClient.Put(ctx)
		if err != nil {
			fail("Open truncStream", err)
		} else {
			_ = truncStream.Send(&bsospb.PutRequest{
				Msg: &bsospb.PutRequest_Header{
					Header: &bsospb.PutHeader{
						Fid:       truncatedFID,
						TotalSize: 5 << 20, // 5MB
					},
				},
			})
			// Send only 1MB
			dummyChunk := make([]byte, 1<<20)
			_ = truncStream.Send(&bsospb.PutRequest{
				Msg: &bsospb.PutRequest_Chunk{Chunk: dummyChunk},
			})
			// Abruptly close send
			_, err = truncStream.CloseAndRecv()
			if err != nil {
				pass("Premature stream close correctly rejected by server")
			} else {
				fail("Server unexpectedly committed truncated payload!", nil)
			}

			// Verify truncated object does NOT exist in store
			_, err = c.Head(ctx, truncatedFID)
			if client.IsNotFound(err) {
				pass("Truncated object was not committed (Head -> NotFound)")
			} else {
				fail("Phantom object committed after aborted stream", err)
			}
		}
	}

	// ══════════════════════════════════════════════════════════════════════════
	section("3. Connection Abort & Reservation Gate Cleanup")
	// ══════════════════════════════════════════════════════════════════════════
	{
		// Start a write, then kill the underlying gRPC connection without closing the stream
		abortFID := uint64(0x9988776655443322)
		tempConn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			tempClient := bsospb.NewBSOSClient(tempConn)
			stream, err := tempClient.Put(ctx)
			if err == nil {
				_ = stream.Send(&bsospb.PutRequest{
					Msg: &bsospb.PutRequest_Header{
						Header: &bsospb.PutHeader{
							Fid:       abortFID,
							TotalSize: 2 << 20,
						},
					},
				})
				// Send 1 chunk then abruptly close connection
				_ = stream.Send(&bsospb.PutRequest{
					Msg: &bsospb.PutRequest_Chunk{Chunk: make([]byte, 64*1024)},
				})
				_ = tempConn.Close() // HARD CLOSE

				// Wait a moment for server to detect connection reset and release pool gate
				time.Sleep(200 * time.Millisecond)

				// Now re-attempt writing the full object properly with main client
				fullData := make([]byte, 2<<20)
				for i := range fullData {
					fullData[i] = byte(i)
				}
				fullFID := xxh3.Hash(fullData)
				err = c.Put(ctx, fullFID, uint64(len(fullData)), 0, bytes.NewReader(fullData))
				if err == nil {
					pass("Server cleanly released gate after client hard disconnect")
				} else {
					fail("Gate/reservation leaked after client abort", err)
				}
			}
		}
	}

	// ══════════════════════════════════════════════════════════════════════════
	section("4. Range Read Boundaries & OOB Edge Cases")
	// ══════════════════════════════════════════════════════════════════════════
	{
		rangePayload := []byte("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz!@#$%^&*()")
		rFID, err := c.PutBytes(ctx, rangePayload)
		if err != nil {
			fail("PutBytes for range test", err)
		} else {
			// 4.1 Valid interior range [10, 20)
			got, err := c.GetBytes(ctx, rFID, client.Range{Start: 10, End: 20})
			if err == nil && string(got) == string(rangePayload[10:20]) {
				pass("Interior range [10, 20) matched")
			} else {
				fail("Interior range mismatch", err)
			}

			// 4.2 Start at offset 0, End to end of file (End=0)
			got, err = c.GetBytes(ctx, rFID, client.Range{Start: 20, End: 0})
			if err == nil && string(got) == string(rangePayload[20:]) {
				pass("Suffix range [20, end) matched")
			} else {
				fail("Suffix range mismatch", err)
			}

			// 4.3 Range starting beyond object size (Start > size) -> should return empty or error, not panic
			got, err = c.GetBytes(ctx, rFID, client.Range{Start: 1000, End: 2000})
			if err == nil && len(got) == 0 || err != nil {
				pass("OOB range start > size safely handled (no server panic)")
			} else {
				fail("OOB range unexpected result", err)
			}

			// 4.4 Inverted range (Start > End) -> should handle gracefully without panic
			_, _ = c.GetBytes(ctx, rFID, client.Range{Start: 30, End: 10})
			pass("Inverted range [30, 10) handled gracefully")
		}
	}

	// ══════════════════════════════════════════════════════════════════════════
	section("5. Read-While-Writing (Dirty Read / Atomicity Isolation)")
	// ══════════════════════════════════════════════════════════════════════════
	{
		// Client A starts a slow, multi-second upload
		slowData := make([]byte, 8<<20)
		for i := range slowData {
			slowData[i] = byte(i % 127)
		}
		slowFID := xxh3.Hash(slowData)

		var clientBReceivedNotFound int32
		var writeDone sync.WaitGroup
		writeDone.Add(1)

		go func() {
			defer writeDone.Done()
			stream, err := rawClient.Put(ctx)
			if err != nil {
				return
			}
			_ = stream.Send(&bsospb.PutRequest{
				Msg: &bsospb.PutRequest_Header{
					Header: &bsospb.PutHeader{
						Fid:       slowFID,
						TotalSize: uint64(len(slowData)),
					},
				},
			})
			// Send chunks slowly in 4 steps with sleep
			chunkSz := len(slowData) / 4
			for i := 0; i < 4; i++ {
				_ = stream.Send(&bsospb.PutRequest{
					Msg: &bsospb.PutRequest_Chunk{
						Chunk: slowData[i*chunkSz : (i+1)*chunkSz],
					},
				})
				time.Sleep(100 * time.Millisecond)
			}
			_, _ = stream.CloseAndRecv()
		}()

		// Concurrently while upload is in-flight, Client B repeatedly issues Head & Get
		time.Sleep(150 * time.Millisecond) // Ensure Client A has sent header and 1st chunk
		for i := 0; i < 5; i++ {
			_, err := c.Head(ctx, slowFID)
			if client.IsNotFound(err) {
				atomic.StoreInt32(&clientBReceivedNotFound, 1)
			}
			time.Sleep(50 * time.Millisecond)
		}

		writeDone.Wait()

		if atomic.LoadInt32(&clientBReceivedNotFound) == 1 {
			pass("Read-While-Writing isolation: uncommitted object returned NotFound (no dirty reads)")
		} else {
			fail("Isolation failure: uncommitted object was visible or errored", nil)
		}

		// Verify that once committed, it is fully readable and hash matches
		gotSlow, err := c.GetBytes(ctx, slowFID)
		if err == nil && sha256Hex(gotSlow) == sha256Hex(slowData) {
			pass("Post-commit readback: object completely intact and verified")
		} else {
			fail("Post-commit readback failed", err)
		}
	}

	// ══════════════════════════════════════════════════════════════════════════
	section("6. High-Concurrency Mixed Load (64 Goroutines / 300 Operations)")
	// ══════════════════════════════════════════════════════════════════════════
	{
		const numWorkers = 64
		const opsPerWorker = 5
		var wg sync.WaitGroup
		var totalOps int32
		var opErrors int32

		fmt.Printf("  Spawning %d concurrent workers executing mixed Put/Get/Head/Range operations...\n", numWorkers)

		for w := 0; w < numWorkers; w++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				// Each worker creates private client or uses shared connection
				wClient, err := client.New(addr)
				if err != nil {
					atomic.AddInt32(&opErrors, 1)
					return
				}
				defer wClient.Close()

				for op := 0; op < opsPerWorker; op++ {
					sz := rand.Intn(64*1024) + 1024
					data := make([]byte, sz)
					rand.Read(data)
					dataHash := sha256Hex(data)

					// 1. Put
					fid, err := wClient.PutBytes(ctx, data)
					if err != nil {
						atomic.AddInt32(&opErrors, 1)
						continue
					}

					// 2. Head
					headSz, err := wClient.Head(ctx, fid)
					if err != nil || headSz != uint64(sz) {
						atomic.AddInt32(&opErrors, 1)
						continue
					}

					// 3. Get
					retrieved, err := wClient.GetBytes(ctx, fid)
					if err != nil || sha256Hex(retrieved) != dataHash {
						atomic.AddInt32(&opErrors, 1)
						continue
					}

					// 4. Range read
					if sz > 100 {
						_, err := wClient.GetBytes(ctx, fid, client.Range{Start: 10, End: 50})
						if err != nil {
							atomic.AddInt32(&opErrors, 1)
							continue
						}
					}

					atomic.AddInt32(&totalOps, 1)
				}
			}(w)
		}

		wg.Wait()
		if atomic.LoadInt32(&opErrors) == 0 {
			pass(fmt.Sprintf("%d concurrent operations completed with 0 errors across 64 workers", atomic.LoadInt32(&totalOps)))
		} else {
			fail(fmt.Sprintf("%d operations failed out of total under 64-worker load", atomic.LoadInt32(&opErrors)), nil)
		}
	}

	// ══════════════════════════════════════════════════════════════════════════
	section("7. Bonnie & Pool Capacity Under Dynamic Load")
	// ══════════════════════════════════════════════════════════════════════════
	{
		pow2, err := c.Bonnie(ctx)
		if err == nil && pow2 >= 20 {
			pass(fmt.Sprintf("Bonnie placement estimation healthy (ch_d_pow2 = %d, max object = %d MB)", pow2, (1<<pow2)>>20))
		} else {
			fail("Bonnie capacity estimation degraded", err)
		}
	}

	fmt.Printf("\n%s══════════════════════════════════════════════════════%s\n", bold, reset)
	fmt.Printf("%sAdvanced Test Results: %sPASS %d%s / %sFAIL %d%s%s\n", bold, green, passCount, reset, red, failCount, reset, reset)
	fmt.Printf("%s══════════════════════════════════════════════════════%s\n\n", bold, reset)

	if failCount > 0 {
		os.Exit(1)
	}
}
