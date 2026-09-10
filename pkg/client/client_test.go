package client

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/zeebo/xxh3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/echo983/BSOS/internal/blk"
	"github.com/echo983/BSOS/internal/daemon"
	"github.com/echo983/BSOS/internal/pan"
)

func setupTestDaemon(t *testing.T) (*Client, func()) {
	t.Helper()
	dir := t.TempDir()
	diskPath := filepath.Join(dir, "disk.img")
	f, err := os.Create(diskPath)
	if err != nil {
		t.Fatal(err)
	}
	const diskSize = blk.GridStart + (32 << 20) // 32MB data grid
	if err := f.Truncate(diskSize); err != nil {
		t.Fatal(err)
	}
	hdr := make([]byte, blk.HeaderBytes)
	copy(hdr, "NBSS")
	binary.LittleEndian.PutUint16(hdr[4:6], blk.FormatVersion)
	binary.LittleEndian.PutUint64(hdr[8:16], 0x12345678)
	if _, err := f.WriteAt(hdr, 0); err != nil {
		t.Fatal(err)
	}
	f.Close()

	panPath := filepath.Join(dir, "pan.json")
	pool := pan.File{
		Version: 1,
		Devices: []pan.Device{
			{DevicePath: diskPath, DiskID: "0x12345678", Status: "match"},
		},
	}
	raw, err := json.Marshal(pool)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(panPath, raw, 0644); err != nil {
		t.Fatal(err)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	lis.Close()

	cfg := daemon.DefaultConfig()
	cfg.PanPath = panPath
	cfg.GRPCListen = addr
	cfg.TrimInterval = 0 // test controls trim

	srv, err := daemon.NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	go func() {
		_ = srv.Serve(addr)
	}()

	var c *Client
	for i := 0; i < 50; i++ {
		time.Sleep(20 * time.Millisecond)
		c, err = New(addr)
		if err == nil {
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			ok, hErr := c.Health(ctx)
			cancel()
			if hErr == nil && ok {
				break
			}
			_ = c.Close()
			c = nil
		}
	}
	if c == nil {
		_ = srv.Close()
		t.Fatalf("failed to connect to daemon at %s: %v", addr, err)
	}

	cleanup := func() {
		_ = c.Close()
		_ = srv.Close()
	}
	return c, cleanup
}

func TestComputeFID(t *testing.T) {
	data := []byte("hello bare space object storage")
	want := xxh3.Hash(data)
	got := ComputeFID(data)
	if got != want {
		t.Fatalf("ComputeFID mismatch: got 0x%X, want 0x%X", got, want)
	}
}

func TestClientPutGetBytesAndHead(t *testing.T) {
	c, cleanup := setupTestDaemon(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	payload := []byte("The quick brown fox jumps over the lazy dog. 1234567890!")
	fid, err := c.PutBytes(ctx, payload)
	if err != nil {
		t.Fatalf("PutBytes failed: %v", err)
	}
	if fid != ComputeFID(payload) {
		t.Fatalf("FID mismatch: got %d, want %d", fid, ComputeFID(payload))
	}

	// 1. Head check
	size, err := c.Head(ctx, fid)
	if err != nil {
		t.Fatalf("Head failed: %v", err)
	}
	if size != uint64(len(payload)) {
		t.Fatalf("Head size mismatch: got %d, want %d", size, len(payload))
	}

	// 2. Full Get
	data, err := c.GetBytes(ctx, fid)
	if err != nil {
		t.Fatalf("GetBytes failed: %v", err)
	}
	if !bytes.Equal(data, payload) {
		t.Fatalf("GetBytes payload mismatch: got %q, want %q", data, payload)
	}

	// 3. Range Get
	part, err := c.GetBytes(ctx, fid, Range{Start: 4, End: 19})
	if err != nil {
		t.Fatalf("Range GetBytes failed: %v", err)
	}
	if !bytes.Equal(part, payload[4:19]) {
		t.Fatalf("Range mismatch: got %q, want %q", part, payload[4:19])
	}

	// 4. Duplicate Put fails with conflict
	_, err = c.PutBytes(ctx, payload)
	if !IsConflict(err) {
		t.Fatalf("expected conflict on duplicate Put, got %v", err)
	}

	// 5. Non-existent Head / Get returns NotFound
	badFID := fid ^ 0xDEADBEEF
	_, err = c.Head(ctx, badFID)
	if !IsNotFound(err) {
		t.Fatalf("expected NotFound on bad Head, got %v", err)
	}
	_, err = c.GetBytes(ctx, badFID)
	if !IsNotFound(err) {
		t.Fatalf("expected NotFound on bad Get, got %v", err)
	}
}

func TestClientPutStreamingMultiChunk(t *testing.T) {
	c, cleanup := setupTestDaemon(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Use small chunk size (128 KB) and 1.5 MB payload to force 12 chunks
	smallChunkClient := &Client{
		conn:      c.conn,
		pb:        c.pb,
		chunkSize: 128 << 10,
	}

	payload := bytes.Repeat([]byte("BSOS-Streaming-Multi-Chunk-Data-Pattern-"), 40000) // ~1.6MB
	fid := ComputeFID(payload)

	err := smallChunkClient.Put(ctx, fid, uint64(len(payload)), 0, bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("streaming Put failed: %v", err)
	}

	readback, err := c.GetBytes(ctx, fid)
	if err != nil {
		t.Fatalf("readback failed: %v", err)
	}
	if !bytes.Equal(readback, payload) {
		t.Fatalf("streaming payload mismatch: length got %d, want %d", len(readback), len(payload))
	}
}

func TestClientEarlyRejectionStop(t *testing.T) {
	c, cleanup := setupTestDaemon(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Server rejects total_size = 0 per docs/CLIENT_SPEC.md transport details
	err := c.Put(ctx, 12345, 0, 0, bytes.NewReader(nil))
	if err == nil {
		t.Fatalf("expected error on total_size = 0")
	}
	if !IsInvalidArgument(err) {
		t.Fatalf("expected InvalidArgument, got %v", err)
	}

	// Server rejects self-alias (alias_for == fid)
	payload := []byte("self-alias test")
	fid := ComputeFID(payload)
	err = c.Put(ctx, fid, uint64(len(payload)), fid, bytes.NewReader(payload))
	if err == nil {
		t.Fatalf("expected error on self-alias")
	}
	if !IsInvalidArgument(err) {
		t.Fatalf("expected InvalidArgument on self-alias, got %v", err)
	}
}

func TestClientPutWithJumpRetry(t *testing.T) {
	c, cleanup := setupTestDaemon(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const diskSize = blk.GridStart + (32 << 20)

	// 1. Initial write of base object
	basePayload := []byte("First object occupying a specific data grid slot")
	baseFID, err := c.PutBytes(ctx, basePayload)
	if err != nil {
		t.Fatalf("initial PutBytes failed: %v", err)
	}

	_, occupiedSlot, _, err := blk.AddrForFID(diskSize, baseFID, uint64(len(basePayload)))
	if err != nil {
		t.Fatalf("AddrForFID failed: %v", err)
	}

	// 2. Find a colliding payload that hashes to the exact same slot but has different content
	var collidingPayload []byte
	var collidingFID uint64
	for i := 0; ; i++ {
		candidate := []byte(fmt.Sprintf("collision-candidate-payload-%d", i))
		cFID := ComputeFID(candidate)
		if cFID == baseFID {
			continue
		}
		_, slot, _, cErr := blk.AddrForFID(diskSize, cFID, uint64(len(candidate)))
		if cErr == nil && slot == occupiedSlot {
			j1 := append(append([]byte(nil), candidate...), 1)
			jFID := ComputeFID(j1)
			_, jSlot, _, jErr := blk.AddrForFID(diskSize, jFID, uint64(len(j1)))
			if jErr == nil && jSlot != occupiedSlot {
				collidingPayload = candidate
				collidingFID = cFID
				break
			}
		}
	}

	// 3. PutWithJumpRetry on collidingPayload:
	// Direct write encounters extent collision on occupiedSlot,
	// so it automatically executes jumpCode 1 and places the object via alias.
	res, err := c.PutWithJumpRetry(ctx, collidingPayload)
	if err != nil {
		t.Fatalf("PutWithJumpRetry failed: %v", err)
	}
	if res.JumpsTaken != 1 {
		t.Fatalf("expected JumpsTaken = 1, got %d", res.JumpsTaken)
	}
	if res.FID != collidingFID {
		t.Fatalf("expected logical FID = 0x%X, got 0x%X", collidingFID, res.FID)
	}
	if res.TargetFID == res.FID {
		t.Fatalf("expected TargetFID != FID after jump")
	}

	// 4. Readback via logical FID returns collidingPayload (salt byte stripped)
	data, err := c.GetBytes(ctx, res.FID)
	if err != nil {
		t.Fatalf("readback via logical FID failed: %v", err)
	}
	if !bytes.Equal(data, collidingPayload) {
		t.Fatalf("readback content mismatch: got %q, want %q", data, collidingPayload)
	}

	// 5. Readback via TargetFID returns physical payload with salt byte intact
	targetData, err := c.GetBytes(ctx, res.TargetFID)
	if err != nil {
		t.Fatalf("readback via TargetFID failed: %v", err)
	}
	if len(targetData) != len(collidingPayload)+1 || targetData[len(targetData)-1] != 1 {
		t.Fatalf("targetData suffix mismatch: %v", targetData)
	}

	// 6. Test jump exhaustion when an already confirmed object is retried with jump retry
	// (since alias_for cannot overwrite an existing confirmed object)
	_, err = c.PutWithJumpRetry(ctx, basePayload, JumpOptions{MaxJumps: 2})
	if err != ErrJumpExhausted {
		t.Fatalf("expected ErrJumpExhausted on confirmed object, got %v", err)
	}
}

func TestClientVerifyContent(t *testing.T) {
	c, cleanup := setupTestDaemon(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	payload := []byte("Verify content retry-safety payload")
	fid, err := c.PutBytes(ctx, payload)
	if err != nil {
		t.Fatalf("PutBytes failed: %v", err)
	}

	// 1. Identical content match
	matched, err := c.VerifyContent(ctx, fid, payload)
	if err != nil || !matched {
		t.Fatalf("VerifyContent identical failed: matched=%v, err=%v", matched, err)
	}

	// 2. Different content mismatch
	matched, err = c.VerifyContent(ctx, fid, []byte("different data of same len!!"))
	if err != nil || matched {
		t.Fatalf("VerifyContent mismatch failed: matched=%v, err=%v", matched, err)
	}

	// 3. Head size-only check
	matched, err = c.VerifyContent(ctx, fid, payload, VerifyOptions{
		CompareContent: false,
	})
	if err != nil || !matched {
		t.Fatalf("VerifyContent size check failed: matched=%v, err=%v", matched, err)
	}

	// 4. Missing fid
	_, err = c.VerifyContent(ctx, fid^0xFFFF, payload, VerifyOptions{
		MaxAttempts:    2,
		InitialBackoff: 10 * time.Millisecond,
	})
	if err == nil {
		t.Fatalf("expected error on missing fid verify")
	}
}

func TestClientBonnieAndHealth(t *testing.T) {
	c, cleanup := setupTestDaemon(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Health
	ok, err := c.Health(ctx)
	if err != nil || !ok {
		t.Fatalf("Health check failed: ok=%v, err=%v", ok, err)
	}

	// Bonnie
	chd, err := c.Bonnie(ctx)
	if err != nil {
		t.Fatalf("Bonnie RPC failed: %v", err)
	}
	if chd == 0 {
		t.Fatalf("expected non-zero Bonnie ch_d_pow2")
	}
}

func TestClientErrorHelpers(t *testing.T) {
	conflictErr := status.Error(codes.AlreadyExists, "already exists")
	if !IsConflict(conflictErr) {
		t.Fatalf("expected IsConflict to be true")
	}
	if IsNotFound(conflictErr) {
		t.Fatalf("expected IsNotFound to be false")
	}

	notFoundErr := status.Error(codes.NotFound, "not found")
	if !IsNotFound(notFoundErr) {
		t.Fatalf("expected IsNotFound to be true")
	}

	exhaustedErr := status.Error(codes.ResourceExhausted, "exhausted")
	if !IsResourceExhausted(exhaustedErr) {
		t.Fatalf("expected IsResourceExhausted to be true")
	}

	invalidErr := status.Error(codes.InvalidArgument, "invalid")
	if !IsInvalidArgument(invalidErr) {
		t.Fatalf("expected IsInvalidArgument to be true")
	}
}

func TestClientFileHelpers(t *testing.T) {
	c, cleanup := setupTestDaemon(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dir := t.TempDir()
	testPath := filepath.Join(dir, "sample.bin")
	data := bytes.Repeat([]byte("File-Helper-Test-Data-Payload-1234567890"), 5000) // ~200KB
	if err := os.WriteFile(testPath, data, 0644); err != nil {
		t.Fatal(err)
	}

	// 1. ComputeFileFID
	fid, size, err := ComputeFileFID(testPath)
	if err != nil {
		t.Fatalf("ComputeFileFID failed: %v", err)
	}
	if fid != ComputeFID(data) {
		t.Fatalf("ComputeFileFID fid mismatch: got 0x%X, want 0x%X", fid, ComputeFID(data))
	}
	if size != uint64(len(data)) {
		t.Fatalf("ComputeFileFID size mismatch: got %d, want %d", size, len(data))
	}

	// 2. PutFile
	putFID, err := c.PutFile(ctx, testPath)
	if err != nil {
		t.Fatalf("PutFile failed: %v", err)
	}
	if putFID != fid {
		t.Fatalf("PutFile fid mismatch: got 0x%X, want 0x%X", putFID, fid)
	}

	// 3. GetFile
	downloadPath := filepath.Join(dir, "downloaded.bin")
	if err := c.GetFile(ctx, fid, downloadPath); err != nil {
		t.Fatalf("GetFile failed: %v", err)
	}
	downloaded, err := os.ReadFile(downloadPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(downloaded, data) {
		t.Fatalf("GetFile content mismatch")
	}

	// 4. GetFile with Range
	rangePath := filepath.Join(dir, "slice.bin")
	if err := c.GetFile(ctx, fid, rangePath, Range{Start: 10, End: 50}); err != nil {
		t.Fatalf("GetFile with range failed: %v", err)
	}
	sliceData, err := os.ReadFile(rangePath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(sliceData, data[10:50]) {
		t.Fatalf("GetFile slice mismatch")
	}

	// 5. GetFile with non-existent FID cleans up orphan file
	badPath := filepath.Join(dir, "bad.bin")
	err = c.GetFile(ctx, fid^0xDEAD, badPath)
	if err == nil {
		t.Fatalf("expected error downloading non-existent fid")
	}
	if _, statErr := os.Stat(badPath); !os.IsNotExist(statErr) {
		t.Fatalf("expected orphan file to be deleted on failure")
	}

	// 6. PutFile on empty file fails
	emptyPath := filepath.Join(dir, "empty.bin")
	if err := os.WriteFile(emptyPath, nil, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := c.PutFile(ctx, emptyPath); err == nil {
		t.Fatalf("expected error on empty file PutFile")
	}

	// 7. PutFileWithJumpRetry on colliding payload file
	const diskSize = blk.GridStart + (32 << 20)
	_, occupiedSlot, _, err := blk.AddrForFID(diskSize, fid, uint64(len(data)))
	if err != nil {
		t.Fatal(err)
	}

	var collidingData []byte
	for i := 0; ; i++ {
		cand := []byte(fmt.Sprintf("file-jump-candidate-%d", i))
		cFID := ComputeFID(cand)
		if cFID == fid {
			continue
		}
		_, slot, _, cErr := blk.AddrForFID(diskSize, cFID, uint64(len(cand)))
		if cErr == nil && slot == occupiedSlot {
			j1 := append(append([]byte(nil), cand...), 1)
			jFID := ComputeFID(j1)
			_, jSlot, _, jErr := blk.AddrForFID(diskSize, jFID, uint64(len(j1)))
			if jErr == nil && jSlot != occupiedSlot {
				collidingData = cand
				break
			}
		}
	}

	collidingFile := filepath.Join(dir, "colliding.bin")
	if err := os.WriteFile(collidingFile, collidingData, 0644); err != nil {
		t.Fatal(err)
	}

	jRes, err := c.PutFileWithJumpRetry(ctx, collidingFile)
	if err != nil {
		t.Fatalf("PutFileWithJumpRetry failed: %v", err)
	}
	if jRes.JumpsTaken != 1 {
		t.Fatalf("expected JumpsTaken = 1, got %d", jRes.JumpsTaken)
	}

	// Read back via logical fid to file
	collidingDl := filepath.Join(dir, "colliding_dl.bin")
	if err := c.GetFile(ctx, jRes.FID, collidingDl); err != nil {
		t.Fatalf("GetFile on jumped FID failed: %v", err)
	}
	readBack, err := os.ReadFile(collidingDl)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(readBack, collidingData) {
		t.Fatalf("readback data mismatch on PutFileWithJumpRetry")
	}
}
