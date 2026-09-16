package client

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"math/rand"
	"testing"
	"time"

	"github.com/zeebo/xxh3"
)

func sha256Str(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func TestClientCDCLargeFileRoundTrip(t *testing.T) {
	c, cleanup := setupTestDaemon(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 1. Generate 12 MB of pseudo-random data
	const size = 12 << 20
	data := make([]byte, size)
	r := rand.New(rand.NewSource(42))
	r.Read(data)
	origHash := sha256Str(data)
	origFID := xxh3.Hash(data)

	// 2. Upload via PutCDC with min 512KB, target 1MB, max 2MB
	opts := CDCOptions{
		Workers:         4,
		MinChunkSize:    512 << 10,
		TargetChunkSize: 1 << 20,
		MaxChunkSize:    2 << 20,
		Filename:        "large-test.bin",
	}

	res, err := c.PutCDC(ctx, bytes.NewReader(data), uint64(len(data)), opts)
	if err != nil {
		t.Fatalf("PutCDC failed: %v", err)
	}

	if res.ManifestFID == 0 {
		t.Fatalf("ManifestFID is 0")
	}
	if res.TotalSize != uint64(len(data)) {
		t.Fatalf("TotalSize mismatch: got %d, want %d", res.TotalSize, len(data))
	}
	if res.FullContentHash != origFID {
		t.Fatalf("FullContentHash mismatch: got 0x%X, want 0x%X", res.FullContentHash, origFID)
	}
	if res.ChunkCount < 4 {
		t.Fatalf("expected >= 4 chunks, got %d", res.ChunkCount)
	}

	t.Logf("PutCDC: %d chunks created, ManifestFID = 0x%016X", res.ChunkCount, res.ManifestFID)

	// 3. Inspect Manifest
	m, err := c.InspectManifest(ctx, res.ManifestFID)
	if err != nil {
		t.Fatalf("InspectManifest failed: %v", err)
	}
	if m.TotalSize != uint64(len(data)) || m.Filename != "large-test.bin" {
		t.Fatalf("Manifest content mismatch: %+v", m)
	}

	// 4. Download and reassemble full file via GetCDC
	var reassembled bytes.Buffer
	if err := c.GetCDC(ctx, res.ManifestFID, &reassembled); err != nil {
		t.Fatalf("GetCDC failed: %v", err)
	}

	if sha256Str(reassembled.Bytes()) != origHash {
		t.Fatalf("GetCDC content sha256 mismatch")
	}

	// 5. Test GetAuto transparent auto-detection
	var autoReassembled bytes.Buffer
	if err := c.GetAuto(ctx, res.ManifestFID, &autoReassembled); err != nil {
		t.Fatalf("GetAuto on Manifest failed: %v", err)
	}
	if sha256Str(autoReassembled.Bytes()) != origHash {
		t.Fatalf("GetAuto content sha256 mismatch")
	}

	// 6. Test Sparse Range Read on CDC file: range [2MB, 7MB)
	const rStart = 2 << 20
	const rEnd = 7 << 20
	var rangeOut bytes.Buffer
	if err := c.GetCDC(ctx, res.ManifestFID, &rangeOut, Range{Start: rStart, End: rEnd}); err != nil {
		t.Fatalf("GetCDC range read failed: %v", err)
	}
	if !bytes.Equal(rangeOut.Bytes(), data[rStart:rEnd]) {
		t.Fatalf("GetCDC range read content mismatch (got %d bytes, want %d)", rangeOut.Len(), rEnd-rStart)
	}

	// 7. Incremental Deduplication Test: upload same data again -> 100% deduplicated
	res2, err := c.PutCDC(ctx, bytes.NewReader(data), uint64(len(data)), opts)
	if err != nil {
		t.Fatalf("PutCDC duplicate failed: %v", err)
	}
	if res2.ManifestFID != res.ManifestFID {
		m1, _ := c.InspectManifest(ctx, res.ManifestFID)
		m2, _ := c.InspectManifest(ctx, res2.ManifestFID)
		t.Fatalf("Duplicate ManifestFID mismatch: got 0x%X, want 0x%X\nM1: %+v\nM2: %+v", res2.ManifestFID, res.ManifestFID, m1, m2)
	}
}
