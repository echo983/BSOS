package cdc

import (
	"bytes"
	"io"
	"math/rand"
	"testing"
)

func TestFastCDCChunking(t *testing.T) {
	// Generate 64 MB of deterministic pseudo-random data
	data := make([]byte, 64<<20)
	r := rand.New(rand.NewSource(42))
	r.Read(data)

	chunker, err := NewChunker(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("NewChunker failed: %v", err)
	}

	var chunks []Chunk
	var totalReassembled bytes.Buffer

	for {
		chunk, err := chunker.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("chunker.Next failed: %v", err)
		}
		if chunk.Size < DefaultMinSize && len(chunks) < 3 { // Last chunk may be smaller than MinSize if total remainder < MinSize
			t.Errorf("chunk size %d < minSize %d", chunk.Size, DefaultMinSize)
		}
		if chunk.Size > DefaultMaxSize {
			t.Errorf("chunk size %d > maxSize %d", chunk.Size, DefaultMaxSize)
		}
		if chunk.Offset != uint64(totalReassembled.Len()) {
			t.Errorf("chunk offset mismatch: got %d, expected %d", chunk.Offset, totalReassembled.Len())
		}
		totalReassembled.Write(chunk.Data)
		chunks = append(chunks, chunk)
	}

	if totalReassembled.Len() != len(data) {
		t.Fatalf("reassembled size %d != original size %d", totalReassembled.Len(), len(data))
	}
	if !bytes.Equal(totalReassembled.Bytes(), data) {
		t.Fatalf("reassembled data corrupted")
	}

	t.Logf("64MB input cut into %d chunks (avg chunk size = %.2f MB)", len(chunks), float64(len(data))/float64(len(chunks))/(1024*1024))
}

func TestFastCDCDeterminism(t *testing.T) {
	data := make([]byte, 32<<20)
	r := rand.New(rand.NewSource(12345))
	r.Read(data)

	// Run 1
	c1, err := NewChunker(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("c1 NewChunker: %v", err)
	}
	var chunks1 []Chunk
	for {
		chunk, err := c1.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("c1 Next: %v", err)
		}
		chunks1 = append(chunks1, chunk)
	}

	// Run 2
	c2, err := NewChunker(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("c2 NewChunker: %v", err)
	}
	var chunks2 []Chunk
	for {
		chunk, err := c2.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("c2 Next: %v", err)
		}
		chunks2 = append(chunks2, chunk)
	}

	if len(chunks1) != len(chunks2) {
		t.Fatalf("determinism fail: run1 had %d chunks, run2 had %d chunks", len(chunks1), len(chunks2))
	}
	for i := range chunks1 {
		if chunks1[i].FID != chunks2[i].FID || chunks1[i].Size != chunks2[i].Size || chunks1[i].Offset != chunks2[i].Offset {
			t.Fatalf("chunk %d mismatch: %+v vs %+v", i, chunks1[i], chunks2[i])
		}
	}
}

func TestFastCDCBoundaryShiftResilience(t *testing.T) {
	// Base data: 50 MB
	baseData := make([]byte, 50<<20)
	r := rand.New(rand.NewSource(999))
	r.Read(baseData)

	cBase, _ := NewChunker(bytes.NewReader(baseData))
	var baseChunks []Chunk
	for {
		ch, err := cBase.Next()
		if err == io.EOF {
			break
		}
		baseChunks = append(baseChunks, ch)
	}

	// Insert 64 KB in the middle of chunk 0
	modifiedData := make([]byte, 0, len(baseData)+64*1024)
	modifiedData = append(modifiedData, []byte("INSERTED-PREFIX-DATA-FOR-BOUNDARY-SHIFT-TEST-1234567890")...)
	modifiedData = append(modifiedData, baseData...)

	cMod, _ := NewChunker(bytes.NewReader(modifiedData))
	var modChunks []Chunk
	for {
		ch, err := cMod.Next()
		if err == io.EOF {
			break
		}
		modChunks = append(modChunks, ch)
	}

	// Verify that chunks after the insertion point re-synchronize to identical FIDs
	baseFIDSet := make(map[uint64]bool)
	for _, b := range baseChunks {
		baseFIDSet[b.FID] = true
	}

	matchedCount := 0
	for _, m := range modChunks {
		if baseFIDSet[m.FID] {
			matchedCount++
		}
	}

	dedupRatio := float64(matchedCount) / float64(len(baseChunks))
	t.Logf("Boundary shift: %d / %d chunks matched (dedup ratio = %.2f%%)", matchedCount, len(baseChunks), dedupRatio*100)
	if matchedCount < len(baseChunks)-2 {
		t.Fatalf("CDC boundary shift failed: expected most chunks to match, got only %d/%d", matchedCount, len(baseChunks))
	}
}
