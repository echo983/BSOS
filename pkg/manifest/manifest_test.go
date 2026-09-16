package manifest

import (
	"bytes"
	"testing"

	"github.com/echo983/BSOS/internal/daemon/bsospb"
)

func TestManifestEncodeDecode(t *testing.T) {
	orig := &bsospb.FileManifest{
		Version:         CurrentVersion,
		TotalSize:       1024 * 1024 * 100, // 100 MB
		FullContentHash: 0x1122334455667788,
		Filename:        "test-video.mp4",
		ChunkTargetSize: 16 * 1024 * 1024,
		Chunks: []*bsospb.ChunkDescriptor{
			{
				Fid:        0xAAAA1111BBBB2222,
				TargetFid:  0xAAAA1111BBBB2222,
				Offset:     0,
				Size:       16 * 1024 * 1024,
				JumpsTaken: 0,
			},
			{
				Fid:        0xCCCC3333DDDD4444,
				TargetFid:  0xEEEE5555FFFF6666,
				Offset:     16 * 1024 * 1024,
				Size:       16 * 1024 * 1024,
				JumpsTaken: 1,
			},
		},
	}

	encoded, err := Encode(orig)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	if !IsManifest(encoded) {
		t.Fatalf("IsManifest returned false on encoded data")
	}

	if !bytes.Equal(encoded[:len(MagicHeader)], MagicHeader) {
		t.Fatalf("encoded prefix != MagicHeader")
	}

	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.TotalSize != orig.TotalSize {
		t.Errorf("TotalSize: got %d, want %d", decoded.TotalSize, orig.TotalSize)
	}
	if decoded.FullContentHash != orig.FullContentHash {
		t.Errorf("FullContentHash: got 0x%X, want 0x%X", decoded.FullContentHash, orig.FullContentHash)
	}
	if decoded.Filename != orig.Filename {
		t.Errorf("Filename: got %q, want %q", decoded.Filename, orig.Filename)
	}
	if len(decoded.Chunks) != len(orig.Chunks) {
		t.Fatalf("Chunk count: got %d, want %d", len(decoded.Chunks), len(orig.Chunks))
	}
	for i := range orig.Chunks {
		if decoded.Chunks[i].Fid != orig.Chunks[i].Fid ||
			decoded.Chunks[i].TargetFid != orig.Chunks[i].TargetFid ||
			decoded.Chunks[i].Offset != orig.Chunks[i].Offset ||
			decoded.Chunks[i].Size != orig.Chunks[i].Size ||
			decoded.Chunks[i].JumpsTaken != orig.Chunks[i].JumpsTaken {
			t.Errorf("chunk %d mismatch: %+v vs %+v", i, decoded.Chunks[i], orig.Chunks[i])
		}
	}
}

func TestManifestInvalidData(t *testing.T) {
	// Raw arbitrary data without magic header
	raw := []byte("plain text file content without magic header")
	if IsManifest(raw) {
		t.Errorf("IsManifest returned true for raw text")
	}
	_, err := Decode(raw)
	if err != ErrInvalidMagic {
		t.Errorf("expected ErrInvalidMagic, got %v", err)
	}
}
