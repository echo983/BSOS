package blk

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCollectLatestIndexEntriesPreservesPackedAnchor(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.nbssIndex")

	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer f.Close()

	header := make([]byte, HeaderBytes)
	copy(header[:4], []byte("NBSS"))
	header[4] = byte(FormatVersion)
	if _, err := f.WriteAt(header, 0); err != nil {
		t.Fatalf("WriteAt header: %v", err)
	}

	writeEntry := func(idx int, fid uint64, size uint64, jumpCode byte) {
		entry, err := BuildIndexEntry(fid, size, jumpCode)
		if err != nil {
			t.Fatalf("BuildIndexEntry(%d): %v", idx, err)
		}
		offset := int64(IndexStart + uint64(idx)*IndexEntrySize)
		if _, err := f.WriteAt(entry, offset); err != nil {
			t.Fatalf("WriteAt entry %d: %v", idx, err)
		}
	}

	writeEntry(0, 0xAAA, 4096, 0)
	writeEntry(1, 0xBBB, IndexPackedSentinel, PackedAnchorCode)
	writeEntry(2, 0xBBB, 2048, 0)

	records, err := CollectLatestIndexRecords(f)
	if err != nil {
		t.Fatalf("collectLatestIndexEntries: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("len(records)=%d want 2", len(records))
	}
	if !records[1].Packed {
		t.Fatalf("expected packed record, got %+v", records[1])
	}
	if records[1].FID != 0xBBB || records[1].ActualFID != 0xBBB || records[1].ActualSize != 2048 {
		t.Fatalf("unexpected packed record: %+v", records[1])
	}
}
