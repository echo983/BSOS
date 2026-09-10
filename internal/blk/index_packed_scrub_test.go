package blk

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

func TestFindPackedAnchorIssuesDetectsBadAndKeepsGood(t *testing.T) {
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

	// Good packed table.
	goodTableFID := uint64(0x1001)
	goodTableSize := uint64(packedTableHeaderSize + packedTableEntrySize)
	writeEntry(0, goodTableFID, IndexPackedSentinel, PackedAnchorCode)
	writeEntry(1, goodTableFID, goodTableSize, 0)
	goodBlob := make([]byte, goodTableSize)
	copy(goodBlob[:4], []byte(packedTableMagic))
	binary.LittleEndian.PutUint16(goodBlob[4:6], packedTableVersion)
	binary.LittleEndian.PutUint16(goodBlob[6:8], packedTableHeaderSize)
	binary.LittleEndian.PutUint64(goodBlob[8:16], 0xD15C0)
	binary.LittleEndian.PutUint64(goodBlob[16:24], 0x2002)
	binary.LittleEndian.PutUint64(goodBlob[24:32], 4096)
	binary.LittleEndian.PutUint64(goodBlob[32:40], 1)
	binary.LittleEndian.PutUint64(goodBlob[48:56], 0x3003)
	binary.LittleEndian.PutUint64(goodBlob[56:64], 0)
	binary.LittleEndian.PutUint64(goodBlob[64:72], 512)
	goodAddr, _, _, err := AddrForFID(GridStart+1024*SlotSize, goodTableFID, goodTableSize)
	if err != nil {
		t.Fatalf("AddrForFID good: %v", err)
	}
	if _, err := f.WriteAt(goodBlob, int64(goodAddr)); err != nil {
		t.Fatalf("WriteAt good blob: %v", err)
	}

	// Bad packed table: anchor exists, object is zero-filled.
	badTableFID := uint64(0x1002)
	badTableSize := uint64(4096)
	writeEntry(2, badTableFID, IndexPackedSentinel, PackedAnchorCode)
	writeEntry(3, badTableFID, badTableSize, 0)

	records, err := CollectLatestIndexRecords(f)
	if err != nil {
		t.Fatalf("CollectLatestIndexRecords: %v", err)
	}
	issues, err := findPackedAnchorIssues(f, GridStart+1024*SlotSize, records)
	if err != nil {
		t.Fatalf("findPackedAnchorIssues: %v", err)
	}
	if len(issues) != 1 {
		t.Fatalf("len(issues)=%d want 1", len(issues))
	}
	if issues[0].TableFID != badTableFID {
		t.Fatalf("bad table fid = 0x%X want 0x%X", issues[0].TableFID, badTableFID)
	}

	filtered := make([]LatestIndexRecord, 0, len(records))
	for _, record := range records {
		if record.Packed && record.FID == badTableFID {
			continue
		}
		filtered = append(filtered, record)
	}
	if len(filtered) != len(records)-1 {
		t.Fatalf("len(filtered)=%d want %d", len(filtered), len(records)-1)
	}
	for _, record := range filtered {
		if record.Packed && record.FID == badTableFID {
			t.Fatalf("bad packed record still present after filtering")
		}
	}
}
