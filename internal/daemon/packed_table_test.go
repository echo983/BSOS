package daemon

import "testing"

func TestPackedTableRoundTrip(t *testing.T) {
	header := packedTableHeader{
		diskID:        0xABCD,
		containerFID:  0x1234,
		containerSize: 8192,
		entryCount:    2,
	}
	entries := []packedTableEntry{
		{fid: 0x1, offset: 0, size: 100},
		{fid: 0x2, offset: 100, size: 200},
	}
	blob := encodePackedTable(header, entries)
	gotHeader, gotEntries, err := decodePackedTable(blob)
	if err != nil {
		t.Fatalf("decodePackedTable: %v", err)
	}
	if gotHeader.diskID != header.diskID || gotHeader.containerFID != header.containerFID || gotHeader.containerSize != header.containerSize {
		t.Fatalf("header mismatch: got %+v want %+v", gotHeader, header)
	}
	if len(gotEntries) != len(entries) {
		t.Fatalf("entry count mismatch: got %d want %d", len(gotEntries), len(entries))
	}
	if gotEntries[1] != entries[1] {
		t.Fatalf("entry mismatch: got %+v want %+v", gotEntries[1], entries[1])
	}
}

func TestPackedTableRejectsOutOfRangeEntry(t *testing.T) {
	header := packedTableHeader{
		diskID:        0xABCD,
		containerFID:  0x1234,
		containerSize: 128,
		entryCount:    1,
	}
	blob := encodePackedTable(header, []packedTableEntry{{fid: 0x1, offset: 100, size: 40}})
	if _, _, err := decodePackedTable(blob); err == nil {
		t.Fatal("expected decodePackedTable to reject out-of-range entry")
	}
}
