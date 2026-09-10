package blk

import "testing"

func TestIsPackedAnchor(t *testing.T) {
	entry, err := BuildIndexEntry(0x1234, IndexPackedSentinel, PackedAnchorCode)
	if err != nil {
		t.Fatalf("BuildIndexEntry error: %v", err)
	}
	parsed, err := ParseIndexEntry(entry)
	if err != nil {
		t.Fatalf("ParseIndexEntry error: %v", err)
	}
	if !IsPackedAnchor(parsed) {
		t.Fatalf("expected packed anchor, got %+v", parsed)
	}
	if IsJumpIndicator(parsed) {
		t.Fatalf("packed anchor must not be treated as jump indicator")
	}
}

func TestBuildIndexEntryRejectsUnexpectedSentinelCombination(t *testing.T) {
	if _, err := BuildIndexEntry(0x1234, 4096, PackedAnchorCode); err == nil {
		t.Fatal("expected error for packed anchor code with normal size")
	}
	if _, err := BuildIndexEntry(0x1234, IndexPackedSentinel, 0x01); err == nil {
		t.Fatal("expected error for non-packed jumpcode with packed sentinel")
	}
}
