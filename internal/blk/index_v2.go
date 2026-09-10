package blk

import (
	"encoding/binary"
	"fmt"
)

const (
	IndexJumpSentinel   = (1 << 56) - 1
	IndexPackedSentinel = (1 << 56) - 2
	PackedAnchorCode    = 0xFF
)

type IndexEntry struct {
	FID      uint64
	Size     uint64
	JumpCode byte
}

func DecodeSize7(buf []byte) uint64 {
	if len(buf) < 7 {
		return 0
	}
	return uint64(buf[0]) |
		uint64(buf[1])<<8 |
		uint64(buf[2])<<16 |
		uint64(buf[3])<<24 |
		uint64(buf[4])<<32 |
		uint64(buf[5])<<40 |
		uint64(buf[6])<<48
}

func EncodeSize7(dst []byte, size uint64) {
	if len(dst) < 7 {
		return
	}
	dst[0] = byte(size)
	dst[1] = byte(size >> 8)
	dst[2] = byte(size >> 16)
	dst[3] = byte(size >> 24)
	dst[4] = byte(size >> 32)
	dst[5] = byte(size >> 40)
	dst[6] = byte(size >> 48)
}

func ParseIndexEntry(entry []byte) (IndexEntry, error) {
	if len(entry) < 16 {
		return IndexEntry{}, fmt.Errorf("entry size %d", len(entry))
	}
	fid := binary.LittleEndian.Uint64(entry[0:8])
	size := DecodeSize7(entry[8:15])
	return IndexEntry{FID: fid, Size: size, JumpCode: entry[15]}, nil
}

func IsJumpIndicator(entry IndexEntry) bool {
	return entry.JumpCode != 0 && entry.Size == IndexJumpSentinel
}

func IsPackedAnchor(entry IndexEntry) bool {
	return entry.JumpCode == PackedAnchorCode && entry.Size == IndexPackedSentinel
}

func IsTombstone(entry IndexEntry) bool {
	return entry.JumpCode == 0 && entry.Size == 0
}

func BuildIndexEntry(fid uint64, size uint64, jumpCode byte) ([]byte, error) {
	if size > IndexJumpSentinel {
		return nil, fmt.Errorf("size exceeds 56-bit limit: %d", size)
	}
	if jumpCode != 0 {
		switch {
		case size == IndexJumpSentinel:
		case jumpCode == PackedAnchorCode && size == IndexPackedSentinel:
		default:
			return nil, fmt.Errorf("non-zero jumpcode requires reserved sentinel")
		}
	}
	entry := make([]byte, 16)
	binary.LittleEndian.PutUint64(entry[0:8], fid)
	EncodeSize7(entry[8:15], size)
	entry[15] = jumpCode
	return entry, nil
}
