package daemon

import (
	"encoding/binary"
	"fmt"
)

const (
	packedTableMagic      = "NBPT"
	packedTableVersion    = 1
	packedTableHeaderSize = 48
	packedTableEntrySize  = 24
)

type packedRecord struct {
	containerFID  uint64
	containerSize uint64
	offset        uint64
	logicSize     uint64
	tableFID      uint64
	tableSize     uint64
}

type packedTableHeader struct {
	diskID        uint64
	containerFID  uint64
	containerSize uint64
	entryCount    uint64
	reserved      uint64
}

type packedTableEntry struct {
	fid    uint64
	offset uint64
	size   uint64
}

func encodePackedTable(header packedTableHeader, entries []packedTableEntry) []byte {
	buf := make([]byte, packedTableHeaderSize+len(entries)*packedTableEntrySize)
	copy(buf[0:4], []byte(packedTableMagic))
	binary.LittleEndian.PutUint16(buf[4:6], packedTableVersion)
	binary.LittleEndian.PutUint16(buf[6:8], packedTableHeaderSize)
	binary.LittleEndian.PutUint64(buf[8:16], header.diskID)
	binary.LittleEndian.PutUint64(buf[16:24], header.containerFID)
	binary.LittleEndian.PutUint64(buf[24:32], header.containerSize)
	binary.LittleEndian.PutUint64(buf[32:40], uint64(len(entries)))
	binary.LittleEndian.PutUint64(buf[40:48], header.reserved)
	offset := packedTableHeaderSize
	for _, entry := range entries {
		binary.LittleEndian.PutUint64(buf[offset:offset+8], entry.fid)
		binary.LittleEndian.PutUint64(buf[offset+8:offset+16], entry.offset)
		binary.LittleEndian.PutUint64(buf[offset+16:offset+24], entry.size)
		offset += packedTableEntrySize
	}
	return buf
}

func decodePackedTable(blob []byte) (packedTableHeader, []packedTableEntry, error) {
	if len(blob) < packedTableHeaderSize {
		return packedTableHeader{}, nil, fmt.Errorf("packed table too small: %d", len(blob))
	}
	if string(blob[0:4]) != packedTableMagic {
		return packedTableHeader{}, nil, fmt.Errorf("invalid packed table magic: %q", string(blob[0:4]))
	}
	version := binary.LittleEndian.Uint16(blob[4:6])
	if version != packedTableVersion {
		return packedTableHeader{}, nil, fmt.Errorf("unsupported packed table version: %d", version)
	}
	headerBytes := binary.LittleEndian.Uint16(blob[6:8])
	if headerBytes != packedTableHeaderSize {
		return packedTableHeader{}, nil, fmt.Errorf("invalid packed table header size: %d", headerBytes)
	}
	header := packedTableHeader{
		diskID:        binary.LittleEndian.Uint64(blob[8:16]),
		containerFID:  binary.LittleEndian.Uint64(blob[16:24]),
		containerSize: binary.LittleEndian.Uint64(blob[24:32]),
		entryCount:    binary.LittleEndian.Uint64(blob[32:40]),
		reserved:      binary.LittleEndian.Uint64(blob[40:48]),
	}
	expectedLen := int(packedTableHeaderSize + header.entryCount*packedTableEntrySize)
	if len(blob) != expectedLen {
		return packedTableHeader{}, nil, fmt.Errorf("packed table length mismatch: got %d want %d", len(blob), expectedLen)
	}
	entries := make([]packedTableEntry, 0, header.entryCount)
	offset := packedTableHeaderSize
	for i := uint64(0); i < header.entryCount; i++ {
		entry := packedTableEntry{
			fid:    binary.LittleEndian.Uint64(blob[offset : offset+8]),
			offset: binary.LittleEndian.Uint64(blob[offset+8 : offset+16]),
			size:   binary.LittleEndian.Uint64(blob[offset+16 : offset+24]),
		}
		if entry.size == 0 {
			return packedTableHeader{}, nil, fmt.Errorf("packed table entry %d has zero size", i)
		}
		if entry.offset+entry.size > header.containerSize {
			return packedTableHeader{}, nil, fmt.Errorf("packed table entry %d exceeds container size: offset=%d size=%d container=%d", i, entry.offset, entry.size, header.containerSize)
		}
		entries = append(entries, entry)
		offset += packedTableEntrySize
	}
	return header, entries, nil
}
