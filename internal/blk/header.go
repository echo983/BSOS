package blk

import (
	"encoding/binary"
	"errors"
)

type Header struct {
	Version    uint16
	CapacityGB uint16
	DiskID     uint64
	NoteLen    int
}

func ParseHeader(buf []byte) (Header, error) {
	if len(buf) < HeaderBytes {
		return Header{}, errors.New("header buffer too small")
	}
	if string(buf[:4]) != "NBSS" {
		return Header{}, errors.New("magic mismatch")
	}

	version := binary.LittleEndian.Uint16(buf[4:6])
	capacityGB := binary.LittleEndian.Uint16(buf[6:8])
	diskID := binary.LittleEndian.Uint64(buf[8:16])
	noteLen := 0
	for i := 16; i < HeaderBytes; i++ {
		if buf[i] == 0x00 {
			break
		}
		noteLen++
	}

	return Header{
		Version:    version,
		CapacityGB: capacityGB,
		DiskID:     diskID,
		NoteLen:    noteLen,
	}, nil
}
