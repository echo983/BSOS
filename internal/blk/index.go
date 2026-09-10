package blk

import (
	"errors"
	"fmt"
	"io"
	"os"
)

const (
	IndexEntrySize = 16
)

var ErrIndexFull = errors.New("index stream full")
var ErrIndexNotFound = errors.New("index entry not found")

func FindIndexEnd(f *os.File) (uint64, error) {
	const chunkSize = 4 * 1024 * 1024
	buf := make([]byte, chunkSize)

	var offset uint64
	for offset < IndexBytes {
		remaining := IndexBytes - offset
		readSize := chunkSize
		if remaining < uint64(chunkSize) {
			readSize = int(remaining)
		}
		n, err := f.ReadAt(buf[:readSize], int64(IndexStart+offset))
		if err != nil && err != io.EOF {
			return 0, err
		}
		limit := n - (n % IndexEntrySize)
		for i := 0; i+IndexEntrySize <= limit; i += IndexEntrySize {
			entry := buf[i : i+IndexEntrySize]
			allZero := true
			for _, b := range entry {
				if b != 0x00 {
					allZero = false
					break
				}
			}
			if allZero {
				return IndexStart + offset + uint64(i), nil
			}
		}
		if n == 0 {
			break
		}
		offset += uint64(n)
	}
	return 0, ErrIndexFull
}

func FindLatestIndexEntry(f *os.File, fid uint64) (uint64, uint64, uint64, bool, bool, error) {
	const chunkSize = 4 * 1024 * 1024
	buf := make([]byte, chunkSize)
	var (
		logicSize  uint64
		actualFID  uint64
		actualSize uint64
		found      bool
		jump       bool
		pending    *IndexEntry
	)

	var offset uint64
	for offset < IndexBytes {
		remaining := IndexBytes - offset
		readSize := chunkSize
		if remaining < uint64(chunkSize) {
			readSize = int(remaining)
		}
		n, readErr := f.ReadAt(buf[:readSize], int64(IndexStart+offset))
		if readErr != nil && readErr != io.EOF {
			return 0, 0, 0, false, false, readErr
		}
		limit := n - (n % IndexEntrySize)
		for i := 0; i+IndexEntrySize <= limit; i += IndexEntrySize {
			entry := buf[i : i+IndexEntrySize]
			allZero := true
			for _, b := range entry {
				if b != 0x00 {
					allZero = false
					break
				}
			}
			if allZero {
				if found && logicSize == 0 {
					return 0, 0, 0, false, false, ErrIndexNotFound
				}
				return logicSize, actualFID, actualSize, found, jump, nil
			}
			parsed, err := ParseIndexEntry(entry)
			if err != nil {
				return 0, 0, 0, false, false, err
			}
			if pending != nil {
				if parsed.JumpCode != 0 || parsed.Size == 0 {
					return 0, 0, 0, false, false, fmt.Errorf("invalid jump pair for fid 0x%X", pending.FID)
				}
				if pending.FID == fid {
					logicSize = parsed.Size - 1
					actualFID = parsed.FID
					actualSize = parsed.Size
					found = true
					jump = true
				}
				pending = nil
			}
			if IsJumpIndicator(parsed) {
				pending = &parsed
				continue
			}
			if parsed.FID == fid {
				found = true
				jump = false
				if IsTombstone(parsed) {
					logicSize = 0
					actualFID = 0
					actualSize = 0
					continue
				}
				logicSize = parsed.Size
				actualFID = parsed.FID
				actualSize = parsed.Size
			}
		}
		if n == 0 {
			break
		}
		offset += uint64(n)
	}
	if found && logicSize == 0 {
		return 0, 0, 0, false, false, ErrIndexNotFound
	}
	if !found {
		return 0, 0, 0, false, false, ErrIndexNotFound
	}
	return logicSize, actualFID, actualSize, found, jump, nil
}
