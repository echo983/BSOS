package daemon

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"bsos/internal/blk"
)

var (
	ErrConflict = errors.New("conflict")
	ErrNotFound = errors.New("not found")
)

// aliasJumpCode is the fixed jump-indicator marker BSOS writes for a
// client-driven alias registration (docs/DESIGN.md §3.4). Its specific
// numeric value carries no meaning on read — blk.FindLatestIndexEntry
// only checks IsJumpIndicator (JumpCode != 0) — so a single fixed value
// is sufficient; NBSS's server-side jumpcode retry loop used the byte to
// count attempts, which has no equivalent here since the client already
// picked the target fid before calling Put.
const aliasJumpCode = 1

// DeviceState is milestone 2's single-disk state: correct, but not yet
// the two-phase reservation model from docs/DESIGN.md §3.3. It holds
// its lock for an entire write, same as NBSS does today, deferring the
// concurrency optimization to a later milestone (docs/IMPLEMENTATION_PLAN.md).
type DeviceState struct {
	mu         sync.Mutex
	file       *os.File
	devicePath string
	diskID     uint64
	diskBytes  uint64
}

func OpenDevice(devicePath string, diskID uint64) (*DeviceState, error) {
	f, err := os.OpenFile(devicePath, os.O_RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", devicePath, err)
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("stat %s: %w", devicePath, err)
	}
	diskBytes, err := blk.DeviceSizeBytes(f, st)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("size %s: %w", devicePath, err)
	}
	return &DeviceState{
		file:       f,
		devicePath: devicePath,
		diskID:     diskID,
		diskBytes:  diskBytes,
	}, nil
}

func (s *DeviceState) Close() error {
	return s.file.Close()
}

// Put writes exactly totalSize bytes read from r under fid, optionally
// registering aliasFor -> fid as a one-hop jump pointer in the same
// operation (docs/DESIGN.md §3.4). It holds the disk lock for the whole
// call; docs/DESIGN.md §3.3's two-phase model (short reservation, then
// an unlocked streaming write) is a later milestone.
func (s *DeviceState) Put(fid uint64, totalSize uint64, aliasFor uint64, r io.Reader) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, _, _, found, _, err := blk.FindLatestIndexEntry(s.file, fid); err != nil {
		return fmt.Errorf("check fid: %w", err)
	} else if found {
		return ErrConflict
	}
	if aliasFor != 0 {
		if _, _, _, found, _, err := blk.FindLatestIndexEntry(s.file, aliasFor); err != nil {
			return fmt.Errorf("check alias_for: %w", err)
		} else if found {
			return ErrConflict
		}
	}

	addr, _, slotsNeeded, err := blk.AddrForFID(s.diskBytes, fid, totalSize)
	if err != nil {
		return fmt.Errorf("address: %w", err)
	}

	if err := writeExact(s.file, addr, totalSize, r); err != nil {
		return fmt.Errorf("write: %w", err)
	}
	pad := slotsNeeded*blk.SlotSize - totalSize
	if pad > 0 {
		if err := writeZeros(s.file, addr+totalSize, pad); err != nil {
			return fmt.Errorf("pad: %w", err)
		}
	}

	entries := 1
	if aliasFor != 0 {
		entries = 2
	}
	indexOffset, err := blk.FindIndexEnd(s.file)
	if err != nil {
		return fmt.Errorf("index end: %w", err)
	}
	if indexOffset%blk.IndexEntrySize != 0 {
		return fmt.Errorf("index offset 0x%X not aligned", indexOffset)
	}
	if indexOffset+uint64(entries)*blk.IndexEntrySize > blk.IndexStart+blk.IndexBytes {
		return fmt.Errorf("index stream full")
	}

	off := indexOffset
	if aliasFor != 0 {
		jumpEntry, err := blk.BuildIndexEntry(aliasFor, blk.IndexJumpSentinel, aliasJumpCode)
		if err != nil {
			return fmt.Errorf("build jump entry: %w", err)
		}
		if _, err := s.file.WriteAt(jumpEntry, int64(off)); err != nil {
			return fmt.Errorf("write jump entry: %w", err)
		}
		off += blk.IndexEntrySize
	}
	realEntry, err := blk.BuildIndexEntry(fid, totalSize, 0)
	if err != nil {
		return fmt.Errorf("build entry: %w", err)
	}
	if _, err := s.file.WriteAt(realEntry, int64(off)); err != nil {
		return fmt.Errorf("write entry: %w", err)
	}
	return nil
}

// Get returns the logical content and size for fid, resolving a one-hop
// alias if present. docs/DESIGN.md §5's footnote: a jump target's stored
// payload carries one extra byte versus its logical content, trimmed
// here on read, same as NBSS.
func (s *DeviceState) Get(fid uint64) (data []byte, size uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.getLocked(fid)
}

func (s *DeviceState) getLocked(fid uint64) ([]byte, uint64, error) {
	logicSize, actualFID, actualSize, found, jump, err := blk.FindLatestIndexEntry(s.file, fid)
	if err != nil {
		return nil, 0, fmt.Errorf("lookup: %w", err)
	}
	if !found {
		return nil, 0, ErrNotFound
	}
	addr, _, _, err := blk.AddrForFID(s.diskBytes, actualFID, actualSize)
	if err != nil {
		return nil, 0, fmt.Errorf("address: %w", err)
	}
	buf := make([]byte, actualSize)
	if _, err := s.file.ReadAt(buf, int64(addr)); err != nil && err != io.EOF {
		return nil, 0, fmt.Errorf("read: %w", err)
	}
	if jump && len(buf) > 0 {
		return buf[:len(buf)-1], logicSize, nil
	}
	return buf, logicSize, nil
}

// Head returns only the size for fid, without reading its data.
func (s *DeviceState) Head(fid uint64) (size uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	logicSize, _, _, found, _, err := blk.FindLatestIndexEntry(s.file, fid)
	if err != nil {
		return 0, fmt.Errorf("lookup: %w", err)
	}
	if !found {
		return 0, ErrNotFound
	}
	return logicSize, nil
}

func writeExact(f *os.File, addr uint64, total uint64, r io.Reader) error {
	const chunkSize = 4 << 20
	buf := make([]byte, chunkSize)
	var written uint64
	for written < total {
		want := uint64(chunkSize)
		if remaining := total - written; remaining < want {
			want = remaining
		}
		n, err := io.ReadFull(r, buf[:want])
		if n > 0 {
			if _, werr := f.WriteAt(buf[:n], int64(addr+written)); werr != nil {
				return werr
			}
			written += uint64(n)
		}
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return fmt.Errorf("stream ended after %d of %d declared bytes", written, total)
			}
			return err
		}
	}
	// Reject any extra bytes beyond total_size (docs/DESIGN.md §3.3:
	// abort on either a short or an over-long stream, never truncate
	// silently).
	extra := make([]byte, 1)
	if n, err := r.Read(extra); n > 0 || (err != nil && err != io.EOF) {
		return fmt.Errorf("stream carried more than declared total_size=%d", total)
	}
	return nil
}

func writeZeros(f *os.File, addr uint64, length uint64) error {
	const chunkSize = 4 << 20
	zero := make([]byte, chunkSize)
	var written uint64
	for written < length {
		want := uint64(chunkSize)
		if remaining := length - written; remaining < want {
			want = remaining
		}
		if _, err := f.WriteAt(zero[:want], int64(addr+written)); err != nil {
			return err
		}
		written += want
	}
	return nil
}
