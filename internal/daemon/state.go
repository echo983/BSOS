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

// DeviceState is one disk's state, implementing docs/DESIGN.md §3.3's
// two-phase commit: reserveExtent/confirmExtent/releaseExtent (step 1,
// disk-scoped) plus a bounded-concurrency I/O dispatcher (§3.12). The
// pool-wide fid gate (§3.3 step 0) lives one level up, in Server — fid
// identity is pool-wide, this type only knows about its own disk.
type DeviceState struct {
	mu         sync.Mutex
	file       *os.File
	devicePath string
	diskID     uint64
	diskBytes  uint64

	intervals        []interval // confirmed occupied extents
	pendingIntervals []interval // reserved, not yet committed

	ioSem chan struct{} // bounded-concurrency dispatcher, §3.12: one per disk
}

func OpenDevice(devicePath string, diskID uint64, ioConcurrency int) (*DeviceState, error) {
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
	intervals, err := scanConfirmedIntervals(f, diskBytes)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("scan intervals %s: %w", devicePath, err)
	}
	if ioConcurrency <= 0 {
		ioConcurrency = 64
	}
	return &DeviceState{
		file:       f,
		devicePath: devicePath,
		diskID:     diskID,
		diskBytes:  diskBytes,
		intervals:  intervals,
		ioSem:      make(chan struct{}, ioConcurrency),
	}, nil
}

func (s *DeviceState) Close() error {
	return s.file.Close()
}

func (s *DeviceState) acquireIO() func() {
	s.ioSem <- struct{}{}
	return func() { <-s.ioSem }
}

// PreparedWrite is docs/DESIGN.md §3.3 step 1's result: the extent is
// reserved (pending) and the caller may now stream bytes into it
// without holding any lock. Exactly one of Commit or Abort must be
// called afterward.
type PreparedWrite struct {
	disk *DeviceState
	fid  uint64
	size uint64
	addr uint64
	iv   interval
}

// PrepareWrite reserves the data-grid extent fid+size implies, on this
// disk, against confirmed and other pending extents. Callers run this
// only after docs/DESIGN.md §3.3 step 0's pool-wide fid gate has
// already passed for fid (and alias_for, if any).
func (s *DeviceState) PrepareWrite(fid uint64, size uint64) (*PreparedWrite, error) {
	addr, slotIndex, slotsNeeded, err := blk.AddrForFID(s.diskBytes, fid, size)
	if err != nil {
		return nil, fmt.Errorf("address: %w", err)
	}
	iv := interval{start: slotIndex, end: slotIndex + slotsNeeded - 1}
	if err := s.reserveExtent(iv); err != nil {
		return nil, err
	}
	return &PreparedWrite{disk: s, fid: fid, size: size, addr: addr, iv: iv}, nil
}

// WriteFrom streams exactly pw.size bytes from r into the reserved
// extent, without holding the disk lock (docs/DESIGN.md §3.3 step 2),
// then zero-pads to the slot boundary. It aborts — no partial commit —
// on a short read, an over-long stream, or any I/O error; the caller is
// still responsible for calling Abort in that case (WriteFrom itself
// only writes bytes, it does not release the reservation).
func (pw *PreparedWrite) WriteFrom(r io.Reader) error {
	release := pw.disk.acquireIO()
	defer release()

	if err := writeExact(pw.disk.file, pw.addr, pw.size, r); err != nil {
		return err
	}
	slotsNeeded := pw.iv.end - pw.iv.start + 1
	pad := slotsNeeded*blk.SlotSize - pw.size
	if pad > 0 {
		if err := writeZeros(pw.disk.file, pw.addr+pw.size, pad); err != nil {
			return fmt.Errorf("pad: %w", err)
		}
	}
	return nil
}

// Commit is docs/DESIGN.md §3.3 step 3: durably write the index
// entry (and the alias_for pair, if set) and flip the extent from
// pending to confirmed. Takes the disk lock only for this.
func (pw *PreparedWrite) Commit(aliasFor uint64) error {
	s := pw.disk
	s.mu.Lock()
	defer s.mu.Unlock()

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
	realEntry, err := blk.BuildIndexEntry(pw.fid, pw.size, 0)
	if err != nil {
		return fmt.Errorf("build entry: %w", err)
	}
	if _, err := s.file.WriteAt(realEntry, int64(off)); err != nil {
		return fmt.Errorf("write entry: %w", err)
	}

	s.pendingIntervals = removeInterval(s.pendingIntervals, pw.iv)
	s.intervals = append(s.intervals, pw.iv)
	return nil
}

// Abort is docs/DESIGN.md §3.3 step 4: release the extent reservation
// without ever writing an index entry. Safe to call even if some or all
// bytes were already written to the (never-indexed, so unreachable)
// extent — the next successful write to an overlapping fid simply
// overwrites that garbage.
func (pw *PreparedWrite) Abort() {
	pw.disk.releaseExtent(pw.iv)
}

// Get returns the logical content and size for fid, resolving a one-hop
// alias if present. docs/DESIGN.md §5's footnote: a jump target's stored
// payload carries one extra byte versus its logical content, trimmed
// here on read, same as NBSS.
func (s *DeviceState) Get(fid uint64) (data []byte, size uint64, err error) {
	release := s.acquireIO()
	defer release()

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
	logicSize, _, _, found, _, err := blk.FindLatestIndexEntry(s.file, fid)
	if err != nil {
		return 0, fmt.Errorf("lookup: %w", err)
	}
	if !found {
		return 0, ErrNotFound
	}
	return logicSize, nil
}

// Confirmed reports whether fid already has a durable entry on this
// disk — the disk-scoped half of the pool-wide existence check
// docs/DESIGN.md §3.3 step 0 fans out to (§3.11's broadcast).
func (s *DeviceState) Confirmed(fid uint64) (bool, error) {
	_, _, _, found, _, err := blk.FindLatestIndexEntry(s.file, fid)
	return found, err
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
