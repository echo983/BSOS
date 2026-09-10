package daemon

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"bsos/internal/blk"
)

var (
	ErrConflict = errors.New("conflict")
	ErrNotFound = errors.New("not found")
)

type trimStatus struct {
	DiskID           string    `json:"disk_id"`
	DevicePath       string    `json:"device_path"`
	PackedEntries    int       `json:"packed_entries"`
	LastCheckAt      time.Time `json:"last_check_at,omitempty"`
	LastRunAt        time.Time `json:"last_run_at,omitempty"`
	LastSuccessAt    time.Time `json:"last_success_at,omitempty"`
	LastTrigger      bool      `json:"last_trigger"`
	LastThreshold    uint64    `json:"last_threshold_bytes"`
	LastTotalFiles   int       `json:"last_total_files"`
	LastSmallFiles   int       `json:"last_small_files"`
	LastPackedFiles  int       `json:"last_packed_files"`
	LastContainerFID string    `json:"last_container_fid,omitempty"`
	LastTableFID     string    `json:"last_table_fid,omitempty"`
	LastBackupPath   string    `json:"last_backup_path,omitempty"`
	LastError        string    `json:"last_error,omitempty"`
}

// aliasJumpCode is the fixed jump-indicator marker BSOS writes for a
// client-driven alias registration (docs/DESIGN.md §3.4). Its specific
// numeric value carries no meaning on read — blk.FindLatestIndexEntry
// only checks IsJumpIndicator (JumpCode != 0) — so a single fixed value
// is sufficient; NBSS's server-side jumpcode retry loop used the byte to
// count attempts, which has no equivalent here since the client already
// picked the target fid before calling Put.
const aliasJumpCode = 1

type deviceFile interface {
	io.ReaderAt
	io.WriterAt
	Sync() error
	Close() error
}

// DeviceState is one disk's state, implementing docs/DESIGN.md §3.3's
// two-phase commit: reserveExtent/confirmExtent/releaseExtent (step 1,
// disk-scoped) plus a bounded-concurrency I/O dispatcher (§3.12). The
// pool-wide fid gate (§3.3 step 0) lives one level up, in Server — fid
// identity is pool-wide, this type only knows about its own disk.
type DeviceState struct {
	mu         sync.Mutex
	file       deviceFile
	devicePath string
	diskID     uint64
	diskBytes  uint64

	intervals        []interval // confirmed occupied extents
	pendingIntervals []interval // reserved, not yet committed

	indexMu   sync.RWMutex
	confirmed map[uint64]objectRef
	packed    map[uint64]packedRecord
	indexEnd  uint64
	indexErr  error

	trimMu    sync.Mutex
	trimState trimStatus
	failPoint string
	cfg       Config

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
	headerBytes := make([]byte, blk.HeaderBytes)
	if _, err := f.ReadAt(headerBytes, 0); err != nil {
		f.Close()
		return nil, fmt.Errorf("read header: %w", err)
	}
	header, err := blk.ParseHeader(headerBytes)
	if err != nil || header.Version != blk.FormatVersion || header.DiskID != diskID || diskBytes <= blk.GridStart {
		f.Close()
		return nil, fmt.Errorf("invalid device header, identity or size: %v", err)
	}
	confirmed, packed, intervals, indexEnd, err := replayIndex(f, diskBytes)
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
		confirmed:  confirmed,
		packed:     packed,
		indexEnd:   indexEnd,
		cfg:        DefaultConfig(),
		ioSem:      make(chan struct{}, ioConcurrency),
	}, nil
}

func (s *DeviceState) Close() error {
	return s.file.Close()
}

func (s *DeviceState) acquireIO(ctx context.Context) (func(), error) {
	select {
	case s.ioSem <- struct{}{}:
		return func() { <-s.ioSem }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
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
	if size == 0 {
		return nil, fmt.Errorf("empty object")
	}
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
	return pw.WriteFromContext(context.Background(), r)
}

func (pw *PreparedWrite) WriteFromContext(ctx context.Context, r io.Reader) error {
	queueCtx := ctx
	cancel := func() {}
	if stream, ok := r.(*stallingChunkReader); ok {
		queueCtx, cancel = context.WithTimeout(ctx, stream.timeout)
	}
	release, err := pw.disk.acquireIO(queueCtx)
	cancel()
	if err != nil {
		return err
	}
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

	if aliasFor == pw.fid && aliasFor != 0 || aliasFor != 0 && pw.size < 2 {
		return fmt.Errorf("invalid alias")
	}
	s.indexMu.RLock()
	fault := s.indexErr
	_, exists := s.confirmed[pw.fid]
	_, aliasExists := s.confirmed[aliasFor]
	s.indexMu.RUnlock()
	if fault != nil {
		return fault
	}
	if exists || aliasFor != 0 && aliasExists {
		return ErrConflict
	}
	entry, err := blk.BuildIndexEntry(pw.fid, pw.size, 0)
	if err != nil {
		return err
	}
	if aliasFor != 0 {
		jump, err := blk.BuildIndexEntry(aliasFor, blk.IndexJumpSentinel, aliasJumpCode)
		if err != nil {
			return err
		}
		entry = append(jump, entry...)
	}
	off := s.indexEnd
	if off+uint64(len(entry)) > blk.GridStart {
		return blk.ErrIndexFull
	}
	// Flush data before publishing a reference to it. A successful response
	// requires the index flush too; no on-disk format change is introduced.
	if err := s.file.Sync(); err != nil {
		return fmt.Errorf("sync payload: %w", err)
	}
	n, err := s.file.WriteAt(entry, int64(off))
	if err == nil && n != len(entry) {
		err = io.ErrShortWrite
	}
	if err == nil {
		err = s.file.Sync()
	}
	if err != nil {
		// Restore the unwritten tail before allowing this extent to be reused.
		cleared, clearErr := s.file.WriteAt(make([]byte, len(entry)), int64(off))
		if clearErr == nil && cleared != len(entry) {
			clearErr = io.ErrShortWrite
		}
		if clearErr == nil {
			clearErr = s.file.Sync()
		}
		if clearErr != nil {
			s.indexMu.Lock()
			s.indexErr = fmt.Errorf("index rollback failed; reopen required: %w", clearErr)
			s.indexMu.Unlock()
		}
		return fmt.Errorf("commit index: %w", err)
	}
	s.indexMu.Lock()
	s.confirmed[pw.fid] = objectRef{fid: pw.fid, storedSize: pw.size, size: pw.size}
	if aliasFor != 0 {
		s.confirmed[aliasFor] = objectRef{fid: pw.fid, storedSize: pw.size, size: pw.size - 1}
	}
	s.indexMu.Unlock()
	s.indexEnd += uint64(len(entry))

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
func (s *DeviceState) Get(fid uint64) ([]byte, uint64, error) {
	ref, err := s.lookup(fid)
	if err != nil {
		return nil, 0, err
	}
	data, err := s.readRange(context.Background(), ref, 0, ref.size)
	return data, ref.size, err
}

func (s *DeviceState) readRange(ctx context.Context, ref objectRef, start, end uint64) ([]byte, error) {
	release, err := s.acquireIO(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	addr, _, _, err := blk.AddrForFID(s.diskBytes, ref.fid, ref.storedSize)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, end-start)
	n, err := s.file.ReadAt(buf, int64(addr+ref.offset+start))
	if err != nil {
		return nil, fmt.Errorf("read payload: %w", err)
	}
	if n != len(buf) {
		return nil, io.ErrUnexpectedEOF
	}
	return buf, nil
}

func (s *DeviceState) Head(fid uint64) (uint64, error) {
	ref, err := s.lookup(fid)
	return ref.size, err
}
func (s *DeviceState) Confirmed(fid uint64) (bool, error) {
	_, err := s.lookup(fid)
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	return err == nil, err
}

// currentCHD recomputes this disk's CH_d (docs/DESIGN.md §4/§3.11) over
// both confirmed and pending extents, so it never overpromises capacity
// that an in-flight write has already reserved.
func (s *DeviceState) currentCHD(targetP float64) uint64 {
	s.mu.Lock()
	all := make([]interval, 0, len(s.intervals)+len(s.pendingIntervals))
	all = append(all, s.intervals...)
	all = append(all, s.pendingIntervals...)
	diskBytes, diskID := s.diskBytes, s.diskID
	s.mu.Unlock()
	return computeCHD(diskBytes, diskID, all, targetP)
}

func writeExact(f io.WriterAt, addr uint64, total uint64, r io.Reader) error {
	const chunkSize = 4 << 20
	buf := make([]byte, min(uint64(chunkSize), total))
	var written uint64
	for written < total {
		want := uint64(chunkSize)
		if remaining := total - written; remaining < want {
			want = remaining
		}
		n, err := io.ReadFull(r, buf[:want])
		if n > 0 {
			if count, werr := f.WriteAt(buf[:n], int64(addr+written)); werr != nil {
				return werr
			} else if count != n {
				return io.ErrShortWrite
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
	n, err := io.ReadFull(r, extra)
	if n > 0 {
		return fmt.Errorf("stream carried more than declared total_size=%d", total)
	}
	if err != io.EOF {
		return fmt.Errorf("finish stream: %w", err)
	}
	return nil
}

func writeZeros(f io.WriterAt, addr uint64, length uint64) error {
	const chunkSize = 4 << 20
	zero := make([]byte, min(uint64(chunkSize), length))
	var written uint64
	for written < length {
		want := uint64(chunkSize)
		if remaining := length - written; remaining < want {
			want = remaining
		}
		if n, err := f.WriteAt(zero[:want], int64(addr+written)); err != nil {
			return err
		} else if uint64(n) != want {
			return io.ErrShortWrite
		}
		written += want
	}
	return nil
}
