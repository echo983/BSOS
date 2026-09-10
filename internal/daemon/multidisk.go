package daemon

import (
	"fmt"
	"math"
	"sync"

	"github.com/echo983/BSOS/internal/blk"
	"github.com/echo983/BSOS/internal/zram"
)

// docs/DESIGN.md §3.11: disk selection is a server-owned best-fit
// decision over each disk's CH_d, entirely independent of fid ownership
// (§3.1/§3.2) — ported from NBSS's multidisk.go unchanged in kind.

func smallFileBytes(pow2 int) uint64 {
	if pow2 < 0 {
		return 0
	}
	if pow2 >= 51 {
		return math.MaxUint64
	}
	return 1 << (12 + pow2)
}

func (s *Server) isSmallFile(size uint64) bool {
	return size < s.smallFileBytes
}

func (s *Server) canWrite(disk *DeviceState, size uint64) bool {
	if size == 0 || disk.diskBytes <= blk.GridStart {
		return false
	}
	disk.indexMu.RLock()
	fault := disk.indexErr
	disk.indexMu.RUnlock()
	if fault != nil {
		return false
	}
	capacity := (disk.diskBytes - blk.GridStart) / blk.SlotSize * blk.SlotSize
	return size <= capacity
}

// selectDiskForWrite is docs/DESIGN.md §3.3 step 1's disk-choice half:
// only needs size, so it runs after step 0's pool-wide fid gate but
// before any bytes arrive.
func (s *Server) selectDiskForWrite(size uint64) (*DeviceState, error) {
	if s.isSmallFile(size) {
		if disk := s.pickZram(size); disk != nil {
			return disk, nil
		}
	}
	return s.pickNonZram(size)
}

func (s *Server) pickZram(size uint64) *DeviceState {
	var best *DeviceState
	var bestCHD uint64
	for _, disk := range s.disks {
		if !zram.IsZramDevicePath(disk.devicePath) {
			continue
		}
		if !s.canWrite(disk, size) {
			continue
		}
		chd := disk.currentCHD(s.chdTargetP)
		if chd < size {
			continue
		}
		if best == nil || chd < bestCHD {
			best, bestCHD = disk, chd
		}
	}
	return best
}

func (s *Server) pickNonZram(size uint64) (*DeviceState, error) {
	var best, fallback *DeviceState
	var bestCHD, fallbackCHD uint64
	for _, disk := range s.disks {
		if zram.IsZramDevicePath(disk.devicePath) {
			continue
		}
		if !s.canWrite(disk, size) {
			continue
		}
		chd := disk.currentCHD(s.chdTargetP)
		if chd >= size {
			if best == nil || chd < bestCHD {
				best, bestCHD = disk, chd
			}
			continue
		}
		if fallback == nil || chd > fallbackCHD {
			fallback, fallbackCHD = disk, chd
		}
	}
	if best != nil {
		return best, nil
	}
	if fallback != nil {
		return fallback, nil
	}
	return nil, fmt.Errorf("no available disk for size %d", size)
}

// confirmedAnywhere is docs/DESIGN.md §3.3 step 0's confirmed-existence
// check: fan out to every disk's own (RAM-resident) index concurrently
// — in-process goroutine calls, not network I/O — and report whether
// fid is registered anywhere in the pool. §3.3's fan-out timeout wraps
// this at the call site (Server.confirmed).
func (s *Server) confirmedAnywhere(fid uint64) (bool, error) {
	type result struct {
		found bool
		err   error
	}
	ch := make(chan result, len(s.disks))
	for _, disk := range s.disks {
		d := disk
		go func() {
			found, err := d.Confirmed(fid)
			ch <- result{found, err}
		}()
	}
	var firstErr error
	for range s.disks {
		r := <-ch
		if r.err != nil && firstErr == nil {
			firstErr = r.err
			continue
		}
		if r.found {
			return true, nil
		}
	}
	return false, firstErr
}

// getAny broadcasts a Get to every disk in parallel and returns the
// first successful answer, per §3.11 — there is no fid->disk directory
// to consult instead.
func (s *Server) getAny(fid uint64) ([]byte, uint64, error) {
	type result struct {
		data []byte
		size uint64
		err  error
	}
	ch := make(chan result, len(s.disks))
	var wg sync.WaitGroup
	for _, disk := range s.disks {
		d := disk
		wg.Add(1)
		go func() {
			defer wg.Done()
			data, size, err := d.Get(fid)
			ch <- result{data, size, err}
		}()
	}
	go func() { wg.Wait(); close(ch) }()

	notFound := 0
	var firstErr error
	for r := range ch {
		if r.err == nil {
			return r.data, r.size, nil
		}
		if r.err == ErrNotFound {
			notFound++
			continue
		}
		if firstErr == nil {
			firstErr = r.err
		}
	}
	if firstErr != nil {
		return nil, 0, firstErr
	}
	return nil, 0, ErrNotFound
}

// headAny mirrors getAny for Head.
func (s *Server) headAny(fid uint64) (uint64, error) {
	type result struct {
		size uint64
		err  error
	}
	ch := make(chan result, len(s.disks))
	var wg sync.WaitGroup
	for _, disk := range s.disks {
		d := disk
		wg.Add(1)
		go func() {
			defer wg.Done()
			size, err := d.Head(fid)
			ch <- result{size, err}
		}()
	}
	go func() { wg.Wait(); close(ch) }()

	var firstErr error
	for r := range ch {
		if r.err == nil {
			return r.size, nil
		}
		if r.err == ErrNotFound {
			continue
		}
		if firstErr == nil {
			firstErr = r.err
		}
	}
	if firstErr != nil {
		return 0, firstErr
	}
	return 0, ErrNotFound
}
