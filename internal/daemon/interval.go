package daemon

import (
	"fmt"
	"os"

	"bsos/internal/blk"
)

// interval is an inclusive slot-index range on one disk's data grid.
type interval struct {
	start, end uint64
}

func overlaps(a, b interval) bool {
	return a.start <= b.end && b.start <= a.end
}

// scanConfirmedIntervals rebuilds the set of currently-occupied
// data-grid extents by scanning the disk's whole index stream once, at
// startup — the same replay NBSS does to rebuild its in-memory index,
// here narrowed to just the physical extents each live entry occupies.
// A jump-indicator entry claims no extent of its own (docs/DESIGN.md
// §3.3's "why fid-level reservation is separate from extent
// reservation"); only its paired real entry does.
func scanConfirmedIntervals(f *os.File, diskBytes uint64) ([]interval, error) {
	const chunkSize = 4 << 20
	buf := make([]byte, chunkSize)
	var out []interval
	var offset uint64
	for offset < blk.IndexBytes {
		remaining := blk.IndexBytes - offset
		readSize := uint64(chunkSize)
		if remaining < readSize {
			readSize = remaining
		}
		n, err := f.ReadAt(buf[:readSize], int64(blk.IndexStart+offset))
		if err != nil && n == 0 {
			break
		}
		limit := n - (n % blk.IndexEntrySize)
		for i := 0; i+blk.IndexEntrySize <= limit; i += blk.IndexEntrySize {
			raw := buf[i : i+blk.IndexEntrySize]
			allZero := true
			for _, b := range raw {
				if b != 0 {
					allZero = false
					break
				}
			}
			if allZero {
				return out, nil // reached the unwritten tail of the index stream
			}
			entry, err := blk.ParseIndexEntry(raw)
			if err != nil {
				return nil, fmt.Errorf("parse index entry: %w", err)
			}
			if blk.IsJumpIndicator(entry) {
				continue // no extent of its own; the paired real entry has one
			}
			addr, slotIndex, slotsNeeded, err := blk.AddrForFID(diskBytes, entry.FID, entry.Size)
			if err != nil {
				return nil, fmt.Errorf("address for fid 0x%X: %w", entry.FID, err)
			}
			_ = addr
			out = append(out, interval{start: slotIndex, end: slotIndex + slotsNeeded - 1})
		}
		if uint64(n) < readSize {
			break
		}
		offset += uint64(n)
	}
	return out, nil
}

// reserveExtent checks iv against both confirmed and other pending
// extents on this disk and, if free, adds it to pending. This is
// docs/DESIGN.md §3.3 step 1 — disk-scoped, run only after step 0's
// pool-wide fid gate has already passed.
func (s *DeviceState) reserveExtent(iv interval) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range s.intervals {
		if overlaps(e, iv) {
			return ErrConflict
		}
	}
	for _, e := range s.pendingIntervals {
		if overlaps(e, iv) {
			return ErrConflict
		}
	}
	s.pendingIntervals = append(s.pendingIntervals, iv)
	return nil
}

// confirmExtent moves iv from pending to confirmed, called once the
// index entry for it has been durably written.
func (s *DeviceState) confirmExtent(iv interval) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pendingIntervals = removeInterval(s.pendingIntervals, iv)
	s.intervals = append(s.intervals, iv)
}

// releaseExtent drops iv from pending without confirming it — the
// write was aborted, so the extent is free again.
func (s *DeviceState) releaseExtent(iv interval) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pendingIntervals = removeInterval(s.pendingIntervals, iv)
}

func removeInterval(list []interval, target interval) []interval {
	out := list[:0]
	for _, e := range list {
		if e != target {
			out = append(out, e)
		}
	}
	return out
}
