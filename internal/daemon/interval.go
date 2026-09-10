package daemon

// interval is an inclusive slot-index range on one disk's data grid.
type interval struct {
	start, end uint64
}

func overlaps(a, b interval) bool {
	return a.start <= b.end && b.start <= a.end
}

// reserveExtent checks iv against both confirmed and other pending
// extents on this disk and, if free, adds it to pending. This is
// docs/DESIGN.md §3.3 step 1 — disk-scoped, run only after step 0's
// pool-wide fid gate has already passed.
func (s *DeviceState) reserveExtent(iv interval) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.indexMu.RLock()
	fault := s.indexErr
	s.indexMu.RUnlock()
	if fault != nil {
		return fault
	}
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
