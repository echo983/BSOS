package daemon

import "sync"

// poolGate is docs/DESIGN.md §3.3 step 0: a single pool-wide (not
// per-disk) reservation for fid-level identity, checked and reserved
// before disk selection even runs. It exists because fid identity is a
// pool-wide concept (existence checks broadcast across every disk,
// §3.11) while disk selection is a per-attempt heuristic choice — a
// per-disk lock cannot see two concurrent operations for the same fid
// routed to different disks. See §3.3's "why that fid-level reservation
// has to be pool-wide" for the failure mode this closes.
//
// It is held only for an in-memory set operation plus a confirmed-state
// check (fanned out to disks, itself bounded by its own timeout, §3.3);
// never for I/O, never nested with a per-disk lock.
type poolGate struct {
	mu      sync.Mutex
	pending map[uint64]struct{}
}

func newPoolGate() *poolGate {
	return &poolGate{pending: make(map[uint64]struct{})}
}

// confirmedFunc reports whether fid is already registered anywhere in
// the pool (confirmed, on-disk state) — the pool's existing broadcast
// existence check (§3.11). It is called while the gate's lock is held,
// so it must not itself try to take the gate's lock, and should respect
// its own bounded timeout rather than block indefinitely (§3.3's
// fan-out timeout note) — the caller is responsible for that bound.
type confirmedFunc func(fid uint64) (bool, error)

// reserve checks fid, and aliasFor if non-zero, against both pending
// and confirmed state, and — only if both are free — marks them pending
// atomically. Returns ErrConflict if either is already spoken for.
func (g *poolGate) reserve(fid, aliasFor uint64, confirmed confirmedFunc) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	fids := []uint64{fid}
	if aliasFor != 0 {
		fids = append(fids, aliasFor)
	}

	for _, f := range fids {
		if _, busy := g.pending[f]; busy {
			return ErrConflict
		}
	}
	for _, f := range fids {
		found, err := confirmed(f)
		if err != nil {
			return err
		}
		if found {
			return ErrConflict
		}
	}
	for _, f := range fids {
		g.pending[f] = struct{}{}
	}
	return nil
}

// release drops fid and aliasFor (if non-zero) from the pending set,
// whether the write is being committed (its state is now confirmed
// on-disk instead) or aborted (the attempt never happened).
func (g *poolGate) release(fid, aliasFor uint64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.pending, fid)
	if aliasFor != 0 {
		delete(g.pending, aliasFor)
	}
}
