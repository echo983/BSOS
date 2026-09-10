package daemon

import "time"

// Config is intentionally minimal for now. File-based config
// (bsosd.toml, per docs/IMPLEMENTATION_PLAN.md §5) lands once there are
// enough knobs to justify a file over flags.
type Config struct {
	PanPath     string
	GRPCListen  string
	MaxPutBytes uint64

	// docs/DESIGN.md §3.3: how long a write may go with no forward
	// progress (no byte received since the header, or since the last
	// chunk) before the server unilaterally releases its reservations.
	ReservationStallTimeout time.Duration

	// docs/DESIGN.md §3.3: bound on the pool-wide gate's fan-out to
	// every disk's confirmed state, separate from the write-level stall
	// timeout — one unresponsive disk must not freeze the whole pool's
	// write path.
	PoolGateFanoutTimeout time.Duration

	// docs/DESIGN.md §3.12: per-disk bounded-concurrency dispatcher cap
	// (no sorting, no read/write priority — just a resource-use bound).
	WriteDispatchConcurrency int

	// docs/DESIGN.md §3.11: small-file threshold for zram preference,
	// 4KB * 2^SmallFilePow2, same convention as NBSS.
	SmallFilePow2 int

	// docs/DESIGN.md §4: CH_d target success probability.
	ChdTargetP float64

	// docs/DESIGN.md §3.13: where zram snapshots are flushed to and
	// auto-loaded from at startup.
	ZramSnapshotDir string
}

func DefaultConfig() Config {
	return Config{
		PanPath:                  "pan.json",
		GRPCListen:               ":9090",
		MaxPutBytes:              256 << 20,
		ReservationStallTimeout:  30 * time.Second,
		PoolGateFanoutTimeout:    2 * time.Second,
		WriteDispatchConcurrency: 64,
		SmallFilePow2:            6,
		ChdTargetP:               0.2,
		ZramSnapshotDir:          "~/.bsos_zram_snapshots",
	}
}
