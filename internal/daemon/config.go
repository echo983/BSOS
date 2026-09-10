package daemon

// Config is intentionally minimal for milestone 2 (single-disk,
// not-yet-concurrency-optimized Put/Get). File-based config
// (bsosd.toml, per docs/IMPLEMENTATION_PLAN.md §5) lands with the
// concurrency-model milestone, once there are more knobs worth putting
// in a file.
type Config struct {
	PanPath     string
	GRPCListen  string
	MaxPutBytes uint64
}

func DefaultConfig() Config {
	return Config{
		PanPath:     "pan.json",
		GRPCListen:  ":9090",
		MaxPutBytes: 256 << 20,
	}
}
