package daemon

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
)

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

	// docs/DESIGN.md §3.8: Trim configuration (mandatory, no on/off switch).
	TrimInterval          time.Duration
	TrimMinThresholdBytes uint64
	TrimMaxThresholdBytes uint64
	TrimMinFileCount      int
	TrimThresholdRatio    float64
	TrimTempDir           string
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
		TrimInterval:             15 * time.Minute,
		TrimMinThresholdBytes:    4096,
		TrimMaxThresholdBytes:    256 << 20,
		TrimMinFileCount:         100,
		TrimThresholdRatio:       0.20,
		TrimTempDir:              "",
	}
}

type fileConfig struct {
	PanPath                   string   `toml:"pan_path"`
	GRPCListen                string   `toml:"grpc_listen"`
	MaxPut                    string   `toml:"max_put"`
	ReservationStallTimeoutMS *int64   `toml:"reservation_stall_timeout_ms"`
	PoolGateFanoutTimeoutMS   *int64   `toml:"pool_gate_fanout_timeout_ms"`
	WriteDispatchConcurrency  *int     `toml:"write_dispatch_concurrency"`
	SmallFilePow2             *int     `toml:"small_file_pow2"`
	ChdTargetP                *float64 `toml:"chd_target_p"`
	ZramSnapshotDir           string   `toml:"zram_snapshot_dir"`
	TrimIntervalSeconds       *int64   `toml:"trim_interval_seconds"`
	TrimMinThresholdBytes     string   `toml:"trim_min_threshold_bytes"`
	TrimMaxThresholdBytes     string   `toml:"trim_max_threshold_bytes"`
	TrimMinFileCount          *int     `toml:"trim_min_file_count"`
	TrimThresholdRatio        *float64 `toml:"trim_threshold_ratio"`
	TrimTempDir               string   `toml:"trim_temp_dir"`
}

func LoadConfig(path string) (Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	var file fileConfig
	if err := toml.Unmarshal(raw, &file); err != nil {
		return Config{}, fmt.Errorf("parse %s: %w", path, err)
	}
	cfg := DefaultConfig()
	if strings.TrimSpace(file.PanPath) != "" {
		cfg.PanPath = strings.TrimSpace(file.PanPath)
	}
	if strings.TrimSpace(file.GRPCListen) != "" {
		cfg.GRPCListen = strings.TrimSpace(file.GRPCListen)
	}
	if strings.TrimSpace(file.MaxPut) != "" {
		size, err := ParseSize(file.MaxPut)
		if err != nil {
			return Config{}, fmt.Errorf("max_put: %w", err)
		}
		cfg.MaxPutBytes = size
	}
	if file.ReservationStallTimeoutMS != nil {
		if *file.ReservationStallTimeoutMS <= 0 {
			return Config{}, fmt.Errorf("reservation_stall_timeout_ms must be positive")
		}
		cfg.ReservationStallTimeout = time.Duration(*file.ReservationStallTimeoutMS) * time.Millisecond
	}
	if file.PoolGateFanoutTimeoutMS != nil {
		if *file.PoolGateFanoutTimeoutMS <= 0 {
			return Config{}, fmt.Errorf("pool_gate_fanout_timeout_ms must be positive")
		}
		cfg.PoolGateFanoutTimeout = time.Duration(*file.PoolGateFanoutTimeoutMS) * time.Millisecond
	}
	if file.WriteDispatchConcurrency != nil {
		if *file.WriteDispatchConcurrency <= 0 {
			return Config{}, fmt.Errorf("write_dispatch_concurrency must be positive")
		}
		cfg.WriteDispatchConcurrency = *file.WriteDispatchConcurrency
	}
	if file.SmallFilePow2 != nil {
		if *file.SmallFilePow2 < 0 {
			return Config{}, fmt.Errorf("small_file_pow2 must be >= 0")
		}
		cfg.SmallFilePow2 = *file.SmallFilePow2
	}
	if file.ChdTargetP != nil {
		if *file.ChdTargetP <= 0 || *file.ChdTargetP > 1 {
			return Config{}, fmt.Errorf("chd_target_p must be in (0, 1]")
		}
		cfg.ChdTargetP = *file.ChdTargetP
	}
	if file.ZramSnapshotDir != "" {
		cfg.ZramSnapshotDir = expandHome(strings.TrimSpace(file.ZramSnapshotDir))
	}
	if file.TrimIntervalSeconds != nil {
		if *file.TrimIntervalSeconds <= 0 {
			return Config{}, fmt.Errorf("trim_interval_seconds must be positive")
		}
		cfg.TrimInterval = time.Duration(*file.TrimIntervalSeconds) * time.Second
	}
	if strings.TrimSpace(file.TrimMinThresholdBytes) != "" {
		size, err := ParseSize(file.TrimMinThresholdBytes)
		if err != nil {
			return Config{}, fmt.Errorf("trim_min_threshold_bytes: %w", err)
		}
		cfg.TrimMinThresholdBytes = size
	}
	if strings.TrimSpace(file.TrimMaxThresholdBytes) != "" {
		size, err := ParseSize(file.TrimMaxThresholdBytes)
		if err != nil {
			return Config{}, fmt.Errorf("trim_max_threshold_bytes: %w", err)
		}
		cfg.TrimMaxThresholdBytes = size
	}
	if cfg.TrimMaxThresholdBytes > 0 && cfg.TrimMinThresholdBytes > cfg.TrimMaxThresholdBytes {
		return Config{}, fmt.Errorf("trim_min_threshold_bytes must be <= trim_max_threshold_bytes")
	}
	if file.TrimMinFileCount != nil {
		if *file.TrimMinFileCount < 0 {
			return Config{}, fmt.Errorf("trim_min_file_count must be >= 0")
		}
		cfg.TrimMinFileCount = *file.TrimMinFileCount
	}
	if file.TrimThresholdRatio != nil {
		if *file.TrimThresholdRatio < 0 || *file.TrimThresholdRatio > 1 {
			return Config{}, fmt.Errorf("trim_threshold_ratio must be in [0, 1]")
		}
		cfg.TrimThresholdRatio = *file.TrimThresholdRatio
	}
	if strings.TrimSpace(file.TrimTempDir) != "" {
		cfg.TrimTempDir = strings.TrimSpace(file.TrimTempDir)
	}
	return cfg, nil
}

func ParseSize(input string) (uint64, error) {
	trimmed := strings.TrimSpace(strings.ToUpper(input))
	if trimmed == "" {
		return 0, fmt.Errorf("empty size")
	}
	multiplier := uint64(1)
	switch {
	case strings.HasSuffix(trimmed, "GB") || strings.HasSuffix(trimmed, "G"):
		multiplier = 1024 * 1024 * 1024
		trimmed = strings.TrimSuffix(strings.TrimSuffix(trimmed, "GB"), "G")
	case strings.HasSuffix(trimmed, "MB") || strings.HasSuffix(trimmed, "M"):
		multiplier = 1024 * 1024
		trimmed = strings.TrimSuffix(strings.TrimSuffix(trimmed, "MB"), "M")
	case strings.HasSuffix(trimmed, "KB") || strings.HasSuffix(trimmed, "K"):
		multiplier = 1024
		trimmed = strings.TrimSuffix(strings.TrimSuffix(trimmed, "KB"), "K")
	case strings.HasSuffix(trimmed, "B"):
		multiplier = 1
		trimmed = strings.TrimSuffix(trimmed, "B")
	}
	value, err := strconv.ParseUint(strings.TrimSpace(trimmed), 10, 64)
	if err != nil {
		return 0, err
	}
	return value * multiplier, nil
}

func expandHome(path string) string {
	if strings.HasPrefix(path, "~/") || path == "~" {
		if home, err := os.UserHomeDir(); err == nil {
			return filepath.Join(home, strings.TrimPrefix(path, "~"))
		}
	}
	return path
}
