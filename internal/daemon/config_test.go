package daemon

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestParseSize(t *testing.T) {
	tests := []struct {
		input   string
		want    uint64
		wantErr bool
	}{
		{"1024", 1024, false},
		{"4KB", 4096, false},
		{"4K", 4096, false},
		{"1MB", 1 << 20, false},
		{"256M", 256 << 20, false},
		{"1GB", 1 << 30, false},
		{"2G", 2 << 30, false},
		{"512B", 512, false},
		{"", 0, true},
		{"invalid", 0, true},
	}
	for _, tc := range tests {
		got, err := ParseSize(tc.input)
		if (err != nil) != tc.wantErr {
			t.Errorf("ParseSize(%q) error = %v, wantErr %v", tc.input, err, tc.wantErr)
			continue
		}
		if !tc.wantErr && got != tc.want {
			t.Errorf("ParseSize(%q) = %d, want %d", tc.input, got, tc.want)
		}
	}
}

func TestLoadConfigValid(t *testing.T) {
	tmpDir := t.TempDir()
	tomlPath := filepath.Join(tmpDir, "bsosd.toml")
	tomlContent := `
pan_path = "/tmp/pan.json"
grpc_listen = ":19090"
max_put = "128MB"
small_file_pow2 = 8
zram_snapshot_dir = "/tmp/snapshots"
reservation_stall_timeout_ms = 15000
pool_gate_fanout_timeout_ms = 1000
write_dispatch_concurrency = 32
chd_target_p = 0.25
trim_interval_seconds = 600
trim_min_threshold_bytes = "8KB"
trim_max_threshold_bytes = "64MB"
trim_min_file_count = 50
trim_threshold_ratio = 0.15
trim_temp_dir = "/tmp/trim"
`
	if err := os.WriteFile(tomlPath, []byte(tomlContent), 0600); err != nil {
		t.Fatal(err)
	}

	cfg, err := LoadConfig(tomlPath)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}

	if cfg.PanPath != "/tmp/pan.json" {
		t.Errorf("PanPath = %q, want /tmp/pan.json", cfg.PanPath)
	}
	if cfg.GRPCListen != ":19090" {
		t.Errorf("GRPCListen = %q, want :19090", cfg.GRPCListen)
	}
	if cfg.MaxPutBytes != 128<<20 {
		t.Errorf("MaxPutBytes = %d, want %d", cfg.MaxPutBytes, 128<<20)
	}
	if cfg.SmallFilePow2 != 8 {
		t.Errorf("SmallFilePow2 = %d, want 8", cfg.SmallFilePow2)
	}
	if cfg.ReservationStallTimeout != 15*time.Second {
		t.Errorf("ReservationStallTimeout = %v, want 15s", cfg.ReservationStallTimeout)
	}
	if cfg.PoolGateFanoutTimeout != 1*time.Second {
		t.Errorf("PoolGateFanoutTimeout = %v, want 1s", cfg.PoolGateFanoutTimeout)
	}
	if cfg.WriteDispatchConcurrency != 32 {
		t.Errorf("WriteDispatchConcurrency = %d, want 32", cfg.WriteDispatchConcurrency)
	}
	if cfg.ChdTargetP != 0.25 {
		t.Errorf("ChdTargetP = %v, want 0.25", cfg.ChdTargetP)
	}
	if cfg.TrimInterval != 10*time.Minute {
		t.Errorf("TrimInterval = %v, want 10m", cfg.TrimInterval)
	}
	if cfg.TrimMinThresholdBytes != 8192 {
		t.Errorf("TrimMinThresholdBytes = %d, want 8192", cfg.TrimMinThresholdBytes)
	}
	if cfg.TrimMaxThresholdBytes != 64<<20 {
		t.Errorf("TrimMaxThresholdBytes = %d, want %d", cfg.TrimMaxThresholdBytes, 64<<20)
	}
	if cfg.TrimMinFileCount != 50 {
		t.Errorf("TrimMinFileCount = %d, want 50", cfg.TrimMinFileCount)
	}
	if cfg.TrimThresholdRatio != 0.15 {
		t.Errorf("TrimThresholdRatio = %v, want 0.15", cfg.TrimThresholdRatio)
	}
	if cfg.TrimTempDir != "/tmp/trim" {
		t.Errorf("TrimTempDir = %q, want /tmp/trim", cfg.TrimTempDir)
	}
}

func TestLoadConfigInvalidBounds(t *testing.T) {
	invalidCases := []struct {
		name string
		toml string
	}{
		{"negative_small_file_pow2", `small_file_pow2 = -1`},
		{"zero_stall_timeout", `reservation_stall_timeout_ms = 0`},
		{"zero_gate_timeout", `pool_gate_fanout_timeout_ms = -5`},
		{"zero_concurrency", `write_dispatch_concurrency = 0`},
		{"invalid_chd_target_p_low", `chd_target_p = 0`},
		{"invalid_chd_target_p_high", `chd_target_p = 1.5`},
		{"zero_trim_interval", `trim_interval_seconds = 0`},
		{"trim_min_greater_than_max", "trim_min_threshold_bytes = \"64MB\"\ntrim_max_threshold_bytes = \"4KB\""},
		{"negative_trim_min_file_count", `trim_min_file_count = -1`},
		{"invalid_trim_ratio", `trim_threshold_ratio = 2.0`},
	}
	for _, tc := range invalidCases {
		t.Run(tc.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			tomlPath := filepath.Join(tmpDir, "invalid.toml")
			if err := os.WriteFile(tomlPath, []byte(tc.toml), 0600); err != nil {
				t.Fatal(err)
			}
			_, err := LoadConfig(tomlPath)
			if err == nil {
				t.Errorf("expected error for case %s, got nil", tc.name)
			}
		})
	}
}
