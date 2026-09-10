package daemon

import (
	"testing"
)

func TestComputeFragmentationThreshold(t *testing.T) {
	tests := []struct {
		name          string
		sizes         []uint64
		wantThreshold uint64
		wantMaxDelta  int
	}{
		{
			name:          "empty",
			sizes:         []uint64{},
			wantThreshold: 0,
			wantMaxDelta:  0,
		},
		{
			name:          "single_bucket",
			sizes:         []uint64{4096, 5000, 6000},
			wantThreshold: 8192,
			wantMaxDelta:  3,
		},
		{
			name:          "two_buckets_equal_size",
			sizes:         []uint64{1024, 2048, 4096, 8192},
			wantThreshold: 2048,
			wantMaxDelta:  1,
		},
		{
			name:          "finds_largest_delta",
			sizes:         []uint64{4096, 5000, 6000, 7000, 8192, 1 << 20, 2 << 20},
			wantThreshold: 8192,
			wantMaxDelta:  4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := computeFragmentationThreshold(tt.sizes)
			if got.thresholdBytes != tt.wantThreshold {
				t.Errorf("thresholdBytes=%d want %d", got.thresholdBytes, tt.wantThreshold)
			}
			if got.maxDeltaCount != tt.wantMaxDelta {
				t.Errorf("maxDeltaCount=%d want %d", got.maxDeltaCount, tt.wantMaxDelta)
			}
		})
	}
}

func TestFloorLog2(t *testing.T) {
	tests := []struct {
		v    uint64
		want int
	}{
		{1, 0},
		{2, 1},
		{3, 1},
		{4, 2},
		{7, 2},
		{8, 3},
		{1023, 9},
		{1024, 10},
		{1 << 20, 20},
		{1<<20 + 500, 20},
	}

	for _, tt := range tests {
		got := floorLog2(tt.v)
		if got != tt.want {
			t.Errorf("floorLog2(%d)=%d want %d", tt.v, got, tt.want)
		}
	}
}
