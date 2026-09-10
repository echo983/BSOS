package daemon

import (
	"math/bits"
	"slices"
)

type fragmentationBin struct {
	LowerBoundBytes uint64
	UpperBoundBytes uint64
	CumulativeCount int
	BucketCount     int
}

type fragmentationMetrics struct {
	thresholdBytes  uint64
	cliffLowerBytes uint64
	cliffUpperBytes uint64
	maxDeltaCount   int
	bins            []fragmentationBin
}

func computeFragmentationThreshold(sizes []uint64) fragmentationMetrics {
	if len(sizes) == 0 {
		return fragmentationMetrics{}
	}

	maxPow := 0
	counts := make(map[int]int, len(sizes))
	for _, size := range sizes {
		if size == 0 {
			continue
		}
		pow := floorLog2(size)
		counts[pow]++
		if pow > maxPow {
			maxPow = pow
		}
	}
	if len(counts) == 0 {
		return fragmentationMetrics{}
	}

	bins := make([]fragmentationBin, 0, maxPow+1)
	cumulative := 0
	bestPow := 0
	bestDelta := -1
	for pow := maxPow; pow >= 0; pow-- {
		cumulative += counts[pow]
		bin := fragmentationBin{
			LowerBoundBytes: pow2Bytes(pow),
			UpperBoundBytes: pow2Bytes(pow + 1),
			CumulativeCount: cumulative,
			BucketCount:     counts[pow],
		}
		bins = append(bins, bin)
	}
	slices.Reverse(bins)

	for idx, bin := range bins {
		if bin.BucketCount > bestDelta {
			bestDelta = bin.BucketCount
			bestPow = idx
		}
	}

	return fragmentationMetrics{
		thresholdBytes:  bins[bestPow].UpperBoundBytes,
		cliffLowerBytes: bins[bestPow].LowerBoundBytes,
		cliffUpperBytes: bins[bestPow].UpperBoundBytes,
		maxDeltaCount:   bestDelta,
		bins:            bins,
	}
}

func floorLog2(v uint64) int {
	return bits.Len64(v) - 1
}

func pow2Bytes(pow int) uint64 {
	if pow <= 0 {
		return 1
	}
	if pow >= 63 {
		return 1 << 63
	}
	return 1 << pow
}

func (s *DeviceState) logicalFileSizes() []uint64 {
	s.indexMu.RLock()
	defer s.indexMu.RUnlock()

	aliasTargets := make(map[uint64]struct{})
	for key, ref := range s.confirmed {
		if key != ref.fid && !ref.packed {
			aliasTargets[ref.fid] = struct{}{}
		}
	}

	var sizes []uint64
	for key, ref := range s.confirmed {
		if key != ref.fid {
			continue
		}
		if _, ok := aliasTargets[key]; ok {
			continue
		}
		if ref.packed {
			continue
		}
		if ref.size > 0 {
			sizes = append(sizes, ref.size)
		}
	}
	slices.Sort(sizes)
	return sizes
}
