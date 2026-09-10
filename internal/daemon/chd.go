package daemon

import (
	"fmt"
	"math"
	"math/bits"

	"github.com/zeebo/xxh3"

	"github.com/echo983/BSOS/internal/blk"
)

// computeCHD is docs/DESIGN.md §4's Bonnie source, ported unchanged
// from NBSS: sample chdSamples synthetic candidate fids (unrelated to
// any real object's identity — purely a capacity-estimation heuristic,
// so this doesn't reopen §3.6's "server never hashes real content"),
// binary-search for the largest extent size for which at least
// targetP's share of samples would place without overlapping existing
// (confirmed + pending) extents. It reads whatever interval slice it's
// given, so callers decide whether that includes pending reservations.
func computeCHD(diskBytes uint64, diskID uint64, intervals []interval, targetP float64) uint64 {
	if diskBytes <= blk.GridStart {
		return 0
	}
	const chdSamples = 64
	totalSlots := (diskBytes - blk.GridStart) / blk.SlotSize
	if totalSlots == 0 {
		return 0
	}
	if math.IsNaN(targetP) || targetP <= 0 || targetP > 1 {
		targetP = 0.2
	}
	required := int(math.Ceil(targetP * float64(chdSamples)))
	if required < 1 {
		required = 1
	}
	fids := sampleFIDs(diskID, chdSamples)

	bestSlots := uint64(0)
	lo, hi := uint64(1), totalSlots
	for lo <= hi {
		mid := lo + (hi-lo)/2
		if chdSuccessRate(diskBytes, intervals, fids, mid, required) {
			bestSlots = mid
			lo = mid + 1
			continue
		}
		if mid == 0 {
			break
		}
		hi = mid - 1
	}
	return bestSlots * blk.SlotSize
}

func sampleFIDs(diskID uint64, count int) []uint64 {
	fids := make([]uint64, 0, count)
	for i := 0; i < count; i++ {
		seed := fmt.Sprintf("chd:%d:%d", diskID, i)
		fids = append(fids, xxh3.HashString(seed))
	}
	return fids
}

func chdSuccessRate(diskBytes uint64, intervals []interval, fids []uint64, slotsNeeded uint64, required int) bool {
	if slotsNeeded == 0 || len(fids) == 0 {
		return false
	}
	sizeBytes := slotsNeeded * blk.SlotSize
	success := 0
	for _, fid := range fids {
		_, slotIndex, slots, err := blk.AddrForFID(diskBytes, fid, sizeBytes)
		if err != nil || slots == 0 {
			continue
		}
		if anyOverlap(intervals, interval{start: slotIndex, end: slotIndex + slots - 1}) {
			continue
		}
		success++
		if success >= required {
			return true
		}
	}
	return false
}

func anyOverlap(list []interval, test interval) bool {
	for _, e := range list {
		if overlaps(e, test) {
			return true
		}
	}
	return false
}

func pow2Log(value uint64) int {
	if value == 0 {
		return 0
	}
	return bits.Len64(value) - 1
}
