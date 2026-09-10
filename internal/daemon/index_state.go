package daemon

import (
	"bytes"
	"fmt"
	"io"
	"sort"

	"bsos/internal/blk"
)

// objectRef is immutable after publication. Alias and target references are
// published together, only after the complete index append has been synced.
type objectRef struct{ fid, storedSize, size uint64 }

func replayIndex(r io.ReaderAt, diskBytes uint64) (map[uint64]objectRef, []interval, uint64, error) {
	refs := make(map[uint64]objectRef)
	var extents []interval
	var alias *blk.IndexEntry
	buf := make([]byte, 4<<20)
	finish := func(end uint64) (map[uint64]objectRef, []interval, uint64, error) {
		if alias != nil {
			return nil, nil, 0, fmt.Errorf("incomplete alias pair at index tail")
		}
		sort.Slice(extents, func(i, j int) bool { return extents[i].start < extents[j].start })
		for i := 1; i < len(extents); i++ {
			if overlaps(extents[i-1], extents[i]) {
				return nil, nil, 0, fmt.Errorf("overlapping confirmed extents")
			}
		}
		return refs, extents, end, nil
	}
	for off := uint64(blk.IndexStart); off < blk.GridStart; {
		size := min(uint64(len(buf)), uint64(blk.GridStart)-off)
		n, err := r.ReadAt(buf[:size], int64(off))
		if err != nil {
			return nil, nil, 0, fmt.Errorf("read index at %d: %w", off, err)
		}
		if uint64(n) != size {
			return nil, nil, 0, io.ErrUnexpectedEOF
		}
		for i := 0; i < n; i += blk.IndexEntrySize {
			raw := buf[i : i+blk.IndexEntrySize]
			if bytes.Equal(raw, make([]byte, blk.IndexEntrySize)) {
				return finish(off + uint64(i))
			}
			e, err := blk.ParseIndexEntry(raw)
			if err != nil {
				return nil, nil, 0, err
			}
			if blk.IsJumpIndicator(e) {
				if alias != nil || e.FID == 0 {
					return nil, nil, 0, fmt.Errorf("invalid alias indicator")
				}
				if _, ok := refs[e.FID]; ok {
					return nil, nil, 0, fmt.Errorf("duplicate alias fid %d", e.FID)
				}
				alias = &e
				continue
			}
			// Packed/tombstone records are not produced until the Trim milestone.
			// Reject them explicitly rather than interpreting them as direct extents.
			if e.JumpCode != 0 || e.Size == 0 || e.Size >= blk.IndexPackedSentinel {
				return nil, nil, 0, fmt.Errorf("unsupported or invalid index entry for fid %d", e.FID)
			}
			if _, ok := refs[e.FID]; ok {
				return nil, nil, 0, fmt.Errorf("duplicate fid %d", e.FID)
			}
			_, slot, count, err := blk.AddrForFID(diskBytes, e.FID, e.Size)
			if err != nil {
				return nil, nil, 0, err
			}
			refs[e.FID] = objectRef{e.FID, e.Size, e.Size}
			extents = append(extents, interval{slot, slot + count - 1})
			if alias != nil {
				if alias.FID == e.FID || e.Size < 2 {
					return nil, nil, 0, fmt.Errorf("invalid alias target")
				}
				refs[alias.FID] = objectRef{e.FID, e.Size, e.Size - 1}
				alias = nil
			}
		}
		off += size
	}
	return finish(blk.GridStart)
}

func (s *DeviceState) lookup(fid uint64) (objectRef, error) {
	s.indexMu.RLock()
	defer s.indexMu.RUnlock()
	if s.indexErr != nil {
		return objectRef{}, s.indexErr
	}
	ref, ok := s.confirmed[fid]
	if !ok {
		return objectRef{}, ErrNotFound
	}
	return ref, nil
}
