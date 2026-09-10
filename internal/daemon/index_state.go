package daemon

import (
	"bytes"
	"fmt"
	"io"
	"sort"

	"github.com/echo983/BSOS/internal/blk"
)

// objectRef is immutable after publication. Alias and target references are
// published together, only after the complete index append has been synced.
type objectRef struct {
	fid        uint64
	storedSize uint64
	size       uint64
	offset     uint64
	packed     bool
}

func replayIndex(r io.ReaderAt, diskBytes uint64) (map[uint64]objectRef, map[uint64]packedRecord, []interval, uint64, error) {
	refs := make(map[uint64]objectRef)
	packed := make(map[uint64]packedRecord)
	extentsMap := make(map[uint64]interval)
	var extents []interval
	var alias *blk.IndexEntry
	var pendingPacked *blk.IndexEntry
	var packedTableFIDs []uint64
	buf := make([]byte, 4<<20)
	finish := func(end uint64) (map[uint64]objectRef, map[uint64]packedRecord, []interval, uint64, error) {
		if alias != nil {
			return nil, nil, nil, 0, fmt.Errorf("incomplete alias pair at index tail")
		}
		if pendingPacked != nil {
			return nil, nil, nil, 0, fmt.Errorf("incomplete packed anchor pair at index tail")
		}
		for _, tableFID := range packedTableFIDs {
			tableRef, ok := refs[tableFID]
			if !ok {
				continue
			}
			addr, _, _, err := blk.AddrForFID(diskBytes, tableRef.fid, tableRef.storedSize)
			if err != nil {
				return nil, nil, nil, 0, err
			}
			blob := make([]byte, tableRef.size)
			if _, err := r.ReadAt(blob, int64(addr)); err != nil {
				return nil, nil, nil, 0, fmt.Errorf("read packed table 0x%X: %w", tableFID, err)
			}
			header, entries, err := decodePackedTable(blob)
			if err != nil {
				return nil, nil, nil, 0, fmt.Errorf("decode packed table 0x%X: %w", tableFID, err)
			}
			for _, entry := range entries {
				refs[entry.fid] = objectRef{
					fid:        header.containerFID,
					storedSize: header.containerSize,
					size:       entry.size,
					offset:     entry.offset,
					packed:     true,
				}
				packed[entry.fid] = packedRecord{
					containerFID:  header.containerFID,
					containerSize: header.containerSize,
					offset:        entry.offset,
					logicSize:     entry.size,
					tableFID:      tableFID,
					tableSize:     tableRef.size,
				}
			}
		}

		for _, iv := range extentsMap {
			extents = append(extents, iv)
		}
		sort.Slice(extents, func(i, j int) bool { return extents[i].start < extents[j].start })
		for i := 1; i < len(extents); i++ {
			if overlaps(extents[i-1], extents[i]) {
				return nil, nil, nil, 0, fmt.Errorf("overlapping confirmed extents")
			}
		}
		return refs, packed, extents, end, nil
	}
	for off := uint64(blk.IndexStart); off < blk.GridStart; {
		size := min(uint64(len(buf)), uint64(blk.GridStart)-off)
		n, err := r.ReadAt(buf[:size], int64(off))
		if err != nil {
			return nil, nil, nil, 0, fmt.Errorf("read index at %d: %w", off, err)
		}
		if uint64(n) != size {
			return nil, nil, nil, 0, io.ErrUnexpectedEOF
		}
		for i := 0; i < n; i += blk.IndexEntrySize {
			raw := buf[i : i+blk.IndexEntrySize]
			if bytes.Equal(raw, make([]byte, blk.IndexEntrySize)) {
				return finish(off + uint64(i))
			}
			e, err := blk.ParseIndexEntry(raw)
			if err != nil {
				return nil, nil, nil, 0, err
			}
			if pendingPacked != nil {
				if e.JumpCode != 0 || e.Size == 0 {
					return nil, nil, nil, 0, fmt.Errorf("invalid packed table entry following anchor")
				}
				tableFID := e.FID
				tableSize := e.Size
				packedTableFIDs = append(packedTableFIDs, tableFID)
				_, slot, count, err := blk.AddrForFID(diskBytes, tableFID, tableSize)
				if err != nil {
					return nil, nil, nil, 0, err
				}
				refs[tableFID] = objectRef{fid: tableFID, storedSize: tableSize, size: tableSize}
				extentsMap[tableFID] = interval{slot, slot + count - 1}
				pendingPacked = nil
				continue
			}
			if blk.IsPackedAnchor(e) {
				if alias != nil || e.FID == 0 {
					return nil, nil, nil, 0, fmt.Errorf("invalid packed anchor indicator")
				}
				pendingPacked = &e
				continue
			}
			if blk.IsJumpIndicator(e) {
				if alias != nil || e.FID == 0 {
					return nil, nil, nil, 0, fmt.Errorf("invalid alias indicator")
				}
				if _, ok := refs[e.FID]; ok {
					return nil, nil, nil, 0, fmt.Errorf("duplicate alias fid %d", e.FID)
				}
				alias = &e
				continue
			}
			if blk.IsTombstone(e) {
				delete(refs, e.FID)
				delete(packed, e.FID)
				delete(extentsMap, e.FID)
				continue
			}
			if e.JumpCode != 0 || e.Size == 0 || e.Size >= blk.IndexPackedSentinel {
				return nil, nil, nil, 0, fmt.Errorf("unsupported or invalid index entry for fid %d", e.FID)
			}
			if _, ok := refs[e.FID]; ok {
				return nil, nil, nil, 0, fmt.Errorf("duplicate fid %d", e.FID)
			}
			_, slot, count, err := blk.AddrForFID(diskBytes, e.FID, e.Size)
			if err != nil {
				return nil, nil, nil, 0, err
			}
			refs[e.FID] = objectRef{fid: e.FID, storedSize: e.Size, size: e.Size}
			extentsMap[e.FID] = interval{slot, slot + count - 1}
			if alias != nil {
				if alias.FID == e.FID || e.Size < 2 {
					return nil, nil, nil, 0, fmt.Errorf("invalid alias target")
				}
				refs[alias.FID] = objectRef{fid: e.FID, storedSize: e.Size, size: e.Size - 1}
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
