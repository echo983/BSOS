package daemon

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/zeebo/xxh3"

	"github.com/echo983/BSOS/internal/blk"
	"github.com/echo983/BSOS/internal/pan"
)

type trimCandidate struct {
	fid  uint64
	size uint64
}

type containerEntry struct {
	fid    uint64
	offset uint64
	size   uint64
}

func formatHexFID(fid uint64) string {
	if fid == 0 {
		return ""
	}
	return "0x" + strings.ToUpper(strconv.FormatUint(fid, 16))
}

func (s *DeviceState) readLogicalObjectLocked(fid uint64) ([]byte, uint64, error) {
	s.indexMu.RLock()
	ref, ok := s.confirmed[fid]
	fault := s.indexErr
	s.indexMu.RUnlock()
	if fault != nil {
		return nil, 0, fault
	}
	if !ok {
		return nil, 0, ErrNotFound
	}
	data, err := s.readRange(context.Background(), ref, 0, ref.size)
	if err != nil {
		return nil, 0, err
	}
	return data, ref.size, nil
}

func (s *DeviceState) collectTrimCandidatesLocked() ([]trimCandidate, uint64, int, int, error) {
	records, err := blk.CollectLatestIndexRecords(s.file)
	if err != nil {
		return nil, 0, 0, 0, err
	}
	packedTableFIDs := make(map[uint64]struct{})
	packedContainerFIDs := make(map[uint64]struct{})
	jumpTargets := make(map[uint64]struct{})
	for _, record := range records {
		if record.Jump {
			jumpTargets[record.ActualFID] = struct{}{}
			continue
		}
		if !record.Packed {
			continue
		}
		packedTableFIDs[record.FID] = struct{}{}
		blob, _, err := s.readLogicalObjectLocked(record.FID)
		if err != nil {
			return nil, 0, 0, 0, fmt.Errorf("read packed table 0x%X: %w", record.FID, err)
		}
		header, _, err := decodePackedTable(blob)
		if err != nil {
			return nil, 0, 0, 0, fmt.Errorf("decode packed table 0x%X: %w", record.FID, err)
		}
		packedContainerFIDs[header.containerFID] = struct{}{}
	}

	eligible := make([]trimCandidate, 0, len(records))
	sizes := make([]uint64, 0, len(records))
	for _, record := range records {
		if record.Packed || record.Jump {
			continue
		}
		if _, ok := jumpTargets[record.FID]; ok {
			continue
		}
		if _, ok := packedTableFIDs[record.FID]; ok {
			continue
		}
		if _, ok := packedContainerFIDs[record.FID]; ok {
			continue
		}
		eligible = append(eligible, trimCandidate{fid: record.FID, size: record.Size})
		sizes = append(sizes, record.Size)
	}
	eligibleSmallCount := 0
	candidates := make([]trimCandidate, 0, len(eligible))
	currentBatchSize := uint64(0)

	metrics := computeFragmentationThreshold(sizes)
	threshold := metrics.thresholdBytes
	if threshold < s.cfg.TrimMinThresholdBytes {
		threshold = s.cfg.TrimMinThresholdBytes
	}
	if s.cfg.TrimMaxThresholdBytes > 0 && threshold > s.cfg.TrimMaxThresholdBytes {
		threshold = s.cfg.TrimMaxThresholdBytes
	}

	for _, e := range eligible {
		if e.size < threshold {
			eligibleSmallCount++
		}
	}

	batchLimit := computeCHD(s.diskBytes, s.diskID, s.intervals, s.cfg.ChdTargetP)
	if batchLimit < 8<<20 {
		batchLimit = 8 << 20
	}
	if batchLimit > 256<<20 {
		batchLimit = 256 << 20
	}
	if s.cfg.MaxPutBytes > 0 && batchLimit > s.cfg.MaxPutBytes {
		batchLimit = s.cfg.MaxPutBytes
	}

	for _, candidate := range eligible {
		if candidate.size < threshold {
			if currentBatchSize+candidate.size > batchLimit && len(candidates) > 0 {
				break
			}
			candidates = append(candidates, candidate)
			currentBatchSize += candidate.size
		}
	}
	return candidates, threshold, eligibleSmallCount, len(eligible), nil
}

func (s *DeviceState) buildContainerFileLocked(candidates []trimCandidate) (string, []containerEntry, error) {
	dir := s.cfg.TrimTempDir
	tmp, err := os.CreateTemp(dir, "bsos-trim-container-*.bin")
	if err != nil {
		return "", nil, err
	}
	defer tmp.Close()

	offset := uint64(0)
	entries := make([]containerEntry, 0, len(candidates))
	for _, candidate := range candidates {
		data, size, err := s.readLogicalObjectLocked(candidate.fid)
		if err != nil {
			return "", nil, fmt.Errorf("read fid 0x%X: %w", candidate.fid, err)
		}
		if _, err := tmp.Write(data); err != nil {
			return "", nil, err
		}
		entries = append(entries, containerEntry{fid: candidate.fid, offset: offset, size: size})
		offset += size
	}
	return tmp.Name(), entries, nil
}

func (s *DeviceState) writeFileObjectNoJumpLocked(path string) (uint64, uint64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return 0, 0, err
	}
	size := uint64(info.Size())
	if size == 0 {
		return 0, 0, fmt.Errorf("empty object")
	}
	hasher := xxh3.New()
	if _, err := io.Copy(hasher, f); err != nil {
		return 0, 0, err
	}
	fid := hasher.Sum64()
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return 0, 0, err
	}

	s.indexMu.RLock()
	_, exists := s.confirmed[fid]
	fault := s.indexErr
	s.indexMu.RUnlock()
	if fault != nil {
		return 0, 0, fault
	}
	if exists {
		return 0, 0, ErrConflict
	}

	addr, slotIndex, slotsNeeded, err := blk.AddrForFID(s.diskBytes, fid, size)
	if err != nil {
		return 0, 0, err
	}
	newInterval := interval{start: slotIndex, end: slotIndex + slotsNeeded - 1}
	if anyOverlap(s.intervals, newInterval) || anyOverlap(s.pendingIntervals, newInterval) {
		return 0, 0, ErrConflict
	}

	buf := make([]byte, 4*1024*1024)
	var written uint64
	for written < size {
		n, err := f.Read(buf)
		if err != nil && err != io.EOF {
			return 0, 0, err
		}
		if n == 0 {
			break
		}
		if _, err := s.file.WriteAt(buf[:n], int64(addr+written)); err != nil {
			return 0, 0, err
		}
		written += uint64(n)
	}
	pad := slotsNeeded*blk.SlotSize - size
	if pad > 0 {
		if err := writeZeros(s.file, addr+size, pad); err != nil {
			return 0, 0, err
		}
	}
	if err := s.file.Sync(); err != nil {
		return 0, 0, fmt.Errorf("sync container payload: %w", err)
	}

	entry, err := blk.BuildIndexEntry(fid, size, 0)
	if err != nil {
		return 0, 0, err
	}
	if s.indexEnd+uint64(len(entry)) > blk.GridStart {
		return 0, 0, blk.ErrIndexFull
	}
	if _, err := s.file.WriteAt(entry, int64(s.indexEnd)); err != nil {
		return 0, 0, err
	}
	if err := s.file.Sync(); err != nil {
		return 0, 0, fmt.Errorf("sync container index: %w", err)
	}

	s.indexMu.Lock()
	s.confirmed[fid] = objectRef{fid: fid, storedSize: size, size: size}
	s.indexMu.Unlock()
	s.indexEnd += uint64(len(entry))
	s.intervals = append(s.intervals, newInterval)

	return fid, size, nil
}

func (s *DeviceState) writeContainerObjectNoJumpLocked(path string) (uint64, uint64, error) {
	for nonce := uint64(0); nonce < 256; nonce++ {
		fid, size, err := s.writeFileObjectNoJumpLocked(path)
		if err == nil {
			return fid, size, nil
		}
		if !errors.Is(err, ErrConflict) {
			return 0, 0, err
		}
		f, openErr := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
		if openErr != nil {
			return 0, 0, openErr
		}
		var buf [8]byte
		binary.LittleEndian.PutUint64(buf[:], nonce+1)
		_, writeErr := f.Write(buf[:])
		_ = f.Close()
		if writeErr != nil {
			return 0, 0, writeErr
		}
	}
	return 0, 0, ErrConflict
}

func (s *DeviceState) writeObjectNoJumpLocked(data []byte) (uint64, error) {
	size := uint64(len(data))
	if size == 0 {
		return 0, fmt.Errorf("empty object")
	}
	fid := xxh3.Hash(data)

	s.indexMu.RLock()
	_, exists := s.confirmed[fid]
	fault := s.indexErr
	s.indexMu.RUnlock()
	if fault != nil {
		return 0, fault
	}
	if exists {
		return 0, ErrConflict
	}

	addr, slotIndex, slotsNeeded, err := blk.AddrForFID(s.diskBytes, fid, size)
	if err != nil {
		return 0, err
	}
	newInterval := interval{start: slotIndex, end: slotIndex + slotsNeeded - 1}
	if anyOverlap(s.intervals, newInterval) || anyOverlap(s.pendingIntervals, newInterval) {
		return 0, ErrConflict
	}

	if _, err := s.file.WriteAt(data, int64(addr)); err != nil {
		return 0, err
	}
	pad := slotsNeeded*blk.SlotSize - size
	if pad > 0 {
		if err := writeZeros(s.file, addr+size, pad); err != nil {
			return 0, err
		}
	}
	if err := s.file.Sync(); err != nil {
		return 0, fmt.Errorf("sync packed table payload: %w", err)
	}

	entry, err := blk.BuildIndexEntry(fid, size, 0)
	if err != nil {
		return 0, err
	}
	if s.indexEnd+uint64(len(entry)) > blk.GridStart {
		return 0, blk.ErrIndexFull
	}
	if _, err := s.file.WriteAt(entry, int64(s.indexEnd)); err != nil {
		return 0, err
	}
	if err := s.file.Sync(); err != nil {
		return 0, fmt.Errorf("sync packed table index: %w", err)
	}

	s.indexMu.Lock()
	s.confirmed[fid] = objectRef{fid: fid, storedSize: size, size: size}
	s.indexMu.Unlock()
	s.indexEnd += uint64(len(entry))
	s.intervals = append(s.intervals, newInterval)

	return fid, nil
}

func (s *DeviceState) writePackedTableObjectNoJumpLocked(header packedTableHeader, entries []packedTableEntry) (uint64, uint64, error) {
	for nonce := uint64(0); nonce < 256; nonce++ {
		header.reserved = nonce
		blob := encodePackedTable(header, entries)
		fid, err := s.writeObjectNoJumpLocked(blob)
		if err == nil {
			return fid, uint64(len(blob)), nil
		}
		if !errors.Is(err, ErrConflict) {
			return 0, 0, err
		}
	}
	return 0, 0, ErrConflict
}

func (s *DeviceState) appendPackedAnchorLocked(tableFID uint64, tableSize uint64) error {
	entryPacked, err := blk.BuildIndexEntry(tableFID, blk.IndexPackedSentinel, blk.PackedAnchorCode)
	if err != nil {
		return err
	}
	entryReal, err := blk.BuildIndexEntry(tableFID, tableSize, 0)
	if err != nil {
		return err
	}
	needed := uint64(len(entryPacked) + len(entryReal))
	if s.indexEnd+needed > blk.GridStart {
		return blk.ErrIndexFull
	}
	if _, err := s.file.WriteAt(entryPacked, int64(s.indexEnd)); err != nil {
		return err
	}
	if _, err := s.file.WriteAt(entryReal, int64(s.indexEnd+uint64(len(entryPacked)))); err != nil {
		return err
	}
	if err := s.file.Sync(); err != nil {
		return fmt.Errorf("sync packed anchor: %w", err)
	}
	s.indexEnd += needed
	return nil
}

func (s *DeviceState) loadPackedTablesLocked(tableFIDs []uint64) error {
	for _, tableFID := range tableFIDs {
		blob, tableSize, err := s.readLogicalObjectLocked(tableFID)
		if err != nil {
			return fmt.Errorf("load packed table 0x%X: %w", tableFID, err)
		}
		header, entries, err := decodePackedTable(blob)
		if err != nil {
			return fmt.Errorf("decode packed table 0x%X: %w", tableFID, err)
		}
		if header.diskID != s.diskID {
			return fmt.Errorf("packed table 0x%X disk_id mismatch: got 0x%X want 0x%X", tableFID, header.diskID, s.diskID)
		}
		if header.containerSize == 0 {
			return fmt.Errorf("packed table 0x%X has zero container size", tableFID)
		}
		s.indexMu.Lock()
		for _, entry := range entries {
			s.confirmed[entry.fid] = objectRef{
				fid:        header.containerFID,
				storedSize: header.containerSize,
				size:       entry.size,
				offset:     entry.offset,
				packed:     true,
			}
			s.packed[entry.fid] = packedRecord{
				containerFID:  header.containerFID,
				containerSize: header.containerSize,
				offset:        entry.offset,
				logicSize:     entry.size,
				tableFID:      tableFID,
				tableSize:     tableSize,
			}
		}
		s.indexMu.Unlock()
	}
	return nil
}

func (s *DeviceState) verifyPackedCandidatesLocked(candidates []trimCandidate) error {
	s.indexMu.RLock()
	defer s.indexMu.RUnlock()
	for _, candidate := range candidates {
		record, ok := s.packed[candidate.fid]
		if !ok {
			return fmt.Errorf("missing packed mapping for fid 0x%X", candidate.fid)
		}
		ref, ok := s.confirmed[candidate.fid]
		if !ok || !ref.packed {
			return fmt.Errorf("missing confirmed packed ref for fid 0x%X", candidate.fid)
		}
		addr, _, _, err := blk.AddrForFID(s.diskBytes, record.containerFID, record.containerSize)
		if err != nil {
			return err
		}
		buf := make([]byte, record.logicSize)
		n, err := s.file.ReadAt(buf, int64(addr+record.offset))
		if err != nil && n == 0 {
			return err
		}
		if uint64(len(buf)) != candidate.size {
			return fmt.Errorf("packed size mismatch fid 0x%X: got %d want %d", candidate.fid, len(buf), candidate.size)
		}
	}
	return nil
}

func (s *DeviceState) appendTombstoneLocked(fid uint64) error {
	entry, err := blk.BuildIndexEntry(fid, 0, 0)
	if err != nil {
		return err
	}
	if s.indexEnd+uint64(len(entry)) > blk.GridStart {
		return blk.ErrIndexFull
	}
	if _, err := s.file.WriteAt(entry, int64(s.indexEnd)); err != nil {
		return err
	}
	s.indexEnd += uint64(len(entry))
	return nil
}

func (s *DeviceState) compactIndexLocked() error {
	if err := s.file.Sync(); err != nil {
		return fmt.Errorf("sync before compact: %w", err)
	}
	records, err := blk.CollectLatestIndexRecords(s.file)
	if err != nil {
		return fmt.Errorf("collect records: %w", err)
	}
	tmp, err := os.CreateTemp("", "bsos-compact-*.nbssIndex")
	if err != nil {
		return fmt.Errorf("create temp compact index: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	defer tmp.Close()

	header := make([]byte, blk.HeaderBytes)
	if _, err := s.file.ReadAt(header, 0); err != nil && err != io.EOF {
		return fmt.Errorf("read header: %w", err)
	}
	if _, err := tmp.WriteAt(header, 0); err != nil {
		return fmt.Errorf("write temp header: %w", err)
	}
	entryCount, err := blk.WriteLatestIndexRecords(tmp, records)
	if err != nil {
		return fmt.Errorf("write compacted records: %w", err)
	}
	if err := writeZeros(tmp, blk.IndexStart+uint64(entryCount)*blk.IndexEntrySize, blk.GridStart-(blk.IndexStart+uint64(entryCount)*blk.IndexEntrySize)); err != nil {
		return fmt.Errorf("zero fill compacted index: %w", err)
	}
	if err := blk.ValidateCompacted(tmpPath); err != nil {
		return fmt.Errorf("validate compacted: %w", err)
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return err
	}
	section := io.NewSectionReader(tmp, 0, blk.GridStart)
	buf := make([]byte, 1024*1024)
	if _, err := io.CopyBuffer(io.NewOffsetWriter(s.file, 0), section, buf); err != nil {
		return fmt.Errorf("copy compacted index to device: %w", err)
	}
	if err := s.file.Sync(); err != nil {
		return fmt.Errorf("sync compacted index: %w", err)
	}

	confirmed, packed, intervals, indexEnd, err := replayIndex(s.file, s.diskBytes)
	if err != nil {
		s.indexMu.Lock()
		s.indexErr = fmt.Errorf("replay compacted index: %w", err)
		s.indexMu.Unlock()
		return s.indexErr
	}
	s.indexMu.Lock()
	s.confirmed = confirmed
	s.packed = packed
	s.indexEnd = indexEnd
	s.indexErr = nil
	s.indexMu.Unlock()
	s.intervals = intervals
	return nil
}

func (s *DeviceState) maybeTrim() (err error) {
	s.trimMu.Lock()
	defer s.trimMu.Unlock()

	s.mu.Lock()
	s.trimState.DiskID = formatHexFID(s.diskID)
	s.trimState.DevicePath = s.devicePath
	s.trimState.LastCheckAt = time.Now().UTC()
	s.trimState.LastError = ""
	defer s.mu.Unlock()

	defer func() {
		if err != nil {
			s.trimState.LastError = err.Error()
		}
	}()

	candidates, threshold, smallFiles, totalFiles, err := s.collectTrimCandidatesLocked()
	if err != nil {
		return err
	}
	s.trimState.LastThreshold = threshold
	s.trimState.LastTotalFiles = totalFiles
	s.trimState.LastSmallFiles = smallFiles
	s.trimState.LastTrigger = false
	if len(candidates) == 0 || totalFiles == 0 {
		return nil
	}
	if smallFiles <= s.cfg.TrimMinFileCount || float64(smallFiles) <= float64(totalFiles)*s.cfg.TrimThresholdRatio {
		return nil
	}
	s.trimState.LastTrigger = true
	s.trimState.LastRunAt = time.Now().UTC()

	backupDir := "bsos_trim_backup"
	if s.cfg.TrimTempDir != "" {
		backupDir = filepath.Join(s.cfg.TrimTempDir, "bsos_trim_backup")
	}
	if err := os.MkdirAll(backupDir, 0o755); err != nil {
		return fmt.Errorf("create backup dir: %w", err)
	}
	backupPath := filepath.Join(backupDir, fmt.Sprintf("index_0x%X.nbssIndex", s.diskID))
	dev := pan.Device{Status: "match", DevicePath: s.devicePath, DiskID: fmt.Sprintf("0x%X", s.diskID)}
	if err := blk.BackupDeviceIndexToPath(dev, backupPath); err != nil {
		return fmt.Errorf("backup index: %w", err)
	}
	s.trimState.LastBackupPath = backupPath
	if s.failPoint == "after-backup" {
		return fmt.Errorf("failpoint: after-backup")
	}

	containerPath, entries, err := s.buildContainerFileLocked(candidates)
	if err != nil {
		return err
	}
	defer os.Remove(containerPath)
	if s.failPoint == "after-container-build" {
		return fmt.Errorf("failpoint: after-container-build")
	}

	containerFID, containerSize, err := s.writeContainerObjectNoJumpLocked(containerPath)
	if err != nil {
		return fmt.Errorf("write container object: %w", err)
	}
	s.trimState.LastContainerFID = formatHexFID(containerFID)
	if s.failPoint == "after-container-write" {
		return fmt.Errorf("failpoint: after-container-write")
	}

	tableEntries := make([]packedTableEntry, 0, len(entries))
	for _, entry := range entries {
		tableEntries = append(tableEntries, packedTableEntry{fid: entry.fid, offset: entry.offset, size: entry.size})
	}
	tableFID, tableSize, err := s.writePackedTableObjectNoJumpLocked(packedTableHeader{
		diskID:        s.diskID,
		containerFID:  containerFID,
		containerSize: containerSize,
		entryCount:    uint64(len(tableEntries)),
	}, tableEntries)
	if err != nil {
		return fmt.Errorf("write packed table object: %w", err)
	}
	s.trimState.LastTableFID = formatHexFID(tableFID)
	if s.failPoint == "after-table-write" {
		return fmt.Errorf("failpoint: after-table-write")
	}

	if err := s.appendPackedAnchorLocked(tableFID, tableSize); err != nil {
		return fmt.Errorf("append packed anchor: %w", err)
	}
	if s.failPoint == "after-anchor-append" {
		return fmt.Errorf("failpoint: after-anchor-append")
	}

	if err := s.loadPackedTablesLocked([]uint64{tableFID}); err != nil {
		return fmt.Errorf("expand packed table: %w", err)
	}
	if err := s.verifyPackedCandidatesLocked(candidates); err != nil {
		return fmt.Errorf("verify packed candidates: %w", err)
	}
	for _, candidate := range candidates {
		if err := s.appendTombstoneLocked(candidate.fid); err != nil {
			return fmt.Errorf("tombstone candidate 0x%X: %w", candidate.fid, err)
		}
	}
	if err := s.file.Sync(); err != nil {
		return fmt.Errorf("sync tombstones: %w", err)
	}
	if s.failPoint == "after-delete" {
		return fmt.Errorf("failpoint: after-delete")
	}

	if err := s.compactIndexLocked(); err != nil {
		return fmt.Errorf("compact index: %w", err)
	}

	s.trimState.LastPackedFiles = len(candidates)
	s.trimState.LastSuccessAt = time.Now().UTC()
	return nil
}
