package blk

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const (
	compactExitOK      = 0
	compactExitBadArgs = 2
	compactExitIO      = 3
	compactExitNotNBSS = 5
)

type compactOptions struct {
	file string
}

type compactRecord struct {
	fid      uint64
	size     uint64
	jumpFID  uint64
	jumpCode byte
	seq      uint64
	jump     bool
	packed   bool
}

type LatestIndexRecord struct {
	FID        uint64
	Size       uint64
	ActualFID  uint64
	ActualSize uint64
	JumpCode   byte
	Seq        uint64
	Jump       bool
	Packed     bool
}

func RunIndexCompact(args []string) int {
	opts := compactOptions{}
	fs := flag.NewFlagSet("nbss blk index-compact", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&opts.file, "file", "", "")
	if err := fs.Parse(args); err != nil {
		fmt.Fprintf(os.Stderr, "E_BAD_ARGS: %v\n", err)
		return compactExitBadArgs
	}
	if fs.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "E_BAD_ARGS: unexpected arguments")
		return compactExitBadArgs
	}
	if strings.TrimSpace(opts.file) == "" {
		fmt.Fprintln(os.Stderr, "E_BAD_ARGS: --file required")
		return compactExitBadArgs
	}

	in, err := os.Open(opts.file)
	if err != nil {
		fmt.Fprintf(os.Stderr, "E_BACKUP_OPEN: %v\n", err)
		return compactExitIO
	}
	defer in.Close()

	info, err := in.Stat()
	if err != nil {
		fmt.Fprintf(os.Stderr, "E_BACKUP_STAT: %v\n", err)
		return compactExitIO
	}
	if uint64(info.Size()) < GridStart {
		fmt.Fprintf(os.Stderr, "E_BACKUP_SIZE: %d bytes (need >= %d)\n", info.Size(), GridStart)
		return compactExitIO
	}

	headerBuf := make([]byte, HeaderBytes)
	if _, err := in.ReadAt(headerBuf, 0); err != nil && !errors.Is(err, io.EOF) {
		fmt.Fprintf(os.Stderr, "E_BACKUP_READ: %v\n", err)
		return compactExitIO
	}
	header, err := ParseHeader(headerBuf)
	if err != nil {
		fmt.Fprintf(os.Stderr, "E_BACKUP_NOT_NBSS: %v\n", err)
		return compactExitNotNBSS
	}

	records, err := CollectLatestIndexRecords(in)
	if err != nil {
		fmt.Fprintf(os.Stderr, "E_COMPACT_SCAN: %v\n", err)
		return compactExitIO
	}

	outPath := compactedPath(opts.file)
	out, err := os.OpenFile(outPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "E_OUT_OPEN: %v\n", err)
		return compactExitIO
	}
	defer out.Close()

	if _, err := out.Write(headerBuf); err != nil {
		fmt.Fprintf(os.Stderr, "E_OUT_WRITE: %v\n", err)
		return compactExitIO
	}

	entryCount, err := WriteLatestIndexRecords(out, records)
	if err != nil {
		fmt.Fprintf(os.Stderr, "E_OUT_INDEX: %v\n", err)
		return compactExitIO
	}
	if err := zeroFill(out, IndexStart+uint64(entryCount)*IndexEntrySize, GridStart); err != nil {
		fmt.Fprintf(os.Stderr, "E_OUT_ZERO: %v\n", err)
		return compactExitIO
	}

	if err := ValidateCompacted(outPath); err != nil {
		fmt.Fprintf(os.Stderr, "E_VALIDATE: %v\n", err)
		return compactExitIO
	}

	fmt.Printf("OK: compacted disk_id=0x%X entries=%d -> %s\n", header.DiskID, entryCount, outPath)
	return compactExitOK
}

func compactedPath(input string) string {
	dir := filepath.Dir(input)
	base := filepath.Base(input)
	const suffix = ".nbssIndex"
	if strings.HasSuffix(base, suffix) {
		base = strings.TrimSuffix(base, suffix) + ".compacted" + suffix
	} else {
		base = base + ".compacted"
	}
	return filepath.Join(dir, base)
}

func CollectLatestIndexRecords(r io.ReaderAt) ([]LatestIndexRecord, error) {
	recordMap := make(map[uint64]compactRecord)
	var seq uint64
	var pending *IndexEntry
	var pendingSeq uint64

	err := scanIndexStream(r, func(entry IndexEntry) error {
		seq++
		if pending != nil {
			if entry.JumpCode != 0 || entry.Size == 0 {
				pending = nil
				return nil
			}
			record := compactRecord{
				fid:      pending.FID,
				size:     entry.Size,
				jumpFID:  entry.FID,
				jumpCode: pending.JumpCode,
				seq:      pendingSeq,
				jump:     true,
			}
			if IsPackedAnchor(*pending) {
				record.jump = false
				record.packed = true
			}
			recordMap[pending.FID] = record
			pending = nil
			return nil
		}
		if IsJumpIndicator(entry) || IsPackedAnchor(entry) {
			pending = &entry
			pendingSeq = seq
			return nil
		}
		if IsTombstone(entry) {
			delete(recordMap, entry.FID)
			return nil
		}
		if entry.JumpCode != 0 {
			return fmt.Errorf("unexpected jumpcode for fid 0x%X", entry.FID)
		}
		recordMap[entry.FID] = compactRecord{
			fid:  entry.FID,
			size: entry.Size,
			seq:  seq,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	pending = nil

	records := make([]LatestIndexRecord, 0, len(recordMap))
	for _, entry := range recordMap {
		record := LatestIndexRecord{
			FID:      entry.fid,
			Size:     entry.size,
			JumpCode: entry.jumpCode,
			Seq:      entry.seq,
			Jump:     entry.jump,
			Packed:   entry.packed,
		}
		if entry.jump || entry.packed {
			record.ActualFID = entry.jumpFID
			record.ActualSize = entry.size
			if entry.jump {
				record.Size = entry.size - 1
			}
		} else {
			record.ActualFID = entry.fid
			record.ActualSize = entry.size
		}
		records = append(records, record)
	}
	sort.Slice(records, func(i, j int) bool { return records[i].Seq < records[j].Seq })
	var entryCount uint64
	for _, entry := range records {
		if entry.Jump || entry.Packed {
			entryCount += 2
		} else {
			entryCount++
		}
	}
	if entryCount*IndexEntrySize > IndexBytes {
		return nil, fmt.Errorf("compacted index too large: %d entries", entryCount)
	}
	return records, nil
}

func WriteLatestIndexRecords(w io.WriterAt, records []LatestIndexRecord) (int, error) {
	offset := int64(IndexStart)
	count := 0
	for _, record := range records {
		if record.Packed {
			entryPacked, err := BuildIndexEntry(record.FID, IndexPackedSentinel, PackedAnchorCode)
			if err != nil {
				return count, err
			}
			if _, err := w.WriteAt(entryPacked, offset); err != nil {
				return count, err
			}
			offset += IndexEntrySize
			count++
			entryReal, err := BuildIndexEntry(record.ActualFID, record.ActualSize, 0)
			if err != nil {
				return count, err
			}
			if _, err := w.WriteAt(entryReal, offset); err != nil {
				return count, err
			}
			offset += IndexEntrySize
			count++
			continue
		}
		if record.Jump {
			entryJump, err := BuildIndexEntry(record.FID, IndexJumpSentinel, record.JumpCode)
			if err != nil {
				return count, err
			}
			if _, err := w.WriteAt(entryJump, offset); err != nil {
				return count, err
			}
			offset += IndexEntrySize
			count++
			entryReal, err := BuildIndexEntry(record.ActualFID, record.ActualSize, 0)
			if err != nil {
				return count, err
			}
			if _, err := w.WriteAt(entryReal, offset); err != nil {
				return count, err
			}
			offset += IndexEntrySize
			count++
			continue
		}
		entry, err := BuildIndexEntry(record.FID, record.Size, 0)
		if err != nil {
			return count, err
		}
		if _, err := w.WriteAt(entry, offset); err != nil {
			return count, err
		}
		offset += IndexEntrySize
		count++
	}
	return count, nil
}

func zeroFill(w io.WriterAt, start uint64, end uint64) error {
	if start >= end {
		return nil
	}
	const chunkSize = 4 * 1024 * 1024
	zeroBuf := make([]byte, chunkSize)
	offset := start
	for offset < end {
		remaining := end - offset
		writeSize := int64(chunkSize)
		if remaining < chunkSize {
			writeSize = int64(remaining)
		}
		if _, err := w.WriteAt(zeroBuf[:writeSize], int64(offset)); err != nil {
			return err
		}
		offset += uint64(writeSize)
	}
	return nil
}

func ValidateCompacted(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	seen := make(map[uint64]bool)
	var pending *IndexEntry
	return scanIndexStream(f, func(entry IndexEntry) error {
		if pending != nil {
			if entry.JumpCode != 0 || entry.Size == 0 {
				return fmt.Errorf("invalid pair for fid 0x%X", pending.FID)
			}
			if seen[pending.FID] {
				return fmt.Errorf("duplicate fid 0x%X", pending.FID)
			}
			seen[pending.FID] = true
			pending = nil
			return nil
		}
		if IsJumpIndicator(entry) || IsPackedAnchor(entry) {
			pending = &entry
			return nil
		}
		if IsTombstone(entry) {
			return fmt.Errorf("zero size entry for fid 0x%X", entry.FID)
		}
		if entry.JumpCode != 0 {
			return fmt.Errorf("invalid jumpcode for fid 0x%X", entry.FID)
		}
		if seen[entry.FID] {
			return fmt.Errorf("duplicate fid 0x%X", entry.FID)
		}
		seen[entry.FID] = true
		return nil
	})
}

func scanIndexStream(r io.ReaderAt, onEntry func(entry IndexEntry) error) error {
	const chunkSize = 4 * 1024 * 1024
	buf := make([]byte, chunkSize)
	zeroRun := 0

	var offset uint64
	for offset < IndexBytes {
		remaining := IndexBytes - offset
		readSize := chunkSize
		if remaining < uint64(chunkSize) {
			readSize = int(remaining)
		}
		n, err := r.ReadAt(buf[:readSize], int64(IndexStart+offset))
		if err != nil && !errors.Is(err, io.EOF) {
			return err
		}
		if n == 0 {
			break
		}
		limit := n - (n % IndexEntrySize)
		for i := 0; i+IndexEntrySize <= limit; i += IndexEntrySize {
			entry := buf[i : i+IndexEntrySize]
			if isZeroEntry(entry) {
				return nil
			}
			parsed, err := ParseIndexEntry(entry)
			if err != nil {
				return err
			}
			if err := onEntry(parsed); err != nil {
				return err
			}
		}
		for i := 0; i < n; i++ {
			if buf[i] == 0x00 {
				zeroRun++
				if zeroRun >= IndexEntrySize {
					return nil
				}
			} else {
				zeroRun = 0
			}
		}
		offset += uint64(n)
	}
	return nil
}

func isZeroEntry(entry []byte) bool {
	for _, b := range entry {
		if b != 0x00 {
			return false
		}
	}
	return true
}
