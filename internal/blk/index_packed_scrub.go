package blk

import (
	"bufio"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"bsos/internal/pan"
)

const (
	packedScrubExitOK      = 0
	packedScrubExitBadArgs = 2
	packedScrubExitIO      = 3
	packedScrubExitPerm    = 4
	packedScrubExitNotNBSS = 5

	packedTableMagic      = "NBPT"
	packedTableVersion    = 1
	packedTableHeaderSize = 48
	packedTableEntrySize  = 24
)

type packedScrubOptions struct {
	panPath string
	diskID  string
	fix     bool
	yes     bool
}

type PackedAnchorIssue struct {
	TableFID   uint64
	TableSize  uint64
	Reason     string
	Addr       uint64
	HeaderHex  string
	ZeroFilled bool
}

func RunPackedScrub(args []string) int {
	opts := packedScrubOptions{}
	fs := flag.NewFlagSet("nbss blk packed-scrub", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&opts.panPath, "pan", "pan.json", "")
	fs.StringVar(&opts.diskID, "disk", "", "")
	fs.BoolVar(&opts.fix, "fix", false, "")
	fs.BoolVar(&opts.yes, "yes", false, "")
	if err := fs.Parse(args); err != nil {
		fmt.Fprintf(os.Stderr, "E_BAD_ARGS: %v\n", err)
		return packedScrubExitBadArgs
	}
	if fs.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "E_BAD_ARGS: unexpected arguments")
		return packedScrubExitBadArgs
	}

	panFile, err := pan.Read(opts.panPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			fmt.Fprintln(os.Stderr, "E_PAN_NOT_FOUND: pan.json not found; run `nbss blk find` first")
			return packedScrubExitIO
		}
		fmt.Fprintf(os.Stderr, "E_PAN_READ_FAILED: %v\n", err)
		return packedScrubExitIO
	}
	if len(panFile.Devices) == 0 {
		fmt.Fprintln(os.Stderr, "E_NO_PAN_DEVICES: no devices in pan.json")
		return packedScrubExitIO
	}

	var devices []pan.Device
	if strings.TrimSpace(opts.diskID) != "" {
		dev, err := pan.SelectDevice(panFile, opts.diskID)
		if err != nil {
			fmt.Fprintf(os.Stderr, "E_BAD_DISK: %v\n", err)
			return packedScrubExitBadArgs
		}
		devices = append(devices, dev)
	} else {
		devices = append(devices, panFile.Devices...)
	}

	exitCode := packedScrubExitOK
	for _, dev := range devices {
		if dev.Status != "" && dev.Status != "match" {
			fmt.Fprintf(os.Stderr, "SKIP: %s status=%s\n", dev.DevicePath, dev.Status)
			continue
		}
		issues, err := inspectPackedAnchorsOnDevice(dev)
		if err != nil {
			switch {
			case errors.Is(err, os.ErrPermission):
				fmt.Fprintf(os.Stderr, "E_PERMISSION: %v\n", err)
				exitCode = packedScrubExitPerm
			case errors.Is(err, errNotNBSS):
				fmt.Fprintf(os.Stderr, "E_NOT_NBSS: %v\n", err)
				exitCode = packedScrubExitNotNBSS
			default:
				fmt.Fprintf(os.Stderr, "E_SCRUB_FAILED: %v\n", err)
				exitCode = packedScrubExitIO
			}
			continue
		}
		printPackedScrubReport(dev, issues)
		if !opts.fix || len(issues) == 0 {
			continue
		}
		if !opts.yes && !confirmPackedScrub(dev, len(issues)) {
			fmt.Fprintln(os.Stderr, "E_ABORTED: confirmation required")
			return packedScrubExitBadArgs
		}
		if err := fixPackedAnchorsOnDevice(dev, issues); err != nil {
			switch {
			case errors.Is(err, os.ErrPermission):
				fmt.Fprintf(os.Stderr, "E_PERMISSION: %v\n", err)
				exitCode = packedScrubExitPerm
			case errors.Is(err, errNotNBSS):
				fmt.Fprintf(os.Stderr, "E_NOT_NBSS: %v\n", err)
				exitCode = packedScrubExitNotNBSS
			default:
				fmt.Fprintf(os.Stderr, "E_FIX_FAILED: %v\n", err)
				exitCode = packedScrubExitIO
			}
			continue
		}
		fmt.Printf("OK: scrubbed disk_id=%s removed_bad_packed_anchors=%d\n", dev.DiskID, len(issues))
	}
	return exitCode
}

func printPackedScrubReport(dev pan.Device, issues []PackedAnchorIssue) {
	fmt.Printf("Disk: %s id=%s bad_packed_anchors=%d\n", dev.DevicePath, dev.DiskID, len(issues))
	for _, issue := range issues {
		fmt.Printf("  table_fid=0x%X size=%d addr=0x%X reason=%s zero=%t header=%s\n",
			issue.TableFID, issue.TableSize, issue.Addr, issue.Reason, issue.ZeroFilled, issue.HeaderHex)
	}
}

func confirmPackedScrub(dev pan.Device, count int) bool {
	fmt.Printf("\nAbout to scrub bad packed anchors on %s\n", dev.DevicePath)
	fmt.Printf("Disk ID: %s\n", dev.DiskID)
	fmt.Printf("Invalid packed anchors to remove: %d\n", count)
	fmt.Println("A backup of the current index will be written before rewrite.")
	fmt.Print("Type 'yes' to continue: ")
	reader := bufio.NewReader(os.Stdin)
	line, _ := reader.ReadString('\n')
	return strings.TrimSpace(line) == "yes"
}

func inspectPackedAnchorsOnDevice(dev pan.Device) ([]PackedAnchorIssue, error) {
	f, diskBytes, _, err := openNBSSDevice(dev, os.O_RDONLY)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	records, err := CollectLatestIndexRecords(f)
	if err != nil {
		return nil, err
	}
	return findPackedAnchorIssues(f, diskBytes, records)
}

func fixPackedAnchorsOnDevice(dev pan.Device, issues []PackedAnchorIssue) error {
	if len(issues) == 0 {
		return nil
	}
	f, _, headerBuf, err := openNBSSDevice(dev, os.O_RDWR)
	if err != nil {
		return err
	}
	defer f.Close()

	records, err := CollectLatestIndexRecords(f)
	if err != nil {
		return err
	}
	bad := make(map[uint64]struct{}, len(issues))
	for _, issue := range issues {
		bad[issue.TableFID] = struct{}{}
	}
	filtered := make([]LatestIndexRecord, 0, len(records))
	for _, record := range records {
		if record.Packed {
			if _, ok := bad[record.FID]; ok {
				continue
			}
		}
		filtered = append(filtered, record)
	}

	backupDir := "nbss_index_backup"
	if err := os.MkdirAll(backupDir, 0o755); err != nil {
		return err
	}
	timestamp := time.Now().UTC().Format("20060102_150405")
	backupPath := filepath.Join(backupDir, fmt.Sprintf("index_%s_%s.pre-scrub.nbssIndex", pan.NormalizeID(dev.DiskID), timestamp))
	if err := BackupDeviceIndexToPathVerbose(dev, backupPath, true); err != nil {
		return err
	}

	tmp, err := os.CreateTemp("", "nbss-packed-scrub-*.nbssIndex")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	defer tmp.Close()

	if _, err := tmp.WriteAt(headerBuf, 0); err != nil {
		return err
	}
	entryCount, err := WriteLatestIndexRecords(tmp, filtered)
	if err != nil {
		return err
	}
	if err := zeroFill(tmp, IndexStart+uint64(entryCount)*IndexEntrySize, GridStart); err != nil {
		return err
	}
	if err := ValidateCompacted(tmpPath); err != nil {
		return err
	}

	reader := io.NewSectionReader(tmp, 0, GridStart)
	buf := make([]byte, 1024*1024)
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if _, err := io.CopyBuffer(f, reader, buf); err != nil {
		return err
	}
	return nil
}

func openNBSSDevice(dev pan.Device, flags int) (*os.File, uint64, []byte, error) {
	st, err := os.Stat(dev.DevicePath)
	if err != nil {
		return nil, 0, nil, err
	}
	if IsBlockDevice(st.Mode()) && os.Geteuid() != 0 {
		return nil, 0, nil, os.ErrPermission
	}
	f, err := os.OpenFile(dev.DevicePath, flags, 0)
	if err != nil {
		return nil, 0, nil, err
	}
	st, err = f.Stat()
	if err != nil {
		f.Close()
		return nil, 0, nil, err
	}
	diskBytes, err := DeviceSizeBytes(f, st)
	if err != nil {
		f.Close()
		return nil, 0, nil, err
	}
	if diskBytes < GridStart {
		f.Close()
		return nil, 0, nil, fmt.Errorf("disk too small: %d bytes", diskBytes)
	}
	headerBuf := make([]byte, HeaderBytes)
	if _, err := f.ReadAt(headerBuf, 0); err != nil && !errors.Is(err, io.EOF) {
		f.Close()
		return nil, 0, nil, err
	}
	header, err := ParseHeader(headerBuf)
	if err != nil {
		f.Close()
		return nil, 0, nil, fmt.Errorf("%w: %v", errNotNBSS, err)
	}
	if header.Version != FormatVersion {
		f.Close()
		return nil, 0, nil, fmt.Errorf("unsupported version %d", header.Version)
	}
	wantID, err := pan.ParseDiskID(dev.DiskID)
	if err != nil {
		f.Close()
		return nil, 0, nil, err
	}
	if header.DiskID != wantID {
		f.Close()
		return nil, 0, nil, fmt.Errorf("disk id mismatch: header=0x%X pan=%s", header.DiskID, dev.DiskID)
	}
	return f, diskBytes, headerBuf, nil
}

func findPackedAnchorIssues(r io.ReaderAt, diskBytes uint64, records []LatestIndexRecord) ([]PackedAnchorIssue, error) {
	issues := make([]PackedAnchorIssue, 0)
	for _, record := range records {
		if !record.Packed {
			continue
		}
		addr, _, _, err := AddrForFID(diskBytes, record.ActualFID, record.ActualSize)
		if err != nil {
			issues = append(issues, PackedAnchorIssue{
				TableFID:  record.FID,
				TableSize: record.ActualSize,
				Reason:    err.Error(),
			})
			continue
		}
		header := make([]byte, minInt(int(record.ActualSize), 64))
		if len(header) == 0 {
			issues = append(issues, PackedAnchorIssue{
				TableFID:  record.FID,
				TableSize: record.ActualSize,
				Addr:      addr,
				Reason:    "zero-size packed table",
			})
			continue
		}
		n, err := r.ReadAt(header, int64(addr))
		if err != nil && !errors.Is(err, io.EOF) {
			issues = append(issues, PackedAnchorIssue{
				TableFID:  record.FID,
				TableSize: record.ActualSize,
				Addr:      addr,
				Reason:    err.Error(),
			})
			continue
		}
		header = header[:n]
		zero := true
		for _, b := range header {
			if b != 0 {
				zero = false
				break
			}
		}
		if err := validatePackedTableObject(r, diskBytes, record.ActualFID, record.ActualSize); err != nil {
			issues = append(issues, PackedAnchorIssue{
				TableFID:   record.FID,
				TableSize:  record.ActualSize,
				Addr:       addr,
				Reason:     err.Error(),
				HeaderHex:  fmt.Sprintf("%x", header[:minInt(len(header), 16)]),
				ZeroFilled: zero,
			})
		}
	}
	sort.Slice(issues, func(i, j int) bool { return issues[i].TableFID < issues[j].TableFID })
	return issues, nil
}

func validatePackedTableObject(r io.ReaderAt, diskBytes uint64, fid uint64, size uint64) error {
	if size < packedTableHeaderSize {
		return fmt.Errorf("packed table too small: %d", size)
	}
	addr, _, _, err := AddrForFID(diskBytes, fid, size)
	if err != nil {
		return err
	}
	header := make([]byte, packedTableHeaderSize)
	if _, err := r.ReadAt(header, int64(addr)); err != nil {
		return err
	}
	if string(header[0:4]) != packedTableMagic {
		return fmt.Errorf("bad magic")
	}
	if binary.LittleEndian.Uint16(header[4:6]) != packedTableVersion {
		return fmt.Errorf("bad version %d", binary.LittleEndian.Uint16(header[4:6]))
	}
	if binary.LittleEndian.Uint16(header[6:8]) != packedTableHeaderSize {
		return fmt.Errorf("bad header size %d", binary.LittleEndian.Uint16(header[6:8]))
	}
	containerSize := binary.LittleEndian.Uint64(header[24:32])
	entryCount := binary.LittleEndian.Uint64(header[32:40])
	if containerSize == 0 {
		return fmt.Errorf("zero container size")
	}
	wantSize := packedTableHeaderSize + entryCount*packedTableEntrySize
	if size != wantSize {
		return fmt.Errorf("size mismatch: want %d got %d", wantSize, size)
	}
	if entryCount == 0 {
		return nil
	}
	blob := make([]byte, int(size))
	if _, err := r.ReadAt(blob, int64(addr)); err != nil {
		return err
	}
	offset := int(packedTableHeaderSize)
	for i := uint64(0); i < entryCount; i++ {
		entryOffset := binary.LittleEndian.Uint64(blob[offset+8 : offset+16])
		entrySize := binary.LittleEndian.Uint64(blob[offset+16 : offset+24])
		if entrySize == 0 {
			return fmt.Errorf("zero-size entry")
		}
		if entryOffset+entrySize > containerSize {
			return fmt.Errorf("entry out of range")
		}
		offset += int(packedTableEntrySize)
	}
	return nil
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
