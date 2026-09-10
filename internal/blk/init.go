package blk

import (
	"bufio"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

const (
	maxNoteBytes = HeaderBytes - 16
)

const (
	exitOK             = 0
	exitBadArgs        = 2
	exitIO             = 5
	exitDevBusy        = 6
	exitAlreadyNBSS    = 7
	exitIndexNotEmpty  = 8
	exitTooSmall       = 9
	exitNotBlockDevice = 10
	exitNoteTooLong    = 11
	exitBadID          = 12
	exitTooLarge       = 13
	exitAborted        = 14
	exitPermission     = 15
)

type initOptions struct {
	note   string
	id     string
	force  bool
	yes    bool
	dryRun bool
}

func RunInit(args []string) int {
	opts := initOptions{}
	fs := newInitFlagSet(&opts)
	if err := fs.Parse(args); err != nil {
		fmt.Fprintf(os.Stderr, "E_BAD_ARGS: %v\n", err)
		return exitBadArgs
	}

	devicePath := ""
	if fs.NArg() == 1 {
		devicePath = fs.Arg(0)
	} else if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		// Support `nbss blk init <device> --flag ...` by reparsing tail args as flags.
		opts = initOptions{}
		fs = newInitFlagSet(&opts)
		if err := fs.Parse(args[1:]); err != nil {
			fmt.Fprintf(os.Stderr, "E_BAD_ARGS: %v\n", err)
			return exitBadArgs
		}
		if fs.NArg() != 0 {
			fmt.Fprintln(os.Stderr, "E_BAD_ARGS: unexpected arguments")
			return exitBadArgs
		}
		devicePath = args[0]
	} else {
		fmt.Fprintln(os.Stderr, "E_BAD_ARGS: device path required")
		return exitBadArgs
	}
	if len(opts.note) > maxNoteBytes {
		fmt.Fprintf(os.Stderr, "E_NOTE_TOO_LONG: note exceeds %d bytes\n", maxNoteBytes)
		return exitNoteTooLong
	}

	st, err := os.Stat(devicePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "E_STAT_FAILED: %v\n", err)
		return exitIO
	}
	if IsBlockDevice(st.Mode()) && os.Geteuid() != 0 {
		fmt.Fprintln(os.Stderr, "E_PERMISSION: root privileges required for block device access")
		return exitPermission
	}

	f, err := os.OpenFile(devicePath, os.O_RDWR, 0)
	if err != nil {
		if errors.Is(err, syscall.EBUSY) {
			fmt.Fprintf(os.Stderr, "E_DEV_BUSY: %v\n", err)
			return exitDevBusy
		}
		fmt.Fprintf(os.Stderr, "E_OPEN_FAILED: %v\n", err)
		return exitIO
	}
	defer f.Close()

	st, err = f.Stat()
	if err != nil {
		fmt.Fprintf(os.Stderr, "E_STAT_FAILED: %v\n", err)
		return exitIO
	}

	if !IsBlockDevice(st.Mode()) && !st.Mode().IsRegular() {
		fmt.Fprintf(os.Stderr, "E_NOT_BLOCKDEV: %s\n", devicePath)
		return exitNotBlockDevice
	}

	diskBytes, err := deviceSizeBytes(f, st)
	if err != nil {
		fmt.Fprintf(os.Stderr, "E_SIZE_FAILED: %v\n", err)
		return exitIO
	}

	if diskBytes < GridStart+SlotSize {
		fmt.Fprintf(os.Stderr, "E_TOO_SMALL: disk bytes %d\n", diskBytes)
		return exitTooSmall
	}

	capacityGB := diskBytes / 1_000_000_000
	if capacityGB > 0xFFFF {
		fmt.Fprintf(os.Stderr, "E_TOO_LARGE: capacity %d GB exceeds 65535\n", capacityGB)
		return exitTooLarge
	}

	diskID, err := resolveDiskID(opts.id, diskBytes, devicePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "E_BAD_ID: %v\n", err)
		return exitBadID
	}

	if !opts.force {
		if err := checkExistingHeader(f); err != nil {
			fmt.Fprintf(os.Stderr, "E_ALREADY_NBSS: %v\n", err)
			return exitAlreadyNBSS
		}
		if err := checkIndexNotEmpty(f); err != nil {
			fmt.Fprintf(os.Stderr, "E_INDEX_NOT_EMPTY: %v\n", err)
			return exitIndexNotEmpty
		}
	}

	printLayout(devicePath, diskBytes, capacityGB, diskID, opts.note)
	if opts.dryRun {
		return exitOK
	}

	if !opts.yes {
		if !confirmInit(devicePath, diskBytes) {
			fmt.Fprintln(os.Stderr, "E_ABORTED: initialization cancelled")
			return exitAborted
		}
	}

	if err := zeroRegion(f, IndexStart, IndexBytes); err != nil {
		fmt.Fprintf(os.Stderr, "E_IO: zeroing index stream failed: %v\n", err)
		return exitIO
	}

	header := buildHeader(uint16(capacityGB), diskID, opts.note)
	if _, err := f.WriteAt(header, 0); err != nil {
		fmt.Fprintf(os.Stderr, "E_IO: header write failed: %v\n", err)
		return exitIO
	}

	fmt.Printf("OK: NBSS v%d initialized\n", FormatVersion)
	return exitOK
}

func newInitFlagSet(opts *initOptions) *flag.FlagSet {
	fs := flag.NewFlagSet("nbss blk init", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&opts.note, "note", "", "")
	fs.StringVar(&opts.id, "id", "auto", "")
	fs.BoolVar(&opts.force, "force", false, "")
	fs.BoolVar(&opts.yes, "yes", false, "")
	fs.BoolVar(&opts.dryRun, "dry-run", false, "")
	return fs
}

func resolveDiskID(idArg string, diskBytes uint64, devicePath string) (uint64, error) {
	if idArg == "" || strings.EqualFold(idArg, "auto") {
		return autoDiskID(diskBytes, devicePath)
	}
	parsed, err := strconv.ParseUint(strings.TrimSpace(idArg), 0, 64)
	if err != nil {
		return 0, err
	}
	return parsed, nil
}

func autoDiskID(diskBytes uint64, devicePath string) (uint64, error) {
	var salt [16]byte
	if _, err := rand.Read(salt[:]); err != nil {
		return 0, err
	}

	normPath := filepath.Clean(devicePath)
	data := make([]byte, 0, 8+len(normPath)+len(salt))
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, diskBytes)
	data = append(data, buf...)
	data = append(data, []byte(normPath)...)
	data = append(data, salt[:]...)
	checksum := sha256.Sum256(data)
	return binary.LittleEndian.Uint64(checksum[:8]), nil
}

func checkExistingHeader(f *os.File) error {
	header := make([]byte, HeaderBytes)
	if _, err := f.ReadAt(header, 0); err != nil && err != io.EOF {
		return err
	}
	if string(header[:4]) == "NBSS" {
		version := binary.LittleEndian.Uint16(header[4:6])
		if version == FormatVersion {
			return fmt.Errorf("existing header version %d", version)
		}
	}
	return nil
}

func checkIndexNotEmpty(f *os.File) error {
	buf := make([]byte, 4096)
	if _, err := f.ReadAt(buf, IndexStart); err != nil && err != io.EOF {
		return err
	}
	for _, b := range buf {
		if b != 0 {
			return fmt.Errorf("index contains non-zero data")
		}
	}
	return nil
}

func buildHeader(capacityGB uint16, diskID uint64, note string) []byte {
	header := make([]byte, HeaderBytes)
	copy(header[:4], []byte("NBSS"))
	binary.LittleEndian.PutUint16(header[4:6], FormatVersion)
	binary.LittleEndian.PutUint16(header[6:8], capacityGB)
	binary.LittleEndian.PutUint64(header[8:16], diskID)
	copy(header[16:], []byte(note))
	return header
}

func printLayout(devicePath string, diskBytes uint64, capacityGB uint64, diskID uint64, note string) {
	slots := uint64(0)
	if diskBytes > GridStart {
		slots = (diskBytes - GridStart) / SlotSize
	}
	fmt.Printf("Device: %s\n", devicePath)
	fmt.Printf("Size: %d bytes (%.2f GiB)\n", diskBytes, float64(diskBytes)/(1024*1024*1024))
	fmt.Printf("GRID_START: 0x%X\n", GridStart)
	fmt.Printf("SLOTS: %d\n", slots)
	fmt.Printf("Header: magic=NBSS version=%d capacity_gb=%d disk_id=%d note_bytes=%d\n", FormatVersion, capacityGB, diskID, len(note))
	fmt.Printf("Index Stream: [0x%X, 0x%X) (%d bytes)\n", IndexStart, IndexStart+IndexBytes, IndexBytes)
}

func confirmInit(devicePath string, diskBytes uint64) bool {
	fmt.Printf("\nAbout to initialize NBSS on %s\n", devicePath)
	fmt.Printf("Device size: %d bytes (%.2f GiB)\n", diskBytes, float64(diskBytes)/(1024*1024*1024))
	fmt.Printf("Will zero Index Stream: [0x%X, 0x%X) (%d bytes)\n", IndexStart, IndexStart+IndexBytes, IndexBytes)
	fmt.Println("WARNING: This will destroy all existing data on this device.")
	fmt.Print("Type 'yes' to continue: ")

	reader := bufio.NewReader(os.Stdin)
	line, _ := reader.ReadString('\n')
	line = strings.TrimSpace(line)
	return line == "yes"
}

func zeroRegion(f *os.File, start uint64, length uint64) error {
	const chunkSize = 4 * 1024 * 1024
	zeroBuf := make([]byte, chunkSize)

	written := uint64(0)
	for written < length {
		remaining := length - written
		writeSize := int64(chunkSize)
		if remaining < chunkSize {
			writeSize = int64(remaining)
		}
		if _, err := f.WriteAt(zeroBuf[:writeSize], int64(start+written)); err != nil {
			return err
		}
		written += uint64(writeSize)
	}
	return nil
}
