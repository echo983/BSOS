package blk

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
)

const (
	infoExitOK       = 0
	infoExitBadArgs  = 2
	infoExitIO       = 3
	infoExitPerm     = 4
	infoExitNotNBSS  = 5
	infoExitTooSmall = 6
)

func RunInfo(args []string) int {
	fs := flag.NewFlagSet("nbss blk info", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	if err := fs.Parse(args); err != nil {
		fmt.Fprintf(os.Stderr, "E_BAD_ARGS: %v\n", err)
		return infoExitBadArgs
	}
	if fs.NArg() != 1 {
		fmt.Fprintln(os.Stderr, "E_BAD_ARGS: device path required")
		return infoExitBadArgs
	}

	devicePath := fs.Arg(0)
	st, err := os.Stat(devicePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "E_STAT_FAILED: %v\n", err)
		return infoExitIO
	}
	if IsBlockDevice(st.Mode()) && os.Geteuid() != 0 {
		fmt.Fprintln(os.Stderr, "E_PERMISSION: root privileges required for block device access")
		return infoExitPerm
	}

	f, err := os.Open(devicePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "E_OPEN_FAILED: %v\n", err)
		return infoExitIO
	}
	defer f.Close()

	st, err = f.Stat()
	if err != nil {
		fmt.Fprintf(os.Stderr, "E_STAT_FAILED: %v\n", err)
		return infoExitIO
	}

	diskBytes, err := DeviceSizeBytes(f, st)
	if err != nil {
		fmt.Fprintf(os.Stderr, "E_SIZE_FAILED: %v\n", err)
		return infoExitIO
	}
	if diskBytes < GridStart+SlotSize {
		fmt.Fprintf(os.Stderr, "E_TOO_SMALL: disk bytes %d\n", diskBytes)
		return infoExitTooSmall
	}

	buf := make([]byte, HeaderBytes)
	if _, err := f.ReadAt(buf, 0); err != nil && !errors.Is(err, io.EOF) {
		fmt.Fprintf(os.Stderr, "E_READ_FAILED: %v\n", err)
		return infoExitIO
	}
	header, err := ParseHeader(buf)
	if err != nil {
		fmt.Fprintf(os.Stderr, "E_NOT_NBSS: %v\n", err)
		return infoExitNotNBSS
	}

	slots := uint64(0)
	if diskBytes > GridStart {
		slots = (diskBytes - GridStart) / SlotSize
	}
	fmt.Printf("Device: %s\n", devicePath)
	fmt.Printf("Size: %d bytes (%.2f GiB)\n", diskBytes, float64(diskBytes)/(1024*1024*1024))
	fmt.Printf("GRID_START: 0x%X\n", GridStart)
	fmt.Printf("SLOTS: %d\n", slots)
	fmt.Printf("Header: magic=NBSS version=%d capacity_gb=%d disk_id=0x%X note_bytes=%d\n",
		header.Version, header.CapacityGB, header.DiskID, header.NoteLen)
	fmt.Printf("Index Stream: [0x%X, 0x%X) (%d bytes)\n", IndexStart, IndexStart+IndexBytes, IndexBytes)
	return infoExitOK
}
