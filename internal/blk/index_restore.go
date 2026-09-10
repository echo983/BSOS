package blk

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
)

const (
	restoreExitOK      = 0
	restoreExitBadArgs = 2
	restoreExitIO      = 3
	restoreExitPerm    = 4
	restoreExitNotNBSS = 5
)

type restoreOptions struct {
	file  string
	force bool
	yes   bool
}

func RunIndexRestore(args []string) int {
	opts := restoreOptions{}
	fs := flag.NewFlagSet("nbss blk index-restore", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&opts.file, "file", "", "")
	fs.BoolVar(&opts.force, "force", false, "")
	fs.BoolVar(&opts.yes, "yes", false, "")
	if err := fs.Parse(args); err != nil {
		fmt.Fprintf(os.Stderr, "E_BAD_ARGS: %v\n", err)
		return restoreExitBadArgs
	}
	if fs.NArg() != 1 {
		fmt.Fprintln(os.Stderr, "E_BAD_ARGS: device path required")
		return restoreExitBadArgs
	}
	if strings.TrimSpace(opts.file) == "" {
		fmt.Fprintln(os.Stderr, "E_BAD_ARGS: --file required")
		return restoreExitBadArgs
	}

	devicePath := fs.Arg(0)

	backup, err := os.Open(opts.file)
	if err != nil {
		fmt.Fprintf(os.Stderr, "E_BACKUP_OPEN: %v\n", err)
		return restoreExitIO
	}
	defer backup.Close()

	backupInfo, err := backup.Stat()
	if err != nil {
		fmt.Fprintf(os.Stderr, "E_BACKUP_STAT: %v\n", err)
		return restoreExitIO
	}
	if uint64(backupInfo.Size()) < GridStart {
		fmt.Fprintf(os.Stderr, "E_BACKUP_SIZE: %d bytes (need >= %d)\n", backupInfo.Size(), GridStart)
		return restoreExitIO
	}

	backupHeader, err := readHeader(backup)
	if err != nil {
		fmt.Fprintf(os.Stderr, "E_BACKUP_NOT_NBSS: %v\n", err)
		return restoreExitNotNBSS
	}
	if backupHeader.Version != FormatVersion {
		fmt.Fprintf(os.Stderr, "E_BACKUP_VERSION: unsupported version %d\n", backupHeader.Version)
		return restoreExitNotNBSS
	}

	st, err := os.Stat(devicePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "E_STAT_FAILED: %v\n", err)
		return restoreExitIO
	}
	if IsBlockDevice(st.Mode()) && os.Geteuid() != 0 {
		fmt.Fprintln(os.Stderr, "E_PERMISSION: root privileges required for block device access")
		return restoreExitPerm
	}

	out, err := os.OpenFile(devicePath, os.O_RDWR, 0)
	if err != nil {
		fmt.Fprintf(os.Stderr, "E_OPEN_FAILED: %v\n", err)
		return restoreExitIO
	}
	defer out.Close()

	st, err = out.Stat()
	if err != nil {
		fmt.Fprintf(os.Stderr, "E_STAT_FAILED: %v\n", err)
		return restoreExitIO
	}
	diskBytes, err := DeviceSizeBytes(out, st)
	if err != nil {
		fmt.Fprintf(os.Stderr, "E_SIZE_FAILED: %v\n", err)
		return restoreExitIO
	}
	if diskBytes < GridStart {
		fmt.Fprintf(os.Stderr, "E_TOO_SMALL: disk bytes %d\n", diskBytes)
		return restoreExitIO
	}

	deviceHeader, err := readHeader(out)
	if err != nil {
		fmt.Fprintf(os.Stderr, "E_DEVICE_NOT_NBSS: %v\n", err)
		return restoreExitNotNBSS
	}
	if deviceHeader.Version != FormatVersion {
		fmt.Fprintf(os.Stderr, "E_DEVICE_VERSION: unsupported version %d\n", deviceHeader.Version)
		return restoreExitNotNBSS
	}

	backupID := backupHeader.DiskID
	deviceID := deviceHeader.DiskID
	if backupID != deviceID && !opts.force {
		fmt.Fprintf(os.Stderr, "E_DISK_ID_MISMATCH: backup=0x%X device=0x%X (use --force to override)\n", backupID, deviceID)
		return restoreExitBadArgs
	}

	if !opts.yes {
		if !confirmRestore(devicePath, opts.file, backupID, deviceID, opts.force) {
			fmt.Fprintln(os.Stderr, "E_ABORTED: confirmation required")
			return restoreExitBadArgs
		}
	}

	if _, err := out.Seek(0, io.SeekStart); err != nil {
		fmt.Fprintf(os.Stderr, "E_SEEK_FAILED: %v\n", err)
		return restoreExitIO
	}
	reader := io.NewSectionReader(backup, 0, GridStart)
	buf := make([]byte, 1024*1024)
	if _, err := io.CopyBuffer(out, reader, buf); err != nil {
		fmt.Fprintf(os.Stderr, "E_RESTORE_FAILED: %v\n", err)
		return restoreExitIO
	}

	fmt.Printf("OK: %s <= %s size=%d\n", devicePath, opts.file, GridStart)
	return restoreExitOK
}

func readHeader(f *os.File) (Header, error) {
	buf := make([]byte, HeaderBytes)
	if _, err := f.ReadAt(buf, 0); err != nil && !errors.Is(err, io.EOF) {
		return Header{}, err
	}
	return ParseHeader(buf)
}

func confirmRestore(devicePath, backupPath string, backupID, deviceID uint64, forced bool) bool {
	fmt.Printf("\nAbout to restore NBSS index on %s\n", devicePath)
	fmt.Printf("Backup file: %s\n", backupPath)
	fmt.Printf("Backup disk_id: 0x%X\n", backupID)
	fmt.Printf("Device disk_id: 0x%X\n", deviceID)
	fmt.Printf("Will overwrite [0x0, 0x%X) (%d bytes)\n", GridStart, GridStart)
	if backupID != deviceID {
		if forced {
			fmt.Println("WARNING: disk_id mismatch; --force is set.")
		} else {
			fmt.Println("WARNING: disk_id mismatch.")
		}
	}
	fmt.Println("WARNING: This will overwrite the header and Index Stream on this device.")
	fmt.Print("Type 'yes' to continue: ")

	reader := bufio.NewReader(os.Stdin)
	line, _ := reader.ReadString('\n')
	line = strings.TrimSpace(line)
	return line == "yes"
}
