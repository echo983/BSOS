package blk

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/echo983/BSOS/internal/pan"
)

const (
	backupExitOK      = 0
	backupExitBadArgs = 2
	backupExitIO      = 3
	backupExitPerm    = 4
	backupExitNotNBSS = 5
)

type backupOptions struct {
	panPath string
	diskID  string
}

func RunIndexBackup(args []string) int {
	opts := backupOptions{}
	fs := flag.NewFlagSet("nbss blk index-backup", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&opts.panPath, "pan", "pan.json", "")
	fs.StringVar(&opts.diskID, "disk", "", "")
	if err := fs.Parse(args); err != nil {
		fmt.Fprintf(os.Stderr, "E_BAD_ARGS: %v\n", err)
		return backupExitBadArgs
	}
	if fs.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "E_BAD_ARGS: unexpected arguments")
		return backupExitBadArgs
	}

	panFile, err := pan.Read(opts.panPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			fmt.Fprintln(os.Stderr, "E_PAN_NOT_FOUND: pan.json not found; run `nbss blk find` first")
			return backupExitIO
		}
		fmt.Fprintf(os.Stderr, "E_PAN_READ_FAILED: %v\n", err)
		return backupExitIO
	}
	if len(panFile.Devices) == 0 {
		fmt.Fprintln(os.Stderr, "E_NO_PAN_DEVICES: no devices in pan.json")
		return backupExitIO
	}

	var devices []pan.Device
	if strings.TrimSpace(opts.diskID) != "" {
		dev, err := pan.SelectDevice(panFile, opts.diskID)
		if err != nil {
			fmt.Fprintf(os.Stderr, "E_BAD_DISK: %v\n", err)
			return backupExitBadArgs
		}
		devices = append(devices, dev)
	} else {
		devices = append(devices, panFile.Devices...)
	}

	outDir := "nbss_index_backup"
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "E_OUT_DIR: %v\n", err)
		return backupExitIO
	}

	for _, dev := range devices {
		if dev.Status != "" && dev.Status != "match" {
			fmt.Fprintf(os.Stderr, "SKIP: %s status=%s\n", dev.DevicePath, dev.Status)
			continue
		}
		if err := backupDeviceIndex(dev, outDir); err != nil {
			switch {
			case errors.Is(err, os.ErrPermission):
				fmt.Fprintf(os.Stderr, "E_PERMISSION: %v\n", err)
				return backupExitPerm
			case errors.Is(err, errNotNBSS):
				fmt.Fprintf(os.Stderr, "E_NOT_NBSS: %v\n", err)
				return backupExitNotNBSS
			default:
				fmt.Fprintf(os.Stderr, "E_BACKUP_FAILED: %v\n", err)
				return backupExitIO
			}
		}
	}

	return backupExitOK
}

var errNotNBSS = errors.New("not nbss")

func backupDeviceIndex(dev pan.Device, outDir string) error {
	timestamp := time.Now().UTC().Format("20060102_150405")
	name := fmt.Sprintf("index_%s_%s.nbssIndex", pan.NormalizeID(dev.DiskID), timestamp)
	outPath := filepath.Join(outDir, name)
	return BackupDeviceIndexToPathVerbose(dev, outPath, true)
}

func BackupDeviceIndexToPath(dev pan.Device, outPath string) error {
	return BackupDeviceIndexToPathVerbose(dev, outPath, false)
}

func BackupDeviceIndexToPathVerbose(dev pan.Device, outPath string, verbose bool) error {
	st, err := os.Stat(dev.DevicePath)
	if err != nil {
		return err
	}
	if IsBlockDevice(st.Mode()) && os.Geteuid() != 0 {
		return os.ErrPermission
	}

	f, err := os.Open(dev.DevicePath)
	if err != nil {
		return err
	}
	defer f.Close()

	st, err = f.Stat()
	if err != nil {
		return err
	}

	diskBytes, err := DeviceSizeBytes(f, st)
	if err != nil {
		return err
	}
	if diskBytes < GridStart {
		return fmt.Errorf("disk too small: %d bytes", diskBytes)
	}

	headerBuf := make([]byte, HeaderBytes)
	if _, err := f.ReadAt(headerBuf, 0); err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	header, err := ParseHeader(headerBuf)
	if err != nil {
		return fmt.Errorf("%w: %v", errNotNBSS, err)
	}
	if header.Version != FormatVersion {
		return fmt.Errorf("unsupported version %d", header.Version)
	}
	wantID, err := pan.ParseDiskID(dev.DiskID)
	if err != nil {
		return err
	}
	if header.DiskID != wantID {
		return fmt.Errorf("disk id mismatch: header=0x%X pan=%s", header.DiskID, dev.DiskID)
	}

	if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
		return err
	}
	out, err := os.OpenFile(outPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer out.Close()

	section := io.NewSectionReader(f, 0, GridStart)
	buf := make([]byte, 1024*1024)
	if _, err := io.CopyBuffer(out, section, buf); err != nil {
		return err
	}
	if verbose {
		fmt.Printf("OK: %s -> %s size=%d\n", dev.DevicePath, outPath, GridStart)
	}
	return nil
}
