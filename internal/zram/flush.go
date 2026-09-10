package zram

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"bsos/internal/blk"
	"bsos/internal/pan"
)

type flushOptions struct {
	panPath string
	outDir  string
}

func RunFlush(args []string) int {
	opts := flushOptions{}
	fs := flag.NewFlagSet("bsos zram flush", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&opts.panPath, "pan", "pan.json", "")
	fs.StringVar(&opts.outDir, "out", "", "")
	if err := fs.Parse(args); err != nil {
		fmt.Fprintf(os.Stderr, "E_BAD_ARGS: %v\n", err)
		return exitBadArgs
	}
	if fs.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "E_BAD_ARGS: unexpected arguments")
		return exitBadArgs
	}

	if os.Geteuid() != 0 {
		fmt.Fprintln(os.Stderr, "E_PERMISSION: root privileges required for zram access")
		return exitPerm
	}

	panFile, err := pan.Read(opts.panPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			fmt.Fprintln(os.Stderr, "E_PAN_NOT_FOUND: pan.json not found; run `bsos blk find` first")
			return exitNotFound
		}
		fmt.Fprintf(os.Stderr, "E_PAN_READ_FAILED: %v\n", err)
		return exitIO
	}
	if len(panFile.Devices) == 0 {
		fmt.Fprintln(os.Stderr, "E_NO_PAN_DEVICES: no devices in pan.json")
		return exitNotFound
	}

	err = FlushDevices(panFile.Devices, strings.TrimSpace(opts.outDir),
		func(format string, args ...any) { fmt.Printf(format+"\n", args...) },
		func(format string, args ...any) { fmt.Fprintf(os.Stderr, format+"\n", args...) },
	)
	if err != nil {
		return exitIO
	}
	return exitOK
}

type Logf func(format string, args ...any)

func FlushDevices(devices []pan.Device, outDir string, logf Logf, errf Logf) error {
	if strings.TrimSpace(outDir) == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return err
		}
		outDir = filepath.Join(home, ".bsos_zram_snapshots")
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}

	var firstErr error
	for _, dev := range devices {
		if !strings.HasPrefix(dev.DevicePath, "/dev/zram") {
			if logf != nil {
				logf("SKIP: %s not zram", dev.DevicePath)
			}
			continue
		}
		if dev.Status != "" && dev.Status != "match" {
			if logf != nil {
				logf("SKIP: %s status=%s", dev.DevicePath, dev.Status)
			}
			continue
		}
		if dev.SizeBytes == 0 {
			if logf != nil {
				logf("SKIP: %s size=0", dev.DevicePath)
			}
			continue
		}
		if err := readNBSSHeader(dev.DevicePath); err != nil {
			if logf != nil {
				logf("SKIP: %s not nbss (%v)", dev.DevicePath, err)
			}
			continue
		}
		if err := flushDevice(dev, outDir, logf); err != nil {
			if errf != nil {
				errf("E_FLUSH_FAILED: %s %v", dev.DevicePath, err)
			}
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

func flushDevice(dev pan.Device, outDir string, logf Logf) error {
	devicePath := dev.DevicePath
	size := dev.SizeBytes
	diskID := pan.NormalizeID(dev.DiskID)
	if diskID == "" {
		return fmt.Errorf("missing disk id")
	}
	f, err := os.Open(devicePath)
	if err != nil {
		return err
	}
	header := make([]byte, blk.HeaderBytes)
	_, err = io.ReadFull(f, header)
	f.Close()
	if err != nil {
		return err
	}
	h, err := blk.ParseHeader(header)
	if err != nil {
		return err
	}
	wantID, err := pan.ParseDiskID(dev.DiskID)
	if err != nil {
		return err
	}
	if h.DiskID != wantID || h.Version != blk.FormatVersion {
		return fmt.Errorf("snapshot device identity/version does not match pan.json")
	}

	sumPath := filepath.Join(outDir, diskID+".sha256")
	prevSize, prevHash, _ := readSnapshotMeta(sumPath)

	hash, err := hashDevice(devicePath, size)
	if err != nil {
		return err
	}
	if prevSize == size && prevHash == hash {
		if logf != nil {
			logf("SKIP: %s unchanged", devicePath)
		}
		return nil
	}

	outPath := filepath.Join(outDir, diskID+".zst")
	tmpPath := outPath + ".tmp"
	if err := snapshotDeviceCompressed(devicePath, tmpPath, size); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, outPath); err != nil {
		return err
	}
	if err := writeSnapshotMeta(sumPath, size, hash); err != nil {
		return err
	}
	if logf != nil {
		logf("OK: %s -> %s size=%d", devicePath, outPath, size)
	}
	return nil
}

func snapshotDeviceCompressed(path, outPath string, size uint64) error {
	in, err := os.Open(path)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.OpenFile(outPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	encoder, err := newZstdEncoder(out)
	if err != nil {
		out.Close()
		return err
	}

	reader := io.LimitReader(in, int64(size))
	buf := make([]byte, 1024*1024)
	if _, err := io.CopyBuffer(encoder, reader, buf); err != nil {
		encoder.Close()
		out.Close()
		return err
	}
	if err := encoder.Close(); err != nil {
		out.Close()
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	return validateZstd(outPath)
}

func validateZstd(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	decoder, err := newZstdDecoder(f)
	if err != nil {
		return err
	}
	defer decoder.Close()

	buf := make([]byte, 1024*1024)
	_, err = io.CopyBuffer(io.Discard, decoder, buf)
	return err
}

func readSnapshotMeta(path string) (uint64, [32]byte, error) {
	var zero [32]byte
	f, err := os.Open(path)
	if err != nil {
		return 0, zero, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	if !scanner.Scan() {
		return 0, zero, errors.New("empty meta")
	}
	fields := strings.Fields(scanner.Text())
	if len(fields) != 2 {
		return 0, zero, errors.New("bad meta")
	}
	size, err := strconv.ParseUint(fields[0], 10, 64)
	if err != nil {
		return 0, zero, err
	}
	decoded, err := hex.DecodeString(fields[1])
	if err != nil || len(decoded) != sha256.Size {
		return 0, zero, errors.New("bad hash")
	}
	var hash [32]byte
	copy(hash[:], decoded)
	return size, hash, nil
}

func writeSnapshotMeta(path string, size uint64, hash [32]byte) error {
	line := fmt.Sprintf("%d %s\n", size, hex.EncodeToString(hash[:]))
	return os.WriteFile(path, []byte(line), 0o644)
}
