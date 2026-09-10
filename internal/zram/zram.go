package zram

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"bsos/internal/blk"
	"bsos/internal/pan"
)

const (
	exitOK       = 0
	exitBadArgs  = 2
	exitIO       = 5
	exitPerm     = 15
	exitNotFound = 16
)

func parseSize(input string) (uint64, error) {
	trimmed := strings.TrimSpace(strings.ToUpper(input))
	if trimmed == "" {
		return 0, fmt.Errorf("empty size")
	}
	multiplier := uint64(1)
	switch {
	case strings.HasSuffix(trimmed, "GB"):
		multiplier = 1024 * 1024 * 1024
		trimmed = strings.TrimSuffix(trimmed, "GB")
	case strings.HasSuffix(trimmed, "G"):
		multiplier = 1024 * 1024 * 1024
		trimmed = strings.TrimSuffix(trimmed, "G")
	case strings.HasSuffix(trimmed, "MB"):
		multiplier = 1024 * 1024
		trimmed = strings.TrimSuffix(trimmed, "MB")
	case strings.HasSuffix(trimmed, "M"):
		multiplier = 1024 * 1024
		trimmed = strings.TrimSuffix(trimmed, "M")
	case strings.HasSuffix(trimmed, "KB"):
		multiplier = 1024
		trimmed = strings.TrimSuffix(trimmed, "KB")
	case strings.HasSuffix(trimmed, "K"):
		multiplier = 1024
		trimmed = strings.TrimSuffix(trimmed, "K")
	case strings.HasSuffix(trimmed, "B"):
		multiplier = 1
		trimmed = strings.TrimSuffix(trimmed, "B")
	}
	value, err := strconv.ParseUint(strings.TrimSpace(trimmed), 10, 64)
	if err != nil {
		return 0, err
	}
	return value * multiplier, nil
}

func isZramPath(path string) bool {
	base := filepath.Base(path)
	return strings.HasPrefix(base, "zram")
}

func IsZramDevicePath(path string) bool {
	return isZramPath(path)
}

func resolveZramDevice(spec string) (string, error) {
	if spec == "" {
		return "", nil
	}
	if !strings.HasPrefix(spec, "/dev/") {
		spec = "/dev/" + spec
	}
	if !isZramPath(spec) {
		return "", fmt.Errorf("not a zram device: %s", spec)
	}
	if _, err := os.Stat(spec); err != nil {
		return "", err
	}
	return spec, nil
}

func zramSysPath(devicePath, name string) string {
	base := filepath.Base(devicePath)
	return filepath.Join("/sys/block", base, name)
}

func readZramDiskSize(devicePath string) (uint64, error) {
	raw, err := os.ReadFile(zramSysPath(devicePath, "disksize"))
	if err != nil {
		return 0, err
	}
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" {
		return 0, nil
	}
	return strconv.ParseUint(trimmed, 10, 64)
}

func writeZramDiskSize(devicePath string, size uint64) error {
	path := zramSysPath(devicePath, "disksize")
	return writeSysfs(path, strconv.FormatUint(size, 10))
}

func resetZramDevice(devicePath string) error {
	path := zramSysPath(devicePath, "reset")
	return writeSysfs(path, "1")
}

func hotAddWritable() bool {
	path := "/sys/class/zram-control/hot_add"
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		return false
	}
	f.Close()
	return true
}

func listUnusedZramDevices() ([]string, error) {
	entries, err := os.ReadDir("/sys/block")
	if err != nil {
		return nil, err
	}
	var devices []string
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, "zram") {
			continue
		}
		devPath := filepath.Join("/dev", name)
		size, err := readZramDiskSize(devPath)
		if err != nil {
			continue
		}
		if size == 0 {
			devices = append(devices, devPath)
		}
	}
	sort.Strings(devices)
	return devices, nil
}

func writeSysfs(path, value string) error {
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := f.WriteString(value); err != nil {
		return err
	}
	return nil
}

func hashDevice(path string, size uint64) ([32]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return [32]byte{}, err
	}
	defer f.Close()

	hasher := sha256.New()
	reader := io.LimitReader(f, int64(size))
	buf := make([]byte, 1024*1024)
	if _, err := io.CopyBuffer(hasher, reader, buf); err != nil {
		return [32]byte{}, err
	}
	var sum [32]byte
	copy(sum[:], hasher.Sum(nil))
	return sum, nil
}

func readNBSSHeader(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	buf := make([]byte, blk.HeaderBytes)
	if _, err := io.ReadFull(f, buf); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			return fmt.Errorf("device too small")
		}
		return err
	}
	_, err = blk.ParseHeader(buf)
	return err
}

func EnsureLoaded(devicePath, diskID, snapshotDir string) (bool, error) {
	if !isZramPath(devicePath) {
		return false, fmt.Errorf("not a zram device: %s", devicePath)
	}
	if snapshotDir == "" {
		return false, fmt.Errorf("snapshot dir required")
	}
	if _, err := os.Stat(devicePath); err != nil {
		return false, err
	}

	wantID, err := pan.ParseDiskID(diskID)
	if err != nil {
		return false, err
	}

	headerOK := false
	if err := readNBSSHeader(devicePath); err == nil {
		f, err := os.Open(devicePath)
		if err == nil {
			defer f.Close()
			buf := make([]byte, blk.HeaderBytes)
			if _, err := io.ReadFull(f, buf); err == nil {
				if header, err := blk.ParseHeader(buf); err == nil && header.DiskID == wantID {
					headerOK = true
				}
			}
		}
	}
	if headerOK {
		return false, nil
	}

	snapshotPath := filepath.Join(snapshotDir, pan.NormalizeID(diskID))
	info, err := os.Stat(snapshotPath)
	if err == nil {
		if info.Size() <= 0 {
			return false, fmt.Errorf("snapshot empty")
		}
		if err := loadSnapshotToDevice(devicePath, snapshotPath, uint64(info.Size())); err != nil {
			return false, err
		}
	} else {
		if !errors.Is(err, os.ErrNotExist) {
			return false, err
		}
		zstPath := snapshotPath + ".zst"
		if _, zstErr := os.Stat(zstPath); zstErr != nil {
			return false, zstErr
		}
		size, _, metaErr := readSnapshotMeta(snapshotPath + ".sha256")
		if metaErr != nil || size == 0 {
			return false, fmt.Errorf("missing snapshot meta for %s", zstPath)
		}
		if err := loadSnapshotToDeviceCompressed(devicePath, zstPath, size); err != nil {
			return false, err
		}
	}
	if err := readNBSSHeader(devicePath); err != nil {
		return false, fmt.Errorf("snapshot not nbss: %v", err)
	}
	f, err := os.Open(devicePath)
	if err != nil {
		return false, err
	}
	defer f.Close()
	buf := make([]byte, blk.HeaderBytes)
	if _, err := io.ReadFull(f, buf); err != nil {
		return false, err
	}
	header, err := blk.ParseHeader(buf)
	if err != nil {
		return false, err
	}
	if header.DiskID != wantID {
		return false, fmt.Errorf("disk id mismatch after load: got 0x%X want 0x%X", header.DiskID, wantID)
	}
	return true, nil
}

func loadSnapshotToDevice(devicePath, snapshotPath string, size uint64) error {
	if err := resetZramDevice(devicePath); err != nil {
		return err
	}
	if err := writeZramDiskSize(devicePath, size); err != nil {
		return err
	}

	in, err := os.Open(snapshotPath)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.OpenFile(devicePath, os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	defer out.Close()

	buf := make([]byte, 1024*1024)
	if _, err := io.CopyBuffer(out, in, buf); err != nil {
		return err
	}
	return nil
}

func loadSnapshotToDeviceCompressed(devicePath, snapshotPath string, size uint64) error {
	if err := resetZramDevice(devicePath); err != nil {
		return err
	}
	return loadCompressed(devicePath, snapshotPath, size)
}
