package zram

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math"
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
	if value > math.MaxUint64/multiplier {
		return 0, fmt.Errorf("size overflow")
	}
	return value * multiplier, nil
}

func isZramPath(path string) bool {
	base := filepath.Base(path)
	if !strings.HasPrefix(base, "zram") || len(base) == 4 {
		return false
	}
	for _, c := range base[4:] {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
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

func hotAddAvailable() bool {
	path := "/sys/class/zram-control/hot_add"
	f, err := os.OpenFile(path, os.O_RDONLY, 0)
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
	if n, err := io.CopyBuffer(hasher, reader, buf); err != nil {
		return [32]byte{}, err
	} else if uint64(n) != size {
		return [32]byte{}, io.ErrUnexpectedEOF
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

// EnsureLoaded restores only into an empty device; a mismatching live device
// belongs to another disk and must never be reset based on a stale path.
func EnsureLoaded(devicePath, diskID, snapshotDir string) (bool, error) {
	if !IsZramDevicePath(devicePath) {
		return false, fmt.Errorf("not a zram device: %s", devicePath)
	}
	id, err := pan.ParseDiskID(diskID)
	if err != nil {
		return false, err
	}
	live, err := (kernelRecovery{}).devices()
	if err != nil {
		return false, err
	}
	for _, d := range live {
		if d.DevicePath == devicePath {
			liveID, _ := pan.ParseDiskID(d.DiskID)
			if liveID == id {
				return false, nil
			}
			return false, fmt.Errorf("zram device belongs to another disk")
		}
	}
	paths, err := discoverSnapshots(snapshotDir)
	if err != nil {
		return false, err
	}
	path, ok := paths[id]
	if !ok {
		return false, fmt.Errorf("snapshot missing for disk %d", id)
	}
	snap, err := validateSnapshot(path, id)
	if err != nil {
		return false, err
	}
	if err = (kernelRecovery{}).restore(devicePath, snap); err != nil {
		return false, err
	}
	return true, nil
}
