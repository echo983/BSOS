package zram

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"bsos/internal/blk"
	"bsos/internal/pan"
)

// Snapshot metadata is checked before any zram device is allocated or reset.
type snapshot struct {
	path     string
	id, size uint64
}

func snapshotReader(path string) (io.ReadCloser, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	if !strings.HasSuffix(path, ".zst") {
		return f, nil
	}
	d, err := newZstdDecoder(f)
	if err != nil {
		f.Close()
		return nil, err
	}
	return &decodedSnapshot{Reader: d, close: func() error { d.Close(); return f.Close() }}, nil
}

type decodedSnapshot struct {
	io.Reader
	close func() error
}

func (d *decodedSnapshot) Close() error { return d.close() }
func validateSnapshot(path string, id uint64) (snapshot, error) {
	size, err := snapshotSize(path)
	if err != nil {
		return snapshot{}, err
	}
	if size <= blk.GridStart || size%blk.SlotSize != 0 {
		return snapshot{}, fmt.Errorf("invalid snapshot size %d", size)
	}
	r, err := snapshotReader(path)
	if err != nil {
		return snapshot{}, err
	}
	defer r.Close()
	header := make([]byte, blk.HeaderBytes)
	if _, err = io.ReadFull(r, header); err != nil {
		return snapshot{}, err
	}
	h, err := blk.ParseHeader(header)
	if err != nil {
		return snapshot{}, err
	}
	if h.Version != blk.FormatVersion || h.DiskID != id {
		return snapshot{}, fmt.Errorf("snapshot header identity/version mismatch")
	}
	hash := sha256.New()
	hash.Write(header)
	n, err := io.Copy(hash, io.LimitReader(r, int64(size)-blk.HeaderBytes+1))
	if err != nil {
		return snapshot{}, err
	}
	if uint64(n)+blk.HeaderBytes != size {
		return snapshot{}, fmt.Errorf("snapshot length mismatch")
	}
	base := strings.TrimSuffix(path, ".zst")
	metaSize, want, err := readSnapshotMeta(base + ".sha256")
	if err == nil {
		if metaSize != size || string(hash.Sum(nil)) != string(want[:]) {
			return snapshot{}, fmt.Errorf("snapshot checksum mismatch")
		}
	} else if !errors.Is(err, os.ErrNotExist) || strings.HasSuffix(path, ".zst") {
		return snapshot{}, err
	}
	return snapshot{path, id, size}, nil
}

func discoverSnapshots(dir string) (map[uint64]string, error) {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return map[uint64]string{}, nil
	}
	if err != nil {
		return nil, err
	}
	out := make(map[uint64]string)
	for _, e := range entries {
		if e.IsDir() || strings.HasSuffix(e.Name(), ".sha256") || strings.HasSuffix(e.Name(), ".tmp") {
			continue
		}
		id, err := pan.ParseDiskID(strings.TrimSuffix(e.Name(), ".zst"))
		if err != nil {
			continue
		}
		path := filepath.Join(dir, e.Name())
		if old, ok := out[id]; !ok || strings.HasSuffix(path, ".zst") && !strings.HasSuffix(old, ".zst") {
			out[id] = path
		}
	}
	return out, nil
}

// recoveryBackend separates host device operations from restart policy.
// Tests exercise identical policy against an isolated fake kernel.
type recoveryBackend interface {
	devices() ([]pan.Device, error)
	allocate() (string, error)
	restore(string, snapshot) error
}
type kernelRecovery struct{}

func (kernelRecovery) devices() ([]pan.Device, error) {
	names, err := listZramDevices()
	if err != nil {
		return nil, err
	}
	var devices []pan.Device
	for name := range names {
		path := filepath.Join("/dev", name)
		f, err := os.Open(path)
		if err != nil {
			continue
		}
		header := make([]byte, blk.HeaderBytes)
		_, err = io.ReadFull(f, header)
		f.Close()
		if err != nil {
			continue
		}
		h, err := blk.ParseHeader(header)
		if err != nil || h.Version != blk.FormatVersion {
			continue
		}
		size, err := readZramDiskSize(path)
		if err != nil {
			return nil, err
		}
		devices = append(devices, pan.Device{DevicePath: path, DiskID: fmt.Sprintf("0x%X", h.DiskID), Version: h.Version, SizeBytes: size, Status: "match"})
	}
	return devices, nil
}
func (kernelRecovery) allocate() (string, error) {
	a, err := newZramAllocator()
	if err != nil {
		return "", err
	}
	return a.next()
}
func (kernelRecovery) restore(path string, s snapshot) error {
	// Allocate only empty devices. Never reset an occupied stale pan.json path.
	size, err := readZramDiskSize(path)
	if err != nil {
		return err
	}
	if size != 0 {
		return fmt.Errorf("zram device already in use: %s", path)
	}
	if err = writeZramDiskSize(path, s.size); err != nil {
		return err
	}
	ok := false
	defer func() {
		if !ok {
			_ = resetZramDevice(path)
		}
	}()
	r, err := snapshotReader(s.path)
	if err != nil {
		return err
	}
	defer r.Close()
	out, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	defer out.Close()
	n, err := io.Copy(out, io.LimitReader(r, int64(s.size)+1))
	if err != nil {
		return err
	}
	if uint64(n) != s.size {
		return fmt.Errorf("snapshot changed during restore")
	}
	if err = out.Sync(); err != nil {
		return err
	}
	// Revalidate the snapshot and restored header before exposing the device.
	if _, err = validateSnapshot(s.path, s.id); err != nil {
		return err
	}
	got, err := hashDevice(path, s.size)
	if err != nil {
		return err
	}
	src, err := snapshotReader(s.path)
	if err != nil {
		return err
	}
	defer src.Close()
	hash := sha256.New()
	if _, err = io.Copy(hash, src); err != nil {
		return err
	}
	if string(got[:]) != string(hash.Sum(nil)) {
		return fmt.Errorf("restored snapshot checksum mismatch")
	}
	ok = true
	return nil
}

// RestorePool discovers snapshots by stable disk ID and resolves current zram
// paths, replacing obsolete pan.json paths in the returned runtime pool.
// Missing/broken tiers are reported explicitly; ordinary disks are preserved.
func RestorePool(devices []pan.Device, dir string) ([]pan.Device, []error, error) {
	if strings.HasPrefix(dir, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, nil, err
		}
		dir = filepath.Join(home, dir[2:])
	}
	return restorePool(devices, dir, kernelRecovery{})
}
func restorePool(configured []pan.Device, dir string, kernel recoveryBackend) ([]pan.Device, []error, error) {
	paths, err := discoverSnapshots(dir)
	if err != nil {
		return nil, nil, err
	}
	want := make(map[uint64]bool)
	seen := make(map[uint64]bool)
	ordinary := make(map[uint64]bool)
	var out []pan.Device
	for _, d := range configured {
		id, err := pan.ParseDiskID(d.DiskID)
		if err != nil {
			return nil, nil, err
		}
		if seen[id] {
			return nil, nil, fmt.Errorf("duplicate configured disk id %d", id)
		}
		seen[id] = true
		if !IsZramDevicePath(d.DevicePath) {
			ordinary[id] = true
			out = append(out, d)
			continue
		}
		want[id] = true
	}
	for id := range paths {
		if ordinary[id] {
			return nil, nil, fmt.Errorf("snapshot disk id %d conflicts with an ordinary disk", id)
		}
		want[id] = true
	}
	if len(want) == 0 {
		return out, nil, nil
	}
	live, err := kernel.devices()
	if err != nil {
		return nil, nil, err
	}
	byID := make(map[uint64]pan.Device)
	for _, d := range live {
		id, err := pan.ParseDiskID(d.DiskID)
		if err != nil {
			return nil, nil, err
		}
		if _, ok := byID[id]; ok {
			return nil, nil, fmt.Errorf("duplicate live zram disk id %d", id)
		}
		byID[id] = d
	}
	var ids []uint64
	for id := range want {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	var warnings []error
	for _, id := range ids {
		if d, ok := byID[id]; ok {
			out = append(out, d)
			continue
		}
		path, ok := paths[id]
		if !ok {
			warnings = append(warnings, fmt.Errorf("zram disk %d unavailable: snapshot missing", id))
			continue
		}
		snap, err := validateSnapshot(path, id)
		if err != nil {
			warnings = append(warnings, fmt.Errorf("zram disk %d snapshot: %w", id, err))
			continue
		}
		device, err := kernel.allocate()
		if err == nil {
			err = kernel.restore(device, snap)
		}
		if err != nil {
			warnings = append(warnings, fmt.Errorf("zram disk %d restore: %w", id, err))
			continue
		}
		out = append(out, pan.Device{DevicePath: device, DiskID: fmt.Sprintf("0x%X", id), Version: blk.FormatVersion, SizeBytes: snap.size, Status: "match"})
	}
	return out, warnings, nil
}
