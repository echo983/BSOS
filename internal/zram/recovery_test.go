package zram

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"bsos/internal/blk"
	"bsos/internal/pan"
)

func snapshotFixture(t *testing.T, dir string, id uint64) string {
	t.Helper()
	path := filepath.Join(dir, fmt.Sprintf("0x%X", id))
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err = f.Truncate(blk.GridStart + blk.SlotSize); err != nil {
		t.Fatal(err)
	}
	header := make([]byte, blk.HeaderBytes)
	copy(header, "NBSS")
	binary.LittleEndian.PutUint16(header[4:6], blk.FormatVersion)
	binary.LittleEndian.PutUint64(header[8:16], id)
	if _, err = f.WriteAt(header, 0); err != nil {
		t.Fatal(err)
	}
	return path
}

type fakeRecovery struct {
	live          []pan.Device
	allocs, loads int
	fail          bool
}

func (f *fakeRecovery) devices() ([]pan.Device, error) { return f.live, nil }
func (f *fakeRecovery) allocate() (string, error) {
	f.allocs++
	return fmt.Sprintf("/dev/zram%d", f.allocs+10), nil
}
func (f *fakeRecovery) restore(path string, s snapshot) error {
	f.loads++
	if f.fail {
		return errors.New("injected device failure")
	}
	f.live = append(f.live, pan.Device{DevicePath: path, DiskID: fmt.Sprintf("0x%X", s.id), SizeBytes: s.size})
	return nil
}
func TestRestorePoolColdStartAndRestart(t *testing.T) {
	dir := t.TempDir()
	snapshotFixture(t, dir, 42)
	k := &fakeRecovery{}
	ordinary := pan.Device{DevicePath: "/dev/test", DiskID: "1"}
	configured := []pan.Device{ordinary, {DevicePath: "/dev/zram0", DiskID: "42"}}
	devices, warnings, err := restorePool(configured, dir, k)
	if err != nil || len(warnings) != 0 || len(devices) != 2 || devices[0] != ordinary || devices[1].DevicePath != "/dev/zram11" || k.loads != 1 {
		t.Fatalf("cold restore: %v %v %v", devices, warnings, err)
	}
	_, warnings, err = restorePool(configured, dir, k)
	if err != nil || len(warnings) != 0 || k.loads != 1 {
		t.Fatalf("restart reloaded live data: loads=%d %v %v", k.loads, warnings, err)
	}
}
func TestRestoreDiscoversUnconfiguredSnapshot(t *testing.T) {
	dir := t.TempDir()
	snapshotFixture(t, dir, 9)
	k := &fakeRecovery{}
	devices, warnings, err := restorePool(nil, dir, k)
	if err != nil || len(warnings) != 0 || len(devices) != 1 || k.loads != 1 {
		t.Fatalf("discovery: %v %v %v", devices, warnings, err)
	}
}
func TestRestoreMissingAndFailedTier(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			dir := t.TempDir()
			if fail {
				snapshotFixture(t, dir, 9)
			}
			k := &fakeRecovery{fail: fail}
			devices, warnings, err := restorePool([]pan.Device{{DevicePath: "/dev/test", DiskID: "1"}, {DevicePath: "/dev/zram0", DiskID: "9"}}, dir, k)
			if err != nil || len(warnings) != 1 || len(devices) != 1 {
				t.Fatalf("partial pool: %v %v %v", devices, warnings, err)
			}
		})
	}
}
func TestBadSnapshotNeverAllocates(t *testing.T) {
	dir := t.TempDir()
	path := snapshotFixture(t, dir, 9)
	if err := os.Rename(path, filepath.Join(dir, "0xA")); err != nil {
		t.Fatal(err)
	}
	k := &fakeRecovery{}
	devices, warnings, err := restorePool(nil, dir, k)
	if err != nil || len(warnings) != 1 || len(devices) != 0 || k.allocs != 0 {
		t.Fatalf("bad header mutated devices: %v %v %v", devices, warnings, err)
	}
}
func TestCompressedSnapshotValidation(t *testing.T) {
	dir := t.TempDir()
	raw := snapshotFixture(t, dir, 9)
	const size = blk.GridStart + blk.SlotSize
	sum, err := hashDevice(raw, size)
	if err != nil {
		t.Fatal(err)
	}
	if err = snapshotDeviceCompressed(raw, raw+".zst", size); err != nil {
		t.Fatal(err)
	}
	if err = writeSnapshotMeta(raw+".sha256", size, sum); err != nil {
		t.Fatal(err)
	}
	if _, err = validateSnapshot(raw+".zst", 9); err != nil {
		t.Fatal(err)
	}
	paths, err := discoverSnapshots(dir)
	if err != nil || len(paths) != 1 || paths[9] != raw+".zst" {
		t.Fatalf("snapshot variants: %v %v", paths, err)
	}
	sum[0] ^= 1
	if err = writeSnapshotMeta(raw+".sha256", size, sum); err != nil {
		t.Fatal(err)
	}
	if _, err = validateSnapshot(raw+".zst", 9); err == nil {
		t.Fatal("checksum mismatch accepted")
	}
	if err = os.Remove(raw + ".sha256"); err != nil {
		t.Fatal(err)
	}
	if _, err = validateSnapshot(raw+".zst", 9); err == nil {
		t.Fatal("missing compressed metadata accepted")
	}
}
func TestRawSnapshotTruncation(t *testing.T) {
	dir := t.TempDir()
	path := snapshotFixture(t, dir, 9)
	sum, err := hashDevice(path, blk.GridStart+blk.SlotSize)
	if err != nil {
		t.Fatal(err)
	}
	if err = writeSnapshotMeta(path+".sha256", blk.GridStart+blk.SlotSize, sum); err != nil {
		t.Fatal(err)
	}
	if err = os.Truncate(path, blk.HeaderBytes); err != nil {
		t.Fatal(err)
	}
	if _, err = validateSnapshot(path, 9); err == nil {
		t.Fatal("truncated snapshot accepted")
	}
}
func TestPathAndSizeBoundaries(t *testing.T) {
	for _, path := range []string{"/dev/zram0", "/dev/zram12"} {
		if !IsZramDevicePath(path) {
			t.Fatal(path)
		}
	}
	for _, path := range []string{"/dev/zram", "/dev/zram-backup", "/dev/zram1x"} {
		if IsZramDevicePath(path) {
			t.Fatal(path)
		}
	}
	if _, err := parseSize(fmt.Sprintf("%dG", uint64(math.MaxUint64))); err == nil {
		t.Fatal("overflow accepted")
	}
}

func TestSnapshotCannotReplaceOrdinaryDisk(t *testing.T) {
	dir := t.TempDir()
	snapshotFixture(t, dir, 9)
	k := &fakeRecovery{}
	_, _, err := restorePool([]pan.Device{{DevicePath: "/dev/test", DiskID: "9"}}, dir, k)
	if err == nil || k.allocs != 0 {
		t.Fatalf("duplicate identity reached allocation: %v", err)
	}
}
func TestFlushRejectsStaleDeviceIdentity(t *testing.T) {
	dir := t.TempDir()
	path := snapshotFixture(t, dir, 9)
	err := flushDevice(pan.Device{DevicePath: path, DiskID: "10", SizeBytes: blk.GridStart + blk.SlotSize}, t.TempDir(), nil)
	if err == nil {
		t.Fatal("snapshot labelled with wrong disk identity")
	}
}

// Opt-in host gate. Allocates only empty zram devices, leaves occupied devices
// alone, and resets every device used by the test on exit. Requires a kernel
// exposing zram and permissions for its sysfs attributes and device nodes.
func TestRealZramRecovery(t *testing.T) {
	if os.Getenv("BSOS_REAL_ZRAM") != "1" {
		t.Skip("set BSOS_REAL_ZRAM=1 on a zram-capable test host")
	}
	k := kernelRecovery{}
	path, err := k.allocate()
	if err != nil {
		t.Fatalf("real zram gate unavailable: %v", err)
	}
	owned := map[string]bool{path: true}
	t.Cleanup(func() {
		for device := range owned {
			if err := resetZramDevice(device); err != nil {
				t.Errorf("reset test device %s: %v", device, err)
			}
		}
	})
	dir := t.TempDir()
	id := uint64(time.Now().UnixNano())
	raw := snapshotFixture(t, dir, id)
	snap, err := validateSnapshot(raw, id)
	if err != nil {
		t.Fatal(err)
	}
	if err = k.restore(path, snap); err != nil {
		t.Fatal(err)
	}
	if err = flushDevice(pan.Device{DevicePath: path, DiskID: fmt.Sprintf("0x%X", id), SizeBytes: snap.size}, dir, nil); err != nil {
		t.Fatal(err)
	}
	if err = os.Remove(raw); err != nil {
		t.Fatal(err)
	}
	if err = resetZramDevice(path); err != nil {
		t.Fatal(err)
	}
	devices, warnings, err := RestorePool([]pan.Device{{DevicePath: path, DiskID: fmt.Sprintf("0x%X", id)}}, dir)
	if err != nil || len(warnings) != 0 || len(devices) != 1 {
		t.Fatalf("real restore: %v %v %v", devices, warnings, err)
	}
	owned[devices[0].DevicePath] = true
	got, err := hashDevice(devices[0].DevicePath, snap.size)
	if err != nil {
		t.Fatal(err)
	}
	_, want, err := readSnapshotMeta(raw + ".sha256")
	if err != nil || got != want {
		t.Fatalf("real restored hash mismatch: %v", err)
	}
	t.Log("PASS: raw restore, compressed snapshot, reset, automatic discovery/reload, checksum readback")
}
