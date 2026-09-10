package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"testing"

	"bsos/internal/blk"
	"bsos/internal/daemon/bsospb"
	"bsos/internal/pan"
)

func routeDisk(id, slots uint64, path string) *DeviceState {
	return &DeviceState{diskID: id, diskBytes: blk.GridStart + slots*blk.SlotSize, devicePath: path}
}
func TestRoutingBoundaries(t *testing.T) {
	small, large, z := routeDisk(1, 16, "/dev/test1"), routeDisk(2, 32, "/dev/test2"), routeDisk(3, 8, "/dev/zram0")
	s := &Server{disks: []*DeviceState{large, small, z}, smallFileBytes: 2 * blk.SlotSize, chdTargetP: .2}
	for _, tc := range []struct {
		size uint64
		want *DeviceState
	}{{1, z}, {2 * blk.SlotSize, small}, {17 * blk.SlotSize, large}, {33 * blk.SlotSize, nil}, {0, nil}} {
		got, err := s.selectDiskForWrite(tc.size)
		if got != tc.want || (tc.want == nil) != (err != nil) {
			t.Fatalf("size %d: got %v err %v", tc.size, got, err)
		}
	}
	z.intervals = []interval{{0, 7}}
	if got, _ := s.selectDiskForWrite(1); got != small {
		t.Fatal("full zram did not fall back")
	}
	small.intervals = []interval{{0, 15}}
	large.intervals = []interval{{0, 31}}
	// No confidence fit: selection retains NBSS's largest-CHD fallback,
	// and PrepareWrite remains responsible for actual extent conflict.
	if got, err := s.selectDiskForWrite(3 * blk.SlotSize); got != large || err != nil {
		t.Fatalf("fallback: %v %v", got, err)
	}
	s.disks = []*DeviceState{z}
	if _, err := s.selectDiskForWrite(3 * blk.SlotSize); err == nil {
		t.Fatal("large object routed to zram-only tier")
	}
	s.disks = nil
	if _, err := s.selectDiskForWrite(1); err == nil {
		t.Fatal("empty pool accepted")
	}
}
func TestCHDReservationLifecycle(t *testing.T) {
	d := newTestDisk(t)
	initial := d.currentCHD(.2)
	if initial != 8<<20 {
		t.Fatalf("empty disk CHD %d", initial)
	}
	pw, err := d.PrepareWrite(0, 8<<20)
	if err != nil {
		t.Fatal(err)
	}
	if got := d.currentCHD(.2); got != 0 {
		t.Fatalf("pending full extent invisible: %d", got)
	}
	pw.Abort()
	if got := d.currentCHD(.2); got != initial {
		t.Fatal("abort did not restore CHD")
	}
	pw, err = d.PrepareWrite(0, 8<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = pw.WriteFrom(bytes.NewReader(make([]byte, 8<<20))); err != nil {
		t.Fatal(err)
	}
	if err = pw.Commit(0); err != nil {
		t.Fatal(err)
	}
	if got := d.currentCHD(.2); got != 0 {
		t.Fatalf("confirmed full extent invisible: %d", got)
	}
	reopened, err := OpenDevice(d.devicePath, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.currentCHD(.2) != 0 {
		t.Fatal("restart lost occupancy")
	}
}
func TestRoutingInvalidInputs(t *testing.T) {
	for _, pow := range []int{-1, 0, 6, 51, math.MaxInt} {
		got := smallFileBytes(pow)
		if pow < 0 && got != 0 || pow == 0 && got != 4096 || pow >= 51 && got != math.MaxUint64 {
			t.Fatalf("pow %d => %d", pow, got)
		}
	}
	s := &Server{}
	if s.canWrite(&DeviceState{diskBytes: 1}, 1) {
		t.Fatal("undersized device accepted via unsigned underflow")
	}
	d := routeDisk(1, 16, "/dev/test")
	d.indexErr = errors.New("failed disk")
	if s.canWrite(d, 1) {
		t.Fatal("faulted disk remains writable")
	}
	for _, p := range []float64{0, -1, 2, math.NaN(), math.Inf(1)} {
		got := computeCHD(blk.GridStart+16*blk.SlotSize, 1, []interval{{0, 7}}, p)
		want := computeCHD(blk.GridStart+16*blk.SlotSize, 1, []interval{{0, 7}}, .2)
		if got != want {
			t.Fatalf("invalid probability %v: %d != %d", p, got, want)
		}
	}
}

func TestDegradedTierIsVisibleInHealth(t *testing.T) {
	s := &Server{disks: []*DeviceState{routeDisk(1, 1, "/dev/test")}, degraded: true}
	h, err := s.Health(context.Background(), &bsospb.Empty{})
	if err != nil || h.Ok {
		t.Fatalf("degraded startup looks healthy: %v %v", h, err)
	}
}

func TestPersistRecoveredPoolMapping(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pan.json")
	original := pan.File{Version: 1, Devices: []pan.Device{{DevicePath: "/dev/test", DiskID: "1"}, {DevicePath: "/dev/zram0", DiskID: "2"}, {DevicePath: "/dev/zram1", DiskID: "3"}}}
	raw, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	runtime := []pan.Device{original.Devices[0], {DevicePath: "/dev/zram9", DiskID: "2"}, {DevicePath: "/dev/zram10", DiskID: "4"}}
	if err = persistPoolMapping(path, original, runtime); err != nil {
		t.Fatal(err)
	}
	got, err := pan.Read(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Devices) != 4 || got.Devices[1].DevicePath != "/dev/zram9" || got.Devices[2] != original.Devices[2] || got.Devices[3].DiskID != "4" {
		t.Fatalf("mapping: %+v", got)
	}
}
