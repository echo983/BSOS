package daemon

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/echo983/BSOS/internal/blk"
	"github.com/echo983/BSOS/internal/daemon/bsospb"
	"github.com/echo983/BSOS/internal/pan"
	"github.com/zeebo/xxh3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Explicit opt-in: appends small test objects to two pre-initialized test
// devices. It never initializes, erases or resets a device. Objects remain
// stored because BSOS intentionally has no DELETE API.
func TestRealMultiDisk(t *testing.T) {
	raw := os.Getenv("BSOS_REAL_DEVICE_PATHS")
	if raw == "" {
		t.Skip("set BSOS_REAL_DEVICE_PATHS to a JSON array of two approved /dev/disk/by-id paths; test appends persistent objects")
	}
	var paths []string
	if err := json.Unmarshal([]byte(raw), &paths); err != nil || len(paths) != 2 {
		t.Fatal("expected two device paths")
	}
	var devices []pan.Device
	for _, path := range paths {
		if !strings.HasPrefix(path, "/dev/disk/by-id/") {
			t.Fatal("use stable by-id device paths")
		}
		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		st, err := f.Stat()
		if err != nil {
			f.Close()
			t.Fatal(err)
		}
		if st.Mode()&os.ModeDevice == 0 {
			f.Close()
			t.Fatal("not a device")
		}
		header := make([]byte, blk.HeaderBytes)
		_, err = f.ReadAt(header, 0)
		f.Close()
		if err != nil {
			t.Fatal(err)
		}
		h, err := blk.ParseHeader(header)
		if err != nil {
			t.Fatal(err)
		}
		devices = append(devices, pan.Device{DevicePath: path, DiskID: fmt.Sprintf("0x%X", h.DiskID)})
	}
	open := func(ds []pan.Device) *Server {
		t.Helper()
		raw, err := json.Marshal(pan.File{Version: 1, Devices: ds})
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(t.TempDir(), "pan.json")
		if err = os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
		cfg := DefaultConfig()
		cfg.PanPath = path
		cfg.ZramSnapshotDir = ""
		s, err := NewServer(cfg)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	stamp := time.Now().UTC().Format(time.RFC3339Nano)
	type object struct {
		fid  uint64
		data []byte
	}
	var objects []object
	for i, device := range devices {
		s := open([]pan.Device{device})
		c := testRPCClient(t, s)
		data := bytes.Repeat([]byte{byte(i + 1)}, (i+1)<<20)
		copy(data, stamp)
		fid := xxh3.Hash(data)
		started := time.Now()
		err := rpcPut(ctx, c, fid, 0, uint64(len(data)), data)
		s.Close()
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("seed disk %d: bytes=%d elapsed=%s sha256=%x", i, len(data), time.Since(started), sha256.Sum256(data))
		objects = append(objects, object{fid, data})
	}
	s := open(devices)
	c := testRPCClient(t, s)
	for i, o := range objects {
		got, err := rpcRead(ctx, c, &bsospb.GetRequest{Fid: o.fid})
		if err != nil || !bytes.Equal(got, o.data) {
			s.Close()
			t.Fatalf("pool read disk %d: %v", i, err)
		}
		if err = rpcPut(ctx, c, o.fid, 0, uint64(len(o.data)), nil); status.Code(err) != codes.AlreadyExists {
			s.Close()
			t.Fatalf("pool duplicate: %v", err)
		}
	}
	data := []byte("BSOS live alias " + stamp)
	origin := xxh3.Hash(data)
	jump := append(append([]byte{}, data...), 1)
	target := xxh3.Hash(jump)
	if err := rpcPut(ctx, c, target, origin, uint64(len(jump)), jump); err != nil {
		s.Close()
		t.Fatal(err)
	}
	s.Close()
	s = open(devices)
	defer s.Close()
	c = testRPCClient(t, s)
	got, err := rpcRead(ctx, c, &bsospb.GetRequest{Fid: origin})
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("reopened alias: %q %v", got, err)
	}
	for i, o := range objects {
		got, err := rpcRead(ctx, c, &bsospb.GetRequest{Fid: o.fid, HasRange: true, RangeStart: 7, RangeEnd: 77})
		if err != nil || !bytes.Equal(got, o.data[7:77]) {
			t.Fatalf("reopened range disk %d: %v", i, err)
		}
	}
	t.Log("PASS: distinct physical disks, pool reads, pool duplicate rejection, alias, close/reopen, range readback")
}
