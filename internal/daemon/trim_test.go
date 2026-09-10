package daemon

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/zeebo/xxh3"
)

func writeTestObject(t *testing.T, disk *DeviceState, fid uint64, aliasFor uint64, data []byte) {
	t.Helper()
	pw, err := disk.PrepareWrite(fid, uint64(len(data)))
	if err != nil {
		t.Fatalf("PrepareWrite(0x%X): %v", fid, err)
	}
	if err := pw.WriteFrom(bytes.NewReader(data)); err != nil {
		pw.Abort()
		t.Fatalf("WriteFrom(0x%X): %v", fid, err)
	}
	if err := pw.Commit(aliasFor); err != nil {
		pw.Abort()
		t.Fatalf("Commit(0x%X): %v", fid, err)
	}
}

func TestTrimPacksSmallObjectsAndPreservesReads(t *testing.T) {
	disk := newTestDiskID(t, 0xD15C0)
	tmpDir := t.TempDir()
	disk.cfg.TrimTempDir = tmpDir
	disk.cfg.TrimMinFileCount = 100
	disk.cfg.TrimThresholdRatio = 0.20

	type obj struct {
		fid  uint64
		data []byte
	}
	var objects []obj
	for i := 0; i < 105; i++ {
		data := []byte(fmt.Sprintf("trim-small-object-payload-%04d", i))
		fid := xxh3.Hash(data)
		writeTestObject(t, disk, fid, 0, data)
		objects = append(objects, obj{fid: fid, data: data})
	}

	if err := disk.maybeTrim(); err != nil {
		t.Fatalf("maybeTrim: %v", err)
	}

	if !disk.trimState.LastTrigger {
		t.Fatalf("expected trim to trigger, got false")
	}
	if disk.trimState.LastPackedFiles != len(objects) {
		t.Fatalf("packed files = %d, want %d", disk.trimState.LastPackedFiles, len(objects))
	}
	if len(disk.packed) != len(objects) {
		t.Fatalf("len(disk.packed) = %d, want %d", len(disk.packed), len(objects))
	}

	for _, o := range objects {
		got, size, err := disk.Get(o.fid)
		if err != nil {
			t.Fatalf("Get(0x%X) error: %v", o.fid, err)
		}
		if size != uint64(len(o.data)) || !bytes.Equal(got, o.data) {
			t.Fatalf("Get(0x%X) data mismatch: got %q want %q", o.fid, got, o.data)
		}
	}

	// Reopen disk to test cold-load replay of packed anchor and packed table
	devicePath := disk.devicePath
	diskID := disk.diskID
	reopened, err := OpenDevice(devicePath, diskID, 64)
	if err != nil {
		t.Fatalf("reopen OpenDevice: %v", err)
	}
	defer reopened.Close()

	if len(reopened.packed) != len(objects) {
		t.Fatalf("reopened len(packed) = %d, want %d", len(reopened.packed), len(objects))
	}
	for _, o := range objects {
		got, size, err := reopened.Get(o.fid)
		if err != nil {
			t.Fatalf("reopened Get(0x%X) error: %v", o.fid, err)
		}
		if size != uint64(len(o.data)) || !bytes.Equal(got, o.data) {
			t.Fatalf("reopened Get(0x%X) mismatch", o.fid)
		}
	}
}

func TestTrimExcludesJumpAlias(t *testing.T) {
	disk := newTestDiskID(t, 0xD15C1)
	disk.cfg.TrimTempDir = t.TempDir()
	disk.cfg.TrimMinFileCount = 5
	disk.cfg.TrimThresholdRatio = 0.1

	// Write an alias pair: targetFID and aliasFID
	aliasData := []byte("alias-target-content-001\x01")
	targetFID := xxh3.Hash(aliasData)
	aliasFID := uint64(0xAAAA1111)
	writeTestObject(t, disk, targetFID, aliasFID, aliasData)

	// Write small objects
	for i := 0; i < 20; i++ {
		data := []byte(fmt.Sprintf("small-%d", i))
		fid := xxh3.Hash(data)
		writeTestObject(t, disk, fid, 0, data)
	}

	candidates, _, _, _, err := disk.collectTrimCandidatesLocked()
	if err != nil {
		t.Fatalf("collectTrimCandidatesLocked: %v", err)
	}

	for _, c := range candidates {
		if c.fid == targetFID {
			t.Fatalf("targetFID 0x%X was unexpectedly included in trim candidates", targetFID)
		}
		if c.fid == aliasFID {
			t.Fatalf("aliasFID 0x%X was unexpectedly included in trim candidates", aliasFID)
		}
	}
}

func TestTrimExcludesPackedTablesAndContainers(t *testing.T) {
	disk := newTestDiskID(t, 0xD15C2)
	disk.cfg.TrimTempDir = t.TempDir()
	disk.cfg.TrimMinFileCount = 5
	disk.cfg.TrimThresholdRatio = 0.1

	for i := 0; i < 10; i++ {
		data := []byte(fmt.Sprintf("payload-%d", i))
		fid := xxh3.Hash(data)
		writeTestObject(t, disk, fid, 0, data)
	}

	if err := disk.maybeTrim(); err != nil {
		t.Fatalf("maybeTrim: %v", err)
	}

	candidates, _, _, _, err := disk.collectTrimCandidatesLocked()
	if err != nil {
		t.Fatalf("collectTrimCandidatesLocked second run: %v", err)
	}
	if len(candidates) != 0 {
		t.Fatalf("expected 0 candidates after trim, got %d", len(candidates))
	}
}

func TestTrimFailureInjection(t *testing.T) {
	failPoints := []string{
		"after-backup",
		"after-container-build",
		"after-container-write",
		"after-table-write",
		"after-anchor-append",
		"after-delete",
	}

	for _, fp := range failPoints {
		t.Run("failpoint_"+fp, func(t *testing.T) {
			disk := newTestDiskID(t, 0xD15C3)
			disk.cfg.TrimTempDir = t.TempDir()
			disk.cfg.TrimMinFileCount = 10
			disk.cfg.TrimThresholdRatio = 0.1

			type obj struct {
				fid  uint64
				data []byte
			}
			var objects []obj
			for i := 0; i < 20; i++ {
				data := []byte(fmt.Sprintf("failpoint-object-%d", i))
				fid := xxh3.Hash(data)
				writeTestObject(t, disk, fid, 0, data)
				objects = append(objects, obj{fid: fid, data: data})
			}

			disk.failPoint = fp
			err := disk.maybeTrim()
			if err == nil {
				t.Fatalf("expected error from failpoint %s, got nil", fp)
			}

			if disk.trimState.LastError == "" {
				t.Errorf("expected LastError to be set after failpoint %s", fp)
			}

			// Verify all objects remain readable
			for _, o := range objects {
				got, size, err := disk.Get(o.fid)
				if err != nil {
					t.Errorf("fid 0x%X not readable after %s: %v", o.fid, fp, err)
					continue
				}
				if size != uint64(len(o.data)) || !bytes.Equal(got, o.data) {
					t.Errorf("data mismatch for fid 0x%X after %s", o.fid, fp)
				}
			}

			// Also verify that reopening the disk recovers cleanly
			reopened, err := OpenDevice(disk.devicePath, disk.diskID, 64)
			if err != nil {
				t.Fatalf("reopen after %s: %v", fp, err)
			}
			defer reopened.Close()
			for _, o := range objects {
				got, size, err := reopened.Get(o.fid)
				if err != nil {
					t.Errorf("reopened fid 0x%X not readable after %s: %v", o.fid, fp, err)
					continue
				}
				if size != uint64(len(o.data)) || !bytes.Equal(got, o.data) {
					t.Errorf("reopened data mismatch for fid 0x%X after %s", o.fid, fp)
				}
			}
		})
	}
}

func TestServerTrimScheduler(t *testing.T) {
	disk := newTestDiskID(t, 0xD15C4)
	disk.cfg.TrimTempDir = t.TempDir()
	disk.cfg.TrimMinFileCount = 1000 // don't trigger full trim, just test scheduler runs
	disk.cfg.TrimThresholdRatio = 0.5

	s := &Server{
		disks:        []*DeviceState{disk},
		stopCh:       make(chan struct{}),
		trimInterval: 10 * time.Millisecond,
	}
	s.startTrimScheduler(10 * time.Millisecond)

	time.Sleep(50 * time.Millisecond)

	disk.mu.Lock()
	checkAt := disk.trimState.LastCheckAt
	disk.mu.Unlock()

	if checkAt.IsZero() {
		t.Fatalf("expected trim scheduler to have checked disk, LastCheckAt is zero")
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Server.Close: %v", err)
	}
}
