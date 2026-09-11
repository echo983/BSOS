package daemon

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"log"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/echo983/BSOS/internal/blk"
)

// fakeAlignedWriter is an in-memory io.WriterAt that records every call so
// tests can assert the O_DIRECT alignment invariant (offset and length
// both multiples of blk.SlotSize) without a real O_DIRECT-capable
// filesystem.
type fakeAlignedWriter struct {
	buf   []byte
	calls []struct{ off, n int }
}

func (w *fakeAlignedWriter) WriteAt(p []byte, off int64) (int, error) {
	w.calls = append(w.calls, struct{ off, n int }{int(off), len(p)})
	end := int(off) + len(p)
	if end > len(w.buf) {
		grown := make([]byte, end)
		copy(grown, w.buf)
		w.buf = grown
	}
	copy(w.buf[off:end], p)
	return len(p), nil
}

func (w *fakeAlignedWriter) assertAligned(t *testing.T) {
	t.Helper()
	if len(w.calls) == 0 {
		t.Fatalf("expected at least one WriteAt call")
	}
	for i, c := range w.calls {
		if c.off%blk.SlotSize != 0 {
			t.Errorf("call %d: offset %d not a multiple of SlotSize %d", i, c.off, blk.SlotSize)
		}
		if c.n%blk.SlotSize != 0 {
			t.Errorf("call %d: length %d not a multiple of SlotSize %d", i, c.n, blk.SlotSize)
		}
	}
}

func alignUp(n uint64) uint64 {
	return ((n + blk.SlotSize - 1) / blk.SlotSize) * blk.SlotSize
}

func runStreamDirectCase(t *testing.T, addr uint64, realSize uint64, wrapReader func([]byte) interface {
	Read(p []byte) (int, error)
}) *fakeAlignedWriter {
	t.Helper()
	total := alignUp(realSize)
	real := make([]byte, realSize)
	if _, err := rand.Read(real); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	var r interface {
		Read(p []byte) (int, error)
	} = bytes.NewReader(real)
	if wrapReader != nil {
		r = wrapReader(real)
	}
	w := &fakeAlignedWriter{}
	if err := writeStreamDirect(w, addr, realSize, total, r); err != nil {
		t.Fatalf("writeStreamDirect: %v", err)
	}
	w.assertAligned(t)
	got := w.buf[addr : addr+total]
	if !bytes.Equal(got[:realSize], real) {
		t.Errorf("real bytes mismatch")
	}
	for i, b := range got[realSize:] {
		if b != 0 {
			t.Errorf("pad byte %d not zero: %d", i, b)
		}
	}
	return w
}

func TestWriteStreamDirectSizes(t *testing.T) {
	cases := []struct {
		name     string
		addr     uint64
		realSize uint64
	}{
		{"minimum object, one slot", 0, 1},
		{"less than one chunk", 0, 100000},
		{"exact one chunk, no padding", 0, directChunkBytes},
		{"remainder crossing chunk boundary", 0, directChunkBytes + 137},
		{"multiple full chunks plus remainder", 0, 3*directChunkBytes + 4097},
		{"exact multiple of several chunks, no padding", 0, 5 * directChunkBytes},
		{"non-zero aligned addr", 37 * blk.SlotSize, directChunkBytes + 137},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			runStreamDirectCase(t, c.addr, c.realSize, nil)
		})
	}
}

func TestWriteStreamDirectOddlySizedReads(t *testing.T) {
	realSize := uint64(directChunkBytes + 137)

	t.Run("one byte at a time", func(t *testing.T) {
		runStreamDirectCase(t, 0, realSize, func(b []byte) interface {
			Read(p []byte) (int, error)
		} {
			return iotest.OneByteReader(bytes.NewReader(b))
		})
	})

	t.Run("half reader", func(t *testing.T) {
		runStreamDirectCase(t, 0, realSize, func(b []byte) interface {
			Read(p []byte) (int, error)
		} {
			return iotest.HalfReader(bytes.NewReader(b))
		})
	})
}

func TestWriteStreamDirectShortStream(t *testing.T) {
	realSize := uint64(directChunkBytes + 137)
	total := alignUp(realSize)
	short := make([]byte, realSize-1)
	w := &fakeAlignedWriter{}
	err := writeStreamDirect(w, 0, realSize, total, bytes.NewReader(short))
	if err == nil {
		t.Fatalf("expected error for short stream, got nil")
	}
}

func TestWriteStreamDirectOverLongStream(t *testing.T) {
	realSize := uint64(100000)
	total := alignUp(realSize)
	long := make([]byte, realSize+1)
	w := &fakeAlignedWriter{}
	err := writeStreamDirect(w, 0, realSize, total, bytes.NewReader(long))
	if err == nil {
		t.Fatalf("expected error for over-long stream, got nil")
	}
}

func TestWriteStreamDirectRejectsUnalignedTotal(t *testing.T) {
	w := &fakeAlignedWriter{}
	err := writeStreamDirect(w, 0, 10, 4097, bytes.NewReader(make([]byte, 10)))
	if err == nil {
		t.Fatalf("expected error for unaligned total, got nil")
	}
}

// erroringReader returns a non-EOF error after yielding n bytes, to
// confirm writeStreamDirect propagates real I/O errors instead of
// mislabeling them as a short/EOF stream.
type erroringReader struct {
	data []byte
	err  error
}

func (r *erroringReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, r.err
	}
	n := copy(p, r.data)
	r.data = r.data[n:]
	return n, nil
}

func TestWriteStreamDirectPropagatesReadError(t *testing.T) {
	realSize := uint64(100000)
	total := alignUp(realSize)
	sentinel := errors.New("boom")
	r := &erroringReader{data: make([]byte, realSize-1), err: sentinel}
	w := &fakeAlignedWriter{}
	err := writeStreamDirect(w, 0, realSize, total, r)
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel error to propagate, got %v", err)
	}
}

// fakeDeviceFile adapts fakeAlignedWriter into the full deviceFile
// interface (ReaderAt/WriterAt/Sync/Close), standing in for a
// successfully-opened O_DIRECT fd in tests that don't depend on this
// host's filesystem actually supporting O_DIRECT.
type fakeDeviceFile struct {
	fakeAlignedWriter
}

func (f *fakeDeviceFile) ReadAt(p []byte, off int64) (int, error) {
	end := int(off) + len(p)
	if end > len(f.buf) {
		return 0, io.EOF
	}
	copy(p, f.buf[off:end])
	return len(p), nil
}
func (f *fakeDeviceFile) Sync() error  { return nil }
func (f *fakeDeviceFile) Close() error { return nil }

// withStubOpenDirectFile swaps the package-level openDirectFile var for
// the duration of the test and restores it afterward. Safe because no
// test in this package uses t.Parallel() (confirmed), so there's no
// concurrent-mutation risk under -race.
func withStubOpenDirectFile(t *testing.T, stub func(path string) (deviceFile, error)) {
	t.Helper()
	orig := openDirectFile
	openDirectFile = stub
	t.Cleanup(func() { openDirectFile = orig })
}

// TestOpenDeviceFallsBackWhenDirectIOUnsupported exercises decision 2: a
// device whose O_DIRECT open fails must not prevent the daemon from
// starting, must log the reason, and must still serve correct writes via
// the buffered fallback.
func TestOpenDeviceFallsBackWhenDirectIOUnsupported(t *testing.T) {
	withStubOpenDirectFile(t, func(path string) (deviceFile, error) {
		return nil, errors.New("simulated: O_DIRECT not supported")
	})

	var logBuf bytes.Buffer
	origOut := log.Writer()
	log.SetOutput(&logBuf)
	defer log.SetOutput(origOut)

	disk := newTestDisk(t)

	if disk.directFile != nil {
		t.Fatalf("expected directFile to be nil when O_DIRECT open fails")
	}
	if !strings.Contains(logBuf.String(), "O_DIRECT unavailable") {
		t.Errorf("expected log to mention O_DIRECT unavailable, got: %s", logBuf.String())
	}
	if !strings.Contains(logBuf.String(), disk.devicePath) {
		t.Errorf("expected log to mention device path %s, got: %s", disk.devicePath, logBuf.String())
	}

	// Full round trip through the real fallback branch (writeExact +
	// writeZeros), deliberately non-slot-aligned size.
	const fid = uint64(0xABCDEF)
	const size = uint64(10000)
	content := make([]byte, size)
	if _, err := rand.Read(content); err != nil {
		t.Fatal(err)
	}
	pw, err := disk.PrepareWrite(fid, size)
	if err != nil {
		t.Fatalf("PrepareWrite: %v", err)
	}
	if err := pw.WriteFromContext(context.Background(), bytes.NewReader(content)); err != nil {
		t.Fatalf("WriteFromContext: %v", err)
	}
	if err := pw.Commit(0); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	got, gotSize, err := disk.Get(fid)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if gotSize != size || !bytes.Equal(got, content) {
		t.Errorf("round trip mismatch via fallback branch")
	}
}

// TestOpenDeviceUsesDirectFileWhenAvailable exercises the wiring in
// WriteFromContext, not just the writeStreamDirect algorithm in
// isolation: a successful (faked) O_DIRECT open must route the payload
// through writeStreamDirect via the real DeviceState/PreparedWrite call
// path.
func TestOpenDeviceUsesDirectFileWhenAvailable(t *testing.T) {
	var fake *fakeDeviceFile
	withStubOpenDirectFile(t, func(path string) (deviceFile, error) {
		fake = &fakeDeviceFile{}
		return fake, nil
	})

	disk := newTestDisk(t)
	if disk.directFile == nil {
		t.Fatalf("expected directFile to be set when O_DIRECT open succeeds")
	}

	const fid = uint64(0x123456)
	const size = uint64(directChunkBytes + 137)
	content := make([]byte, size)
	if _, err := rand.Read(content); err != nil {
		t.Fatal(err)
	}
	pw, err := disk.PrepareWrite(fid, size)
	if err != nil {
		t.Fatalf("PrepareWrite: %v", err)
	}
	if err := pw.WriteFromContext(context.Background(), bytes.NewReader(content)); err != nil {
		t.Fatalf("WriteFromContext: %v", err)
	}
	if err := pw.Commit(0); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	fake.assertAligned(t)

	// Get() always reads via the buffered fd (reads stay buffered by
	// design), but this test's fake directFile is a disconnected
	// in-memory buffer, not the same backing storage as disk.file — so
	// content lands in the fake, not in what Get() would read. Verify
	// directly against the fake's buffer at the address blk.AddrForFID
	// computes, instead of round-tripping through Get().
	addr, _, slotsNeeded, err := blk.AddrForFID(disk.diskBytes, fid, size)
	if err != nil {
		t.Fatalf("AddrForFID: %v", err)
	}
	total := slotsNeeded * blk.SlotSize
	got := fake.buf[addr : addr+total]
	if !bytes.Equal(got[:size], content) {
		t.Errorf("direct-fd content mismatch")
	}
	for i, b := range got[size:] {
		if b != 0 {
			t.Errorf("pad byte %d not zero: %d", i, b)
		}
	}
}

// TestOpenDeviceRealDirectOpenBestEffort documents today's expected
// sandbox behavior (this dev environment is almost certainly overlayfs,
// which doesn't support O_DIRECT) without making the suite flaky on
// hosts that do support it — the one test that starts actually exercising
// real O_DIRECT the day this runs on an ext4-backed box.
func TestOpenDeviceRealDirectOpenBestEffort(t *testing.T) {
	disk := newTestDisk(t)
	if disk.directFile == nil {
		t.Skip("host filesystem does not support O_DIRECT (expected on overlay/tmpfs sandboxes)")
	}

	const fid = uint64(0x999999)
	const size = uint64(20000)
	content := make([]byte, size)
	if _, err := rand.Read(content); err != nil {
		t.Fatal(err)
	}
	pw, err := disk.PrepareWrite(fid, size)
	if err != nil {
		t.Fatalf("PrepareWrite: %v", err)
	}
	if err := pw.WriteFromContext(context.Background(), bytes.NewReader(content)); err != nil {
		t.Fatalf("WriteFromContext: %v", err)
	}
	if err := pw.Commit(0); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	got, gotSize, err := disk.Get(fid)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if gotSize != size || !bytes.Equal(got, content) {
		t.Errorf("round trip mismatch via real O_DIRECT")
	}
}
