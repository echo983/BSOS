package daemon

// Tests targeting the exact race windows found during docs/DESIGN.md's
// design review (see §3.3's history), per
// docs/IMPLEMENTATION_PLAN.md §1: deliberately constructed concurrent
// scenarios, not reliance on incidental goroutine-scheduling luck. Run
// with -race.

import (
	"bytes"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	"bsos/internal/blk"
	"bsos/internal/daemon/bsospb"
)

// newTestDisk creates a sparse temp file large enough to have a real
// data grid, with no NBSS header needed — DeviceState/blk.AddrForFID
// only care about the byte size and the (initially empty) index stream.
func newTestDisk(t *testing.T) *DeviceState {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "bsos-test-disk-*")
	if err != nil {
		t.Fatal(err)
	}
	const size = blk.GridStart + 8<<20 // header+index region plus 8MB of data grid
	if err := f.Truncate(size); err != nil {
		t.Fatal(err)
	}
	f.Close()

	disk, err := OpenDevice(f.Name(), 1, 64)
	if err != nil {
		t.Fatalf("OpenDevice: %v", err)
	}
	t.Cleanup(func() { disk.Close() })
	return disk
}

// blockingReader yields payload once release is signaled, so a test can
// hold a write "mid-flight" (extent/fid reserved, not yet committed)
// for as long as it needs to deterministically race a second attempt
// against it — the natural test seam the streaming io.Reader design
// already provides, no production-code hooks required.
type blockingReader struct {
	payload []byte
	release chan struct{}
	inner   *bytes.Reader
}

func (r *blockingReader) Read(p []byte) (int, error) {
	if r.inner == nil {
		<-r.release
		r.inner = bytes.NewReader(r.payload)
	}
	return r.inner.Read(p)
}

func putViaGate(t *testing.T, gate *poolGate, disk *DeviceState, fid, aliasFor uint64, payload []byte, r io.Reader) error {
	t.Helper()
	confirmed := func(f uint64) (bool, error) { return disk.Confirmed(f) }
	if err := gate.reserve(fid, aliasFor, confirmed); err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			gate.release(fid, aliasFor)
		}
	}()
	pw, err := disk.PrepareWrite(fid, uint64(len(payload)))
	if err != nil {
		return err
	}
	if err := pw.WriteFrom(r); err != nil {
		pw.Abort()
		return err
	}
	if err := pw.Commit(aliasFor); err != nil {
		pw.Abort()
		return err
	}
	committed = true
	return nil
}

// TestConcurrentSamePutOneWins: two attempts for the same fid, the
// first held mid-flight (reserved but not committed) while the second
// runs — the second must see conflict at the pool gate, before it ever
// touches the disk.
func TestConcurrentSamePutOneWins(t *testing.T) {
	disk := newTestDisk(t)
	gate := newPoolGate()
	const fid = 0x1111
	payload := []byte("same fid, concurrent attempts")

	release := make(chan struct{})
	var firstErr, secondErr error
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		firstErr = putViaGate(t, gate, disk, fid, 0, payload, &blockingReader{payload: payload, release: release})
	}()
	go func() {
		defer wg.Done()
		time.Sleep(20 * time.Millisecond) // let the first attempt reserve first
		secondErr = putViaGate(t, gate, disk, fid, 0, payload, bytes.NewReader(payload))
		close(release) // let the first attempt proceed to commit once the second is done
	}()
	wg.Wait()

	if firstErr != nil {
		t.Fatalf("first attempt: expected success, got %v", firstErr)
	}
	if secondErr != ErrConflict {
		t.Fatalf("second attempt: expected ErrConflict, got %v", secondErr)
	}
}

// TestAliasRacesPlainWrite: the exact bug found during design review
// (docs/DESIGN.md §3.3's "why fid-level reservation has to be
// pool-wide") — a plain write to fid A racing a write with
// alias_for: A. Without the pool-wide gate, both could proceed and one
// would silently overwrite the other's registration. With it, the loser
// must be rejected outright — never a silent, undetected overwrite.
func TestAliasRacesPlainWrite(t *testing.T) {
	disk := newTestDisk(t)
	gate := newPoolGate()
	const fidA = 0x2222 // the fid a plain write targets, and the alias target
	const fidY = 0x3333 // the other write's own fid

	plainPayload := []byte("plain write directly to A")
	yPayload := []byte("Y, aliased from A")

	release := make(chan struct{})
	var plainErr, aliasErr error
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		plainErr = putViaGate(t, gate, disk, fidA, 0, plainPayload, &blockingReader{payload: plainPayload, release: release})
	}()
	go func() {
		defer wg.Done()
		time.Sleep(20 * time.Millisecond)
		aliasErr = putViaGate(t, gate, disk, fidY, fidA, yPayload, bytes.NewReader(yPayload))
		close(release)
	}()
	wg.Wait()

	// Exactly one must have won; the other must have been rejected
	// outright by the pool gate — not silently applied and then
	// overwritten.
	wins := 0
	if plainErr == nil {
		wins++
	}
	if aliasErr == nil {
		wins++
	}
	if wins != 1 {
		t.Fatalf("expected exactly one winner, got plainErr=%v aliasErr=%v", plainErr, aliasErr)
	}
	if plainErr != nil && plainErr != ErrConflict {
		t.Fatalf("plain write's failure should be ErrConflict, got %v", plainErr)
	}
	if aliasErr != nil && aliasErr != ErrConflict {
		t.Fatalf("alias write's failure should be ErrConflict, got %v", aliasErr)
	}

	// Confirm the winner's data is actually readable through fid A and
	// nothing was silently lost: if the plain write won, A holds its
	// own data; if the alias won, A resolves through the jump to Y.
	data, _, err := disk.Get(fidA)
	if err != nil {
		t.Fatalf("Get(A) after the race: %v", err)
	}
	if plainErr == nil && string(data) != string(plainPayload) {
		t.Fatalf("plain write won but Get(A) = %q, want %q", data, plainPayload)
	}
	if aliasErr == nil && string(data) != string(yPayload) {
		t.Fatalf("alias write won but Get(A) = %q, want %q (via the jump to Y)", data, yPayload)
	}
}

// TestReservationReleasedOnAbort: an aborted write (declared size
// doesn't match what's streamed) must leave the fid completely free —
// not stuck reserved — so a subsequent attempt for the same fid
// succeeds immediately after.
func TestReservationReleasedOnAbort(t *testing.T) {
	disk := newTestDisk(t)
	gate := newPoolGate()
	const fid = 0x4444

	// Declare 100 bytes, only provide 9 — matches docs/DESIGN.md §3.3's
	// "declared vs. actual byte count" abort case.
	confirmed := func(f uint64) (bool, error) { return disk.Confirmed(f) }
	if err := gate.reserve(fid, 0, confirmed); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	pw, err := disk.PrepareWrite(fid, 100)
	if err != nil {
		t.Fatalf("PrepareWrite: %v", err)
	}
	if err := pw.WriteFrom(bytes.NewReader([]byte("only nine"))); err == nil {
		t.Fatal("expected a short-stream error, got nil")
	}
	pw.Abort()
	gate.release(fid, 0)

	// A fresh attempt for the same fid must now succeed cleanly.
	payload := []byte("retry after the aborted attempt")
	if err := putViaGate(t, gate, disk, fid, 0, payload, bytes.NewReader(payload)); err != nil {
		t.Fatalf("retry after abort: expected success, got %v", err)
	}
	data, _, err := disk.Get(fid)
	if err != nil || string(data) != string(payload) {
		t.Fatalf("Get after retry: data=%q err=%v, want %q", data, err, payload)
	}
}

// TestStallingChunkReaderTimesOut: a client that sends a header and
// then goes silent must not block Put forever — docs/DESIGN.md §3.3's
// reservation stall timeout.
func TestStallingChunkReaderTimesOut(t *testing.T) {
	r := &stallingChunkReader{
		stream:  &neverRespondingStream{},
		timeout: 30 * time.Millisecond,
	}
	start := time.Now()
	_, err := r.Read(make([]byte, 16))
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected a stall-timeout error, got nil")
	}
	if elapsed > time.Second {
		t.Fatalf("stall timeout took too long: %s", elapsed)
	}
}

// neverRespondingStream simulates a client that sent PutHeader and then
// went silent: Recv never returns, so the only way Read can return is
// via stallingChunkReader's own timeout racing it.
type neverRespondingStream struct {
	grpc.ServerStream
}

func (s *neverRespondingStream) Recv() (*bsospb.PutRequest, error) {
	select {}
}

func (s *neverRespondingStream) SendAndClose(*bsospb.PutResponse) error {
	return nil
}
