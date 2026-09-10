package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"bsos/internal/blk"
	"bsos/internal/daemon/bsospb"
	"bsos/internal/pan"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func rpcFixture(t *testing.T) (*Server, bsospb.BSOSClient) {
	t.Helper()
	s := &Server{disks: []*DeviceState{newTestDiskID(t, 1), newTestDiskID(t, 2)}, gate: newPoolGate(), maxPut: 256 << 20, chdTargetP: .2, stallTimeout: time.Second, gateFanoutLimit: time.Second}
	return s, testRPCClient(t, s)
}

func testRPCClient(t *testing.T, s *Server) bsospb.BSOSClient {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	bsospb.RegisterBSOSServer(gs, s)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)
	cc, err := grpc.NewClient("passthrough:///test", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cc.Close() })
	return bsospb.NewBSOSClient(cc)
}
func rpcPut(ctx context.Context, c bsospb.BSOSClient, fid, alias, size uint64, data []byte) error {
	st, err := c.Put(ctx)
	if err != nil {
		return err
	}
	if err = st.Send(&bsospb.PutRequest{Msg: &bsospb.PutRequest_Header{Header: &bsospb.PutHeader{Fid: fid, AliasFor: alias, TotalSize: size}}}); err != nil {
		return err
	}
	for len(data) > 0 {
		n := min(len(data), 1<<20)
		if err = st.Send(&bsospb.PutRequest{Msg: &bsospb.PutRequest_Chunk{Chunk: data[:n]}}); err != nil {
			break
		}
		data = data[n:]
	}
	_, err = st.CloseAndRecv()
	return err
}
func rpcRead(ctx context.Context, c bsospb.BSOSClient, req *bsospb.GetRequest) ([]byte, error) {
	st, err := c.Get(ctx, req)
	if err != nil {
		return nil, err
	}
	var data []byte
	for {
		r, e := st.Recv()
		if e == io.EOF {
			return data, nil
		}
		if e != nil {
			return nil, e
		}
		data = append(data, r.Data...)
	}
}
func TestRPCCommitReleasesPending(t *testing.T) {
	s, c := rpcFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	data := []byte("alias content\x01")
	if err := rpcPut(ctx, c, 100, 200, uint64(len(data)), data); err != nil {
		t.Fatal(err)
	}
	s.gate.mu.Lock()
	n := len(s.gate.pending)
	s.gate.mu.Unlock()
	if n != 0 {
		t.Fatalf("successful commit retained %d pending FIDs", n)
	}
	for _, fid := range []uint64{100, 200} {
		if err := rpcPut(ctx, c, fid, 0, 1, []byte("x")); status.Code(err) != codes.AlreadyExists {
			t.Fatalf("duplicate %d: %v", fid, err)
		}
	}
	got, err := rpcRead(ctx, c, &bsospb.GetRequest{Fid: 200})
	if err != nil || !bytes.Equal(got, data[:len(data)-1]) {
		t.Fatalf("alias: %q %v", got, err)
	}
}
func TestRPCLargeGet(t *testing.T) {
	_, c := rpcFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	data := bytes.Repeat([]byte("abcde"), 1<<20)
	if err := rpcPut(ctx, c, 0, 0, uint64(len(data)), data); err != nil {
		t.Fatal(err)
	}
	got, err := rpcRead(ctx, c, &bsospb.GetRequest{Fid: 0})
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("large Get: bytes=%d err=%v", len(got), err)
	}
}
func TestRPCAbortAllowsRetry(t *testing.T) {
	for _, tc := range []struct {
		name string
		size uint64
		data string
	}{{"short", 10, "abc"}, {"long", 2, "abc"}} {
		t.Run(tc.name, func(t *testing.T) {
			s, c := rpcFixture(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := rpcPut(ctx, c, 20, 30, tc.size, []byte(tc.data)); err == nil {
				t.Fatal("mismatch accepted")
			}
			for _, fid := range []uint64{20, 30} {
				if _, err := c.Head(ctx, &bsospb.HeadRequest{Fid: fid}); status.Code(err) != codes.NotFound {
					t.Fatalf("aborted fid visible: %v", err)
				}
			}
			s.gate.mu.Lock()
			n := len(s.gate.pending)
			s.gate.mu.Unlock()
			if n != 0 {
				t.Fatal("pending leaked")
			}
			if err := rpcPut(ctx, c, 20, 30, 3, []byte("ok\x01")); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestGetRejectsShortDiskRead(t *testing.T) {
	d := newTestDisk(t)
	pw, err := d.PrepareWrite(10, 3)
	if err != nil {
		t.Fatal(err)
	}
	if err = pw.WriteFrom(bytes.NewReader([]byte("abc"))); err != nil {
		t.Fatal(err)
	}
	if err = pw.Commit(0); err != nil {
		t.Fatal(err)
	}
	if err = os.Truncate(d.devicePath, int64(pw.addr+1)); err != nil {
		t.Fatal(err)
	}
	if _, _, err = d.Get(10); err == nil {
		t.Fatal("short disk read silently returned zero-padded content")
	}
}
func TestReplayRejectsDanglingAlias(t *testing.T) {
	d := newTestDisk(t)
	entry, err := blk.BuildIndexEntry(42, blk.IndexJumpSentinel, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = d.file.WriteAt(entry, blk.IndexStart); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenDevice(d.devicePath, 1, 1)
	if err == nil {
		reopened.Close()
		t.Fatal("orphan alias accepted at startup")
	}
}

// Inject real partial writes at the persistence boundary, then exercise the
// production rollback path and replay the resulting bytes from disk.
type failingFile struct {
	deviceFile
	writes, syncs       int
	failWrite, failSync int
	failRollback        bool
}

func (f *failingFile) WriteAt(p []byte, off int64) (int, error) {
	f.writes++
	if f.writes == f.failWrite {
		n, _ := f.deviceFile.WriteAt(p[:len(p)/2], off)
		return n, io.ErrShortWrite
	}
	if f.failRollback && f.writes > f.failWrite {
		return 0, io.ErrShortWrite
	}
	return f.deviceFile.WriteAt(p, off)
}
func (f *failingFile) Sync() error {
	f.syncs++
	if f.syncs == f.failSync {
		return io.ErrClosedPipe
	}
	return f.deviceFile.Sync()
}
func TestCommitFailureRollback(t *testing.T) {
	for _, tc := range []struct {
		name        string
		write, sync int
		poison      bool
	}{{"payload_sync", 0, 1, false}, {"partial_pair", 1, 0, false}, {"index_sync", 0, 2, false}, {"rollback_failure", 1, 0, true}} {
		t.Run(tc.name, func(t *testing.T) {
			d := newTestDisk(t)
			pw, err := d.PrepareWrite(50, 4)
			if err != nil {
				t.Fatal(err)
			}
			if err = pw.WriteFrom(bytes.NewReader([]byte("abc\x01"))); err != nil {
				t.Fatal(err)
			}
			original := d.file
			d.file = &failingFile{deviceFile: original, failWrite: tc.write, failSync: tc.sync, failRollback: tc.poison}
			if err = pw.Commit(60); err == nil {
				t.Fatal("fault not reported")
			}
			pw.Abort()
			d.file = original
			if _, err = d.Head(50); err == nil {
				t.Fatal("failed commit visible")
			}
			if tc.poison {
				if _, err = d.PrepareWrite(50, 4); err == nil {
					t.Fatal("uncertain disk reused")
				}
				return
			}
			reopened, err := OpenDevice(d.devicePath, 1, 1)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			for _, fid := range []uint64{50, 60} {
				if _, err = reopened.Head(fid); err != ErrNotFound {
					t.Fatalf("failed pair survived restart: %v", err)
				}
			}
			retry, err := d.PrepareWrite(50, 4)
			if err != nil {
				t.Fatal(err)
			}
			if err = retry.WriteFrom(bytes.NewReader([]byte("abc\x01"))); err != nil {
				t.Fatal(err)
			}
			if err = retry.Commit(60); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestRestartReplaysAliasAndOccupancy(t *testing.T) {
	d := newTestDisk(t)
	pw, err := d.PrepareWrite(50, 4)
	if err != nil {
		t.Fatal(err)
	}
	if err = pw.WriteFrom(bytes.NewReader([]byte("abc\x01"))); err != nil {
		t.Fatal(err)
	}
	if err = pw.Commit(60); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenDevice(d.devicePath, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	for _, tc := range []struct {
		fid  uint64
		data string
	}{{50, "abc\x01"}, {60, "abc"}} {
		data, _, err := reopened.Get(tc.fid)
		if err != nil || string(data) != tc.data {
			t.Fatalf("restart %d: %q %v", tc.fid, data, err)
		}
	}
	if _, err = reopened.PrepareWrite(50, 4); err != ErrConflict {
		t.Fatalf("confirmed extent not rebuilt: %v", err)
	}
}
func TestRPCRanges(t *testing.T) {
	_, c := rpcFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	data := []byte("abcdefghij")
	if err := rpcPut(ctx, c, 10, 0, 10, data); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		start, end uint64
		want       string
		code       codes.Code
	}{{2, 5, "cde", codes.OK}, {2, 0, "cdefghij", codes.OK}, {10, 10, "", codes.OK}, {11, 0, "", codes.InvalidArgument}, {5, 2, "", codes.InvalidArgument}} {
		got, err := rpcRead(ctx, c, &bsospb.GetRequest{Fid: 10, HasRange: true, RangeStart: tc.start, RangeEnd: tc.end})
		if status.Code(err) != tc.code || string(got) != tc.want {
			t.Fatalf("range %d:%d: %q %v", tc.start, tc.end, got, err)
		}
	}
}

// Signal after Put has reserved its FIDs and extent and asks for body bytes.
// Tests control this exact boundary without relying on scheduler delays.
type controlledPut struct {
	grpc.ServerStream
	ctx                     context.Context
	header                  *bsospb.PutHeader
	started                 chan struct{}
	body                    chan []byte
	sentHeader, startedBody bool
	sendErr                 error
}

func (s *controlledPut) Context() context.Context { return s.ctx }
func (s *controlledPut) Recv() (*bsospb.PutRequest, error) {
	if !s.sentHeader {
		s.sentHeader = true
		return &bsospb.PutRequest{Msg: &bsospb.PutRequest_Header{Header: s.header}}, nil
	}
	if !s.startedBody {
		s.startedBody = true
		close(s.started)
	}
	select {
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	case b, ok := <-s.body:
		if !ok {
			return nil, io.EOF
		}
		return &bsospb.PutRequest{Msg: &bsospb.PutRequest_Chunk{Chunk: b}}, nil
	}
}
func (s *controlledPut) SendAndClose(*bsospb.PutResponse) error { return s.sendErr }
func TestPoolRaceThroughServer(t *testing.T) {
	for _, aliasWins := range []bool{false, true} {
		t.Run(fmt.Sprint(aliasWins), func(t *testing.T) {
			s, c := rpcFixture(t)
			s.disks[1].diskBytes = blk.GridStart + 16<<20
			if err := os.Truncate(s.disks[1].devicePath, int64(s.disks[1].diskBytes)); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			fid, alias := uint64(100), uint64(0)
			data := []byte("abc")
			if aliasWins {
				fid, alias = 200, 100
				data = []byte("abc\x01")
			}
			first := &controlledPut{ctx: ctx, header: &bsospb.PutHeader{Fid: fid, AliasFor: alias, TotalSize: uint64(len(data))}, started: make(chan struct{}), body: make(chan []byte, 1)}
			done := make(chan error, 1)
			go func() { done <- s.Put(first) }()
			select {
			case <-first.started:
			case <-ctx.Done():
				t.Fatal("first never reserved")
			}
			disk, err := s.selectDiskForWrite(9 << 20)
			if err != nil || disk != s.disks[1] {
				t.Fatalf("large competitor must route to second disk: %v", err)
			}
			for _, f := range []uint64{fid, 100} {
				if _, err = c.Head(ctx, &bsospb.HeadRequest{Fid: f}); status.Code(err) != codes.NotFound {
					t.Fatalf("pending visible: %v", err)
				}
			}
			competitorFID, competitorAlias := uint64(200), uint64(100)
			if aliasWins {
				competitorFID, competitorAlias = 100, 0
			}
			if err = rpcPut(ctx, c, competitorFID, competitorAlias, 9<<20, nil); status.Code(err) != codes.AlreadyExists {
				t.Fatalf("cross-disk conflict: %v", err)
			}
			first.body <- data
			close(first.body)
			if err = <-done; err != nil {
				t.Fatal(err)
			}
			got, err := rpcRead(ctx, c, &bsospb.GetRequest{Fid: 100})
			if err != nil || string(got) != "abc" {
				t.Fatalf("winner lost: %q %v", got, err)
			}
		})
	}
}
func TestPutCancellationAndStallReleaseReservations(t *testing.T) {
	for _, mode := range []string{"cancel", "stall", "empty_chunks", "queue_cancel", "queue_timeout"} {
		t.Run(mode, func(t *testing.T) {
			s, _ := rpcFixture(t)
			s.stallTimeout = 40 * time.Millisecond
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			st := &controlledPut{ctx: ctx, header: &bsospb.PutHeader{Fid: 100, TotalSize: 3}, started: make(chan struct{}), body: make(chan []byte)}
			queued := strings.HasPrefix(mode, "queue_")
			if queued {
				for _, d := range s.disks {
					d.ioSem = make(chan struct{}, 1)
					d.ioSem <- struct{}{}
				}
			}
			done := make(chan error, 1)
			go func() { done <- s.Put(st) }()
			if queued {
				if mode == "queue_cancel" {
					cancel()
				}
			} else {
				select {
				case <-st.started:
				case <-time.After(time.Second):
					t.Fatal("not reserved")
				}
				if mode == "cancel" {
					cancel()
				}
				if mode == "empty_chunks" {
					go func() {
						for {
							select {
							case st.body <- []byte{}:
							case <-ctx.Done():
								return
							}
						}
					}()
				}
			}
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("incomplete stream accepted")
				}
			case <-time.After(2 * time.Second):
				t.Fatal("Put did not stop")
			}
			cancel()
			s.gate.mu.Lock()
			n := len(s.gate.pending)
			s.gate.mu.Unlock()
			if n != 0 {
				t.Fatalf("pending leaked: %d", n)
			}
			for _, d := range s.disks {
				d.mu.Lock()
				n := len(d.pendingIntervals)
				d.mu.Unlock()
				if n != 0 {
					t.Fatal("extent leaked")
				}
			}
		})
	}
}
func TestResponseFailureKeepsCommit(t *testing.T) {
	s, _ := rpcFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	st := &controlledPut{ctx: ctx, header: &bsospb.PutHeader{Fid: 100, TotalSize: 3}, started: make(chan struct{}), body: make(chan []byte, 1), sendErr: io.ErrClosedPipe}
	st.body <- []byte("abc")
	close(st.body)
	if err := s.Put(st); err != io.ErrClosedPipe {
		t.Fatalf("response error: %v", err)
	}
	if len(s.gate.pending) != 0 {
		t.Fatal("pending leaked")
	}
	if _, err := s.Head(ctx, &bsospb.HeadRequest{Fid: 100}); err != nil {
		t.Fatalf("committed object disappeared: %v", err)
	}
}
func TestConcurrentRPCWritesAndReadback(t *testing.T) {
	s, c := rpcFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for fid := uint64(1); fid <= 64; fid++ {
		wg.Add(1)
		go func(fid uint64) {
			defer wg.Done()
			data := []byte(fmt.Sprintf("object-%d", fid))
			if err := rpcPut(ctx, c, fid, 0, uint64(len(data)), data); err != nil {
				t.Errorf("put %d: %v", fid, err)
				return
			}
			got, err := rpcRead(ctx, c, &bsospb.GetRequest{Fid: fid})
			if err != nil || !bytes.Equal(got, data) {
				t.Errorf("get %d: %q %v", fid, got, err)
			}
		}(fid)
	}
	wg.Wait()
	s.gate.mu.Lock()
	defer s.gate.mu.Unlock()
	if len(s.gate.pending) != 0 {
		t.Fatal("pending grew after completed writes")
	}
}

func TestStartupFailureClosesEarlierDevices(t *testing.T) {
	d := newTestDisk(t)
	count := func() int {
		entries, err := os.ReadDir("/proc/self/fd")
		if err != nil {
			t.Fatal(err)
		}
		n := 0
		for _, e := range entries {
			target, _ := os.Readlink("/proc/self/fd/" + e.Name())
			if target == d.devicePath {
				n++
			}
		}
		return n
	}
	for _, badID := range []string{"2", "invalid"} {
		t.Run(badID, func(t *testing.T) {
			p := pan.File{Version: 1, Devices: []pan.Device{{DevicePath: d.devicePath, DiskID: "1"}, {DevicePath: d.devicePath + "-missing", DiskID: badID}}}
			raw, err := json.Marshal(p)
			if err != nil {
				t.Fatal(err)
			}
			path := t.TempDir() + "/pan.json"
			if err = os.WriteFile(path, raw, 0600); err != nil {
				t.Fatal(err)
			}
			before := count()
			cfg := DefaultConfig()
			cfg.ZramSnapshotDir = ""
			cfg.PanPath = path
			if server, err := NewServer(cfg); err == nil {
				server.Close()
				t.Fatal("invalid startup accepted")
			}
			if after := count(); after != before {
				t.Fatalf("descriptor leak: before=%d after=%d", before, after)
			}
		})
	}
}
func TestStartupRejectsWrongHeader(t *testing.T) {
	for _, mode := range []string{"magic", "version", "identity", "size"} {
		t.Run(mode, func(t *testing.T) {
			d := newTestDisk(t)
			switch mode {
			case "magic":
				_, _ = d.file.WriteAt([]byte("oops"), 0)
			case "version":
				_, _ = d.file.WriteAt([]byte{99, 0}, 4)
			case "size":
				_ = os.Truncate(d.devicePath, blk.GridStart)
			}
			id := uint64(1)
			if mode == "identity" {
				id = 2
			}
			opened, err := OpenDevice(d.devicePath, id, 1)
			if err == nil {
				opened.Close()
				t.Fatal("invalid device accepted")
			}
		})
	}
}

type brokenIndexReader struct{}

func (brokenIndexReader) ReadAt([]byte, int64) (int, error) { return 0, io.ErrClosedPipe }
func TestReplayReadErrorIsFatal(t *testing.T) {
	if _, _, _, _, err := replayIndex(brokenIndexReader{}, blk.GridStart+8<<20); err == nil {
		t.Fatal("index I/O error interpreted as empty disk")
	}
}

type pausingIndexFile struct {
	deviceFile
	entered, release chan struct{}
}

func (f *pausingIndexFile) WriteAt(p []byte, off int64) (int, error) {
	n, err := f.deviceFile.WriteAt(p[:16], off)
	if err != nil {
		return n, err
	}
	close(f.entered)
	<-f.release
	m, err := f.deviceFile.WriteAt(p[16:], off+16)
	return n + m, err
}
func TestAliasPairPublishedTogether(t *testing.T) {
	d := newTestDisk(t)
	pw, err := d.PrepareWrite(50, 4)
	if err != nil {
		t.Fatal(err)
	}
	if err = pw.WriteFrom(bytes.NewReader([]byte("abc\x01"))); err != nil {
		t.Fatal(err)
	}
	original := d.file
	f := &pausingIndexFile{deviceFile: original, entered: make(chan struct{}), release: make(chan struct{})}
	d.file = f
	done := make(chan error, 1)
	go func() { done <- pw.Commit(60) }()
	select {
	case <-f.entered:
	case <-time.After(time.Second):
		t.Fatal("commit not reached")
	}
	for _, fid := range []uint64{50, 60} {
		if _, err := d.Head(fid); err != ErrNotFound {
			t.Errorf("half pair visible: %v", err)
		}
		if found, err := d.Confirmed(fid); found || err != nil {
			t.Errorf("half pair registered: %v %v", found, err)
		}
	}
	close(f.release)
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	d.file = original
	for _, fid := range []uint64{50, 60} {
		if _, err := d.Head(fid); err != nil {
			t.Fatal(err)
		}
	}
}

func TestNewServerRebuildsPool(t *testing.T) {
	first, second := newTestDiskID(t, 1), newTestDiskID(t, 2)
	pw, err := second.PrepareWrite(70, 3)
	if err != nil {
		t.Fatal(err)
	}
	if err = pw.WriteFrom(bytes.NewReader([]byte("two"))); err != nil {
		t.Fatal(err)
	}
	if err = pw.Commit(0); err != nil {
		t.Fatal(err)
	}
	p := pan.File{Version: 1, Devices: []pan.Device{{DevicePath: first.devicePath, DiskID: "1"}, {DevicePath: second.devicePath, DiskID: "2"}}}
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	path := t.TempDir() + "/pan.json"
	if err = os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig()
	cfg.ZramSnapshotDir = ""
	cfg.PanPath = path
	s, err := NewServer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if found, err := s.confirmed(70); !found || err != nil {
		t.Fatalf("second disk object missing: %v %v", found, err)
	}
	if size, err := s.headAny(70); size != 3 || err != nil {
		t.Fatalf("head: %d %v", size, err)
	}
}
