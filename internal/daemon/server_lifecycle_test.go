package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"bsos/internal/blk"
	"bsos/internal/daemon/bsospb"
	"bsos/internal/pan"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestServerHealthAndDegraded(t *testing.T) {
	d1 := newTestDiskID(t, 10)
	d2 := newTestDiskID(t, 20)

	s := &Server{
		disks: []*DeviceState{d1, d2},
	}

	if s.DiskCount() != 2 {
		t.Fatalf("DiskCount = %d, want 2", s.DiskCount())
	}
	ids := s.DiskIDs()
	if len(ids) != 2 || ids[0] != 10 || ids[1] != 20 {
		t.Fatalf("DiskIDs = %v, want [10, 20]", ids)
	}
	if s.Degraded() {
		t.Fatalf("expected Degraded = false")
	}

	ctx := context.Background()
	h, err := s.Health(ctx, &bsospb.Empty{})
	if err != nil {
		t.Fatalf("Health RPC: %v", err)
	}
	if !h.Ok {
		t.Fatalf("expected healthy status on clean disks")
	}

	// 1. Degradation flag set
	s.degraded = true
	if !s.Degraded() {
		t.Fatalf("expected Degraded = true")
	}
	h, err = s.Health(ctx, &bsospb.Empty{})
	if err != nil || h.Ok {
		t.Fatalf("expected degraded Health to report Ok=false, got %v, err=%v", h.GetOk(), err)
	}
	s.degraded = false

	// 2. Disk index error
	d1.indexMu.Lock()
	d1.indexErr = fmt.Errorf("simulated index fault")
	d1.indexMu.Unlock()

	h, err = s.Health(ctx, &bsospb.Empty{})
	if err != nil || h.Ok {
		t.Fatalf("expected faulted disk to report Ok=false, got %v, err=%v", h.GetOk(), err)
	}

	// Clear index error
	d1.indexMu.Lock()
	d1.indexErr = nil
	d1.indexMu.Unlock()

	// 3. Disk too small / unwritable
	origBytes := d2.diskBytes
	d2.diskBytes = blk.GridStart // zero capacity
	h, err = s.Health(ctx, &bsospb.Empty{})
	if err != nil || h.Ok {
		t.Fatalf("expected unwritable disk to report Ok=false, got %v, err=%v", h.GetOk(), err)
	}
	d2.diskBytes = origBytes

	// Healthy again
	h, err = s.Health(ctx, &bsospb.Empty{})
	if err != nil || !h.Ok {
		t.Fatalf("expected restored health to report Ok=true, got %v, err=%v", h.GetOk(), err)
	}
}

func TestServerServeAndGracefulStop(t *testing.T) {
	d := newTestDiskID(t, 0x11223344)
	d.file.Close()

	dir := t.TempDir()
	panPath := filepath.Join(dir, "pan.json")
	pool := pan.File{
		Version: 1,
		Devices: []pan.Device{
			{DevicePath: d.devicePath, DiskID: "0x11223344", Status: "match"},
		},
	}
	raw, err := json.Marshal(pool)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(panPath, raw, 0644); err != nil {
		t.Fatal(err)
	}

	// Pick a free port
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	lis.Close()

	cfg := DefaultConfig()
	cfg.PanPath = panPath
	cfg.GRPCListen = addr
	cfg.TrimInterval = 0 // disable background trim for test

	srv, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	serveErrCh := make(chan error, 1)
	go func() {
		serveErrCh <- srv.Serve(addr)
	}()

	// Wait for server to accept connection
	var cc *grpc.ClientConn
	for i := 0; i < 50; i++ {
		time.Sleep(20 * time.Millisecond)
		cc, err = grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			c := bsospb.NewBSOSClient(cc)
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			h, hErr := c.Health(ctx, &bsospb.Empty{})
			cancel()
			if hErr == nil && h.Ok {
				break
			}
			cc.Close()
			cc = nil
		}
	}
	if cc == nil {
		t.Fatalf("failed to connect to serving daemon within timeout")
	}
	cc.Close()

	// Graceful close
	if err := srv.Close(); err != nil {
		t.Fatalf("srv.Close: %v", err)
	}

	select {
	case serveErr := <-serveErrCh:
		if serveErr != nil {
			t.Fatalf("Serve returned unexpected error on graceful stop: %v", serveErr)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("Serve did not exit within 3s of Close")
	}
}
