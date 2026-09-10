package daemon

import (
	"context"
	"testing"
	"time"

	"github.com/echo983/BSOS/internal/daemon/bsospb"
)

func TestBonnieRPCAndCHDPow2(t *testing.T) {
	s, c := rpcFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := c.Bonnie(ctx, &bsospb.Empty{})
	if err != nil {
		t.Fatalf("Bonnie RPC: %v", err)
	}

	pow2, ok := s.bonnieCHDPow2()
	if !ok {
		t.Fatalf("expected bonnieCHDPow2 to return true")
	}
	if resp.GetChDPow2() != uint32(pow2) {
		t.Fatalf("Bonnie RPC returned %d, want %d", resp.GetChDPow2(), pow2)
	}
	if resp.GetChDPow2() == 0 {
		t.Fatalf("expected non-zero ChDPow2 on fresh disks")
	}
}

func TestBonnieExcludesZramAndCapsMaxPut(t *testing.T) {
	disk1 := newTestDiskID(t, 101)
	disk2 := newTestDiskID(t, 102)
	// Mock disk2 as zram device
	disk2.devicePath = "/dev/zram0"

	s := &Server{
		disks:      []*DeviceState{disk1, disk2},
		maxPut:     1 << 20, // 1MB cap
		chdTargetP: 0.2,
	}

	pow2, ok := s.bonnieCHDPow2()
	if !ok {
		t.Fatalf("expected bonnieCHDPow2 to succeed")
	}
	// 1MB is 2^20 bytes, so pow2 should be at most 20
	if pow2 > 20 {
		t.Fatalf("pow2 = %d exceeds maxPut cap of 20", pow2)
	}

	// Now set disk1 to unwritable/faulted and verify ok is false
	disk1.diskBytes = 0
	pow2Zero, okZero := s.bonnieCHDPow2()
	if okZero || pow2Zero != 0 {
		t.Fatalf("expected (0, false) when no non-zram writable disk exists, got (%d, %v)", pow2Zero, okZero)
	}
}
