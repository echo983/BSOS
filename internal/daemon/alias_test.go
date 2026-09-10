package daemon

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/zeebo/xxh3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/echo983/BSOS/internal/daemon/bsospb"
)

func TestOneHopAliasEndToEnd(t *testing.T) {
	s, c := rpcFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	content := []byte("hello one-hop alias world")
	aliasFor := uint64(0x1111222233334444)
	jumpCode := byte(0x07)

	jumpPayload := append(append([]byte(nil), content...), jumpCode)
	jumpFID := xxh3.Hash(jumpPayload)

	// Write via RPC Put with alias_for
	err := rpcPut(ctx, c, jumpFID, aliasFor, uint64(len(jumpPayload)), jumpPayload)
	if err != nil {
		t.Fatalf("rpcPut with alias_for: %v", err)
	}

	// 1. Read alias: Get(aliasFor) returns logical content (trimmed)
	gotAlias, err := rpcRead(ctx, c, &bsospb.GetRequest{Fid: aliasFor})
	if err != nil {
		t.Fatalf("Get(aliasFor): %v", err)
	}
	if !bytes.Equal(gotAlias, content) {
		t.Fatalf("Get(aliasFor) mismatch: got %q, want %q", gotAlias, content)
	}

	// 2. Head(aliasFor) returns logical size
	headAlias, err := c.Head(ctx, &bsospb.HeadRequest{Fid: aliasFor})
	if err != nil {
		t.Fatalf("Head(aliasFor): %v", err)
	}
	if headAlias.GetSize() != uint64(len(content)) {
		t.Fatalf("Head(aliasFor) size: got %d, want %d", headAlias.GetSize(), len(content))
	}

	// 3. Read target directly: Get(jumpFID) returns full stored payload (including jump byte)
	gotTarget, err := rpcRead(ctx, c, &bsospb.GetRequest{Fid: jumpFID})
	if err != nil {
		t.Fatalf("Get(jumpFID): %v", err)
	}
	if !bytes.Equal(gotTarget, jumpPayload) {
		t.Fatalf("Get(jumpFID) mismatch: got %q, want %q", gotTarget, jumpPayload)
	}

	// 4. Head(jumpFID) returns full stored size
	headTarget, err := c.Head(ctx, &bsospb.HeadRequest{Fid: jumpFID})
	if err != nil {
		t.Fatalf("Head(jumpFID): %v", err)
	}
	if headTarget.GetSize() != uint64(len(jumpPayload)) {
		t.Fatalf("Head(jumpFID) size: got %d, want %d", headTarget.GetSize(), len(jumpPayload))
	}

	// 5. Range read on alias: e.g. [6, 13] -> "one-hop"
	rangeReq := &bsospb.GetRequest{
		Fid:        aliasFor,
		HasRange:   true,
		RangeStart: 6,
		RangeEnd:   13,
	}
	gotRange, err := rpcRead(ctx, c, rangeReq)
	if err != nil {
		t.Fatalf("Get range on alias: %v", err)
	}
	expectedRange := content[6:13]
	if !bytes.Equal(gotRange, expectedRange) {
		t.Fatalf("Get range on alias mismatch: got %q, want %q", gotRange, expectedRange)
	}

	// Verify both fids exist on the same disk
	foundAliasDisk := -1
	foundTargetDisk := -1
	for idx, d := range s.disks {
		if ok, _ := d.Confirmed(aliasFor); ok {
			foundAliasDisk = idx
		}
		if ok, _ := d.Confirmed(jumpFID); ok {
			foundTargetDisk = idx
		}
	}
	if foundAliasDisk == -1 || foundTargetDisk == -1 {
		t.Fatalf("alias or target not found on any disk: alias=%d target=%d", foundAliasDisk, foundTargetDisk)
	}
	if foundAliasDisk != foundTargetDisk {
		t.Fatalf("docs/DESIGN.md §3.11 violation: alias on disk %d, target on disk %d", foundAliasDisk, foundTargetDisk)
	}
}

func TestOneHopAliasRejections(t *testing.T) {
	_, c := rpcFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	payload := []byte("sample payload\x01")
	fid := xxh3.Hash(payload)

	// 1. alias_for == fid: rejected with InvalidArgument
	err := rpcPut(ctx, c, fid, fid, uint64(len(payload)), payload)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument for alias_for == fid, got %v", err)
	}

	// 2. total_size < 2 when alias_for != 0: rejected with InvalidArgument
	err = rpcPut(ctx, c, fid, 0x1234, 1, []byte("x"))
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument for alias_for with total_size < 2, got %v", err)
	}

	// 3. Write normal object X
	contentX := []byte("normal object X")
	fidX := xxh3.Hash(contentX)
	if err := rpcPut(ctx, c, fidX, 0, uint64(len(contentX)), contentX); err != nil {
		t.Fatalf("put normal object X: %v", err)
	}

	// 4. Try to write with alias_for: fidX (cannot overwrite existing object X): rejected with AlreadyExists
	payloadY := []byte("payload Y attempting to alias X\x02")
	fidY := xxh3.Hash(payloadY)
	err = rpcPut(ctx, c, fidY, fidX, uint64(len(payloadY)), payloadY)
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("expected AlreadyExists when alias_for points to existing object, got %v", err)
	}

	// 5. Try to write object where fid is already registered as an alias target: rejected with AlreadyExists
	// First write a valid alias pair: A -> B
	aliasA := uint64(0xAAAA0001)
	payloadB := []byte("target B content\x01")
	fidB := xxh3.Hash(payloadB)
	if err := rpcPut(ctx, c, fidB, aliasA, uint64(len(payloadB)), payloadB); err != nil {
		t.Fatalf("put valid alias pair: %v", err)
	}

	// Try chained alias: alias_for: fidB -> fidC (trying to alias an existing target)
	payloadC := []byte("payload C\x03")
	fidC := xxh3.Hash(payloadC)
	err = rpcPut(ctx, c, fidC, fidB, uint64(len(payloadC)), payloadC)
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("expected AlreadyExists for chained alias (aliasing target fidB), got %v", err)
	}

	// Try chained alias: alias_for: aliasA -> fidD (trying to alias an existing alias pointer)
	payloadD := []byte("payload D\x04")
	fidD := xxh3.Hash(payloadD)
	err = rpcPut(ctx, c, fidD, aliasA, uint64(len(payloadD)), payloadD)
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("expected AlreadyExists for chained alias (aliasing pointer aliasA), got %v", err)
	}
}

func TestOneHopAliasClientJumpRetryAlgorithm(t *testing.T) {
	_, c := rpcFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Base content that client wants to store under target alias FID
	targetAliasFID := uint64(0xCAFE0001)
	baseContent := []byte("data that client wants to place via jump")

	// Client follows docs/CLIENT_SPEC.md §3 algorithm:
	// for jumpCode in 1..255:
	//   jumpData = content + byte(jumpCode)
	//   jumpFID  = xxh3_64(jumpData)
	//   try Put(PutHeader{fid: jumpFID, total_size: len(jumpData), alias_for: fid})
	//   if it succeeds: done
	var succeededCode byte
	var succeededFID uint64
	for jumpCode := 1; jumpCode <= 255; jumpCode++ {
		jumpData := append(append([]byte(nil), baseContent...), byte(jumpCode))
		jumpFID := xxh3.Hash(jumpData)

		err := rpcPut(ctx, c, jumpFID, targetAliasFID, uint64(len(jumpData)), jumpData)
		if err == nil {
			succeededCode = byte(jumpCode)
			succeededFID = jumpFID
			break
		}
		if status.Code(err) != codes.AlreadyExists {
			t.Fatalf("unexpected error during jump retry loop: %v", err)
		}
	}

	if succeededCode == 0 {
		t.Fatalf("client jump retry loop failed to place object")
	}

	// Verify object is readable via targetAliasFID
	readBack, err := rpcRead(ctx, c, &bsospb.GetRequest{Fid: targetAliasFID})
	if err != nil {
		t.Fatalf("read via alias: %v", err)
	}
	if !bytes.Equal(readBack, baseContent) {
		t.Fatalf("read via alias mismatch: got %q, want %q", readBack, baseContent)
	}

	// Verify object is also readable via succeededFID
	readDirect, err := rpcRead(ctx, c, &bsospb.GetRequest{Fid: succeededFID})
	if err != nil {
		t.Fatalf("read direct via jumpFID: %v", err)
	}
	expectedDirect := append(append([]byte(nil), baseContent...), succeededCode)
	if !bytes.Equal(readDirect, expectedDirect) {
		t.Fatalf("read direct mismatch: got %q, want %q", readDirect, expectedDirect)
	}
}

func TestOneHopAliasTrimImmunity(t *testing.T) {
	disk := newTestDiskID(t, 0xD15C5)
	disk.cfg.TrimTempDir = t.TempDir()
	disk.cfg.TrimMinFileCount = 100
	disk.cfg.TrimThresholdRatio = 0.20

	// 1. Write an alias pair
	aliasFor := uint64(0x99990001)
	logicalContent := []byte("alias content intended to be immune from trim")
	jumpPayload := append(append([]byte(nil), logicalContent...), 0x01)
	jumpFID := xxh3.Hash(jumpPayload)
	writeTestObject(t, disk, jumpFID, aliasFor, jumpPayload)

	// 2. Write 105 small regular objects to trigger Trim
	type regularObj struct {
		fid  uint64
		data []byte
	}
	var regular []regularObj
	for i := 0; i < 105; i++ {
		data := []byte(fmt.Sprintf("regular-small-obj-%04d", i))
		fid := xxh3.Hash(data)
		writeTestObject(t, disk, fid, 0, data)
		regular = append(regular, regularObj{fid: fid, data: data})
	}

	// 3. Verify collectTrimCandidatesLocked excludes both aliasFor and jumpFID
	candidates, _, _, _, err := disk.collectTrimCandidatesLocked()
	if err != nil {
		t.Fatalf("collectTrimCandidatesLocked: %v", err)
	}
	for _, c := range candidates {
		if c.fid == aliasFor {
			t.Fatalf("aliasFor was selected for trim")
		}
		if c.fid == jumpFID {
			t.Fatalf("jumpFID (alias target) was selected for trim; violates docs/DESIGN.md §3.8")
		}
	}

	// 4. Run Trim
	if err := disk.maybeTrim(); err != nil {
		t.Fatalf("maybeTrim: %v", err)
	}

	// 5. Verify regular objects were packed
	if len(disk.packed) != len(regular) {
		t.Fatalf("expected %d packed objects, got %d", len(regular), len(disk.packed))
	}
	if _, isPacked := disk.packed[aliasFor]; isPacked {
		t.Fatalf("aliasFor unexpectedly found in packed map")
	}
	if _, isPacked := disk.packed[jumpFID]; isPacked {
		t.Fatalf("jumpFID unexpectedly found in packed map")
	}

	// 6. Verify alias still reads correctly after Trim
	gotAlias, sizeAlias, err := disk.Get(aliasFor)
	if err != nil {
		t.Fatalf("Get(aliasFor) after trim: %v", err)
	}
	if sizeAlias != uint64(len(logicalContent)) || !bytes.Equal(gotAlias, logicalContent) {
		t.Fatalf("Get(aliasFor) mismatch after trim")
	}

	gotTarget, sizeTarget, err := disk.Get(jumpFID)
	if err != nil {
		t.Fatalf("Get(jumpFID) after trim: %v", err)
	}
	if sizeTarget != uint64(len(jumpPayload)) || !bytes.Equal(gotTarget, jumpPayload) {
		t.Fatalf("Get(jumpFID) mismatch after trim")
	}

	// 7. Verify reopen after trim preserves alias
	reopened, err := OpenDevice(disk.devicePath, disk.diskID, 64)
	if err != nil {
		t.Fatalf("OpenDevice after trim: %v", err)
	}
	defer reopened.Close()

	gotAliasReopen, _, err := reopened.Get(aliasFor)
	if err != nil || !bytes.Equal(gotAliasReopen, logicalContent) {
		t.Fatalf("reopened Get(aliasFor) failed: %v", err)
	}
}
