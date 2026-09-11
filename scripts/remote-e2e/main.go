package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/echo983/BSOS/pkg/client"
)

func discoverRemoteAddr() (string, error) {
	if addr := os.Getenv("BSOS_REMOTE_ADDR"); addr != "" {
		return addr, nil
	}
	configPath := "config/test-host.local.md"
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		configPath = "../../config/test-host.local.md"
	}
	raw, err := os.ReadFile(configPath)
	if err != nil {
		return "", fmt.Errorf("read %s: %w (or set BSOS_REMOTE_ADDR)", configPath, err)
	}
	re := regexp.MustCompile(`debian@([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)`)
	m := re.FindStringSubmatch(string(raw))
	if len(m) < 2 {
		return "", fmt.Errorf("no IP found in %s", configPath)
	}
	return m[1] + ":19090", nil
}

func main() {
	addrFlag := flag.String("addr", "", "BSOS remote server address (host:port)")
	flag.Parse()

	remoteAddr := *addrFlag
	if remoteAddr == "" {
		discovered, err := discoverRemoteAddr()
		if err != nil {
			log.Fatalf("discover remote addr: %v", err)
		}
		remoteAddr = discovered
	}

	log.Printf("[INIT] Running End-to-End Tests: Local Client -> Remote Server at %s", remoteAddr)

	c, err := client.New(remoteAddr)
	if err != nil {
		log.Fatalf("[FAIL] connect to %s: %v", remoteAddr, err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// -------------------------------------------------------------
	// Phase 1: Health & Bonnie RPCs
	// -------------------------------------------------------------
	log.Printf("==> Phase 1: Checking Remote Health & Bonnie RPCs...")
	hOk, err := c.Health(ctx)
	if err != nil || !hOk {
		log.Fatalf("[FAIL] Health RPC: ok=%v, err=%v", hOk, err)
	}
	log.Printf("  [PASS] Health RPC returned Ok=true")

	chdPow2, err := c.Bonnie(ctx)
	if err != nil {
		log.Fatalf("[FAIL] Bonnie RPC: %v", err)
	}
	log.Printf("  [PASS] Bonnie RPC returned ch_d_pow2=%d (max placement estimate: %d bytes)", chdPow2, uint64(1)<<chdPow2)

	// -------------------------------------------------------------
	// Phase 2: Small Object Round-Trip (zram fast tier, 32 KB)
	// -------------------------------------------------------------
	log.Printf("==> Phase 2: Small Object Round-Trip (32 KB across WAN)...")
	smallData := make([]byte, 32<<10)
	_, _ = rand.Read(smallData)
	copy(smallData, []byte(fmt.Sprintf("BSOS-WAN-Small-%d:", time.Now().UnixNano())))
	smallFID := client.ComputeFID(smallData)
	smallSHA := sha256.Sum256(smallData)

	start := time.Now()
	putFID, err := c.PutBytes(ctx, smallData)
	if err != nil {
		log.Fatalf("[FAIL] PutBytes small: %v", err)
	}
	if putFID != smallFID {
		log.Fatalf("[FAIL] small FID mismatch: got 0x%x, want 0x%x", putFID, smallFID)
	}
	log.Printf("  [PASS] PutBytes 32KB finished in %v (FID=0x%016x)", time.Since(start), smallFID)

	// Head
	size, err := c.Head(ctx, smallFID)
	if err != nil || size != uint64(len(smallData)) {
		log.Fatalf("[FAIL] Head small: size=%d, err=%v", size, err)
	}
	log.Printf("  [PASS] Head returned size=%d", size)

	// Get
	start = time.Now()
	getSmall, err := c.GetBytes(ctx, smallFID)
	if err != nil {
		log.Fatalf("[FAIL] GetBytes small: %v", err)
	}
	if !bytes.Equal(getSmall, smallData) {
		log.Fatalf("[FAIL] small content mismatch!")
	}
	getSHA := sha256.Sum256(getSmall)
	if getSHA != smallSHA {
		log.Fatalf("[FAIL] small SHA-256 mismatch!")
	}
	log.Printf("  [PASS] GetBytes 32KB finished in %v, SHA-256=%x matched perfectly", time.Since(start), getSHA)

	// Range Get
	part, err := c.GetBytes(ctx, smallFID, client.Range{Start: 16, End: 128})
	if err != nil || !bytes.Equal(part, smallData[16:128]) {
		log.Fatalf("[FAIL] small range GetBytes: err=%v, len=%d", err, len(part))
	}
	log.Printf("  [PASS] Range read [16, 128) verified")

	// -------------------------------------------------------------
	// Phase 3: Large Multi-Megabyte Streaming (Disk tier, 6 MiB)
	// -------------------------------------------------------------
	log.Printf("==> Phase 3: Large Streaming Object (6 MiB in 1 MiB chunks across WAN)...")
	const largeSize = 6 << 20
	largeData := make([]byte, largeSize)
	_, _ = rand.Read(largeData)
	copy(largeData, []byte(fmt.Sprintf("BSOS-WAN-Large-%d:", time.Now().UnixNano())))
	largeSHA := sha256.Sum256(largeData)

	start = time.Now()
	res, err := c.PutWithJumpRetry(ctx, largeData)
	if err != nil {
		log.Fatalf("[FAIL] streaming Put 6 MiB: %v", err)
	}
	putDuration := time.Since(start)
	putSpeed := float64(largeSize) / (1024 * 1024) / putDuration.Seconds()
	log.Printf("  [PASS] Streamed 6 MiB Put to remote VPS in %v (%.2f MB/s, jumps=%d)", putDuration, putSpeed, res.JumpsTaken)

	start = time.Now()
	getLarge, err := c.GetBytes(ctx, res.FID)
	if err != nil {
		log.Fatalf("[FAIL] GetBytes 6 MiB: %v", err)
	}
	getDuration := time.Since(start)
	getSpeed := float64(largeSize) / (1024 * 1024) / getDuration.Seconds()
	if !bytes.Equal(getLarge, largeData) {
		log.Fatalf("[FAIL] 6 MiB content mismatch!")
	}
	largeGetSHA := sha256.Sum256(getLarge)
	if largeGetSHA != largeSHA {
		log.Fatalf("[FAIL] 6 MiB SHA-256 mismatch!")
	}
	log.Printf("  [PASS] Streamed 6 MiB Get from remote VPS in %v (%.2f MB/s), SHA-256=%x matched perfectly", getDuration, getSpeed, largeGetSHA)

	// -------------------------------------------------------------
	// Phase 3b: O_DIRECT Alignment-Stress Object (non-chunk-aligned
	// remainder). Phase 3's 6 MiB object lands exactly on 1 MiB
	// directChunkBytes boundaries (internal/daemon/directio.go) — the
	// least interesting case for the server's O_DIRECT streaming
	// aligned-chunk writer. This object is sized to deliberately straddle
	// a chunk boundary with a non-trivial remainder, so a real O_DIRECT
	// write on the remote VPS actually exercises the mixed
	// real-bytes-plus-zero-pad chunk path, not just clean multiples.
	// -------------------------------------------------------------
	log.Printf("==> Phase 3b: O_DIRECT Alignment-Stress Object (2 MiB + 12345 B, non-chunk-aligned)...")
	const directChunkBytes = 1 << 20
	const stressSize = 2*directChunkBytes + 12345
	stressData := make([]byte, stressSize)
	_, _ = rand.Read(stressData)
	copy(stressData, []byte(fmt.Sprintf("BSOS-WAN-ODirectStress-%d:", time.Now().UnixNano())))
	stressSHA := sha256.Sum256(stressData)

	stressRes, err := c.PutWithJumpRetry(ctx, stressData)
	if err != nil {
		log.Fatalf("[FAIL] streaming Put %d B alignment-stress object: %v", stressSize, err)
	}
	getStress, err := c.GetBytes(ctx, stressRes.FID)
	if err != nil {
		log.Fatalf("[FAIL] GetBytes alignment-stress object: %v", err)
	}
	if !bytes.Equal(getStress, stressData) {
		log.Fatalf("[FAIL] alignment-stress object content mismatch!")
	}
	stressGetSHA := sha256.Sum256(getStress)
	if stressGetSHA != stressSHA {
		log.Fatalf("[FAIL] alignment-stress object SHA-256 mismatch!")
	}
	log.Printf("  [PASS] %d B object (2 chunks + non-aligned remainder) round-tripped through real O_DIRECT, SHA-256=%x matched perfectly", stressSize, stressGetSHA)

	// -------------------------------------------------------------
	// Phase 4: Conflict and Not-Found Handling across WAN
	// -------------------------------------------------------------
	log.Printf("==> Phase 4: Validating Remote Conflict & Not-Found Semantics...")
	// Duplicate Put with same content must return conflict
	_, err = c.PutBytes(ctx, smallData)
	if !client.IsConflict(err) {
		log.Fatalf("[FAIL] expected IsConflict on duplicate Put, got %v", err)
	}
	log.Printf("  [PASS] Duplicate Put returned AlreadyExists conflict as expected")

	badFID := smallFID ^ 0xDEADBEEFCAFE0000
	_, err = c.Head(ctx, badFID)
	if !client.IsNotFound(err) {
		log.Fatalf("[FAIL] expected IsNotFound on missing Head, got %v", err)
	}
	_, err = c.GetBytes(ctx, badFID)
	if !client.IsNotFound(err) {
		log.Fatalf("[FAIL] expected IsNotFound on missing Get, got %v", err)
	}
	log.Printf("  [PASS] Non-existent FID returned NotFound as expected")

	// -------------------------------------------------------------
	// Phase 5: Concurrent Client Operations Across WAN
	// -------------------------------------------------------------
	log.Printf("==> Phase 5: Testing Concurrent Writes Across WAN (8 parallel workers)...")
	const numWorkers = 8
	var wg sync.WaitGroup
	errCh := make(chan error, numWorkers)

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			data := make([]byte, 64<<10) // 64 KB each
			_, _ = rand.Read(data)
			copy(data, []byte(fmt.Sprintf("worker-%d-%d:", workerID, time.Now().UnixNano())))
			res, err := c.PutWithJumpRetry(ctx, data)
			if err != nil {
				errCh <- fmt.Errorf("worker %d put: %w", workerID, err)
				return
			}
			fid := res.FID
			headSize, err := c.Head(ctx, fid)
			if err != nil || headSize != uint64(len(data)) {
				errCh <- fmt.Errorf("worker %d head: size=%d, err=%w", workerID, headSize, err)
				return
			}
			readBack, err := c.GetBytes(ctx, fid)
			if err != nil || !bytes.Equal(readBack, data) {
				errCh <- fmt.Errorf("worker %d get mismatch: err=%v", workerID, err)
				return
			}
		}(w)
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		log.Fatalf("[FAIL] concurrent worker failed: %v", err)
	}
	log.Printf("  [PASS] All %d concurrent workers completed successfully with 100%% integrity", numWorkers)

	// -------------------------------------------------------------
	// Phase 6: Retry-Safety Readback Helper Across WAN
	// -------------------------------------------------------------
	log.Printf("==> Phase 6: Testing VerifyContent Retry-Safety Helper...")
	matched, err := c.VerifyContent(ctx, smallFID, smallData)
	if err != nil || !matched {
		log.Fatalf("[FAIL] VerifyContent identical: matched=%v, err=%v", matched, err)
	}
	matched, err = c.VerifyContent(ctx, smallFID, []byte("different data"))
	if err != nil || matched {
		log.Fatalf("[FAIL] VerifyContent different: matched=%v, err=%v", matched, err)
	}
	log.Printf("  [PASS] VerifyContent retry-safety verification verified")

	// -------------------------------------------------------------
	// Phase 7: Local CLI Binary Against Remote Server End-to-End
	// -------------------------------------------------------------
	cliPath := filepath.Join("bin", "bsos")
	if _, err := os.Stat(cliPath); os.IsNotExist(err) {
		cliPath = filepath.Join("..", "..", "bin", "bsos")
	}
	if _, err := os.Stat(cliPath); err != nil {
		cmd := exec.Command("go", "build", "-o", cliPath, "github.com/echo983/BSOS/cmd/bsos")
		if out, err := cmd.CombinedOutput(); err != nil {
			log.Fatalf("[FAIL] build bsos CLI: %v\n%s", err, string(out))
		}
	}

	// 7.1 CLI Health
	cmd := exec.Command(cliPath, "health", "-addr", remoteAddr)
	out, err := cmd.CombinedOutput()
	if err != nil || !strings.Contains(string(out), "OK") {
		log.Fatalf("[FAIL] CLI health: %v, out=%s", err, string(out))
	}
	log.Printf("  [PASS] CLI 'bsos health' -> OK")

	// 7.2 CLI Bonnie
	cmd = exec.Command(cliPath, "bonnie", "-addr", remoteAddr, "-json")
	out, err = cmd.CombinedOutput()
	if err != nil || !strings.Contains(string(out), "ch_d_pow2") {
		log.Fatalf("[FAIL] CLI bonnie: %v, out=%s", err, string(out))
	}
	log.Printf("  [PASS] CLI 'bsos bonnie -json' -> %s", strings.TrimSpace(string(out)))

	// 7.3 CLI Put from File
	tmpIn := filepath.Join(os.TempDir(), fmt.Sprintf("bsos_cli_remote_in_%d.bin", time.Now().UnixNano()))
	cliData := []byte("End-to-End CLI Remote WAN Verification Content!\nTimestamp: " + time.Now().String())
	if err := os.WriteFile(tmpIn, cliData, 0600); err != nil {
		log.Fatalf("write tmpIn: %v", err)
	}
	defer os.Remove(tmpIn)

	cmd = exec.Command(cliPath, "put", "-addr", remoteAddr, tmpIn)
	out, err = cmd.CombinedOutput()
	if err != nil {
		log.Fatalf("[FAIL] CLI put: %v, out=%s", err, string(out))
	}
	cliFIDStr := strings.Fields(string(out))[0]
	log.Printf("  [PASS] CLI 'bsos put' -> FID=%s", cliFIDStr)

	// 7.4 CLI Head
	cmd = exec.Command(cliPath, "head", "-addr", remoteAddr, "-json", cliFIDStr)
	out, err = cmd.CombinedOutput()
	if err != nil || !strings.Contains(string(out), fmt.Sprintf("%d", len(cliData))) {
		log.Fatalf("[FAIL] CLI head: %v, out=%s", err, string(out))
	}
	log.Printf("  [PASS] CLI 'bsos head' -> %s", strings.TrimSpace(string(out)))

	// 7.5 CLI Get to File
	tmpOut := filepath.Join(os.TempDir(), fmt.Sprintf("bsos_cli_remote_out_%d.bin", time.Now().UnixNano()))
	defer os.Remove(tmpOut)

	cmd = exec.Command(cliPath, "get", "-addr", remoteAddr, cliFIDStr, tmpOut)
	out, err = cmd.CombinedOutput()
	if err != nil {
		log.Fatalf("[FAIL] CLI get: %v, out=%s", err, string(out))
	}
	readBack, err := os.ReadFile(tmpOut)
	if err != nil || !bytes.Equal(readBack, cliData) {
		log.Fatalf("[FAIL] CLI get content mismatch!")
	}
	log.Printf("  [PASS] CLI 'bsos get' readback file matches input 100%%")

	// 7.6 CLI Get with Range
	cmd = exec.Command(cliPath, "get", "-addr", remoteAddr, "-range", "0-10", cliFIDStr)
	out, err = cmd.CombinedOutput()
	if err != nil || !bytes.Equal(out, cliData[0:10]) {
		log.Fatalf("[FAIL] CLI get -range mismatch: got %q, want %q", string(out), string(cliData[0:10]))
	}
	log.Printf("  [PASS] CLI 'bsos get -range 0-10' -> %q matched", string(out))

	// 7.7 CLI Conflict with -no-jump
	cmd = exec.Command(cliPath, "put", "-addr", remoteAddr, "-no-jump", tmpIn)
	out, err = cmd.CombinedOutput()
	if err == nil {
		log.Fatalf("[FAIL] expected CLI put -no-jump to fail on duplicate, but succeeded!")
	}
	if !strings.Contains(string(out), "E_CONFLICT") {
		log.Fatalf("[FAIL] expected E_CONFLICT in output, got: %s", string(out))
	}
	log.Printf("  [PASS] CLI 'bsos put -no-jump' correctly rejected duplicate with E_CONFLICT")

	log.Printf("==========================================================================")
	log.Printf("ALL LOCAL-TO-REMOTE END-TO-END TESTS PASSED WITH 100%% SUCCESS!")
	log.Printf("==========================================================================")
}
