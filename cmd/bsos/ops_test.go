package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/echo983/BSOS/internal/blk"
	"github.com/echo983/BSOS/internal/daemon"
	"github.com/echo983/BSOS/internal/pan"
	"github.com/echo983/BSOS/pkg/client"
)

func startCLITestDaemon(t *testing.T) (string, func()) {
	t.Helper()
	dir := t.TempDir()
	diskPath := filepath.Join(dir, "disk.img")
	f, err := os.Create(diskPath)
	if err != nil {
		t.Fatal(err)
	}
	const diskSize = blk.GridStart + (16 << 20)
	if err := f.Truncate(diskSize); err != nil {
		t.Fatal(err)
	}
	hdr := make([]byte, blk.HeaderBytes)
	copy(hdr, "NBSS")
	binary.LittleEndian.PutUint16(hdr[4:6], blk.FormatVersion)
	binary.LittleEndian.PutUint64(hdr[8:16], 0x55667788)
	if _, err := f.WriteAt(hdr, 0); err != nil {
		t.Fatal(err)
	}
	f.Close()

	panPath := filepath.Join(dir, "pan.json")
	pool := pan.File{
		Version: 1,
		Devices: []pan.Device{
			{DevicePath: diskPath, DiskID: "0x55667788", Status: "match"},
		},
	}
	raw, err := json.Marshal(pool)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(panPath, raw, 0644); err != nil {
		t.Fatal(err)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	lis.Close()

	cfg := daemon.DefaultConfig()
	cfg.PanPath = panPath
	cfg.GRPCListen = addr
	cfg.TrimInterval = 0

	srv, err := daemon.NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	go func() {
		_ = srv.Serve(addr)
	}()

	for i := 0; i < 50; i++ {
		time.Sleep(20 * time.Millisecond)
		c, err := client.New(addr)
		if err == nil {
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			ok, hErr := c.Health(ctx)
			cancel()
			c.Close()
			if hErr == nil && ok {
				break
			}
		}
	}

	cleanup := func() {
		_ = srv.Close()
	}
	return addr, cleanup
}

func captureOutput(t *testing.T, fn func() int) (int, string, string) {
	t.Helper()
	oldStdout := os.Stdout
	oldStderr := os.Stderr

	rOut, wOut, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	rErr, wErr, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}

	os.Stdout = wOut
	os.Stderr = wErr

	outCh := make(chan string)
	errCh := make(chan string)

	go func() {
		var buf bytes.Buffer
		_, _ = io.Copy(&buf, rOut)
		outCh <- buf.String()
	}()
	go func() {
		var buf bytes.Buffer
		_, _ = io.Copy(&buf, rErr)
		errCh <- buf.String()
	}()

	code := fn()

	_ = wOut.Close()
	_ = wErr.Close()
	os.Stdout = oldStdout
	os.Stderr = oldStderr

	stdoutStr := <-outCh
	stderrStr := <-errCh

	return code, stdoutStr, stderrStr
}

func TestCLIPutGetHeadLifecycle(t *testing.T) {
	addr, cleanup := startCLITestDaemon(t)
	defer cleanup()

	dir := t.TempDir()
	inputFile := filepath.Join(dir, "input.txt")
	testData := []byte("Hello, BSOS command line interface!\nThis is a complete round-trip test.")
	if err := os.WriteFile(inputFile, testData, 0644); err != nil {
		t.Fatal(err)
	}

	// 1. Health check
	code, stdout, stderr := captureOutput(t, func() int {
		return runHealth([]string{"-addr", addr})
	})
	if code != 0 || !strings.Contains(stdout, "OK") {
		t.Fatalf("runHealth failed: code=%d, out=%q, err=%q", code, stdout, stderr)
	}

	// 2. Bonnie check
	code, stdout, stderr = captureOutput(t, func() int {
		return runBonnie([]string{"-addr", addr, "-json"})
	})
	if code != 0 {
		t.Fatalf("runBonnie failed: code=%d, err=%q", code, stderr)
	}
	var bonnieResp map[string]any
	if err := json.Unmarshal([]byte(stdout), &bonnieResp); err != nil {
		t.Fatalf("runBonnie invalid json: %v, out=%q", err, stdout)
	}

	// 3. Put from file
	code, stdout, stderr = captureOutput(t, func() int {
		return runPut([]string{"-addr", addr, inputFile})
	})
	if code != 0 {
		t.Fatalf("runPut failed: code=%d, out=%q, err=%q", code, stdout, stderr)
	}
	fidHex := strings.TrimSpace(stdout)
	wantFIDHex := fmt.Sprintf("0x%016x", client.ComputeFID(testData))
	if !strings.HasPrefix(fidHex, wantFIDHex) {
		t.Fatalf("runPut fid mismatch: got %q, want prefix %q", fidHex, wantFIDHex)
	}

	// 4. Head check
	code, stdout, stderr = captureOutput(t, func() int {
		return runHead([]string{"-addr", addr, "-json", wantFIDHex})
	})
	if code != 0 {
		t.Fatalf("runHead failed: code=%d, err=%q", code, stderr)
	}
	var headResp map[string]any
	if err := json.Unmarshal([]byte(stdout), &headResp); err != nil {
		t.Fatalf("runHead json parse failed: %v", err)
	}
	if headResp["size"].(float64) != float64(len(testData)) {
		t.Fatalf("runHead size mismatch: got %v, want %d", headResp["size"], len(testData))
	}

	// 5. Get to file
	getOutputFile := filepath.Join(dir, "output.txt")
	code, stdout, stderr = captureOutput(t, func() int {
		return runGet([]string{"-addr", addr, wantFIDHex, getOutputFile})
	})
	if code != 0 {
		t.Fatalf("runGet to file failed: code=%d, err=%q", code, stderr)
	}
	gotData, err := os.ReadFile(getOutputFile)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotData, testData) {
		t.Fatalf("retrieved file content mismatch: got %q, want %q", gotData, testData)
	}

	// 6. Get with -range to stdout
	code, stdout, stderr = captureOutput(t, func() int {
		return runGet([]string{"-addr", addr, "-range", "7-11", wantFIDHex})
	})
	if code != 0 {
		t.Fatalf("runGet range failed: code=%d, err=%q", code, stderr)
	}
	if stdout != string(testData[7:11]) {
		t.Fatalf("range content mismatch: got %q, want %q", stdout, testData[7:11])
	}

	// 7. Get non-existent fid returns 1 and E_NOT_FOUND
	code, stdout, stderr = captureOutput(t, func() int {
		return runGet([]string{"-addr", addr, "0xDEADBEEF00000000"})
	})
	if code != 1 || !strings.Contains(stderr, "E_NOT_FOUND") {
		t.Fatalf("expected E_NOT_FOUND on missing fid, got code=%d, err=%q", code, stderr)
	}
}

func TestCLIPutJSON(t *testing.T) {
	addr, cleanup := startCLITestDaemon(t)
	defer cleanup()

	dir := t.TempDir()
	inputFile := filepath.Join(dir, "json_payload.bin")
	payload := []byte("JSON structured output verification payload for CLI")
	if err := os.WriteFile(inputFile, payload, 0644); err != nil {
		t.Fatal(err)
	}

	// Put with -json
	code, stdout, stderr := captureOutput(t, func() int {
		return runPut([]string{"-addr", addr, "-json", inputFile})
	})
	if code != 0 {
		t.Fatalf("runPut -json failed: code=%d, err=%q", code, stderr)
	}

	var res map[string]any
	if err := json.Unmarshal([]byte(stdout), &res); err != nil {
		t.Fatalf("failed to unmarshal JSON: %v, raw=%q", err, stdout)
	}

	wantFIDHex := fmt.Sprintf("0x%016x", client.ComputeFID(payload))
	if res["fid"] != wantFIDHex {
		t.Fatalf("fid mismatch: got %v, want %v", res["fid"], wantFIDHex)
	}
	if res["target_fid"] != wantFIDHex {
		t.Fatalf("target_fid mismatch: got %v, want %v", res["target_fid"], wantFIDHex)
	}
	if res["size"].(float64) != float64(len(payload)) {
		t.Fatalf("size mismatch: got %v, want %v", res["size"], len(payload))
	}
	if res["jumps"].(float64) != 0 {
		t.Fatalf("jumps mismatch: got %v, want 0", res["jumps"])
	}
}
