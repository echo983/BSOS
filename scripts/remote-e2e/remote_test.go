package main

import (
	"os"
	"testing"
)

func TestRemoteHostE2E(t *testing.T) {
	if _, err := os.Stat("../../config/test-host.local.md"); os.IsNotExist(err) && os.Getenv("BSOS_REMOTE_ADDR") == "" {
		t.Skip("skipping remote host E2E test: no config/test-host.local.md or BSOS_REMOTE_ADDR")
	}
	main()
}
