package main

import (
	"flag"
	"log"

	"bsos/internal/daemon"
)

func main() {
	cfg := daemon.DefaultConfig()
	flag.StringVar(&cfg.PanPath, "pan", cfg.PanPath, "pan.json path")
	flag.StringVar(&cfg.GRPCListen, "grpc-listen", cfg.GRPCListen, "gRPC listen address")
	flag.IntVar(&cfg.SmallFilePow2, "small-file-pow2", cfg.SmallFilePow2, "small file threshold (4KB * 2^n)")
	flag.Float64Var(&cfg.ChdTargetP, "chd-target-p", cfg.ChdTargetP, "CH_d target success probability")
	flag.StringVar(&cfg.ZramSnapshotDir, "zram-snapshot-dir", cfg.ZramSnapshotDir, "zram snapshot dir to auto-load at startup (empty = disabled)")
	flag.Parse()

	srv, err := daemon.NewServer(cfg)
	if err != nil {
		log.Fatalf("bsosd: %v", err)
	}
	defer srv.Close()

	if err := srv.Serve(cfg.GRPCListen); err != nil {
		log.Fatalf("bsosd: %v", err)
	}
}
