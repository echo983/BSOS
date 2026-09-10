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
