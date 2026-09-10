package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"bsos/internal/daemon"
)

func main() {
	var configPath string
	flag.StringVar(&configPath, "config", "", "path to bsosd.toml configuration file")

	cfg := daemon.DefaultConfig()

	panFlag := flag.String("pan", "", "pan.json path (overrides config)")
	grpcListenFlag := flag.String("grpc-listen", "", "gRPC listen address (overrides config)")
	maxPutFlag := flag.String("max-put", "", "max Put size e.g. 256MB (overrides config)")
	smallFilePow2Flag := flag.Int("small-file-pow2", -1, "small file threshold pow2 (overrides config)")
	chdTargetPFlag := flag.Float64("chd-target-p", -1, "CH_d target success probability (overrides config)")
	zramSnapshotDirFlag := flag.String("zram-snapshot-dir", "", "zram snapshot dir (overrides config; 'empty' or '' = disabled)")
	trimIntervalFlag := flag.Duration("trim-interval", 0, "trim interval e.g. 15m (overrides config)")
	flag.Parse()

	if configPath != "" {
		loaded, err := daemon.LoadConfig(configPath)
		if err != nil {
			log.Fatalf("bsosd: load config %s: %v", configPath, err)
		}
		cfg = loaded
	}

	flag.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "pan":
			cfg.PanPath = *panFlag
		case "grpc-listen":
			cfg.GRPCListen = *grpcListenFlag
		case "max-put":
			size, err := daemon.ParseSize(*maxPutFlag)
			if err != nil {
				log.Fatalf("bsosd: invalid -max-put: %v", err)
			}
			cfg.MaxPutBytes = size
		case "small-file-pow2":
			cfg.SmallFilePow2 = *smallFilePow2Flag
		case "chd-target-p":
			cfg.ChdTargetP = *chdTargetPFlag
		case "zram-snapshot-dir":
			cfg.ZramSnapshotDir = *zramSnapshotDirFlag
		case "trim-interval":
			cfg.TrimInterval = *trimIntervalFlag
		}
	})

	srv, err := daemon.NewServer(cfg)
	if err != nil {
		log.Fatalf("bsosd: %v", err)
	}
	defer srv.Close()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Printf("bsosd: received signal %v, shutting down...", sig)
		go func() {
			<-sigCh
			log.Printf("bsosd: forced exit on second signal")
			os.Exit(1)
		}()
		_ = srv.Close()
	}()

	if err := srv.Serve(cfg.GRPCListen); err != nil {
		log.Fatalf("bsosd: %v", err)
	}
}
