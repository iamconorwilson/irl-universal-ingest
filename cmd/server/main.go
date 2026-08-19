package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/iamconorwilson/irl-universal-ingest/internal/arbitration"
	"github.com/iamconorwilson/irl-universal-ingest/internal/config"
	"github.com/iamconorwilson/irl-universal-ingest/internal/health"
	"github.com/iamconorwilson/irl-universal-ingest/internal/ingest/rist"
	"github.com/iamconorwilson/irl-universal-ingest/internal/ingest/rtmp"
	"github.com/iamconorwilson/irl-universal-ingest/internal/ingest/srt"
	"github.com/iamconorwilson/irl-universal-ingest/internal/relay"
	"github.com/iamconorwilson/irl-universal-ingest/internal/stats"
)

func main() {
	configPath := flag.String("config", "config.yaml", "path to configuration file")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("[main] failed to load configuration: %v", err)
	}

	log.Printf("[main] starting IRL Universal Ingest (RTMP: %d, SRT: %d, SRTLA: %d, RIST: %d, stats: %d, output: %s)",
		cfg.RTMP.Port, cfg.SRT.Port, cfg.SRTLA.Port, cfg.RIST.Port, cfg.StatsPort, cfg.Output.URL())

	// Initialize FFmpeg relay
	streamRelay := relay.New(cfg.Output.URL())

	// Tracks component-level failures so /healthz reflects real health.
	healthTracker := health.NewTracker()

	// Initialize arbitration manager
	arbitrationManager := arbitration.NewManager(cfg.SourceTimeout, func(protocol, path string) {
		log.Printf("[arbitration] slot released for %s (%s)", protocol, path)
		streamRelay.StopSession()
	})

	// Initialize RTMP ingest adapter
	rtmpServer := rtmp.NewServer(cfg.RTMP.Port, cfg.Auth.AllowedPaths, arbitrationManager, streamRelay)
	if err := rtmpServer.Start(); err != nil {
		log.Fatalf("[main] failed to start RTMP server: %v", err)
	}

	// Initialize SRT & SRTLA ingest adapter (irl-srt-server)
	srtServer := srt.NewServer(srt.ServerOptions{
		SRTPort:        cfg.SRT.Port,
		SRTLAPort:      cfg.SRTLA.Port,
		PlayerPort:     cfg.SRT.PlayerPort,
		HTTPPort:       cfg.SRT.HTTPPort,
		LatencyMs:      cfg.SRT.LatencyMs,
		LatencyMaxMs:   cfg.SRT.LatencyMaxMs,
		IdleTimeoutSec: cfg.SRT.IdleTimeoutSec,
		Passphrase:     cfg.SRT.Passphrase,
		LogLevel:       cfg.LogLevel,
		AllowedPaths:   cfg.Auth.AllowedPaths,
		Manager:        arbitrationManager,
		Relay:          streamRelay,
		Health:         healthTracker,
	})
	if err := srtServer.Start(); err != nil {
		log.Fatalf("[main] failed to start SRT server: %v", err)
	}

	// Initialize RIST ingest adapter (ristreceiver)
	ristServer := rist.NewServer(rist.ServerOptions{
		RISTPort:     cfg.RIST.Port,
		BufferMs:     cfg.RIST.BufferMs,
		Profile:      cfg.RIST.Profile,
		Secret:       cfg.RIST.Secret,
		LogLevel:     cfg.LogLevel,
		AllowedPaths: cfg.Auth.AllowedPaths,
		Manager:      arbitrationManager,
		Relay:        streamRelay,
		Health:       healthTracker,
	})
	if err := ristServer.Start(); err != nil {
		log.Fatalf("[main] failed to start RIST server: %v", err)
	}

	// Initialize stats endpoint
	collector := stats.NewCollector(arbitrationManager, streamRelay, cfg.Auth.AllowedPaths)
	statsServer := stats.NewServer(cfg.StatsPort, collector, healthTracker)
	if err := statsServer.Start(); err != nil {
		log.Fatalf("[main] failed to start stats server: %v", err)
	}

	log.Println("[main] server is ready and accepting streams")

	// Wait for termination signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Println("[main] shutting down gracefully...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := rtmpServer.Close(); err != nil {
		log.Printf("[main] rtmp server shutdown error: %v", err)
	}
	if err := srtServer.Close(); err != nil {
		log.Printf("[main] srt server shutdown error: %v", err)
	}
	if err := ristServer.Close(); err != nil {
		log.Printf("[main] rist server shutdown error: %v", err)
	}
	if err := statsServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("[main] stats server shutdown error: %v", err)
	}
	streamRelay.StopSession()

	log.Println("[main] shutdown complete")
}
