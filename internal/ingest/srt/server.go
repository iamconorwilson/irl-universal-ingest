package srt

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/iamconorwilson/irl-universal-ingest/internal/arbitration"
	"github.com/iamconorwilson/irl-universal-ingest/internal/relay"
)

// ServerOptions encapsulates settings for the SRT ingest adapter.
type ServerOptions struct {
	SRTPort        int
	SRTLAPort      int
	PlayerPort     int
	HTTPPort       int
	LatencyMs      int
	LatencyMaxMs   int
	IdleTimeoutSec int
	Passphrase     string
	LogLevel       string
	AllowedPaths   []string
	Manager        *arbitration.Manager
	Relay          *relay.Relay
}

// Server supervises the irl-srt-server daemon process and its integration hooks.
type Server struct {
	opts          ServerOptions
	webhookServer *http.Server
	webhookURL    string
	srtCmd        *exec.Cmd
	srtlaCmd      *exec.Cmd
	tempDir       string
	statsClient   *Client
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
}

// NewServer creates a new SRT ingest server supervisor.
func NewServer(opts ServerOptions) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{
		opts:   opts,
		ctx:    ctx,
		cancel: cancel,
	}
}

// Start launches the internal webhook server, writes sls.conf, and starts srt_server and srtla_rec.
func (s *Server) Start() error {
	// 1. Bind an ephemeral port for internal webhooks
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return fmt.Errorf("starting internal srt webhook listener: %w", err)
	}

	webhookPort := listener.Addr().(*net.TCPAddr).Port
	s.webhookURL = fmt.Sprintf("http://127.0.0.1:%d/sls/on_event", webhookPort)

	internalSRTLAPort := s.opts.SRTPort + 1
	if s.opts.SRTLAPort == internalSRTLAPort {
		internalSRTLAPort = s.opts.SRTPort + 10
	}

	handler := NewWebhookHandler(
		s.opts.Manager,
		s.opts.Relay,
		s.opts.AllowedPaths,
		s.opts.PlayerPort,
		internalSRTLAPort,
	)

	mux := http.NewServeMux()
	mux.Handle("/", handler)

	s.webhookServer = &http.Server{
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		_ = s.webhookServer.Serve(listener)
	}()

	// 2. Prepare temporary configuration file
	tempDir, err := os.MkdirTemp("", "irl-sls-*")
	if err != nil {
		_ = s.webhookServer.Close()
		return fmt.Errorf("creating sls temp directory: %w", err)
	}
	s.tempDir = tempDir

	pidPath := filepath.Join(tempDir, "sls_server.pid")
	_ = os.Remove(pidPath)

	configContent := GenerateSLSConfig(SLSConfigOptions{
		SRTPort:        s.opts.SRTPort,
		SRTLAPort:      internalSRTLAPort,
		PlayerPort:     s.opts.PlayerPort,
		HTTPPort:       s.opts.HTTPPort,
		LatencyMs:      s.opts.LatencyMs,
		LatencyMaxMs:   s.opts.LatencyMaxMs,
		IdleTimeoutSec: s.opts.IdleTimeoutSec,
		Passphrase:     s.opts.Passphrase,
		WebhookURL:     s.webhookURL,
		StatPostURL:    fmt.Sprintf("http://127.0.0.1:%d/sls/stat", webhookPort),
		LogLevel:       s.opts.LogLevel,
		PIDFile:        filepath.ToSlash(pidPath),
	})

	configPath := filepath.Join(tempDir, "sls.conf")
	if err := os.WriteFile(configPath, []byte(configContent), 0600); err != nil {
		_ = s.webhookServer.Close()
		_ = os.RemoveAll(tempDir)
		return fmt.Errorf("writing sls.conf: %w", err)
	}

	// 3. Launch srt_server child process if binary is present
	srtBinary, err := exec.LookPath("srt_server")
	if err != nil {
		srtBinary, err = exec.LookPath("sls")
	}

	if err == nil {
		srtCmd := exec.CommandContext(s.ctx, srtBinary, "-c", configPath)
		srtCmd.Dir = tempDir
		srtCmd.Stdout = os.Stdout
		srtCmd.Stderr = os.Stderr

		if err := srtCmd.Start(); err != nil {
			log.Printf("[srt] warning: failed to start srt_server process: %v", err)
		} else {
			s.srtCmd = srtCmd
			log.Printf("[srt] started irl-srt-server daemon (PID %d)", srtCmd.Process.Pid)
		}
	} else {
		log.Printf("[srt] srt_server binary not found on PATH; running in adapter mode")
	}

	// 4. Launch srtla_rec receiver proxy if SRTLA is enabled and binary is present
	if s.opts.SRTLAPort > 0 {
		if srtlaBinary, err := exec.LookPath("srtla_rec"); err == nil {
			srtlaCmd := exec.CommandContext(s.ctx, srtlaBinary, strconv.Itoa(s.opts.SRTLAPort), "127.0.0.1", strconv.Itoa(internalSRTLAPort))
			srtlaCmd.Stdout = os.Stdout
			srtlaCmd.Stderr = os.Stderr
			if err := srtlaCmd.Start(); err != nil {
				log.Printf("[srtla] warning: failed to start srtla_rec: %v", err)
			} else {
				s.srtlaCmd = srtlaCmd
				log.Printf("[srtla] started srtla_rec receiver (port %d -> 127.0.0.1:%d, PID %d)", s.opts.SRTLAPort, internalSRTLAPort, srtlaCmd.Process.Pid)
			}
		} else {
			log.Printf("[srtla] srtla_rec binary not found on PATH; running in adapter mode")
		}
	}

	// 5. Initialize stats polling client
	s.statsClient = NewClient(fmt.Sprintf("http://127.0.0.1:%d/stats", s.opts.HTTPPort))

	s.wg.Add(1)
	go s.pollStatsLoop()

	return nil
}

// pollStatsLoop periodically retrieves metrics from SLS to update arbitration status.
func (s *Server) pollStatsLoop() {
	defer s.wg.Done()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	var lastBytes uint64

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			slot, active := s.opts.Manager.ActiveInfo()
			if !active || (slot.Protocol != "SRT" && slot.Protocol != "SRTLA") {
				lastBytes = 0
				continue
			}

			metric, err := s.statsClient.FetchMetrics(s.ctx, slot.Path)
			if err != nil {
				continue
			}

			hasActiveTraffic := false

			if metric.RTTMs > 0 {
				s.opts.Manager.SetRTT(metric.RTTMs)
				hasActiveTraffic = true
			}

			if metric.BitrateKbps > 0 {
				s.opts.Manager.SetBitrate(metric.BitrateKbps)
				hasActiveTraffic = true
			}

			if metric.Bytes > lastBytes && lastBytes > 0 {
				s.opts.Manager.AddBytes(metric.Bytes - lastBytes)
				hasActiveTraffic = true
			}
			if metric.Bytes > 0 {
				lastBytes = metric.Bytes
			}

			if hasActiveTraffic {
				s.opts.Manager.Touch()
			}
		}
	}
}

// Close terminates the srt_server daemon, cleans temporary files, and stops servers.
func (s *Server) Close() error {
	s.cancel()

	if s.webhookServer != nil {
		_ = s.webhookServer.Close()
	}

	if s.srtCmd != nil && s.srtCmd.Process != nil {
		done := make(chan struct{})
		go func() {
			_ = s.srtCmd.Wait()
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(500 * time.Millisecond):
			_ = s.srtCmd.Process.Kill()
		}
	}

	if s.srtlaCmd != nil && s.srtlaCmd.Process != nil {
		_ = s.srtlaCmd.Process.Kill()
	}

	if s.tempDir != "" {
		_ = os.RemoveAll(s.tempDir)
	}

	s.wg.Wait()
	return nil
}
