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
	"sync"
	"time"

	"github.com/iamconorwilson/irl-universal-ingest/internal/arbitration"
	"github.com/iamconorwilson/irl-universal-ingest/internal/health"
	"github.com/iamconorwilson/irl-universal-ingest/internal/relay"
	"github.com/iamconorwilson/irl-universal-ingest/internal/safego"
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
	Health         *health.Tracker
}

// Server supervises the irl-srt-server daemon process and its integration hooks.
type Server struct {
	opts          ServerOptions
	webhookServer *http.Server
	webhookURL    string
	srtCmd        *exec.Cmd
	srtlaCmd      *exec.Cmd
	tempDir       string
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

	internalSRTLAPort := s.pickInternalSRTLAPort()

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
	safego.Go("srt.webhookServer", func() {
		defer s.wg.Done()
		_ = s.webhookServer.Serve(listener)
	})

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
			s.setUnhealthy("srt_server", err)
		} else {
			s.srtCmd = srtCmd
			log.Printf("[srt] started irl-srt-server daemon (PID %d)", srtCmd.Process.Pid)

			// Wait for irl-srt-server to initialize and begin listening before starting downstream dependencies
			if err := waitForServerReady(s.ctx, s.opts.HTTPPort, 5*time.Second); err != nil {
				log.Printf("[srt] warning: irl-srt-server readiness check: %v", err)
				s.setUnhealthy("srt_server", err)
			} else {
				log.Printf("[srt] irl-srt-server is ready and listening")
				s.clearUnhealthy("srt_server")
			}
		}
	} else {
		log.Printf("[srt] srt_server binary not found on PATH; running in adapter mode")
	}

	// 4. Launch srtla_rec receiver proxy once irl-srt-server is ready
	if s.opts.SRTLAPort > 0 {
		if srtlaBinary, err := exec.LookPath("srtla_rec"); err == nil {
			args := BuildSRTLAArgs(s.opts.SRTLAPort, "127.0.0.1", internalSRTLAPort, s.opts.LogLevel)
			srtlaCmd := exec.CommandContext(s.ctx, srtlaBinary, args...)
			srtlaCmd.Stdout = os.Stdout
			srtlaCmd.Stderr = os.Stderr
			if err := srtlaCmd.Start(); err != nil {
				log.Printf("[srtla] warning: failed to start srtla_rec: %v", err)
				s.setUnhealthy("srtla_rec", err)
			} else {
				s.srtlaCmd = srtlaCmd
				log.Printf("[srtla] started srtla_rec receiver (port %d -> 127.0.0.1:%d, PID %d)", s.opts.SRTLAPort, internalSRTLAPort, srtlaCmd.Process.Pid)
				s.clearUnhealthy("srtla_rec")
			}
		} else {
			log.Printf("[srtla] srtla_rec binary not found on PATH; running in adapter mode")
		}
	}

	return nil
}

// pickInternalSRTLAPort picks a loopback port that doesn't collide with any other configured port.
func (s *Server) pickInternalSRTLAPort() int {
	taken := map[int]bool{
		s.opts.SRTPort:    true,
		s.opts.SRTLAPort:  true,
		s.opts.PlayerPort: true,
		s.opts.HTTPPort:   true,
	}
	for offset := 1; offset < 100; offset++ {
		candidate := s.opts.SRTPort + offset
		if !taken[candidate] {
			return candidate
		}
	}
	// Extremely unlikely fallback: every nearby port is taken.
	return s.opts.SRTPort + 100
}

// setUnhealthy is a no-op if no health tracker was configured.
func (s *Server) setUnhealthy(component string, err error) {
	if s.opts.Health != nil {
		s.opts.Health.SetUnhealthy(component, err.Error())
	}
}

func (s *Server) clearUnhealthy(component string) {
	if s.opts.Health != nil {
		s.opts.Health.ClearUnhealthy(component)
	}
}

// waitForServerReady polls the irl-srt-server HTTP endpoint until it is accepting requests or times out.
func waitForServerReady(ctx context.Context, httpPort int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: 200 * time.Millisecond}
	url := fmt.Sprintf("http://127.0.0.1:%d/stats", httpPort)

	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err == nil {
			req.Header.Set("Authorization", InternalAPIKey)
			resp, err := client.Do(req)
			if err == nil {
				resp.Body.Close()
				return nil
			}
		}

		time.Sleep(50 * time.Millisecond)
	}

	return fmt.Errorf("timed out waiting for irl-srt-server on port %d", httpPort)
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
