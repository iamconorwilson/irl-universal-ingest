package rist

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/iamconorwilson/irl-universal-ingest/internal/arbitration"
	"github.com/iamconorwilson/irl-universal-ingest/internal/auth"
	"github.com/iamconorwilson/irl-universal-ingest/internal/health"
	"github.com/iamconorwilson/irl-universal-ingest/internal/relay"
)

// ServerOptions contains configuration parameters for the RIST ingest supervisor.
type ServerOptions struct {
	RISTPort     int
	PlayerPort   int
	BufferMs     int
	Profile      string
	Secret       string
	LogLevel     string
	AllowedPaths []string
	Manager      *arbitration.Manager
	Relay        *relay.Relay
	Health       *health.Tracker
}

// Server supervises the ristreceiver process and manages RIST stream arbitration.
type Server struct {
	opts       ServerOptions
	binaryPath string
	cmd        *exec.Cmd
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup

	mu                sync.Mutex
	activeSlot        func()
	activePath        string
	active            bool
	lastCNAME         string
	lastActivity      time.Time
	stopping          bool
	restartAttempt    int
	lastForcedRestart time.Time
}

// activityFreshness bounds how long the watchdog stays alive after the last observed traffic line.
const activityFreshness = 3 * time.Second

// restartBaseDelay/restartMaxDelay bound backoff for both unexpected exits and forced restarts.
const (
	restartBaseDelay     = 500 * time.Millisecond
	restartMaxDelay      = 30 * time.Second
	minForcedRestartGap  = 1 * time.Second
	scannerMaxTokenBytes = 1 << 20 // 1MB, well above default 64KB
)

// NewServer creates a new RIST server supervisor.
func NewServer(opts ServerOptions) *Server {
	if opts.PlayerPort <= 0 {
		opts.PlayerPort = opts.RISTPort + 100
	}
	if opts.BufferMs <= 0 {
		opts.BufferMs = 1000
	}
	if opts.Profile == "" {
		opts.Profile = "main"
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &Server{
		opts:   opts,
		ctx:    ctx,
		cancel: cancel,
	}
}

// BuildArgs constructs the command-line arguments for ristreceiver.
func (s *Server) BuildArgs() []string {
	inputURL := fmt.Sprintf("rist://@0.0.0.0:%d", s.opts.RISTPort)
	outputURL := fmt.Sprintf("udp://127.0.0.1:%d", s.opts.PlayerPort)

	profileCode := "1" // main profile default
	switch strings.ToLower(s.opts.Profile) {
	case "simple", "0":
		profileCode = "0"
	case "main", "1":
		profileCode = "1"
	case "advanced", "2":
		profileCode = "2"
	default:
		log.Printf("[rist] warning: unrecognized rist.profile %q, falling back to %q", s.opts.Profile, "main")
	}

	args := []string{
		"-i", inputURL,
		"-o", outputURL,
		"-p", profileCode,
		"-b", strconv.Itoa(s.opts.BufferMs),
		"-S", "1000",
		"-v", ristVerbosity(s.opts.LogLevel),
	}

	if s.opts.Secret != "" {
		args = append(args, "-s", s.opts.Secret)
	}

	return args
}

// ristVerbosity maps the configured log level onto ristreceiver's -v verbosity scale.
func ristVerbosity(logLevel string) string {
	switch strings.ToLower(logLevel) {
	case "debug":
		return "4"
	case "warn", "warning":
		return "2"
	case "error":
		return "1"
	default: // "info" and anything unrecognized
		return "3"
	}
}

// debugLogging reports whether raw ristreceiver output lines should be individually logged.
func (s *Server) debugLogging() bool {
	return strings.EqualFold(s.opts.LogLevel, "debug")
}

// setUnhealthy is a no-op if no health tracker was configured.
func (s *Server) setUnhealthy(err error) {
	if s.opts.Health != nil {
		s.opts.Health.SetUnhealthy("ristreceiver", err.Error())
	}
}

func (s *Server) clearUnhealthy() {
	if s.opts.Health != nil {
		s.opts.Health.ClearUnhealthy("ristreceiver")
	}
}

// Start launches the ristreceiver child process and starts the stream heartbeat.
func (s *Server) Start() error {
	bin, err := exec.LookPath("ristreceiver")
	if err != nil {
		log.Printf("[rist] ristreceiver binary not found on PATH; running in adapter mode")
		return nil
	}

	s.mu.Lock()
	s.binaryPath = bin
	if err := s.startProcessLocked(bin); err != nil {
		s.mu.Unlock()
		s.setUnhealthy(err)
		return err
	}
	s.mu.Unlock()

	s.wg.Add(1)
	go s.heartbeatLoop()

	return nil
}

// heartbeatLoop touches the watchdog only while recent traffic has actually been observed.
func (s *Server) heartbeatLoop() {
	defer s.wg.Done()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.mu.Lock()
			active := s.active
			fresh := active && time.Since(s.lastActivity) <= activityFreshness
			s.mu.Unlock()

			if fresh && s.opts.Relay.IsRunning() {
				s.opts.Manager.Touch()
			}
		}
	}
}

// startProcessLocked starts the ristreceiver process and monitors its output while holding s.mu.
func (s *Server) startProcessLocked(binaryPath string) error {
	args := s.BuildArgs()
	cmd := exec.CommandContext(s.ctx, binaryPath, args...)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("creating rist stdout pipe: %w", err)
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("creating rist stderr pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("starting ristreceiver: %w", err)
	}

	s.cmd = cmd
	s.clearUnhealthy()
	log.Printf("[rist] started ristreceiver daemon on port %d (PID %d, player port %d)", s.opts.RISTPort, cmd.Process.Pid, s.opts.PlayerPort)

	s.wg.Add(3)
	go s.monitorOutput(stdout)
	go s.monitorOutput(stderr)
	go s.reapProcess(cmd)

	return nil
}

// monitorOutput reads and processes log/stats lines from ristreceiver.
func (s *Server) monitorOutput(r io.Reader) {
	defer s.wg.Done()

	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), scannerMaxTokenBytes)

	for scanner.Scan() {
		line := scanner.Text()
		if s.debugLogging() {
			log.Printf("[rist] %s", line)
		}
		s.handleLogLine(line)
	}

	if err := scanner.Err(); err != nil {
		log.Printf("[rist] output scan error: %v", err)
	}
}

// reapProcess reaps ristreceiver on exit and schedules a restart unless it was a deliberate stop.
func (s *Server) reapProcess(cmd *exec.Cmd) {
	defer s.wg.Done()

	err := cmd.Wait()

	s.mu.Lock()
	isCurrent := s.cmd == cmd
	if isCurrent {
		s.cmd = nil
	}
	stopping := s.stopping
	binPath := s.binaryPath
	s.mu.Unlock()

	if !isCurrent || stopping {
		return
	}

	if err != nil {
		log.Printf("[rist] ristreceiver exited unexpectedly: %v", err)
	} else {
		log.Printf("[rist] ristreceiver exited")
	}

	select {
	case <-s.ctx.Done():
		return
	default:
	}

	s.mu.Lock()
	s.stopActiveStreamLocked()
	attempt := s.restartAttempt
	s.restartAttempt++
	s.mu.Unlock()

	delay := restartBaseDelay << attempt
	if delay > restartMaxDelay || delay <= 0 {
		delay = restartMaxDelay
	}
	log.Printf("[rist] restarting ristreceiver in %s (attempt %d)", delay, attempt+1)
	time.Sleep(delay)

	select {
	case <-s.ctx.Done():
		return
	default:
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.stopping && binPath != "" {
		if err := s.startProcessLocked(binPath); err != nil {
			log.Printf("[rist] ristreceiver restart failed: %v", err)
			s.setUnhealthy(err)
		}
	}
}

// handleLogLine parses log lines for connection lifecycle and stream statistics.
func (s *Server) handleLogLine(line string) {
	metric := ParseLogLine(line)

	s.mu.Lock()
	if metric.CNAME != "" {
		s.lastCNAME = metric.CNAME
	}
	s.mu.Unlock()

	if metric.Disconnected || strings.Contains(line, "is dead") || strings.Contains(line, "timed out") {
		s.handleDisconnect()
		return
	}

	if metric.Connected || metric.BitrateKbps > 0 || metric.RTTMs > 0 || strings.Contains(line, "data output") || strings.Contains(line, "peer") || strings.Contains(line, "Flow") {
		s.handleActivity(metric)
	}
}

// handleActivity processes incoming traffic, validates authentication, and manages arbitration.
func (s *Server) handleActivity(metric StatsMetric) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.lastActivity = time.Now()

	streamPath := s.lastCNAME
	if streamPath == "" {
		if s.activePath != "" {
			streamPath = s.activePath
		} else if len(s.opts.AllowedPaths) > 0 {
			streamPath = s.opts.AllowedPaths[0]
		} else {
			streamPath = "/live/stream"
		}
	}
	streamPath = auth.NormalizePath(streamPath)

	// 1. Authorization check
	if !auth.IsAllowed(streamPath, s.opts.AllowedPaths) {
		log.Printf("[rist] rejected unauthorized stream path %q", streamPath)
		s.stopActiveStreamLocked()
		s.restartReceiverLocked()
		return
	}

	// 2. Claim arbitration slot if not yet active
	if !s.active {
		acquired, release := s.opts.Manager.TryAcquire("RIST", streamPath)
		if !acquired {
			return
		}

		playerURL := fmt.Sprintf("udp://127.0.0.1:%d?overrun_nonfatal=1&fifo_size=500000", s.opts.PlayerPort)
		if _, err := s.opts.Relay.StartSession(playerURL, streamPath); err != nil {
			log.Printf("[rist] failed to start relay session: %v", err)
			release()
			return
		}

		s.active = true
		s.activeSlot = release
		s.activePath = streamPath
		log.Printf("[rist] acquired active ingest slot for path %q", streamPath)
	}

	// 3. Update metrics and touch watchdog
	s.opts.Manager.Touch()

	if metric.RTTMs > 0 {
		s.opts.Manager.SetRTT(float64(metric.RTTMs))
	}
	if metric.BitrateKbps > 0 {
		s.opts.Manager.SetBitrate(metric.BitrateKbps)
	}
}

// handleDisconnect releases the slot and stops the relay session.
func (s *Server) handleDisconnect() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stopActiveStreamLocked()
}

// stopActiveStreamLocked releases arbitration and stops relay.
func (s *Server) stopActiveStreamLocked() {
	if s.active {
		if s.activeSlot != nil {
			s.activeSlot()
			s.activeSlot = nil
		}
		s.opts.Relay.StopSession()
		s.active = false
		log.Printf("[rist] stream disconnected and slot released for %q", s.activePath)
		s.activePath = ""
	}
}

// restartReceiverLocked kills and restarts ristreceiver to sever an unauthorized connection.
func (s *Server) restartReceiverLocked() {
	if since := time.Since(s.lastForcedRestart); since < minForcedRestartGap {
		return
	}
	s.lastForcedRestart = time.Now()

	if s.cmd != nil && s.cmd.Process != nil {
		_ = s.cmd.Process.Kill()
		s.cmd = nil
	}

	select {
	case <-s.ctx.Done():
		return
	default:
		if s.binaryPath != "" {
			if err := s.startProcessLocked(s.binaryPath); err != nil {
				log.Printf("[rist] failed to restart ristreceiver: %v", err)
				s.setUnhealthy(err)
			}
		}
	}
}

// Close gracefully stops the ristreceiver daemon and cleans up resources.
func (s *Server) Close() error {
	s.mu.Lock()
	s.stopping = true
	s.mu.Unlock()

	s.cancel()

	s.mu.Lock()
	s.stopActiveStreamLocked()
	if s.cmd != nil && s.cmd.Process != nil {
		_ = s.cmd.Process.Kill()
	}
	s.mu.Unlock()

	s.wg.Wait()
	return nil
}
