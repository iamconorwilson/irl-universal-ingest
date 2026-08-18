package relay

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
)

// restartBaseDelay and restartMaxDelay bound the backoff for auto-restarting ffmpeg after an unexpected exit.
const (
	restartBaseDelay = 500 * time.Millisecond
	restartMaxDelay  = 30 * time.Second
)

// Relay manages the FFmpeg child process remuxing incoming streams to MPEG-TS UDP.
type Relay struct {
	mu            sync.Mutex
	outputBaseURL string
	cmd           *exec.Cmd
	stdinPipe     io.WriteCloser
	done          chan struct{}
	currentURL    string
	running       bool

	// sessionID guards against a pending restart clobbering a session that has since superseded it.
	sessionID uint64
	stopped   bool
	lastInput string
	lastPath  string
	attempt   int
}

// New creates a new Relay instance with the specified base UDP output URL.
func New(outputBaseURL string) *Relay {
	return &Relay{
		outputBaseURL: outputBaseURL,
	}
}

// BuildOutputURL constructs the target UDP URL with low-latency packet settings.
func (r *Relay) BuildOutputURL(path string) string {
	sep := "?"
	if strings.Contains(r.outputBaseURL, "?") {
		sep = "&"
	}
	return fmt.Sprintf("%s%spkt_size=1316", r.outputBaseURL, sep)
}

// StartSession launches an FFmpeg process configured for low-latency stream copying.
// If input is empty or "pipe:0", it reads from standard input and returns a WriteCloser.
// If input is a URL or file path, it reads directly from that source.
func (r *Relay) StartSession(input, path string) (io.WriteCloser, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.running {
		r.stopUnlocked()
	}

	r.stopped = false
	r.lastInput = input
	r.lastPath = path
	r.attempt = 0

	return r.startLocked(input, path)
}

// startLocked spawns the ffmpeg process for the given input/path. Callers must hold r.mu.
func (r *Relay) startLocked(input, path string) (io.WriteCloser, error) {
	targetURL := r.BuildOutputURL(path)
	r.currentURL = targetURL

	isPipe := input == "" || input == "pipe:0"
	args := []string{
		"-hide_banner",
		"-loglevel", "error",
		"-fflags", "+nobuffer+flush_packets",
		"-flags", "+low_delay",
	}

	if isPipe {
		args = append(args,
			"-probesize", "32768",
			"-analyzeduration", "0",
			"-f", "flv",
			"-i", "pipe:0",
		)
	} else {
		args = append(args,
			"-fflags", "+discardcorrupt",
			"-err_detect", "ignore_err",
			"-probesize", "500000",
			"-analyzeduration", "1000000",
			"-i", input,
			"-map", "0",
		)
	}

	args = append(args,
		"-c", "copy",
		"-muxdelay", "0",
		"-flush_packets", "1",
		"-f", "mpegts",
		targetURL,
	)

	cmd := exec.Command("ffmpeg", args...)
	cmd.Stderr = os.Stderr

	var stdin io.WriteCloser
	if isPipe {
		var err error
		stdin, err = cmd.StdinPipe()
		if err != nil {
			return nil, fmt.Errorf("creating ffmpeg stdin pipe: %w", err)
		}
	}

	if err := cmd.Start(); err != nil {
		if stdin != nil {
			_ = stdin.Close()
		}
		return nil, fmt.Errorf("starting ffmpeg process: %w", err)
	}

	done := make(chan struct{})
	r.sessionID++
	mySession := r.sessionID
	r.cmd = cmd
	r.done = done
	r.stdinPipe = stdin
	r.running = true

	go r.watch(cmd, done, mySession)

	return stdin, nil
}

// watch reaps ffmpeg on exit and schedules a restart unless it was a deliberate stop.
func (r *Relay) watch(cmd *exec.Cmd, done chan struct{}, mySession uint64) {
	err := cmd.Wait()
	close(done)

	r.mu.Lock()
	stale := r.sessionID != mySession
	if !stale {
		r.running = false
	}
	stopped := r.stopped
	input, path := r.lastInput, r.lastPath
	r.mu.Unlock()

	if stale || stopped {
		return
	}

	var exitErr *exec.ExitError
	if err != nil && !errors.As(err, &exitErr) {
		log.Printf("[relay] ffmpeg wait error: %v", err)
	} else if err != nil {
		log.Printf("[relay] ffmpeg exited unexpectedly: %v", err)
	}

	r.scheduleRestart(mySession, input, path)
}

// scheduleRestart backs off, then restarts ffmpeg unless the session was stopped or superseded meanwhile.
func (r *Relay) scheduleRestart(mySession uint64, input, path string) {
	r.mu.Lock()
	attempt := r.attempt
	r.attempt++
	r.mu.Unlock()

	delay := restartBaseDelay << attempt
	if delay > restartMaxDelay || delay <= 0 {
		delay = restartMaxDelay
	}

	log.Printf("[relay] restarting ffmpeg in %s (attempt %d)", delay, attempt+1)
	time.Sleep(delay)

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.stopped || r.sessionID != mySession {
		return
	}

	if _, err := r.startLocked(input, path); err != nil {
		log.Printf("[relay] ffmpeg restart failed: %v", err)
	}
}

// StopSession terminates the active FFmpeg process and disables auto-restart for it.
func (r *Relay) StopSession() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stopped = true
	r.stopUnlocked()
}

// stopUnlocked cleanly closes stdin to allow FFmpeg to write its trailer before terminating.
func (r *Relay) stopUnlocked() {
	if r.stdinPipe != nil {
		_ = r.stdinPipe.Close()
		r.stdinPipe = nil
	}

	if r.cmd != nil {
		done := r.done
		cmd := r.cmd
		r.cmd = nil
		r.done = nil
		select {
		case <-done:
		case <-time.After(300 * time.Millisecond):
			if cmd.Process != nil {
				_ = cmd.Process.Kill()
			}
			<-done
		}
	}

	r.running = false
	r.currentURL = ""
}

// CurrentURL returns the actively running output target URL.
func (r *Relay) CurrentURL() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.currentURL
}

// IsRunning reports whether an FFmpeg relay process is currently active.
func (r *Relay) IsRunning() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.running
}
