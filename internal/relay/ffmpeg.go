package relay

import (
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
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
}

// New creates a new Relay instance with the specified base UDP output URL.
func New(outputBaseURL string) *Relay {
	return &Relay{
		outputBaseURL: outputBaseURL,
	}
}

// BuildOutputURL constructs the target UDP URL with the streamid and low-latency packet settings.
func (r *Relay) BuildOutputURL(path string) string {
	sep := "?"
	if strings.Contains(r.outputBaseURL, "?") {
		sep = "&"
	}
	baseWithParams := fmt.Sprintf("%s%spkt_size=1316&streamid=%s", r.outputBaseURL, sep, url.QueryEscape(path))
	return baseWithParams
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
	r.cmd = cmd
	r.done = done
	r.stdinPipe = stdin
	r.running = true

	go func(c *exec.Cmd, d chan struct{}) {
		err := c.Wait()
		close(d)
		r.mu.Lock()
		defer r.mu.Unlock()
		if r.cmd == c {
			r.running = false
			if err != nil && err.Error() != "signal: killed" && !strings.Contains(err.Error(), "killed") {
				log.Printf("[relay] ffmpeg process exited: %v", err)
			}
		}
	}(cmd, done)

	return stdin, nil
}

// StopSession terminates the active FFmpeg child process cleanly.
func (r *Relay) StopSession() {
	r.mu.Lock()
	defer r.mu.Unlock()
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
		select {
		case <-done:
		case <-time.After(300 * time.Millisecond):
			if cmd.Process != nil {
				_ = cmd.Process.Kill()
			}
			<-done
		}
		r.cmd = nil
		r.done = nil
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
