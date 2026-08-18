package stats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/iamconorwilson/irl-universal-ingest/internal/health"
)

// Server provides the HTTP monitoring endpoint for sidecar scripts.
type Server struct {
	port       int
	collector  *Collector
	health     *health.Tracker
	httpServer *http.Server
}

// NewServer creates a new stats HTTP server.
func NewServer(port int, collector *Collector, tracker *health.Tracker) *Server {
	return &Server{
		port:      port,
		collector: collector,
		health:    tracker,
	}
}

// Start binds the configured port synchronously so a bind conflict is returned, not silently swallowed.
func (s *Server) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/stats", s.handleStats)
	mux.HandleFunc("/healthz", s.handleHealthz)

	s.httpServer = &http.Server{
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	if err != nil {
		return fmt.Errorf("binding stats listener on port %d: %w", s.port, err)
	}

	go func() {
		if err := s.httpServer.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("[stats] server error: %v", err)
		}
	}()

	return nil
}

// handleStats returns the JSON-encoded metrics of the current ingest session.
func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	data := s.collector.Collect(r.Context())
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(data)
}

// handleHealthz fails the check on any tracked component failure or arbitration/relay inconsistency.
func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	problems := map[string]string{}
	if s.health != nil {
		problems = s.health.Problems()
	}
	if !s.collector.Healthy() {
		problems["relay"] = "arbitration slot active but relay is not running"
	}

	w.Header().Set("Content-Type", "application/json")
	if len(problems) > 0 {
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(problems)
		return
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}` + "\n"))
}

// Shutdown stops the HTTP server cleanly.
func (s *Server) Shutdown(ctx context.Context) error {
	if s.httpServer == nil {
		return nil
	}
	return s.httpServer.Shutdown(ctx)
}
