package stats

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// Server provides the HTTP monitoring endpoint for sidecar scripts.
type Server struct {
	port       int
	collector  *Collector
	httpServer *http.Server
}

// NewServer creates a new stats HTTP server.
func NewServer(port int, collector *Collector) *Server {
	return &Server{
		port:      port,
		collector: collector,
	}
}

// Start begins serving HTTP requests on the configured port.
func (s *Server) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/stats", s.handleStats)
	mux.HandleFunc("/healthz", s.handleHealthz)

	s.httpServer = &http.Server{
		Addr:         fmt.Sprintf(":%d", s.port),
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	go func() {
		_ = s.httpServer.ListenAndServe()
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

// handleHealthz returns a simple OK status check.
func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// Shutdown stops the HTTP server cleanly.
func (s *Server) Shutdown(ctx context.Context) error {
	if s.httpServer == nil {
		return nil
	}
	return s.httpServer.Shutdown(ctx)
}
