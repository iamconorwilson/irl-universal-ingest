package srt

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/iamconorwilson/irl-universal-ingest/internal/arbitration"
	"github.com/iamconorwilson/irl-universal-ingest/internal/auth"
	"github.com/iamconorwilson/irl-universal-ingest/internal/relay"
)

// WebhookHandler processes lifecycle callbacks emitted by irl-srt-server.
type WebhookHandler struct {
	mu           sync.Mutex
	manager      *arbitration.Manager
	relay        *relay.Relay
	allowedPaths []string
	playerPort   int
	srtlaPort    int
	activeSlot   func()
	activePath   string
}

// NewWebhookHandler creates an HTTP handler for SLS lifecycle events.
func NewWebhookHandler(
	mgr *arbitration.Manager,
	rel *relay.Relay,
	allowedPaths []string,
	playerPort int,
	srtlaPort int,
) *WebhookHandler {
	return &WebhookHandler{
		manager:      mgr,
		relay:        rel,
		allowedPaths: allowedPaths,
		playerPort:   playerPort,
		srtlaPort:    srtlaPort,
	}
}

// ServeHTTP handles on_event_url requests from SLS.
func (h *WebhookHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	// Read body to support form-encoded and JSON payloads
	bodyBytes, _ := io.ReadAll(r.Body)
	_ = r.Body.Close()

	if strings.Contains(r.URL.Path, "stat") {
		h.handleStatsPush(w, bodyBytes)
		return
	}

	var jsonMap map[string]any
	var form url.Values
	if len(bodyBytes) > 0 {
		_ = json.Unmarshal(bodyBytes, &jsonMap)
		form, _ = url.ParseQuery(string(bodyBytes))
	}

	method := getParam(q, form, jsonMap, "method", "event", "action", "type", "cmd", "on_event", "state")
	roleName := getParam(q, form, jsonMap, "role_name", "role", "client_type", "client")
	srtURL := getParam(q, form, jsonMap, "srt_url", "url", "stream_id", "streamid", "stream_name", "stream", "live_key")

	normMethod := strings.ToLower(method)
	normRole := strings.ToLower(roleName)

	// Players are internal or read-only; allow without arbitration
	if normRole == "player" || normRole == "play" {
		w.WriteHeader(http.StatusOK)
		return
	}

	streamPath := extractStreamPath(srtURL)
	protocol := h.detectProtocol(r, srtURL)

	isClose := strings.Contains(normMethod, "close") ||
		strings.Contains(normMethod, "stop") ||
		strings.Contains(normMethod, "disconnect") ||
		strings.Contains(normMethod, "unpublish") ||
		strings.Contains(normMethod, "end") ||
		strings.Contains(normMethod, "del")

	isConnect := strings.Contains(normMethod, "connect") ||
		strings.Contains(normMethod, "publish") ||
		strings.Contains(normMethod, "start") ||
		strings.Contains(normMethod, "add") ||
		(normMethod == "" && srtURL != "" && (normRole == "publisher" || normRole == "publish"))

	if isClose {
		h.handleClose(w, streamPath)
		return
	}

	if isConnect {
		h.handleConnect(w, protocol, streamPath, r.RemoteAddr)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func getParam(q url.Values, form url.Values, jsonMap map[string]any, keys ...string) string {
	for _, k := range keys {
		if v := q.Get(k); v != "" {
			return v
		}
		if form != nil {
			if v := form.Get(k); v != "" {
				return v
			}
		}
		if jsonMap != nil {
			if v, ok := jsonMap[k]; ok {
				if s, ok := v.(string); ok && s != "" {
					return s
				}
			}
		}
	}
	return ""
}

// handleConnect validates authorization, acquires arbitration, and starts the relay.
func (h *WebhookHandler) handleConnect(w http.ResponseWriter, protocol, streamPath, remoteAddr string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// 1. Authorization check
	if !auth.IsAllowed(streamPath, h.allowedPaths) {
		log.Printf("[%s] rejected unauthorized path %q from %s", strings.ToLower(protocol), streamPath, remoteAddr)
		http.Error(w, "unauthorized path", http.StatusForbidden)
		return
	}

	// 2. Arbitration slot check
	acquired, release := h.manager.TryAcquire(protocol, streamPath)
	if !acquired {
		log.Printf("[%s] rejected stream %q: slot already occupied", strings.ToLower(protocol), streamPath)
		http.Error(w, "slot occupied", http.StatusConflict)
		return
	}

	h.activeSlot = release
	h.activePath = streamPath

	// 3. Start relay pulling from local SLS player port with zero loopback buffering
	playerInputURL := fmt.Sprintf("srt://127.0.0.1:%d?streamid=play%s&latency=20", h.playerPort, streamPath)
	if _, err := h.relay.StartSession(playerInputURL, streamPath); err != nil {
		log.Printf("[%s] failed to start relay session: %v", strings.ToLower(protocol), err)
		release()
		h.activeSlot = nil
		h.activePath = ""
		http.Error(w, "failed to start relay", http.StatusInternalServerError)
		return
	}

	log.Printf("[%s] acquired active ingest slot for path %q", strings.ToLower(protocol), streamPath)
	w.WriteHeader(http.StatusOK)
}

// handleClose releases the slot only if the event refers to the currently active stream path.
func (h *WebhookHandler) handleClose(w http.ResponseWriter, streamPath string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.activeSlot != nil && h.activePath == streamPath {
		h.activeSlot()
		h.activeSlot = nil
		h.activePath = ""
		h.relay.StopSession()
		log.Printf("[srt] stream closed and slot released for %q", streamPath)
	}

	w.WriteHeader(http.StatusOK)
}

// handleStatsPush processes stat_post_url pushes from SLS, keying liveness off publisher presence.
func (h *WebhookHandler) handleStatsPush(w http.ResponseWriter, bodyBytes []byte) {
	if len(bodyBytes) == 0 {
		w.WriteHeader(http.StatusOK)
		return
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	slot, active := h.manager.ActiveInfo()
	if !active || (slot.Protocol != "SRT" && slot.Protocol != "SRTLA") {
		w.WriteHeader(http.StatusOK)
		return
	}

	metric, err := ParseStats(bodyBytes, slot.Path)
	if err == nil && metric.Found {
		if metric.RTTMs > 0 {
			h.manager.SetRTT(metric.RTTMs)
		}
		if metric.BitrateKbps > 0 {
			h.manager.SetBitrate(metric.BitrateKbps)
		}
		h.manager.Touch()
	}

	w.WriteHeader(http.StatusOK)
}

// detectProtocol inspects query parameters and URLs to distinguish SRT vs SRTLA.
func (h *WebhookHandler) detectProtocol(r *http.Request, srtURL string) string {
	srtlaPortStr := fmt.Sprintf("%d", h.srtlaPort)
	if r.URL.Query().Get("local_port") == srtlaPortStr || strings.Contains(srtURL, ":"+srtlaPortStr) {
		return "SRTLA"
	}
	return "SRT"
}

// extractStreamPath normalizes SLS stream identifiers into standard URL paths.
func extractStreamPath(raw string) string {
	if raw == "" {
		return "/live/stream"
	}

	if strings.Contains(raw, "://") {
		if parsed, err := url.Parse(raw); err == nil {
			if sid := parsed.Query().Get("streamid"); sid != "" {
				raw = sid
			} else if parsed.Path != "" {
				raw = parsed.Path
			}
		}
	}

	raw = strings.TrimPrefix(raw, "publish/")
	raw = strings.TrimPrefix(raw, "play/")
	raw = strings.TrimPrefix(raw, "publish")
	raw = strings.TrimPrefix(raw, "play")

	return auth.NormalizePath(raw)
}
