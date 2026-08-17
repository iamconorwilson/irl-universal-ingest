package srt

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/iamconorwilson/irl-universal-ingest/internal/arbitration"
	"github.com/iamconorwilson/irl-universal-ingest/internal/relay"
)

func TestWebhookHandlerAuthorization(t *testing.T) {
	mgr := arbitration.NewManager(5*time.Second, nil)
	rel := relay.New("udp://127.0.0.1:8888")
	allowedPaths := []string{"/live/stream"}

	handler := NewWebhookHandler(mgr, rel, allowedPaths, 8190, 6600)

	// Disallowed path should return 403
	req := httptest.NewRequest(http.MethodGet, "/sls/on_event?method=on_connect&role_name=publisher&srt_url=publish/live/unauthorized", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden for unauthorized path, got %d", rec.Code)
	}

	// Allowed path should return 200 OK
	reqOK := httptest.NewRequest(http.MethodGet, "/sls/on_event?method=on_connect&role_name=publisher&srt_url=publish/live/stream", nil)
	recOK := httptest.NewRecorder()
	handler.ServeHTTP(recOK, reqOK)

	if recOK.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for allowed path, got %d", recOK.Code)
	}

	// Verify slot is acquired
	slot, active := mgr.ActiveInfo()
	if !active {
		t.Fatalf("expected active slot after successful on_connect")
	}
	if slot.Protocol != "SRT" {
		t.Errorf("expected protocol SRT, got %s", slot.Protocol)
	}
	if slot.Path != "/live/stream" {
		t.Errorf("expected path /live/stream, got %s", slot.Path)
	}

	// Second connection on another stream should return 409 Conflict (slot occupied)
	reqConflict := httptest.NewRequest(http.MethodGet, "/sls/on_event?method=on_connect&role_name=publisher&srt_url=publish/live/stream", nil)
	recConflict := httptest.NewRecorder()
	handler.ServeHTTP(recConflict, reqConflict)

	if recConflict.Code != http.StatusConflict {
		t.Fatalf("expected 409 Conflict when slot is occupied, got %d", recConflict.Code)
	}

	// Disconnect event should release slot
	reqClose := httptest.NewRequest(http.MethodGet, "/sls/on_event?method=on_close&role_name=publisher&srt_url=publish/live/stream", nil)
	recClose := httptest.NewRecorder()
	handler.ServeHTTP(recClose, reqClose)

	if recClose.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on close, got %d", recClose.Code)
	}

	_, activeAfter := mgr.ActiveInfo()
	if activeAfter {
		t.Fatalf("expected slot to be released after on_close")
	}
}

func TestWebhookHandlerSRTLADetection(t *testing.T) {
	mgr := arbitration.NewManager(5*time.Second, nil)
	rel := relay.New("udp://127.0.0.1:8888")
	allowedPaths := []string{"/live/stream"}

	handler := NewWebhookHandler(mgr, rel, allowedPaths, 8190, 6600)

	req := httptest.NewRequest(http.MethodGet, "/sls/on_event?method=on_connect&role_name=publisher&local_port=6600&srt_url=publish/live/stream", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}

	slot, active := mgr.ActiveInfo()
	if !active {
		t.Fatalf("expected active slot")
	}
	if slot.Protocol != "SRTLA" {
		t.Errorf("expected protocol SRTLA, got %s", slot.Protocol)
	}
}
