package relay

import (
	"testing"
)

func TestBuildOutputURL(t *testing.T) {
	rel := New("udp://127.0.0.1:8888")
	out := rel.BuildOutputURL("/live/stream")
	expected := "udp://127.0.0.1:8888?pkt_size=1316&streamid=%2Flive%2Fstream"
	if out != expected {
		t.Errorf("BuildOutputURL() = %q, expected %q", out, expected)
	}

	relWithQuery := New("udp://127.0.0.1:8888?overrun_nonfatal=1")
	outWithQuery := relWithQuery.BuildOutputURL("/live/feed1")
	expectedWithQuery := "udp://127.0.0.1:8888?overrun_nonfatal=1&pkt_size=1316&streamid=%2Flive%2Ffeed1"
	if outWithQuery != expectedWithQuery {
		t.Errorf("BuildOutputURL() with query = %q, expected %q", outWithQuery, expectedWithQuery)
	}
}

func TestRelayInitialState(t *testing.T) {
	rel := New("udp://127.0.0.1:8888")
	if rel.IsRunning() {
		t.Errorf("expected initial state not running")
	}
	if rel.CurrentURL() != "" {
		t.Errorf("expected initial CurrentURL empty")
	}
}

