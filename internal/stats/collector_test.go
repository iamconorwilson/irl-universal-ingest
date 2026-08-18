package stats

import (
	"context"
	"testing"
	"time"

	"github.com/iamconorwilson/irl-universal-ingest/internal/arbitration"
	"github.com/iamconorwilson/irl-universal-ingest/internal/relay"
)

func TestCollectorFormatting(t *testing.T) {
	mgr := arbitration.NewManager(10*time.Second, nil)
	rel := relay.New("udp://127.0.0.1:8888")
	allowedPaths := []string{"/live/stream", "/live"}

	collector := NewCollector(mgr, rel, allowedPaths)

	// Test inactive state
	inactiveStats := collector.Collect(context.Background())
	if len(inactiveStats) != 2 {
		t.Fatalf("expected 2 path entries, got %d", len(inactiveStats))
	}
	if inactiveStats[0].Active || inactiveStats[0].RTT != nil || inactiveStats[0].Bitrate != 0 {
		t.Errorf("unexpected inactive stats: %+v", inactiveStats[0])
	}

	// Test active state
	acquired, release := mgr.TryAcquire("rtmp", "/live/stream")
	if !acquired {
		t.Fatalf("failed to acquire slot")
	}
	defer release()

	activeStats := collector.Collect(context.Background())
	// In test, relay is not started so Active is false until relay starts or active info matches
	if len(activeStats) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(activeStats))
	}
}
