package stats

import (
	"context"
	"time"

	"github.com/iamconorwilson/irl-universal-ingest/internal/arbitration"
	"github.com/iamconorwilson/irl-universal-ingest/internal/auth"
	"github.com/iamconorwilson/irl-universal-ingest/internal/relay"
)

// PathStat represents the metrics and status for an individual ingest path.
type PathStat struct {
	Path     string   `json:"path"`
	Active   bool     `json:"active"`
	Protocol string   `json:"protocol"`
	Uptime   int64    `json:"uptime"`
	Bitrate  int64    `json:"bitrate"`
	RTT      *float64 `json:"RTT"`
}

// Collector gathers path-level statistics from the arbitration manager and relay.
type Collector struct {
	manager      *arbitration.Manager
	relay        *relay.Relay
	allowedPaths []string
}

// NewCollector creates a stats collector with configured paths and managers.
func NewCollector(mgr *arbitration.Manager, rel *relay.Relay, allowedPaths []string) *Collector {
	return &Collector{
		manager:      mgr,
		relay:        rel,
		allowedPaths: allowedPaths,
	}
}

// Healthy reports false when a slot is held but the relay isn't actually running.
func (c *Collector) Healthy() bool {
	_, active := c.manager.ActiveInfo()
	return !active || c.relay.IsRunning()
}

// Collect returns an array of stats, one entry per configured or active path.
func (c *Collector) Collect(_ context.Context) []PathStat {
	slot, active := c.manager.ActiveInfo()
	isRelayRunning := c.relay.IsRunning()

	// Normalize before de-duplicating so "/live/stream" and "/live/stream/" aren't both listed.
	seenPaths := make(map[string]bool)
	var paths []string

	addPath := func(p string) {
		norm := auth.NormalizePath(p)
		if !seenPaths[norm] {
			seenPaths[norm] = true
			paths = append(paths, norm)
		}
	}

	for _, p := range c.allowedPaths {
		addPath(p)
	}

	if active {
		addPath(slot.Path)
	}

	results := make([]PathStat, 0, len(paths))

	for _, p := range paths {
		if active && isRelayRunning && auth.NormalizePath(slot.Path) == p {
			uptime := int64(time.Since(slot.StartedAt).Seconds())

			results = append(results, PathStat{
				Path:     p,
				Active:   true,
				Protocol: slot.Protocol,
				Uptime:   uptime,
				Bitrate:  slot.BitrateKbps,
				RTT:      slot.RTT,
			})
		} else {
			results = append(results, PathStat{
				Path:     p,
				Active:   false,
				Protocol: "",
				Uptime:   0,
				Bitrate:  0,
				RTT:      nil,
			})
		}
	}

	return results
}
