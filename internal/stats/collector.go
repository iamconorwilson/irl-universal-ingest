package stats

import (
	"context"
	"time"

	"github.com/iamconorwilson/irl-universal-ingest/internal/arbitration"
	"github.com/iamconorwilson/irl-universal-ingest/internal/relay"
)

// PathStat represents the metrics and status for an individual ingest path.
type PathStat struct {
	Path     string `json:"path"`
	Active   bool   `json:"active"`
	Protocol string `json:"protocol"`
	Uptime   int64  `json:"uptime"`
	Bitrate  int64  `json:"bitrate"`
	RTT      any    `json:"RTT"`
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

// Collect returns an array of stats, one entry per configured or active path.
func (c *Collector) Collect(_ context.Context) []PathStat {
	slot, active := c.manager.ActiveInfo()
	isRelayRunning := c.relay.IsRunning()

	// Gather list of paths to report
	seenPaths := make(map[string]bool)
	var paths []string

	for _, p := range c.allowedPaths {
		if !seenPaths[p] {
			seenPaths[p] = true
			paths = append(paths, p)
		}
	}

	if active && !seenPaths[slot.Path] {
		seenPaths[slot.Path] = true
		paths = append(paths, slot.Path)
	}

	results := make([]PathStat, 0, len(paths))

	for _, p := range paths {
		if active && isRelayRunning && slot.Path == p {
			uptime := int64(time.Since(slot.StartedAt).Seconds())
			rtt := slot.RTT
			if rtt == nil {
				rtt = false
			}

			results = append(results, PathStat{
				Path:     p,
				Active:   true,
				Protocol: slot.Protocol,
				Uptime:   uptime,
				Bitrate:  slot.BitrateKbps,
				RTT:      rtt,
			})
		} else {
			results = append(results, PathStat{
				Path:     p,
				Active:   false,
				Protocol: "",
				Uptime:   0,
				Bitrate:  0,
				RTT:      false,
			})
		}
	}

	return results
}
