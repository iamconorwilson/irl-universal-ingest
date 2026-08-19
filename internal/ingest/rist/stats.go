package rist

import (
	"encoding/json"
	"regexp"
	"strconv"
	"strings"
)

// StatsMetric represents parsed statistics from ristreceiver output.
type StatsMetric struct {
	CNAME        string
	BitrateKbps  int64
	RTTMs        int64
	Quality      float64
	LostPackets  uint64
	Connected    bool
	Disconnected bool
	// HasStats is the liveness signal, independent of reported values -- bitrate can read 0 for a live peer.
	HasStats bool
}

var (
	cnameRegex   = regexp.MustCompile(`(?i)cname[:=]\s*([^\s,;]+)`)
	rttRegex     = regexp.MustCompile(`(?i)rtt[:=]\s*([0-9.]+)\s*(ms)?`)
	bitrateRegex = regexp.MustCompile(`(?i)(?:bitrate|bandwidth)[:=]\s*([0-9.]+)\s*(kbps|mbps|bps)?`)
	lostRegex    = regexp.MustCompile(`(?i)lost[:=]\s*([0-9]+)`)
	qualityRegex = regexp.MustCompile(`(?i)quality[:=]\s*([0-9.]+)\s*%?`)

	// Must be checked before the plain substring fallbacks below: librist's disconnect line itself
	// contains the substring "connected" (from "peer connected count is 0").
	connectionStatusRegex = regexp.MustCompile(`(?i)connection status changed .* new status is (\d)`)
)

// rist_connection_status values from librist/peer.h.
const (
	ristStatusEstablished   = "0"
	ristStatusTimedOut      = "1"
	ristStatusClientConn    = "2"
	ristStatusClientTimeout = "3"
)

// ristStatsPayload is the periodic JSON stats blob ristreceiver prints (-S); the regexes above never
// match it since they expect unquoted "key: value" text, not quoted JSON keys.
type ristStatsPayload struct {
	ReceiverStats struct {
		FlowInstant struct {
			Dead  int `json:"dead"`
			Stats struct {
				Quality float64 `json:"quality"`
				Lost    uint64  `json:"lost"`
			} `json:"stats"`
			Peers []struct {
				Dead  int `json:"dead"`
				Stats struct {
					RTT     float64 `json:"rtt"`
					Bitrate int64   `json:"bitrate"`
				} `json:"stats"`
			} `json:"peers"`
		} `json:"flowinstant"`
	} `json:"receiver-stats"`
}

// ParseLogLine extracts metrics and lifecycle events from ristreceiver stdout/stderr.
func ParseLogLine(line string) StatsMetric {
	if idx := strings.IndexByte(line, '{'); idx >= 0 {
		var payload ristStatsPayload
		if err := json.Unmarshal([]byte(line[idx:]), &payload); err == nil {
			flow := payload.ReceiverStats.FlowInstant
			// flow and peer dead flags go stale independently, so both must be checked -- a blob for
			// a dying connection must not count as activity, or teardown races reacquire the slot.
			if peers := flow.Peers; len(peers) > 0 && flow.Dead == 0 && peers[0].Dead == 0 {
				return StatsMetric{
					HasStats:    true,
					RTTMs:       int64(peers[0].Stats.RTT),
					BitrateKbps: peers[0].Stats.Bitrate / 1000,
					Quality:     flow.Stats.Quality,
					LostPackets: flow.Stats.Lost,
				}
			}
		}
	}

	var m StatsMetric
	lower := strings.ToLower(line)

	if match := connectionStatusRegex.FindStringSubmatch(lower); len(match) > 1 {
		switch match[1] {
		case ristStatusEstablished, ristStatusClientConn:
			m.Connected = true
		case ristStatusTimedOut, ristStatusClientTimeout:
			m.Disconnected = true
		}
	} else if strings.Contains(lower, "disconnected") || strings.Contains(lower, "peer removed") || strings.Contains(lower, "connection closed") {
		m.Disconnected = true
	} else if strings.Contains(lower, "connected") || strings.Contains(lower, "peer added") {
		m.Connected = true
	}

	if match := cnameRegex.FindStringSubmatch(line); len(match) > 1 {
		m.CNAME = strings.TrimSpace(match[1])
	}

	if match := rttRegex.FindStringSubmatch(line); len(match) > 1 {
		if val, err := strconv.ParseFloat(match[1], 64); err == nil {
			m.RTTMs = int64(val)
		}
	}

	if match := bitrateRegex.FindStringSubmatch(line); len(match) > 1 {
		if val, err := strconv.ParseFloat(match[1], 64); err == nil {
			unit := strings.ToLower(match[2])
			switch unit {
			case "mbps":
				m.BitrateKbps = int64(val * 1000)
			case "bps":
				m.BitrateKbps = int64(val / 1000)
			default:
				m.BitrateKbps = int64(val)
			}
		}
	}

	if match := lostRegex.FindStringSubmatch(line); len(match) > 1 {
		if val, err := strconv.ParseUint(match[1], 10, 64); err == nil {
			m.LostPackets = val
		}
	}

	if match := qualityRegex.FindStringSubmatch(line); len(match) > 1 {
		if val, err := strconv.ParseFloat(match[1], 64); err == nil {
			m.Quality = val
		}
	}

	return m
}
