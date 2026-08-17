package rist

import (
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
}

var (
	cnameRegex   = regexp.MustCompile(`(?i)cname[:=]\s*([^\s,;]+)`)
	rttRegex     = regexp.MustCompile(`(?i)rtt[:=]\s*([0-9.]+)\s*(ms)?`)
	bitrateRegex = regexp.MustCompile(`(?i)(?:bitrate|bandwidth)[:=]\s*([0-9.]+)\s*(kbps|mbps|bps)?`)
	lostRegex    = regexp.MustCompile(`(?i)lost[:=]\s*([0-9]+)`)
	qualityRegex = regexp.MustCompile(`(?i)quality[:=]\s*([0-9.]+)\s*%?`)
)

// ParseLogLine extracts metrics and lifecycle events from ristreceiver stdout/stderr.
func ParseLogLine(line string) StatsMetric {
	var m StatsMetric
	lower := strings.ToLower(line)

	if strings.Contains(lower, "connected") || strings.Contains(lower, "peer added") {
		m.Connected = true
	}
	if strings.Contains(lower, "disconnected") || strings.Contains(lower, "peer removed") || strings.Contains(lower, "connection closed") {
		m.Disconnected = true
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
