package srt

import (
	"encoding/json"
	"fmt"
	"strings"
)

// StreamMetric holds parsed metric information for an active SRT/SRTLA stream.
type StreamMetric struct {
	StreamID    string  `json:"stream_id"`
	BitrateKbps int64   `json:"bitrate_kbps"`
	RTTMs       float64 `json:"rtt_ms"`
	// Found is the liveness signal: presence in the response, not a bitrate/RTT threshold.
	Found bool `json:"-"`
}

// SLSStatsResponse represents the JSON payload from irl-srt-server /stats.
type SLSStatsResponse struct {
	Status     string                      `json:"status"`
	Publishers map[string]SLSPublisherStat `json:"publishers"`
}

// SLSPublisherStat represents a single publisher entry in the SLS stats.
type SLSPublisherStat struct {
	Bitrate      int64   `json:"bitrate"`
	RTT          float64 `json:"rtt"`
	Latency      int64   `json:"latency"`
	Uptime       int64   `json:"uptime"`
	MbpsBandwith float64 `json:"mbpsBandwidth"`
	MbpsRecvRate float64 `json:"mbpsRecvRate"`
}

// ParseStats extracts stream metrics matching the targetStreamID from raw SLS JSON output.
func ParseStats(data []byte, targetStreamID string) (*StreamMetric, error) {
	var resp SLSStatsResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, fmt.Errorf("decoding sls stats json: %w", err)
	}

	metric := &StreamMetric{
		StreamID: targetStreamID,
	}

	normalizedTarget := normalizePublisherKey(targetStreamID)
	for key, pub := range resp.Publishers {
		if normalizePublisherKey(key) == normalizedTarget {
			metric.BitrateKbps = pub.Bitrate
			metric.RTTMs = pub.RTT
			metric.Found = true
			return metric, nil
		}
	}

	return metric, nil
}

// normalizePublisherKey strips the SLS domain prefix so keys and paths can be compared exactly.
func normalizePublisherKey(key string) string {
	key = strings.TrimPrefix(key, "publish/")
	key = strings.TrimPrefix(key, "/")
	return key
}
