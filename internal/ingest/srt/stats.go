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

// SLSStatsPushPayload is the JSON body irl-srt-server POSTs to stat_post_url -- a flat array of
// per-connection entries, distinct from the "publishers" map the /stats HTTP endpoint returns.
type SLSStatsPushPayload struct {
	Stats []SLSStatEntry `json:"stats"`
}

// SLSStatEntry represents a single connection/role entry in a stat_post_url payload.
type SLSStatEntry struct {
	Role       string `json:"role"`
	StreamName string `json:"stream_name"`
	URL        string `json:"url"`
	KbitRate   int64  `json:"kbitrate"`
}

// ParseStats extracts stream metrics matching the targetStreamID from a stat_post_url payload.
func ParseStats(data []byte, targetStreamID string) (*StreamMetric, error) {
	var payload SLSStatsPushPayload
	if err := json.Unmarshal(data, &payload); err != nil {
		return nil, fmt.Errorf("decoding sls stats json: %w", err)
	}

	metric := &StreamMetric{
		StreamID: targetStreamID,
	}

	normalizedTarget := normalizePublisherKey(targetStreamID)
	for _, entry := range payload.Stats {
		if entry.Role != "publisher" {
			continue
		}
		if normalizePublisherKey(entry.URL) == normalizedTarget {
			metric.BitrateKbps = entry.KbitRate
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
