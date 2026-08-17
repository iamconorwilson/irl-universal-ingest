package srt

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// StreamMetric holds parsed metric information for an active SRT/SRTLA stream.
type StreamMetric struct {
	StreamID    string  `json:"stream_id"`
	BitrateKbps int64   `json:"bitrate_kbps"`
	RTTMs       float64 `json:"rtt_ms"`
	Bytes       uint64  `json:"bytes"`
}

// Client interacts with the internal SLS HTTP API.
type Client struct {
	httpClient *http.Client
	statsURL   string
}

// NewClient creates an HTTP client for retrieving SLS internal metrics.
func NewClient(statsURL string) *Client {
	return &Client{
		httpClient: &http.Client{
			Timeout: 2 * time.Second,
		},
		statsURL: statsURL,
	}
}

// FetchRaw retrieves the raw JSON body from the SLS stats HTTP endpoint.
func (c *Client) FetchRaw(ctx context.Context) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.statsURL, nil)
	if err != nil {
		return nil, fmt.Errorf("creating stats request: %w", err)
	}
	req.Header.Set("Authorization", InternalAPIKey)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetching sls stats: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("sls stats returned status %d", resp.StatusCode)
	}

	return io.ReadAll(resp.Body)
}

// FetchMetrics queries the SLS /stats endpoint and extracts the metrics for the active stream.
func (c *Client) FetchMetrics(ctx context.Context, targetStreamID string) (*StreamMetric, error) {
	body, err := c.FetchRaw(ctx)
	if err != nil {
		return nil, err
	}
	return ParseStats(body, targetStreamID)
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

	normalizedTarget := strings.TrimPrefix(targetStreamID, "/")
	for key, pub := range resp.Publishers {
		normalizedKey := strings.TrimPrefix(key, "publish/")
		normalizedKey = strings.TrimPrefix(normalizedKey, "/")
		if strings.Contains(normalizedKey, normalizedTarget) || len(resp.Publishers) == 1 {
			metric.BitrateKbps = pub.Bitrate
			metric.RTTMs = pub.RTT
			return metric, nil
		}
	}

	return metric, nil
}
