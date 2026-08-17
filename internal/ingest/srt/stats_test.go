package srt

import (
	"testing"
)

func TestParseStats(t *testing.T) {
	rawJSON := `{
		"status": "ok",
		"publishers": {
			"publish/live/stream": {
				"bitrate": 6500,
				"rtt": 28.5,
				"latency": 2000,
				"uptime": 12
			}
		}
	}`

	metric, err := ParseStats([]byte(rawJSON), "/live/stream")
	if err != nil {
		t.Fatalf("unexpected error parsing stats: %v", err)
	}

	if metric.BitrateKbps != 6500 {
		t.Errorf("expected BitrateKbps 6500, got %d", metric.BitrateKbps)
	}
	if metric.RTTMs != 28.5 {
		t.Errorf("expected RTTMs 28.5, got %f", metric.RTTMs)
	}
}

func TestParseStatsSLSResponse(t *testing.T) {
	liveJSON := `{"publishers":{"publish/live/stream":{"bitrate":7760,"bytesRcvDrop":0,"bytesRcvLoss":0,"ingestDiscontinuities":0,"latency":2000,"maxReaderBacklogBytes":2444,"maxReaderBacklogMs":2,"mbpsBandwidth":266.016,"mbpsRecvRate":8.44003095190265,"msRcvBuf":1998,"pktRcvDrop":0,"pktRcvLoss":0,"pktRcvRetrans":0,"pktRecvNAKTotal":0,"pktRetransTotal":0,"pktSentNAKTotal":0,"ringOverruns":0,"rtt":8.775,"sendBackpressure":0,"uptime":3,"viewerPktSndDrop":0}},"status":"ok"}`

	metric, err := ParseStats([]byte(liveJSON), "/live/stream")
	if err != nil {
		t.Fatalf("unexpected error parsing live sls stats: %v", err)
	}

	if metric.BitrateKbps != 7760 {
		t.Errorf("expected BitrateKbps 7760, got %d", metric.BitrateKbps)
	}
	if metric.RTTMs != 8.775 {
		t.Errorf("expected RTTMs 8.775, got %f", metric.RTTMs)
	}
}

func TestExtractStreamPath(t *testing.T) {
	cases := []struct {
		input    string
		expected string
	}{
		{"publish/live/stream", "/live/stream"},
		{"play/live/stream", "/live/stream"},
		{"srt://127.0.0.1:8890?streamid=publish/live/stream", "/live/stream"},
		{"/live/feed1", "/live/feed1"},
		{"feed1", "/feed1"},
	}

	for _, c := range cases {
		got := extractStreamPath(c.input)
		if got != c.expected {
			t.Errorf("extractStreamPath(%q) = %q, expected %q", c.input, got, c.expected)
		}
	}
}
