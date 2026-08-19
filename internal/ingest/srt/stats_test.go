package srt

import (
	"testing"
)

func TestParseStats(t *testing.T) {
	// Real stat_post_url payload shape, captured live -- distinct from GET /stats' "publishers" map.
	rawJSON := `{"stats":[
		{"kbitrate":0,"port":8190,"pub_domain_app":"","remote_ip":"","remote_port":0,"role":"listener-player","start_time":"2026-08-19 20:17:39","stream_name":"","url":""},
		{"kbitrate":0,"port":8891,"pub_domain_app":"","remote_ip":"","remote_port":0,"role":"listener-publisher-srtla","start_time":"2026-08-19 20:17:39","stream_name":"","url":""},
		{"kbitrate":0,"port":8890,"pub_domain_app":"","remote_ip":"","remote_port":0,"role":"listener-publisher","start_time":"2026-08-19 20:17:39","stream_name":"","url":""},
		{"kbitrate":514,"port":8890,"pub_domain_app":"publish/live","remote_ip":"::ffff:127.0.0.1","remote_port":60707,"role":"publisher","start_time":"2026-08-19 20:17:53","stream_name":"stream","url":"publish/live/stream"}
	]}`

	metric, err := ParseStats([]byte(rawJSON), "/live/stream")
	if err != nil {
		t.Fatalf("unexpected error parsing stats: %v", err)
	}

	if !metric.Found {
		t.Fatalf("expected Found=true for an active publisher entry")
	}
	if metric.BitrateKbps != 514 {
		t.Errorf("expected BitrateKbps 514, got %d", metric.BitrateKbps)
	}
}

func TestParseStatsIgnoresNonPublisherRoles(t *testing.T) {
	// Listener/player entries must never be mistaken for a live publisher.
	rawJSON := `{"stats":[
		{"kbitrate":0,"port":8190,"role":"listener-player","stream_name":"","url":""},
		{"kbitrate":0,"port":8890,"role":"listener-publisher","stream_name":"","url":""}
	]}`

	metric, err := ParseStats([]byte(rawJSON), "/live/stream")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if metric.Found {
		t.Errorf("expected Found=false when no role=\"publisher\" entry is present")
	}
}

func TestParseStatsExactMatchNotSubstring(t *testing.T) {
	// Must not match via substring containment against a longer publisher path.
	rawJSON := `{"stats":[
		{"kbitrate":3000,"role":"publisher","stream_name":"stream2","url":"publish/live/stream2"}
	]}`

	metric, err := ParseStats([]byte(rawJSON), "/live/stream")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if metric.Found {
		t.Errorf("expected no match for /live/stream against publisher /live/stream2, got Found=true")
	}
	if metric.BitrateKbps != 0 {
		t.Errorf("expected no bitrate to be attributed to the wrong stream, got %d", metric.BitrateKbps)
	}
}

func TestParseStatsNotFoundWhenAbsent(t *testing.T) {
	rawJSON := `{"stats": []}`

	metric, err := ParseStats([]byte(rawJSON), "/live/stream")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if metric.Found {
		t.Errorf("expected Found=false when no publishers are present")
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
