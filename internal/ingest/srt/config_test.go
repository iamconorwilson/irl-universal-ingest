package srt

import (
	"strings"
	"testing"
)

func TestGenerateSLSConfig(t *testing.T) {
	opts := SLSConfigOptions{
		SRTPort:        8890,
		SRTLAPort:      6600,
		PlayerPort:     8190,
		HTTPPort:       8181,
		LatencyMs:      300,
		IdleTimeoutSec: 15,
		Passphrase:     "mysecretpass",
		WebhookURL:     "http://127.0.0.1:8182/sls/on_event",
		LogLevel:       "debug",
		PIDFile:        "/tmp/test.pid",
	}

	conf := GenerateSLSConfig(opts)

	expectedDirectives := []string{
		"http_port 8181;",
		"log_level debug;",
		"pidfile /tmp/test.pid;",
		"listen_player 8190;",
		"listen_publisher 8890;",
		"listen_publisher_srtla 6600;",
		"latency_min 300;",
		"idle_streams_timeout 15;",
		"srt_passphrase mysecretpass;",
		"on_event_url http://127.0.0.1:8182/sls/on_event;",
	}

	for _, d := range expectedDirectives {
		if !strings.Contains(conf, d) {
			t.Errorf("expected config to contain %q, got:\n%s", d, conf)
		}
	}
}

func TestGenerateSLSConfigDefaults(t *testing.T) {
	opts := SLSConfigOptions{
		SRTPort:    8890,
		SRTLAPort:  6600,
		PlayerPort: 8190,
		HTTPPort:   8181,
	}

	conf := GenerateSLSConfig(opts)

	if !strings.Contains(conf, "log_level info;") {
		t.Errorf("expected default log_level info, got:\n%s", conf)
	}
	if !strings.Contains(conf, "latency_min 200;") {
		t.Errorf("expected default latency_min 200, got:\n%s", conf)
	}
	if !strings.Contains(conf, "latency_max 5000;") {
		t.Errorf("expected default latency_max 5000, got:\n%s", conf)
	}
	if !strings.Contains(conf, "idle_streams_timeout 5;") {
		t.Errorf("expected default idle_streams_timeout 5, got:\n%s", conf)
	}
	if !strings.Contains(conf, "log_file /tmp/srt_server.log;") {
		t.Errorf("expected default log_file in /tmp, got:\n%s", conf)
	}
	if !strings.Contains(conf, "pidfile /tmp/sls_server.pid;") {
		t.Errorf("expected default pidfile in /tmp, got:\n%s", conf)
	}
	if strings.Contains(conf, "srt_passphrase") {
		t.Errorf("expected no srt_passphrase directive when unset, got:\n%s", conf)
	}
}

func TestBuildSRTLAArgs(t *testing.T) {
	args := BuildSRTLAArgs(5000, "127.0.0.1", 8891, "debug")
	expected := []string{
		"--srtla_port", "5000",
		"--srt_hostname", "127.0.0.1",
		"--srt_port", "8891",
		"--log_level", "debug",
	}

	if len(args) != len(expected) {
		t.Fatalf("expected %d args, got %d: %v", len(expected), len(args), args)
	}

	for i, arg := range args {
		if arg != expected[i] {
			t.Errorf("arg[%d] expected %q, got %q", i, expected[i], arg)
		}
	}
}
