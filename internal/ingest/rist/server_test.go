package rist

import (
	"testing"
	"time"

	"github.com/iamconorwilson/irl-universal-ingest/internal/arbitration"
	"github.com/iamconorwilson/irl-universal-ingest/internal/relay"
)

func TestParseLogLine(t *testing.T) {
	tests := []struct {
		name     string
		line     string
		expected StatsMetric
	}{
		{
			name: "stats with cname and rtt",
			line: "[STATS] cname: /live/stream, rtt: 45ms, bitrate: 5500kbps, lost: 2, quality: 99.8%",
			expected: StatsMetric{
				CNAME:       "/live/stream",
				RTTMs:       45,
				BitrateKbps: 5500,
				LostPackets: 2,
				Quality:     99.8,
			},
		},
		{
			name: "connected event",
			line: "2026-08-17 12:00:00 [INFO] Peer 192.168.1.50:5001 connected",
			expected: StatsMetric{
				Connected: true,
			},
		},
		{
			name: "disconnected event",
			line: "2026-08-17 12:05:00 [INFO] Peer disconnected from port 5001",
			expected: StatsMetric{
				Disconnected: true,
			},
		},
		{
			name: "bitrate in mbps",
			line: "bandwidth=4.5mbps rtt=20",
			expected: StatsMetric{
				BitrateKbps: 4500,
				RTTMs:       20,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ParseLogLine(tt.line)
			if got.CNAME != tt.expected.CNAME {
				t.Errorf("expected CNAME %q, got %q", tt.expected.CNAME, got.CNAME)
			}
			if got.RTTMs != tt.expected.RTTMs {
				t.Errorf("expected RTTMs %d, got %d", tt.expected.RTTMs, got.RTTMs)
			}
			if got.BitrateKbps != tt.expected.BitrateKbps {
				t.Errorf("expected BitrateKbps %d, got %d", tt.expected.BitrateKbps, got.BitrateKbps)
			}
			if got.LostPackets != tt.expected.LostPackets {
				t.Errorf("expected LostPackets %d, got %d", tt.expected.LostPackets, got.LostPackets)
			}
			if got.Connected != tt.expected.Connected {
				t.Errorf("expected Connected %v, got %v", tt.expected.Connected, got.Connected)
			}
			if got.Disconnected != tt.expected.Disconnected {
				t.Errorf("expected Disconnected %v, got %v", tt.expected.Disconnected, got.Disconnected)
			}
		})
	}
}

func TestBuildArgs(t *testing.T) {
	mgr := arbitration.NewManager(5*time.Second, nil)
	rel := relay.New("udp://127.0.0.1:8888")

	server := NewServer(ServerOptions{
		RISTPort:     5001,
		PlayerPort:   5101,
		BufferMs:     1500,
		Profile:      "advanced",
		Secret:       "mysecret123",
		AllowedPaths: []string{"/live/stream"},
		Manager:      mgr,
		Relay:        rel,
	})

	args := server.BuildArgs()
	expectedArgs := []string{
		"-i", "rist://@0.0.0.0:5001",
		"-o", "udp://127.0.0.1:5101",
		"-p", "2",
		"-b", "1500",
		"-S", "1000",
		"-v", "4",
		"-s", "mysecret123",
	}

	if len(args) != len(expectedArgs) {
		t.Fatalf("expected %d args, got %d: %v", len(expectedArgs), len(args), args)
	}

	for i, arg := range args {
		if arg != expectedArgs[i] {
			t.Errorf("arg[%d] expected %q, got %q", i, expectedArgs[i], arg)
		}
	}
}

func TestAuthAndArbitration(t *testing.T) {
	mgr := arbitration.NewManager(5*time.Second, nil)
	rel := relay.New("udp://127.0.0.1:8888")

	server := NewServer(ServerOptions{
		RISTPort:     5001,
		PlayerPort:   5101,
		AllowedPaths: []string{"/live/stream"},
		Manager:      mgr,
		Relay:        rel,
	})

	// Unauthorized path should be rejected and not acquire slot
	server.handleLogLine("[STATS] cname: /unauthorized/path, rtt: 20ms, bitrate: 3000kbps")

	_, active := mgr.ActiveInfo()
	if active {
		t.Errorf("expected unauthorized stream to be rejected")
	}

	// Authorized path should acquire slot
	server.handleLogLine("[STATS] cname: /live/stream, rtt: 25ms, bitrate: 4500kbps")

	slot, active := mgr.ActiveInfo()
	if !active {
		t.Fatalf("expected authorized stream to acquire slot")
	}
	if slot.Protocol != "RIST" {
		t.Errorf("expected protocol RIST, got %s", slot.Protocol)
	}
	if slot.Path != "/live/stream" {
		t.Errorf("expected path /live/stream, got %s", slot.Path)
	}

	// Disconnect should release slot
	server.handleDisconnect()
	_, active = mgr.ActiveInfo()
	if active {
		t.Errorf("expected slot to be released on disconnect")
	}
}
