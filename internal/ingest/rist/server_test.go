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
			// Real captured line: rist_connection_status 2 = RIST_CLIENT_CONNECTED.
			name: "connected event",
			line: `1787176093.225970|0.0|[INFO] Connection Status changed for Peer 134442744024176, new status is 2, peer connected count is 1`,
			expected: StatsMetric{
				Connected: true,
			},
		},
		{
			// Real captured line: rist_connection_status 1 = RIST_CONNECTION_TIMED_OUT. The line
			// itself contains the substring "connected" (from "peer connected count is 0"), which
			// is exactly what caused a live bug -- a disconnect being misread as fresh activity.
			name: "disconnected event",
			line: `1787176098.560110|0.0|[INFO] Connection Status changed for Peer 134442744024176, new status is 1, peer connected count is 0`,
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

// TestParseLogLineJSONStats verifies the real periodic stats JSON is parsed as live activity.
func TestParseLogLineJSONStats(t *testing.T) {
	line := `1787172006.256|0.0|[INFO] {"schema_version":4,"receiver-stats":{"flowinstant":{"flow_id":3415714278,"stats":{"quality":100,"lost":2},"peers":[{"id":2,"stats":{"rtt":5.157,"bitrate":563686}}]}}}`

	m := ParseLogLine(line)

	if !m.HasStats {
		t.Fatalf("expected HasStats=true for a peer stats blob")
	}
	if m.RTTMs != 5 {
		t.Errorf("expected RTTMs 5, got %d", m.RTTMs)
	}
	if m.BitrateKbps != 563 {
		t.Errorf("expected BitrateKbps 563, got %d", m.BitrateKbps)
	}
	if m.Quality != 100 {
		t.Errorf("expected Quality 100, got %f", m.Quality)
	}
	if m.LostPackets != 2 {
		t.Errorf("expected LostPackets 2, got %d", m.LostPackets)
	}
}

// TestParseLogLineJSONStatsNoPeers verifies a stats blob with no live peer isn't flagged as activity.
func TestParseLogLineJSONStatsNoPeers(t *testing.T) {
	line := `1787172006.256|0.0|[INFO] {"schema_version":4,"receiver-stats":{"flowinstant":{"flow_id":0,"stats":{"quality":0},"peers":[]}}}`

	m := ParseLogLine(line)

	if m.HasStats {
		t.Errorf("expected HasStats=false when no peers are present")
	}
}

// TestParseLogLineJSONStatsDeadFlow reproduces a live bug: a trailing stats blob for a dying flow was misread as fresh activity.
func TestParseLogLineJSONStatsDeadFlow(t *testing.T) {
	line := `1787176341.336695|0.0|[INFO] {"schema_version":4,"receiver-stats":{"flowinstant":{"flow_id":1821181200,"dead":1,"profile":1,"profile_name":"main","stats":{"quality":100,"lost":0},"peers":[{"id":2,"dead":1,"url":"rist://@0.0.0.0","stats":{"rtt":5.07,"bitrate":249898}}]}}}`

	m := ParseLogLine(line)

	if m.HasStats {
		t.Errorf("expected HasStats=false for a stats blob belonging to a dead flow")
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
		"-v", "6",
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

// TestStartupLinesDoNotTriggerActivity verifies ristreceiver's own listener-init boilerplate never acquires a slot.
func TestStartupLinesDoNotTriggerActivity(t *testing.T) {
	mgr := arbitration.NewManager(5*time.Second, nil)
	rel := relay.New("udp://127.0.0.1:8888")

	server := NewServer(ServerOptions{
		RISTPort:     5001,
		PlayerPort:   5101,
		AllowedPaths: []string{"/live/stream"},
		Manager:      mgr,
		Relay:        rel,
	})

	startupLines := []string{
		`1787175074.928|0.0|[INFO] Starting in Main Profile Mode`,
		`1787175074.928|0.0|[INFO] Link configured with maxrate=100000 bufmin=1000 bufmax=1000 reorder=15 rttmin=5 rttmax=500 congestion_control=1 min_retries=6 max_retries=20`,
		`1787175074.928|0.0|[INFO] Encryption is disabled for this peer`,
		`1787175074.928|0.0|[INFO] URL parsed successfully: Host 0.0.0.0, Port 5001`,
		`1787175074.928|0.0|[INFO] Peer cname is 3c324c6175a4`,
		`1787175074.928|0.0|[INFO] New peer with id #0 was configured with maxrate=100000/0 bufmin=1000 bufmax=1000 reorder=15 rttmin=50 rttmax=500 congestion_control=1 min_retries=6 max_retries=20`,
		`1787175074.928|0.0|[INFO] Initialized Receiver Peer, listening mode ...`,
		`1787175074.928|0.0|[INFO] Active Peer Information, IP:Port => 0.0.0.0:5001 (1), id: 1, ports: 5001->1969`,
	}
	for _, line := range startupLines {
		server.handleLogLine(line)
	}

	if _, active := mgr.ActiveInfo(); active {
		t.Errorf("expected listener startup lines not to acquire a slot")
	}
}

// TestDisconnectStatusLineDoesNotReacquire reproduces a live bug: librist's disconnect line contains the substring "connected" and was misread as activity.
func TestDisconnectStatusLineDoesNotReacquire(t *testing.T) {
	mgr := arbitration.NewManager(5*time.Second, nil)
	rel := relay.New("udp://127.0.0.1:8888")

	server := NewServer(ServerOptions{
		RISTPort:     5001,
		PlayerPort:   5101,
		AllowedPaths: []string{"/live/stream"},
		Manager:      mgr,
		Relay:        rel,
	})
	defer rel.StopSession()

	server.handleLogLine(`1787176093.225970|0.0|[INFO] Connection Status changed for Peer 134442744024176, new status is 2, peer connected count is 1`)
	if _, active := mgr.ActiveInfo(); !active {
		t.Fatalf("expected the connect status line to acquire a slot")
	}

	server.handleLogLine(`1787176098.560110|0.0|[INFO] Connection Status changed for Peer 134442744024176, new status is 1, peer connected count is 0`)
	if _, active := mgr.ActiveInfo(); active {
		t.Errorf("expected the disconnect status line to release the slot, not reacquire it")
	}
}

// TestTeardownLinesDoNotReacquire reproduces a live bug: teardown log lines were misread as fresh activity and reacquired the slot mid-teardown.
func TestTeardownLinesDoNotReacquire(t *testing.T) {
	mgr := arbitration.NewManager(5*time.Second, nil)
	rel := relay.New("udp://127.0.0.1:8888")

	server := NewServer(ServerOptions{
		RISTPort:     5001,
		PlayerPort:   5101,
		AllowedPaths: []string{"/live/stream"},
		Manager:      mgr,
		Relay:        rel,
	})
	defer rel.StopSession()

	server.handleLogLine(`1787176645.069393|0.0|[WARNING] Listening peer 2 timed out after 279 ms`)
	if _, active := mgr.ActiveInfo(); active {
		t.Fatalf("expected the timeout warning to release the slot")
	}

	teardownLines := []string{
		`1787176647.243129|0.0|[INFO] {"schema_version":4,"receiver-stats":{"flowinstant":{"flow_id":1165104892,"dead":1,"stats":{"quality":100,"lost":0},"peers":[{"id":2,"dead":1,"stats":{"rtt":4.28,"bitrate":232034}}]}}}`,
		`1787176647.268673|130218638307344.0|[INFO] 	************** Session Timeout after 2s of no data, deleting flow with id 1165104892 ***************`,
		`1787176647.268675|130218638307344.0|[INFO] Triggering data output thread termination`,
		`1787176647.272619|130218638307344.0|[INFO] Data output thread shutting down`,
	}
	for _, line := range teardownLines {
		server.handleLogLine(line)
	}

	if _, active := mgr.ActiveInfo(); active {
		t.Errorf("expected teardown lines not to reacquire the slot")
	}
}

// TestSinglePathIgnoresCNAME verifies a single allowed path is used regardless of cname.
func TestSinglePathIgnoresCNAME(t *testing.T) {
	mgr := arbitration.NewManager(5*time.Second, nil)
	rel := relay.New("udp://127.0.0.1:8888")

	server := NewServer(ServerOptions{
		RISTPort:     5001,
		PlayerPort:   5101,
		AllowedPaths: []string{"/live/stream"},
		Manager:      mgr,
		Relay:        rel,
	})

	// Real ristreceiver output: an auto-generated cname, then the JSON stats blob.
	server.handleLogLine("1787172006.207785|0.0|[INFO] Authenticated RTCP peer 2 and flow 1076884258 for connection with cname: 3c324c6175a4")
	server.handleLogLine(`1787172006.256|0.0|[INFO] {"schema_version":4,"receiver-stats":{"flowinstant":{"flow_id":1,"stats":{"bitrate":0},"peers":[{"id":2,"stats":{"rtt":5.2,"bitrate":450000}}]}}}`)

	slot, active := mgr.ActiveInfo()
	if !active {
		t.Fatalf("expected the single configured path to acquire a slot despite a non-matching cname")
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

// TestHeartbeatTickDetectsStaleStream verifies a session is torn down once activity goes stale.
func TestHeartbeatTickDetectsStaleStream(t *testing.T) {
	mgr := arbitration.NewManager(30*time.Second, nil)
	rel := relay.New("udp://127.0.0.1:8888")

	server := NewServer(ServerOptions{
		RISTPort:     5001,
		PlayerPort:   5101,
		AllowedPaths: []string{"/live/stream"},
		Manager:      mgr,
		Relay:        rel,
	})

	// Simulate an acquired, previously-active stream that has since gone silent.
	acquired, release := mgr.TryAcquire("RIST", "/live/stream")
	if !acquired {
		t.Fatalf("failed to seed an active slot")
	}
	server.active = true
	server.activeSlot = release
	server.activePath = "/live/stream"
	server.lastActivity = time.Now().Add(-(staleDisconnectThreshold + time.Second))

	server.heartbeatTick()

	if server.active {
		t.Errorf("expected heartbeatTick to clear active state once silence exceeds staleDisconnectThreshold")
	}
	if _, active := mgr.ActiveInfo(); active {
		t.Errorf("expected heartbeatTick to release the arbitration slot")
	}
}

// TestHeartbeatTickResyncsAfterManagerRelease verifies local state resyncs after the manager
// releases a slot on its own.
func TestHeartbeatTickResyncsAfterManagerRelease(t *testing.T) {
	mgr := arbitration.NewManager(5*time.Second, nil)
	rel := relay.New("udp://127.0.0.1:8888")
	defer rel.StopSession()

	server := NewServer(ServerOptions{
		RISTPort:     5001,
		PlayerPort:   5101,
		AllowedPaths: []string{"/live/stream"},
		Manager:      mgr,
		Relay:        rel,
	})

	acquired, release := mgr.TryAcquire("RIST", "/live/stream")
	if !acquired {
		t.Fatalf("failed to seed an active slot")
	}
	server.active = true
	server.activeSlot = release
	server.activePath = "/live/stream"

	// Mirrors the manager's own watchdog releasing the slot without going through us.
	release()

	server.handleActivity(StatsMetric{Connected: true})

	slot, active := mgr.ActiveInfo()
	if !active {
		t.Fatalf("expected handleActivity to re-acquire a slot after resync")
	}
	if slot.Path != "/live/stream" {
		t.Errorf("expected path /live/stream, got %s", slot.Path)
	}
}

// TestMultiPathUsesCNAME verifies cname routes connections when multiple paths are allowed.
func TestMultiPathUsesCNAME(t *testing.T) {
	mgr := arbitration.NewManager(5*time.Second, nil)
	rel := relay.New("udp://127.0.0.1:8888")

	server := NewServer(ServerOptions{
		RISTPort:     5001,
		PlayerPort:   5101,
		AllowedPaths: []string{"/live/stream", "/live/backup"},
		Manager:      mgr,
		Relay:        rel,
	})
	defer rel.StopSession()

	// A cname that doesn't match either configured path should be rejected.
	server.handleLogLine("1787172006.1|0.0|[INFO] Authenticated RTCP peer 2 and flow 1 for connection with cname: /unauthorized/path")
	server.handleLogLine(`1787172006.2|0.0|[INFO] {"receiver-stats":{"flowinstant":{"stats":{"bitrate":0},"peers":[{"stats":{"rtt":5.2,"bitrate":450000}}]}}}`)

	if _, active := mgr.ActiveInfo(); active {
		t.Errorf("expected unauthorized cname to be rejected when multiple paths are configured")
	}

	// A cname matching a configured path should acquire the slot.
	server.handleLogLine("1787172007.1|0.0|[INFO] Authenticated RTCP peer 2 and flow 2 for connection with cname: /live/backup")
	server.handleLogLine(`1787172007.2|0.0|[INFO] {"receiver-stats":{"flowinstant":{"stats":{"bitrate":0},"peers":[{"stats":{"rtt":5.2,"bitrate":450000}}]}}}`)

	slot, active := mgr.ActiveInfo()
	if !active {
		t.Fatalf("expected authorized cname to acquire slot")
	}
	if slot.Path != "/live/backup" {
		t.Errorf("expected path /live/backup, got %s", slot.Path)
	}
}
