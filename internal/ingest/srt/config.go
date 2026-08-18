package srt

import (
	"fmt"
	"strconv"
	"strings"
)

// InternalAPIKey is the shared authentication token between the supervisor and irl-srt-server.
const InternalAPIKey = "irl-internal-api-key"

// SLSConfigOptions contains the parameters used to render an sls.conf file.
type SLSConfigOptions struct {
	SRTPort        int
	SRTLAPort      int
	PlayerPort     int
	HTTPPort       int
	LatencyMs      int
	LatencyMaxMs   int
	IdleTimeoutSec int
	Passphrase     string
	WebhookURL     string
	StatPostURL    string
	LogLevel       string
	PIDFile        string
}

// GenerateSLSConfig renders an sls.conf configuration string for irl-srt-server.
func GenerateSLSConfig(opts SLSConfigOptions) string {
	if opts.LogLevel == "" {
		opts.LogLevel = "info"
	}
	if opts.LatencyMs <= 0 {
		opts.LatencyMs = 200
	}
	if opts.LatencyMaxMs <= 0 {
		opts.LatencyMaxMs = 5000
	}
	if opts.IdleTimeoutSec <= 0 {
		opts.IdleTimeoutSec = 10
	}

	var sb strings.Builder

	sb.WriteString("srt {\n")
	sb.WriteString("    worker_threads 1;\n")
	sb.WriteString("    worker_connections 300;\n")
	fmt.Fprintf(&sb, "    http_port %d;\n", opts.HTTPPort)
	sb.WriteString("    http_bind_addr 127.0.0.1;\n")
	sb.WriteString("    cors_header *;\n")
	sb.WriteString("    log_file /tmp/srt_server.log;\n")
	fmt.Fprintf(&sb, "    log_level %s;\n", opts.LogLevel)
	sb.WriteString("    log_rate_limit_enabled 1;\n")
	sb.WriteString("    log_summary_enabled 0;\n")
	sb.WriteString("    log_session_ids 1;\n")
	fmt.Fprintf(&sb, "    api_keys %s;\n", InternalAPIKey)
	if opts.StatPostURL != "" {
		fmt.Fprintf(&sb, "    stat_post_url %s;\n", opts.StatPostURL)
		sb.WriteString("    stat_post_interval 1;\n")
	}

	pidFile := "/tmp/sls_server.pid"
	if opts.PIDFile != "" {
		pidFile = opts.PIDFile
	}
	fmt.Fprintf(&sb, "    pidfile %s;\n\n", pidFile)

	sb.WriteString("    server {\n")
	fmt.Fprintf(&sb, "        listen_player %d;\n", opts.PlayerPort)
	fmt.Fprintf(&sb, "        listen_publisher %d;\n", opts.SRTPort)
	if opts.SRTLAPort > 0 {
		fmt.Fprintf(&sb, "        listen_publisher_srtla %d;\n", opts.SRTLAPort)
	}
	fmt.Fprintf(&sb, "        latency_min %d;\n", opts.LatencyMs)
	fmt.Fprintf(&sb, "        latency_max %d;\n", opts.LatencyMaxMs)
	sb.WriteString("        domain_player play;\n")
	sb.WriteString("        domain_publisher publish;\n")
	sb.WriteString("        default_sid publish/live/stream;\n")
	sb.WriteString("        backlog 100;\n")
	fmt.Fprintf(&sb, "        idle_streams_timeout %d;\n", opts.IdleTimeoutSec)

	if opts.Passphrase != "" {
		fmt.Fprintf(&sb, "        srt_passphrase %s;\n", opts.Passphrase)
	}

	if opts.WebhookURL != "" {
		fmt.Fprintf(&sb, "        on_event_url %s;\n", opts.WebhookURL)
	}

	sb.WriteString("\n        app {\n")
	sb.WriteString("            app_player live;\n")
	sb.WriteString("            app_publisher live;\n")
	sb.WriteString("            allow publish all;\n")
	sb.WriteString("            allow play all;\n")
	sb.WriteString("        }\n")
	sb.WriteString("    }\n")
	sb.WriteString("}\n")

	return sb.String()
}

// BuildSRTLAArgs constructs the CLI arguments for the srtla_rec receiver proxy.
func BuildSRTLAArgs(srtlaPort int, srtHost string, srtPort int, logLevel string) []string {
	if logLevel == "" {
		logLevel = "info"
	}
	if srtHost == "" {
		srtHost = "127.0.0.1"
	}
	return []string{
		"--srtla_port", strconv.Itoa(srtlaPort),
		"--srt_hostname", srtHost,
		"--srt_port", strconv.Itoa(srtPort),
		"--log_level", logLevel,
	}
}
