package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// AuthConfig defines authorization rules applied across all ingest protocols.
type AuthConfig struct {
	AllowedPaths []string `yaml:"allowed_paths"`
}

// RTMPConfig encapsulates RTMP ingest settings.
type RTMPConfig struct {
	Port int `yaml:"port"`
}

// SRTConfig encapsulates SRT ingest and internal proxy settings.
type SRTConfig struct {
	Port         int    `yaml:"port"`
	LatencyMs    int    `yaml:"latency_ms"`
	LatencyMaxMs int    `yaml:"latency_max_ms"`
	Passphrase   string `yaml:"passphrase"`
	PlayerPort   int    `yaml:"player_port"`
	HTTPPort     int    `yaml:"http_port"`
}

// SRTLAConfig encapsulates SRTLA receiver proxy settings.
type SRTLAConfig struct {
	Port int `yaml:"port"`
}

// RISTConfig encapsulates librist receiver settings.
type RISTConfig struct {
	Port     int    `yaml:"port"`
	BufferMs int    `yaml:"buffer_ms"`
	Profile  string `yaml:"profile"`
	Secret   string `yaml:"secret"`
}

// OutputConfig encapsulates target stream output settings.
type OutputConfig struct {
	Host string `yaml:"host"`
	Port int    `yaml:"port"`
}

// URL constructs the UDP destination URL for relaying.
func (o OutputConfig) URL() string {
	host := o.Host
	if host == "" {
		host = "127.0.0.1"
	}
	port := o.Port
	if port == 0 {
		port = 8888
	}
	return fmt.Sprintf("udp://%s:%d", host, port)
}

// Config represents the application configuration.
type Config struct {
	RTMP          RTMPConfig    `yaml:"rtmp"`
	SRT           SRTConfig     `yaml:"srt"`
	SRTLA         SRTLAConfig   `yaml:"srtla"`
	RIST          RISTConfig    `yaml:"rist"`
	Output        OutputConfig  `yaml:"output"`
	SourceTimeout time.Duration `yaml:"source_timeout"`
	StatsPort     int           `yaml:"stats_port"`
	LogLevel      string        `yaml:"log_level"`
	Auth          AuthConfig    `yaml:"auth"`
}

// Default returns a Config instance initialized with sensible defaults.
func Default() *Config {
	return &Config{
		RTMP: RTMPConfig{
			Port: 1935,
		},
		SRT: SRTConfig{
			Port:         8890,
			LatencyMs:    200,
			LatencyMaxMs: 5000,
			Passphrase:   "",
			PlayerPort:   8190,
			HTTPPort:     8181,
		},
		SRTLA: SRTLAConfig{
			Port: 5000,
		},
		RIST: RISTConfig{
			Port:     5001,
			BufferMs: 1000,
			Profile:  "main",
			Secret:   "",
		},
		Output: OutputConfig{
			Host: "127.0.0.1",
			Port: 8888,
		},
		SourceTimeout: 10 * time.Second,
		StatsPort:     8080,
		LogLevel:      "info",
		Auth: AuthConfig{
			AllowedPaths: []string{},
		},
	}
}

// Load reads and parses a YAML configuration file from the specified path.
func Load(path string) (*Config, error) {
	cfg := Default()

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil
		}
		return nil, fmt.Errorf("reading config file: %w", err)
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parsing config yaml: %w", err)
	}

	return cfg, nil
}
