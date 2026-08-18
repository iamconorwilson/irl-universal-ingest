package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	cfg := Default()
	if cfg.RTMP.Port != 1935 {
		t.Errorf("expected default RTMP port 1935, got %d", cfg.RTMP.Port)
	}
	if cfg.SRT.Port != 8890 {
		t.Errorf("expected default SRT port 8890, got %d", cfg.SRT.Port)
	}
	if cfg.SRTLA.Port != 5000 {
		t.Errorf("expected default SRTLA port 5000, got %d", cfg.SRTLA.Port)
	}
	if cfg.RIST.Port != 5001 {
		t.Errorf("expected default RIST port 5001, got %d", cfg.RIST.Port)
	}
	if cfg.Output.URL() != "udp://127.0.0.1:8888" {
		t.Errorf("expected default Output URL udp://127.0.0.1:8888, got %s", cfg.Output.URL())
	}
	if cfg.StatsPort != 8080 {
		t.Errorf("expected default stats port 8080, got %d", cfg.StatsPort)
	}
	if cfg.SourceTimeout != 10*time.Second {
		t.Errorf("expected default timeout 10s, got %v", cfg.SourceTimeout)
	}
}

func TestLoadConfigFile(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "test-config.yaml")

	content := `
rtmp:
  port: 1936
srt:
  port: 8891
srtla:
  port: 5002
rist:
  port: 5003
output:
  host: "127.0.0.1"
  port: 9999
source_timeout: "5s"
stats_port: 8081
log_level: "debug"
auth:
  allowed_paths:
    - "/custom"
`
	if err := os.WriteFile(configPath, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write test config: %v", err)
	}

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("unexpected error loading config: %v", err)
	}

	if cfg.RTMP.Port != 1936 {
		t.Errorf("expected RTMP port 1936, got %d", cfg.RTMP.Port)
	}
	if cfg.SRT.Port != 8891 {
		t.Errorf("expected SRT port 8891, got %d", cfg.SRT.Port)
	}
	if cfg.SRTLA.Port != 5002 {
		t.Errorf("expected SRTLA port 5002, got %d", cfg.SRTLA.Port)
	}
	if cfg.RIST.Port != 5003 {
		t.Errorf("expected RIST port 5003, got %d", cfg.RIST.Port)
	}
	if cfg.Output.URL() != "udp://127.0.0.1:9999" {
		t.Errorf("expected Output URL udp://127.0.0.1:9999, got %s", cfg.Output.URL())
	}
	if cfg.SourceTimeout != 5*time.Second {
		t.Errorf("expected timeout 5s, got %v", cfg.SourceTimeout)
	}
	if len(cfg.Auth.AllowedPaths) != 1 || cfg.Auth.AllowedPaths[0] != "/custom" {
		t.Errorf("unexpected allowed paths: %v", cfg.Auth.AllowedPaths)
	}
}

func TestValidatePassphraseTooShort(t *testing.T) {
	cfg := Default()
	cfg.SRT.Passphrase = "short"

	if err := cfg.Validate(); err == nil {
		t.Fatalf("expected validation error for short passphrase")
	}
}

func TestValidatePassphraseEmptyIsAllowed(t *testing.T) {
	cfg := Default()
	cfg.SRT.Passphrase = ""

	if err := cfg.Validate(); err != nil {
		t.Fatalf("unexpected validation error for empty passphrase: %v", err)
	}
}

func TestValidatePortCollision(t *testing.T) {
	cfg := Default()
	cfg.SRT.HTTPPort = cfg.RTMP.Port

	if err := cfg.Validate(); err == nil {
		t.Fatalf("expected validation error for colliding ports")
	}
}

func TestValidateDefaultsAreValid(t *testing.T) {
	cfg := Default()

	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected default config to be valid, got: %v", err)
	}
}
