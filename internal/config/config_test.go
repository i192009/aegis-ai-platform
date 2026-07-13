package config

import "testing"

func TestLoadDefaults(t *testing.T) {
	t.Setenv("AEGIS_HTTP_ADDRESS", "")
	t.Setenv("AEGIS_SHUTDOWN_TIMEOUT", "")
	t.Setenv("AEGIS_MAX_HEADER_BYTES", "")

	cfg, err := Load("gateway")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Service != "gateway" || cfg.HTTPAddress != ":8080" {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
}

func TestLoadRejectsBadDuration(t *testing.T) {
	t.Setenv("AEGIS_SHUTDOWN_TIMEOUT", "eventually")
	if _, err := Load("gateway"); err == nil {
		t.Fatal("Load() error = nil, want invalid duration error")
	}
}
