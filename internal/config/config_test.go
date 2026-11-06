package config

import (
	"strings"
	"testing"
)

func setValidEnvironment(t *testing.T) {
	t.Helper()
	t.Setenv("APP_ENV", "development")
	t.Setenv("HTTP_ADDR", ":8081")
	t.Setenv("PUBLIC_BASE_URL", "https://federation.testagram.site")
	t.Setenv("INSTANCE_DOMAIN", "testagram.site")
	t.Setenv("SERVICE_ACTOR_USERNAME", "Testagram")
}

func TestLoadDefaultsAndNormalizesActorName(t *testing.T) {
	setValidEnvironment(t)
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.ServiceActorUsername != "testagram" {
		t.Fatalf("expected normalized username, got %q", cfg.ServiceActorUsername)
	}
}

func TestProductionRejectsHTTPBaseURL(t *testing.T) {
	setValidEnvironment(t)
	t.Setenv("APP_ENV", "production")
	t.Setenv("PUBLIC_BASE_URL", "http://federation.testagram.site")
	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "HTTPS") {
		t.Fatalf("expected HTTPS validation error, got %v", err)
	}
}

func TestLoadRejectsMalformedActorUsername(t *testing.T) {
	setValidEnvironment(t)
	t.Setenv("SERVICE_ACTOR_USERNAME", "bad@handle")
	_, err := Load()
	if err == nil {
		t.Fatal("expected invalid username error")
	}
}
