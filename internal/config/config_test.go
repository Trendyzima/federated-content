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

func TestLoadRejectsPublicURLWithQueryOrFragment(t *testing.T) {
	for _, rawURL := range []string{
		"https://federation.testagram.site?redirect=https://evil.example",
		"https://federation.testagram.site#fragment",
		"https://user:password@federation.testagram.site",
	} {
		t.Run(rawURL, func(t *testing.T) {
			setValidEnvironment(t)
			t.Setenv("PUBLIC_BASE_URL", rawURL)
			if _, err := Load(); err == nil {
				t.Fatalf("expected invalid public URL %q to be rejected", rawURL)
			}
		})
	}
}

func TestLoadRejectsUnrelatedFederationHost(t *testing.T) {
	setValidEnvironment(t)
	t.Setenv("PUBLIC_BASE_URL", "https://federation.attacker.example")
	if _, err := Load(); err == nil {
		t.Fatal("expected public host outside INSTANCE_DOMAIN to be rejected")
	}
}

func TestLoadAllowsFederationSubdomain(t *testing.T) {
	setValidEnvironment(t)
	t.Setenv("PUBLIC_BASE_URL", "https://federation.testagram.site")
	if _, err := Load(); err != nil {
		t.Fatalf("expected federation subdomain to be allowed, got %v", err)
	}
}

func TestLoadRejectsNonDevelopmentHTTP(t *testing.T) {
	setValidEnvironment(t)
	t.Setenv("APP_ENV", "staging")
	t.Setenv("PUBLIC_BASE_URL", "http://federation.testagram.site")
	if _, err := Load(); err == nil {
		t.Fatal("expected HTTP outside development/test to be rejected")
	}
}

func TestLoadRejectsPathLikeActorUsernames(t *testing.T) {
	for _, username := range []string{"../admin", "bad/handle", "bad\\handle", "bad handle", "..", ".", "name?x=1"} {
		t.Run(username, func(t *testing.T) {
			setValidEnvironment(t)
			t.Setenv("SERVICE_ACTOR_USERNAME", username)
			if _, err := Load(); err == nil {
				t.Fatalf("expected invalid username %q to be rejected", username)
			}
		})
	}
}
