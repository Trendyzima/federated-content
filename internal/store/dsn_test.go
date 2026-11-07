package store

import (
	"strings"
	"testing"
)

func TestValidateDatabaseURLRequiresTLSForRemote(t *testing.T) {
	_, err := validateDatabaseURL("postgresql://user:pass@ep-example.neon.tech/federation")
	if err == nil || !strings.Contains(err.Error(), "sslmode") {
		t.Fatalf("expected TLS validation, got %v", err)
	}
}

func TestValidateDatabaseURLAcceptsNeonPooledTLS(t *testing.T) {
	got, err := validateDatabaseURL("postgresql://user:pass@ep-example-pooler.neon.tech/federation?sslmode=require")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, "sslmode=require") || !strings.Contains(got, "connect_timeout=5") {
		t.Fatalf("missing safe connection parameters: %s", got)
	}
}

func TestValidateDatabaseURLAllowsLocalDevelopment(t *testing.T) {
	for _, raw := range []string{
		"postgres://user:pass@localhost:5432/federation",
		"postgres://user:pass@federation-db:5432/federation?sslmode=disable",
	} {
		if _, err := validateDatabaseURL(raw); err != nil {
			t.Fatalf("local DSN rejected: %v", err)
		}
	}
}

func TestValidateDatabaseURLRejectsMalformedValues(t *testing.T) {
	for _, raw := range []string{
		"",
		"postgres://localhost/federation",
		"postgres://user:pass@ep-example.neon.tech/federation?sslmode=disable",
		"postgres://user:pass@ep-example.neon.tech/federation?sslmode=require#fragment",
		"mysql://user:pass@localhost/federation",
	} {
		if _, err := validateDatabaseURL(raw); err == nil {
			t.Fatalf("expected invalid DSN to be rejected: %q", raw)
		}
	}
}
