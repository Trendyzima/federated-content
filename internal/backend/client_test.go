package backend

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestReadyUsesPrivateFederationReadinessContract(t *testing.T) {
	const token = "test-federation-service-token-which-is-long-enough"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/v1/federation/readyz" {
			t.Errorf("readiness path=%q", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer "+token {
			t.Errorf("authorization=%q", got)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client, err := New(server.URL, token, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestReadyRejectsUnavailablePrivateBackend(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	client, err := New(server.URL, strings.Repeat("t", 40), server.Client())
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Ready(context.Background()); err == nil {
		t.Fatal("unavailable private Backend was reported ready")
	}
}
