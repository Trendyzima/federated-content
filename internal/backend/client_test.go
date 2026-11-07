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
		if r.Header.Get("X-Testagram-API-Version") != "1" || r.Header.Get("X-Request-ID") == "" { t.Errorf("missing API version/request ID headers") }
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

func TestListOutboxUsesVersionedPrivateAPIAndDurableCursor(t *testing.T) {
	const token = "test-federation-service-token-which-is-long-enough"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/v1/federation/outbox" { t.Errorf("path=%q", r.URL.Path) }
		if r.URL.Query().Get("limit") != "20" || r.URL.Query().Get("after") != "ZXZlbnQtMQ" { t.Errorf("query=%s", r.URL.RawQuery) }
		if r.Header.Get("Authorization") != "Bearer "+token || r.Header.Get("X-Testagram-API-Version") != "1" || r.Header.Get("X-Request-ID") == "" {
			t.Errorf("missing authenticated versioned request headers")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"data":{"events":[{"$id":"event-2","event_type":"testagram.post.published","actor_id":"user-1","object_id":"post-2","payload":{"content":"hello"},"created_at":"2026-11-07T10:00:00Z"}],"next_cursor":"cursor-2","has_more":false},"error":null}`))
	}))
	defer server.Close()
	client, err := New(server.URL, token, server.Client())
	if err != nil { t.Fatal(err) }
	page, err := client.ListOutbox(context.Background(), "ZXZlbnQtMQ", 20)
	if err != nil { t.Fatal(err) }
	if len(page.Events) != 1 || page.Events[0].ID != "event-2" || page.NextCursor != "cursor-2" || page.HasMore {
		t.Fatalf("unexpected outbox page: %+v", page)
	}
	if _, err := client.ListOutbox(context.Background(), "", 100); err == nil { t.Fatal("expected limit validation") }
}
