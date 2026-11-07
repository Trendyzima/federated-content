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
		if r.URL.Path != "/internal/v1/federation/outbox" {
			t.Errorf("path=%q", r.URL.Path)
		}
		if r.URL.Query().Get("limit") != "20" || r.URL.Query().Get("after") != "ZXZlbnQtMQ" {
			t.Errorf("query=%s", r.URL.RawQuery)
		}
		if r.Header.Get("Authorization") != "Bearer "+token || r.Header.Get("X-Testagram-API-Version") != "1" || r.Header.Get("X-Request-ID") == "" {
			t.Errorf("missing authenticated versioned request headers")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"data":{"events":[{"$id":"event-2","event_type":"testagram.post.published","actor_id":"user-1","object_id":"post-2","payload":{"content":"hello"},"created_at":"2026-11-07T10:00:00Z"}],"next_cursor":"Y3Vyc29yLTI","has_more":false},"error":null}`))
	}))
	defer server.Close()

	client, err := New(server.URL, token, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	page, err := client.ListOutbox(context.Background(), "ZXZlbnQtMQ", 20)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 1 || page.Events[0].ID != "event-2" || page.NextCursor != "Y3Vyc29yLTI" || page.HasMore {
		t.Fatalf("unexpected outbox page: %+v", page)
	}
	if _, err := client.ListOutbox(context.Background(), "", 100); err == nil {
		t.Fatal("expected limit validation")
	}
}

func TestListOutboxRejectsMalformedCursorAndEvents(t *testing.T) {
	const token = "test-federation-service-token-which-is-long-enough"
	cases := []struct {
		name string
		data string
	}{
		{
			name: "invalid next cursor",
			data: `{"ok":true,"data":{"events":[],"next_cursor":"not-base64!","has_more":true}}`,
		},
		{
			name: "duplicate event IDs",
			data: `{"ok":true,"data":{"events":[{"$id":"e1","event_type":"testagram.post.published","actor_id":"u1","object_id":"p1","payload":{},"created_at":"2026-11-07T10:00:00Z"},{"$id":"e1","event_type":"testagram.post.published","actor_id":"u1","object_id":"p2","payload":{},"created_at":"2026-11-07T10:00:00Z"}],"next_cursor":"","has_more":false}}`,
		},
		{
			name: "invalid event timestamp",
			data: `{"ok":true,"data":{"events":[{"$id":"e1","event_type":"testagram.post.published","actor_id":"u1","object_id":"p1","payload":{},"created_at":"yesterday"}],"next_cursor":"","has_more":false}}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(tc.data))
			}))
			defer server.Close()
			client, err := New(server.URL, token, server.Client())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := client.ListOutbox(context.Background(), "", 20); err == nil {
				t.Fatal("expected malformed outbox response rejection")
			}
		})
	}
}
