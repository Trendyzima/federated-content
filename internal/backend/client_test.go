package backend

import (
	"encoding/json"
	"errors"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestReadyUsesPrivateFederationReadinessContract(t *testing.T) {
	const token = "test-federation-service-token-which-is-long-enough"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Testagram-API-Version") != "1" || r.Header.Get("X-Request-ID") == "" {
			t.Errorf("missing API version/request ID headers")
		}
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

func TestResolveActorRequiresFreshAPIEnvelopeAndMatchingHandle(t *testing.T) {
	const token = "test-federation-service-token-which-is-long-enough"
	t.Run("valid envelope", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/internal/v1/federation/actors/alice" {
				t.Errorf("actor lookup path=%q", r.URL.Path)
			}
			if r.Header.Get("Authorization") != "Bearer "+token || r.Header.Get("X-Testagram-API-Version") != "1" || r.Header.Get("X-Request-ID") == "" {
				t.Errorf("private actor request headers are incomplete")
			}
			_, _ = w.Write([]byte(`{"ok":true,"data":{"user_id":"user_123","handle":"alice","display_name":"Alice","summary":"Hello","federation_enabled":true},"error":null}`))
		}))
		defer server.Close()
		client, err := New(server.URL, token, server.Client())
		if err != nil {
			t.Fatal(err)
		}
		actor, err := client.ResolveActor(context.Background(), "Alice")
		if err != nil {
			t.Fatal(err)
		}
		if actor.UserID != "user_123" || actor.Handle != "alice" || !actor.FederationEnabled {
			t.Fatalf("unexpected actor profile: %+v", actor)
		}
	})
	t.Run("bare profile is rejected", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"user_id":"user_123","handle":"alice","federation_enabled":true}`))
		}))
		defer server.Close()
		client, err := New(server.URL, token, server.Client())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := client.ResolveActor(context.Background(), "alice"); err == nil {
			t.Fatal("bare profile response must not satisfy the Fresh API envelope contract")
		}
	})
	t.Run("mismatched handle is rejected", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"ok":true,"data":{"user_id":"user_123","handle":"mallory","federation_enabled":true},"error":null}`))
		}))
		defer server.Close()
		client, err := New(server.URL, token, server.Client())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := client.ResolveActor(context.Background(), "alice"); err == nil {
			t.Fatal("actor handle mismatch must be rejected")
		}
	})
}

func TestRemoteFollowUsesBackendCanonicalAuthority(t *testing.T) {
	const token = "test-federation-service-token-which-is-long-enough"
	actorURI := "https://remote.example/users/bob"
	followID := "https://remote.example/activities/follow-1"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/internal/v1/federation/remote-follows" {
			t.Errorf("method/path = %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer "+token || r.Header.Get("X-Testagram-API-Version") != "1" || r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("private follow request headers are incomplete")
		}
		var input map[string]string
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			t.Error(err)
		}
		if input["local_user_id"] != "123e4567-e89b-12d3-a456-426614174000" || input["remote_actor_uri"] != actorURI || input["activity_id"] != followID {
			t.Errorf("remote follow request body = %#v", input)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"data":{"status":"accepted"},"error":null,"request_id":"test"}`))
	}))
	defer server.Close()
	client, err := New(server.URL, token, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	if err := client.AcceptRemoteFollow(context.Background(), "123e4567-e89b-12d3-a456-426614174000", actorURI, followID); err != nil {
		t.Fatal(err)
	}
}

func TestRemoteFollowMapsBackendRejectionToPermanentError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer server.Close()
	client, err := New(server.URL, strings.Repeat("t", 40), server.Client())
	if err != nil {
		t.Fatal(err)
	}
	err = client.AcceptRemoteFollow(context.Background(), "123e4567-e89b-12d3-a456-426614174000", "https://remote.example/users/bob", "https://remote.example/activities/follow-1")
	if !errors.Is(err, ErrRemoteFollowNotAuthorized) {
		t.Fatalf("error = %v, want ErrRemoteFollowNotAuthorized", err)
	}
}

func TestRemoteUnfollowCallsBackendBeforeLocalRoutingRemoval(t *testing.T) {
	const token = "test-federation-service-token-which-is-long-enough"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/internal/v1/federation/remote-follows/undo" {
			t.Errorf("method/path = %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"data":{"removed":true},"error":null,"request_id":"test"}`))
	}))
	defer server.Close()
	client, err := New(server.URL, token, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	if err := client.RemoveRemoteFollow(context.Background(), "123e4567-e89b-12d3-a456-426614174000", "https://remote.example/users/bob", "https://remote.example/activities/undo-1"); err != nil {
		t.Fatal(err)
	}
}
