package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"log/slog"

	"github.com/Trendyzima/federated-content/internal/backend"
	"github.com/Trendyzima/federated-content/internal/config"
	"github.com/Trendyzima/federated-content/internal/store"
)

func TestLocalWebFingerProvisionsDurableActorFromBackend(t *testing.T) {
	dsn := os.Getenv("FEDERATION_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("FEDERATION_TEST_DATABASE_URL is not configured")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	state, err := store.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	handle := "actor" + time.Now().UTC().Format("150405")
	userID := "user_" + handle
	backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/internal/v1/federation/readyz":
			w.WriteHeader(http.StatusOK)
		case r.URL.Path == "/internal/v1/federation/actors/"+handle:
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "data": map[string]any{"user_id": userID, "handle": handle, "display_name": "Alice", "summary": "A local profile", "federation_enabled": true}, "error": nil})
		default:
			http.NotFound(w, r)
		}
	}))
	defer backendServer.Close()
	backendClient, err := backend.New(backendServer.URL, strings.Repeat("t", 40), backendServer.Client())
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Config{HTTPAddr: ":8081", PublicBaseURL: "https://federation.testagram.site", InstanceDomain: "testagram.site", ServiceActorUsername: "testagram", ServiceActorName: "Testagram"}
	handler := NewWithRuntime(cfg, slog.New(slog.NewTextHandler(os.Stdout, nil)), state, backendClient)

	finger := httptest.NewRecorder()
	handler.ServeHTTP(finger, httptest.NewRequest(http.MethodGet, "/.well-known/webfinger?resource=acct:"+handle+"@testagram.site", nil))
	if finger.Code != http.StatusOK {
		t.Fatalf("WebFinger status=%d body=%s", finger.Code, finger.Body.String())
	}
	var result struct {
		Subject string `json:"subject"`
		Links   []struct {
			Rel  string `json:"rel"`
			Href string `json:"href"`
		} `json:"links"`
	}
	if err := json.Unmarshal(finger.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Subject != "acct:"+handle+"@testagram.site" || len(result.Links) != 1 || result.Links[0].Href != "https://federation.testagram.site/users/"+handle {
		t.Fatalf("unexpected WebFinger response: %#v", result)
	}

	actor := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/users/"+handle, nil)
	req.Header.Set("Accept", "application/activity+json")
	handler.ServeHTTP(actor, req)
	if actor.Code != http.StatusOK {
		t.Fatalf("actor status=%d body=%s", actor.Code, actor.Body.String())
	}
	var document map[string]any
	if err := json.Unmarshal(actor.Body.Bytes(), &document); err != nil {
		t.Fatal(err)
	}
	publicKey, ok := document["publicKey"].(map[string]any)
	if !ok || publicKey["publicKeyPem"] == "" || document["preferredUsername"] != handle {
		t.Fatalf("actor has no persisted key or profile mapping: %#v", document)
	}
}

func TestLocalActorEndpointsRecheckFederationOptIn(t *testing.T) {
	dsn := os.Getenv("FEDERATION_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("FEDERATION_TEST_DATABASE_URL is not configured")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	state, err := store.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()

	handle := "revoke" + strconv.FormatInt(time.Now().UnixNano(), 10)
	userID := "user_" + handle
	enabled := true
	const token = "test-federation-service-token-which-is-long-enough"
	backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+token {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case "/internal/v1/federation/readyz":
			w.WriteHeader(http.StatusOK)
		case "/internal/v1/federation/actors/" + handle:
			if !enabled {
				http.NotFound(w, r)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"ok": true,
				"data": map[string]any{
					"user_id": userID, "handle": handle, "display_name": "Revocable",
					"summary":            "Opt-in must be checked on every protocol endpoint",
					"federation_enabled": true,
				},
				"error": nil,
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer backendServer.Close()
	backendClient, err := backend.New(backendServer.URL, token, backendServer.Client())
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Config{HTTPAddr: ":8081", PublicBaseURL: "https://federation.testagram.site", InstanceDomain: "testagram.site", ServiceActorUsername: "testagram", ServiceActorName: "Testagram"}
	handler := NewWithRuntime(cfg, slog.New(slog.NewTextHandler(os.Stdout, nil)), state, backendClient)

	finger := httptest.NewRecorder()
	handler.ServeHTTP(finger, httptest.NewRequest(http.MethodGet, "/.well-known/webfinger?resource=acct:"+handle+"@testagram.site", nil))
	if finger.Code != http.StatusOK {
		t.Fatalf("initial WebFinger status=%d body=%s", finger.Code, finger.Body.String())
	}
	actorURI := "https://federation.testagram.site/users/" + handle

	// A previously provisioned actor must stop resolving immediately after opt-out.
	enabled = false
	for _, tc := range []struct {
		name    string
		method  string
		path    string
		headers map[string]string
		body    string
	}{
		{"actor document", http.MethodGet, "/users/" + handle, map[string]string{"Accept": activityJSON}, ""},
		{"outbox", http.MethodGet, "/users/" + handle + "/outbox", map[string]string{"Accept": activityJSON}, ""},
		{"inbox", http.MethodPost, "/users/" + handle + "/inbox", map[string]string{"Content-Type": activityJSON}, "{}"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			for key, value := range tc.headers {
				req.Header.Set(key, value)
			}
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			if rec.Code != http.StatusNotFound {
				t.Fatalf("disabled account still accessible: status=%d body=%s actor=%s", rec.Code, rec.Body.String(), actorURI)
			}
		})
	}
}

func TestParseLocalAcctRequiresExactLocalDomainAndValidHandle(t *testing.T) {
	for _, tc := range []struct {
		value string
		ok    bool
	}{
		{"acct:alice@testagram.site", true},
		{"acct:Alice@testagram.site", true},
		{"acct:alice@evil.example", true},
		{"https://testagram.site/@alice", false},
		{"acct:../admin@testagram.site", false},
		{"acct:alice", false},
		{"acct:@testagram.site", false},
	} {
		_, _, ok := parseLocalAcct(tc.value)
		if ok != tc.ok {
			t.Fatalf("parseLocalAcct(%q) ok=%v want=%v", tc.value, ok, tc.ok)
		}
	}
}
