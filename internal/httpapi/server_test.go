package httpapi

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Trendyzima/federated-content/internal/backend"
	"github.com/Trendyzima/federated-content/internal/config"
)

func testServer() http.Handler {
	return New(config.Config{
		HTTPAddr: ":8081", PublicBaseURL: "https://federation.testagram.site",
		InstanceDomain: "testagram.site", ServiceActorUsername: "testagram",
		ServiceActorName: "Testagram", ServiceActorSummary: "Test instance",
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestHealth(t *testing.T) {
	rec := httptest.NewRecorder()
	testServer().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "ok") {
		t.Fatalf("unexpected health response: %d %s", rec.Code, rec.Body.String())
	}
}

func TestReadinessRemainsDisabledUntilSecureFederationIsImplemented(t *testing.T) {
	rec := httptest.NewRecorder()
	testServer().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), "preview_only") {
		t.Fatalf("expected preview-only 503 readiness, got %d %s", rec.Code, rec.Body.String())
	}
}

func TestWebFingerReturnsConfiguredActor(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/.well-known/webfinger?resource=acct:testagram@testagram.site", nil)
	testServer().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "https://federation.testagram.site/users/testagram") {
		t.Fatalf("unexpected WebFinger response: %d %s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); !strings.HasPrefix(got, "application/jrd+json") {
		t.Fatalf("expected JRD content type, got %q", got)
	}
}

func TestWebFingerFailsClosedWhenBackendIsUnavailable(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/.well-known/webfinger?resource=acct:unknown@testagram.site", nil)
	testServer().ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), "federation_identity_unavailable") {
		t.Fatalf("expected fail-closed 503 without Backend identity lookup, got %d %s", rec.Code, rec.Body.String())
	}
}

func TestActorRequiresActivityPubAccept(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/users/testagram", nil)
	testServer().ServeHTTP(rec, req)
	if rec.Code != http.StatusNotAcceptable {
		t.Fatalf("expected 406, got %d", rec.Code)
	}
}

func TestActorReturnsActivityPubDocument(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/users/testagram", nil)
	req.Header.Set("Accept", "application/activity+json")
	testServer().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "\"type\":\"Application\"") {
		t.Fatalf("unexpected actor response: %d %s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); !strings.HasPrefix(got, activityJSON) {
		t.Fatalf("expected ActivityPub content type, got %q", got)
	}
}

func TestOutboxReturnsEmptyOrderedCollection(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/users/testagram/outbox", nil)
	req.Header.Set("Accept", "application/activity+json")
	testServer().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "\"OrderedCollection\"") {
		t.Fatalf("unexpected outbox response: %d %s", rec.Code, rec.Body.String())
	}
}

func TestActivityPubContentTypeValidation(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value string
		want  bool
	}{
		{"activity json", "application/activity+json", true},
		{"activity json with charset", "application/activity+json; charset=utf-8", true},
		{"json-ld activity profile", `application/ld+json; profile="https://www.w3.org/ns/activitystreams"`, true},
		{"case insensitive media type", "Application/Activity+JSON", true},
		{"missing", "", false},
		{"plain json", "application/json", false},
		{"malicious suffix", "application/activity+json-malicious", false},
		{"text", "text/plain", false},
		{"malformed", `application/activity+json; charset="unterminated`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := isActivityPubContentType(tc.value); got != tc.want {
				t.Fatalf("isActivityPubContentType(%q)=%v want=%v", tc.value, got, tc.want)
			}
		})
	}
}

func TestInboxFailsClosed(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/users/testagram/inbox", strings.NewReader("{}"))
	testServer().ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), "inbox_unavailable") {
		t.Fatalf("expected fail-closed 503 without durable inbox storage, got %d %s", rec.Code, rec.Body.String())
	}
}

func TestObjectRequiresActivityPubAccept(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/objects/123e4567-e89b-42d3-a456-426614174001", nil)
	testServer().ServeHTTP(rec, req)
	if rec.Code != http.StatusNotAcceptable {
		t.Fatalf("expected 406 for non-ActivityPub object request, got %d %s", rec.Code, rec.Body.String())
	}
}

func TestObjectFailsClosedWithoutBackend(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/objects/123e4567-e89b-42d3-a456-426614174001", nil)
	req.Header.Set("Accept", activityJSON)
	testServer().ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), "object_unavailable") {
		t.Fatalf("expected fail-closed 503 without Backend object resolver, got %d %s", rec.Code, rec.Body.String())
	}
}

func TestObjectReturnsCanonicalBackendNote(t *testing.T) {
	const token = "test-federation-service-token-which-is-long-enough"
	postID := "123e4567-e89b-42d3-a456-426614174001"
	backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/v1/federation/objects/"+postID || r.Method != http.MethodGet {
			t.Errorf("unexpected Backend request: %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"data":{"id":"https://federation.testagram.site/objects/123e4567-e89b-42d3-a456-426614174001","type":"Note","content":"hello"},"error":null}`))
	}))
	defer backendServer.Close()
	client, err := backend.New(backendServer.URL, token, backendServer.Client())
	if err != nil {
		t.Fatal(err)
	}
	handler := NewWithRuntime(config.Config{PublicBaseURL: "https://federation.testagram.site"}, slog.New(slog.NewTextHandler(io.Discard, nil)), nil, client)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/objects/"+postID, nil)
	req.Header.Set("Accept", activityJSON)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "\"type\":\"Note\"") {
		t.Fatalf("unexpected canonical object response: %d %s", rec.Code, rec.Body.String())
	}
	if !strings.HasPrefix(rec.Header().Get("Content-Type"), activityJSON) {
		t.Fatalf("expected ActivityPub content type, got %q", rec.Header().Get("Content-Type"))
	}
}
