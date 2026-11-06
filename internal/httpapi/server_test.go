package httpapi

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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

func TestWebFingerReturnsConfiguredActor(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/.well-known/webfinger?resource=acct:testagram@testagram.site", nil)
	testServer().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "https://federation.testagram.site/users/testagram") {
		t.Fatalf("unexpected WebFinger response: %d %s", rec.Code, rec.Body.String())
	}
}

func TestWebFingerDoesNotClaimUnknownUsers(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/.well-known/webfinger?resource=acct:unknown@testagram.site", nil)
	testServer().ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
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
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "\"type\": \"Application\"") {
		t.Fatalf("unexpected actor response: %d %s", rec.Code, rec.Body.String())
	}
}

func TestInboxFailsClosed(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/users/testagram/inbox", strings.NewReader("{}"))
	testServer().ServeHTTP(rec, req)
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("expected 501 until signature verification exists, got %d", rec.Code)
	}
}
