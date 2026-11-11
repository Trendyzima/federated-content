package httpapi

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Trendyzima/federated-content/internal/config"
)

func TestInternalActivityEndpointRejectsMissingServiceCredential(t *testing.T) {
	cfg := config.Config{BackendInternalToken: strings.Repeat("x", 40)}
	handler := NewWithRuntime(cfg, slog.Default(), nil, nil)
	req := httptest.NewRequest(http.MethodPost, "/internal/v1/activities", strings.NewReader("{}"))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

func TestInternalActivityEndpointRequiresRuntimeAfterAuthentication(t *testing.T) {
	token := strings.Repeat("s", 40)
	cfg := config.Config{BackendInternalToken: token}
	handler := NewWithRuntime(cfg, slog.Default(), nil, nil)
	req := httptest.NewRequest(http.MethodPost, "/internal/v1/activities", strings.NewReader("{}"))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

func TestValidAuthorizedActivityRequiresMatchingHTTPSIdentity(t *testing.T) {
	valid := activityEnvelope{
		ID:     "https://testagram.site/activities/123",
		Type:   "Create",
		Actor:  "https://testagram.site/users/alice",
		Object: []byte(`{"id":"https://testagram.site/posts/123"}`),
	}
	if !validAuthorizedActivity(valid, valid.ID) {
		t.Fatal("expected valid ActivityPub Create activity")
	}
	if validAuthorizedActivity(valid, "https://testagram.site/activities/other") {
		t.Fatal("accepted activity ID mismatch")
	}
	valid.Actor = "http://testagram.site/users/alice"
	if validAuthorizedActivity(valid, valid.ID) {
		t.Fatal("accepted non-HTTPS actor URI")
	}
	valid.Actor = "https://testagram.site/users/alice"
	valid.Type = "DeleteAll"
	if validAuthorizedActivity(valid, valid.ID) {
		t.Fatal("accepted unsupported activity type")
	}
}

func TestInternalActivityEndpointUsesDedicatedIngressCredential(t *testing.T) {
	backendToken := strings.Repeat("b", 40)
	ingressToken := strings.Repeat("i", 40)
	cfg := config.Config{
		BackendInternalToken:   backendToken,
		FederationIngressToken: ingressToken,
	}
	handler := NewWithRuntime(cfg, slog.Default(), nil, nil)

	wrong := httptest.NewRequest(http.MethodPost, "/internal/v1/activities", strings.NewReader("{}"))
	wrong.Header.Set("Content-Type", "application/json")
	wrong.Header.Set("Authorization", "Bearer "+backendToken)
	wrongRec := httptest.NewRecorder()
	handler.ServeHTTP(wrongRec, wrong)
	if wrongRec.Code != http.StatusUnauthorized {
		t.Fatalf("Backend read token status = %d, want %d", wrongRec.Code, http.StatusUnauthorized)
	}

	correct := httptest.NewRequest(http.MethodPost, "/internal/v1/activities", strings.NewReader("{}"))
	correct.Header.Set("Content-Type", "application/json")
	correct.Header.Set("Authorization", "Bearer "+ingressToken)
	correctRec := httptest.NewRecorder()
	handler.ServeHTTP(correctRec, correct)
	if correctRec.Code != http.StatusServiceUnavailable {
		t.Fatalf("Federation ingress token status = %d, want %d", correctRec.Code, http.StatusServiceUnavailable)
	}
}
