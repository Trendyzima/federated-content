package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Trendyzima/federated-content/internal/atproto"
	"github.com/Trendyzima/federated-content/internal/config"
)

type recordingATProtoFeedClient struct {
	cursors map[string]string
	calls   int
}

func (f *recordingATProtoFeedClient) AuthorFeedByDID(_ context.Context, did string, _ int, cursor string) (atproto.FeedPage, error) {
	f.calls++
	f.cursors[did] = cursor
	return atproto.FeedPage{
		Profile: atproto.Profile{DID: did, Handle: "alice.bsky.social"},
		Cursor:  "next-" + did,
		Source:  "bluesky-atproto",
	}, nil
}

func TestBackendATProtoFeedForwardsCursorPerAuthorizedDID(t *testing.T) {
	const token = "test-backend-to-federation-token-long-enough"
	didA := "did:plc:222222222222222222222222"
	didB := "did:plc:333333333333333333333333"
	fake := &recordingATProtoFeedClient{cursors: make(map[string]string)}
	server := &Server{
		cfg: config.Config{FederationIngressToken: token},
		atproto: fake,
	}
	body := `{"actor_dids":["` + didA + `","` + didB + `"],"limit_per_actor":10,"cursors":{"` + didA + `":"cursor-a","` + didB + `":"cursor-b"}}`
	req := httptest.NewRequest(http.MethodPost, "/internal/v1/atproto/feed", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	server.backendAuthorizedATProtoFeed(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("feed status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if fake.calls != 2 || fake.cursors[didA] != "cursor-a" || fake.cursors[didB] != "cursor-b" {
		t.Fatalf("per-DID cursors were not forwarded: calls=%d cursors=%#v", fake.calls, fake.cursors)
	}
	var envelope struct {
		OK bool `json:"ok"`
		Data struct {
			Actors []backendATProtoActorFeed `json:"actors"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	if !envelope.OK || len(envelope.Data.Actors) != 2 {
		t.Fatalf("unexpected response envelope: %s", rec.Body.String())
	}
}

func TestBackendATProtoFeedRejectsCursorOutsideAuthorizedDIDs(t *testing.T) {
	const token = "test-backend-to-federation-token-long-enough"
	did := "did:plc:222222222222222222222222"
	fake := &recordingATProtoFeedClient{cursors: make(map[string]string)}
	server := &Server{
		cfg: config.Config{FederationIngressToken: token},
		atproto: fake,
	}
	body := `{"actor_dids":["` + did + `"],"limit_per_actor":10,"cursors":{"did:plc:333333333333333333333333":"foreign-cursor"}}`
	req := httptest.NewRequest(http.MethodPost, "/internal/v1/atproto/feed", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	server.backendAuthorizedATProtoFeed(rec, req)
	if rec.Code != http.StatusBadRequest || fake.calls != 0 {
		t.Fatalf("foreign cursor was not rejected before upstream access: status=%d calls=%d body=%s", rec.Code, fake.calls, rec.Body.String())
	}
}

func TestBackendATProtoFeedRejectsNewlineCursor(t *testing.T) {
	const token = "test-backend-to-federation-token-long-enough"
	did := "did:plc:222222222222222222222222"
	fake := &recordingATProtoFeedClient{cursors: make(map[string]string)}
	server := &Server{
		cfg: config.Config{FederationIngressToken: token},
		atproto: fake,
	}
	body := `{"actor_dids":["` + did + `"],"limit_per_actor":10,"cursors":{"` + did + `":"bad\n cursor"}}`
	req := httptest.NewRequest(http.MethodPost, "/internal/v1/atproto/feed", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	server.backendAuthorizedATProtoFeed(rec, req)
	if rec.Code != http.StatusBadRequest || fake.calls != 0 {
		t.Fatalf("newline cursor was not rejected before upstream access: status=%d calls=%d body=%s", rec.Code, fake.calls, rec.Body.String())
	}
}
