package backend

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestVerifyRemoteSubscriptionCallsPrivateBackend(t *testing.T) {
	const owner = "123e4567-e89b-42d3-a456-426614174000"
	const token = "0123456789abcdef0123456789abcdef"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/v1/federation/subscriptions/verify" || r.Method != http.MethodPost {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer "+token || r.Header.Get("X-Testagram-API-Version") != "1" {
			t.Fatal("private Backend authentication headers are missing")
		}
		var input RemoteSubscriptionVerification
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			t.Fatal(err)
		}
		if input.LocalUserID != owner || input.Protocol != "bluesky" || input.Identity != "did:plc:222222222222222222222222" || !input.Verified {
			t.Fatalf("unexpected verification payload: %#v", input)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"data":{"verification_status":"verified"}}`))
	}))
	defer server.Close()
	client, err := New(server.URL, token, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = client.VerifyRemoteSubscription(context.Background(), RemoteSubscriptionVerification{
		LocalUserID: owner, Protocol: "bluesky", Identity: "did:plc:222222222222222222222222",
		Handle: "alice.bsky.social", Verified: true,
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestValidLocalUserIDRequiresCanonicalUUID(t *testing.T) {
	for _, value := range []string{
		"123e4567-e89b-42d3-a456-426614174000",
		"550e8400-e29b-41d4-a716-446655440000",
	} {
		if !ValidLocalUserID(value) {
			t.Errorf("expected valid UUID %q", value)
		}
	}
	for _, value := range []string{"not-a-uuid", "123e4567-e89b-42d3-a456-42661417400z", "00000000-0000-0000-0000-000000000000"} {
		if ValidLocalUserID(value) {
			t.Errorf("expected invalid UUID %q", value)
		}
	}
}
