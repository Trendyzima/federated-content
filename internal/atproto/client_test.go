package atproto

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAuthorFeedUsesOfficialXRPCShapeAndReturnsNormalizedPosts(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/xrpc/com.atproto.identity.resolveHandle", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("handle") != "alice.bsky.social" {
			t.Errorf("unexpected handle: %s", r.URL.RawQuery)
		}
		if r.Header.Get("Authorization") != "" {
			t.Error("public profile lookup must not send user credentials")
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"did": "did:plc:abcdefghijklmnopqrstuvwx"})
	})
	mux.HandleFunc("/xrpc/app.bsky.actor.getProfile", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("actor") != "did:plc:abcdefghijklmnopqrstuvwx" {
			t.Errorf("profile did mismatch: %s", r.URL.RawQuery)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"did": "did:plc:abcdefghijklmnopqrstuvwx", "handle": "alice.bsky.social", "displayName": "Alice"})
	})
	mux.HandleFunc("/xrpc/app.bsky.feed.getAuthorFeed", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("actor") != "did:plc:abcdefghijklmnopqrstuvwx" || r.URL.Query().Get("limit") != "10" {
			t.Errorf("feed query mismatch: %s", r.URL.RawQuery)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"cursor": "next-page", "feed": []any{map[string]any{"post": map[string]any{
			"uri": "at://did:plc:abcdefghijklmnopqrstuvwx/app.bsky.feed.post/abc", "cid": "bafyreia",
			"author":     map[string]any{"did": "did:plc:abcdefghijklmnopqrstuvwx", "handle": "alice.bsky.social"},
			"record":     map[string]any{"text": "Hello from Bluesky", "createdAt": "2026-11-15T10:00:00Z"},
			"replyCount": 2, "repostCount": 3, "likeCount": 4, "quoteCount": 1,
			"labels": []any{map[string]string{"val": "graphic-content"}},
		}}}})
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	client, err := NewClientWithBaseURL(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	page, err := client.AuthorFeed(context.Background(), "Alice.Bsky.Social", 10, "")
	if err != nil {
		t.Fatal(err)
	}
	if page.Profile.DID != "did:plc:abcdefghijklmnopqrstuvwx" || page.Profile.DisplayName != "Alice" {
		t.Fatalf("unexpected profile: %#v", page.Profile)
	}
	if page.Cursor != "next-page" || page.Source != "bluesky-atproto" || len(page.Posts) != 1 {
		t.Fatalf("unexpected feed page: %#v", page)
	}
	post := page.Posts[0]
	if post.Text != "Hello from Bluesky" || post.LikeCount != 4 || post.QuoteCount != 1 || len(post.Labels) != 1 || post.Labels[0] != "graphic-content" {
		t.Fatalf("unexpected normalized post: %#v", post)
	}
}

func TestInvalidHandleRejectedBeforeNetwork(t *testing.T) {
	client, err := NewClientWithBaseURL("http://127.0.0.1:9999", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.ResolveProfile(context.Background(), "https://evil.example/path")
	if err != ErrInvalidHandle {
		t.Fatalf("expected ErrInvalidHandle, got %v", err)
	}
}

func TestResolverCannotSubstituteInvalidDID(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"did": "https://evil.example"})
	}))
	defer server.Close()
	client, err := NewClientWithBaseURL(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.ResolveProfile(context.Background(), "alice.bsky.social")
	if err == nil || !strings.Contains(err.Error(), "invalid DID") {
		t.Fatalf("expected invalid DID rejection, got %v", err)
	}
}

func TestValidHandle(t *testing.T) {
	for _, handle := range []string{"alice.bsky.social", "testagram.example.org", "a-b.example"} {
		if !ValidHandle(handle) {
			t.Errorf("valid handle rejected: %s", handle)
		}
	}
	for _, handle := range []string{"", "Alice Bsky Social", "a..b", "https://example.com", "alice@example.com", "-bad.example"} {
		if ValidHandle(handle) {
			t.Errorf("invalid handle accepted: %s", handle)
		}
	}
}
