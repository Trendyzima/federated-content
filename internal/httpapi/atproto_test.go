package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Trendyzima/federated-content/internal/atproto"
)

func TestATProtoActorHandlerReturnsPublicProfile(t *testing.T) {
	upstream:=httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,r *http.Request){
		switch r.URL.Path {
		case "/xrpc/com.atproto.identity.resolveHandle":
			_ = json.NewEncoder(w).Encode(map[string]string{"did":"did:plc:abcdefghijklmnopqrstuvwx"})
		case "/xrpc/app.bsky.actor.getProfile":
			_ = json.NewEncoder(w).Encode(map[string]any{"did":"did:plc:abcdefghijklmnopqrstuvwx","handle":"alice.bsky.social","displayName":"Alice"})
		default:
			http.NotFound(w,r)
		}
	}))
	defer upstream.Close()
	client,err:=atproto.NewClientWithBaseURL(upstream.URL,upstream.Client());if err!=nil{t.Fatal(err)}
	s:=&Server{atproto:client}
	req:=httptest.NewRequest(http.MethodGet,"/v1/atproto/actors/alice.bsky.social",nil)
	req.SetPathValue("handle","alice.bsky.social")
	rec:=httptest.NewRecorder()
	s.atprotoActor(rec,req)
	if rec.Code!=http.StatusOK { t.Fatalf("wanted 200 got %d: %s",rec.Code,rec.Body.String()) }
	if rec.Header().Get("Cache-Control")!="public, max-age=60" { t.Fatal("public profile cache policy missing") }
}

func TestATProtoFeedHandlerRejectsExcessiveLimit(t *testing.T) {
	s:=&Server{atproto:atproto.NewClient()}
	req:=httptest.NewRequest(http.MethodGet,"/v1/atproto/actors/alice.bsky.social/feed?limit=26",nil)
	req.SetPathValue("handle","alice.bsky.social")
	rec:=httptest.NewRecorder()
	s.atprotoAuthorFeed(rec,req)
	if rec.Code!=http.StatusBadRequest { t.Fatalf("wanted 400 got %d",rec.Code) }
}
