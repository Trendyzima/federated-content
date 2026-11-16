package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Trendyzima/federated-content/internal/atproto"
)

type backendATProtoFeedRequest struct {
	ActorDIDs     []string          `json:"actor_dids"`
	LimitPerActor int               `json:"limit_per_actor"`
	Cursors       map[string]string `json:"cursors,omitempty"`
}

type atprotoAuthorFeedClient interface {
	ResolveProfile(context.Context, string) (atproto.Profile, error)
	AuthorFeed(context.Context, string, int, string) (atproto.FeedPage, error)
	AuthorFeedByDID(context.Context, string, int, string) (atproto.FeedPage, error)
}

type backendATProtoActorFeed struct {
	DID    string         `json:"did"`
	Handle string         `json:"handle"`
	Cursor string         `json:"cursor,omitempty"`
	Posts  []atproto.Post `json:"posts"`
}

// backendAuthorizedATProtoFeed is deliberately private: Backend supplies the
// exact remote DIDs it has already selected from the viewer's authorized graph.
// Federation never chooses followed actors and never creates Testagram users.
func (s *Server) backendAuthorizedATProtoFeed(w http.ResponseWriter, r *http.Request) {
	token := s.cfg.FederationIngressToken
	if token == "" || !validInternalBearer(r, token) {
		writeError(w, http.StatusUnauthorized, "internal_auth_required", "A valid Backend service credential is required")
		return
	}
	if s.atproto == nil {
		writeError(w, http.StatusServiceUnavailable, "atproto_unavailable", "The Bluesky public feed bridge is unavailable")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 16<<10)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var input backendATProtoFeedRequest
	if err := decoder.Decode(&input); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeError(w, http.StatusRequestEntityTooLarge, "request_too_large", "The authorized actor list exceeds 16 KiB")
		} else {
			writeError(w, http.StatusBadRequest, "invalid_request", "The authorized actor list is invalid")
		}
		return
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		writeError(w, http.StatusBadRequest, "invalid_request", "The request must contain exactly one JSON object")
		return
	}
	if len(input.ActorDIDs) == 0 || len(input.ActorDIDs) > 20 {
		writeError(w, http.StatusBadRequest, "invalid_actor_count", "Between 1 and 20 Backend-authorized DIDs are required")
		return
	}
	if input.LimitPerActor == 0 {
		input.LimitPerActor = 10
	}
	if input.LimitPerActor < 1 || input.LimitPerActor > 25 {
		writeError(w, http.StatusBadRequest, "invalid_limit", "limit_per_actor must be between 1 and 25")
		return
	}
	seen := make(map[string]struct{}, len(input.ActorDIDs))
	for i, did := range input.ActorDIDs {
		did = strings.TrimSpace(did)
		input.ActorDIDs[i] = did
		if !atproto.ValidDID(did) {
			writeError(w, http.StatusBadRequest, "invalid_did", "A valid AT Protocol DID is required")
			return
		}
		if _, exists := seen[did]; exists {
			writeError(w, http.StatusBadRequest, "duplicate_did", "The actor list must contain unique DIDs")
			return
		}
		seen[did] = struct{}{}
	}
	if len(input.Cursors) > len(seen) {
		writeError(w, http.StatusBadRequest, "invalid_cursor_map", "Cursors may only be supplied for authorized actor DIDs")
		return
	}
	for did, cursor := range input.Cursors {
		if _, ok := seen[did]; !ok || len(cursor) > 512 || strings.ContainsAny(cursor, "\r\n") {
			writeError(w, http.StatusBadRequest, "invalid_cursor_map", "Each cursor must belong to an authorized DID and be no longer than 512 characters")
			return
		}
	}

	ctx, cancel := context.WithTimeout(r.Context(), 12*time.Second)
	defer cancel()
	results := make([]backendATProtoActorFeed, len(input.ActorDIDs))
	type job struct {
		index int
		did   string
	}
	jobs := make(chan job)
	errs := make(chan error, len(input.ActorDIDs))
	var wg sync.WaitGroup
	workers := 4
	if len(input.ActorDIDs) < workers {
		workers = len(input.ActorDIDs)
	}
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range jobs {
				page, err := s.atproto.AuthorFeedByDID(ctx, item.did, input.LimitPerActor, input.Cursors[item.did])
				if err != nil {
					errs <- err
					continue
				}
				results[item.index] = backendATProtoActorFeed{DID: page.Profile.DID, Handle: page.Profile.Handle, Cursor: page.Cursor, Posts: page.Posts}
			}
		}()
	}
	for i, did := range input.ActorDIDs {
		select {
		case jobs <- job{index: i, did: did}:
		case <-ctx.Done():
			break
		}
	}
	close(jobs)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			writeError(w, http.StatusBadGateway, "atproto_upstream_unavailable", "A requested public author feed could not be retrieved")
			return
		}
	}
	if err := ctx.Err(); err != nil {
		writeError(w, http.StatusGatewayTimeout, "atproto_timeout", "The public author-feed request exceeded its time budget")
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":         true,
		"data":       map[string]any{"source": "bluesky-atproto", "actors": results},
		"error":      nil,
		"request_id": internalRequestID(r),
	})
}
