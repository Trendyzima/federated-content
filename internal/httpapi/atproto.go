package httpapi

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Trendyzima/federated-content/internal/atproto"
)

func (s *Server) atprotoActor(w http.ResponseWriter, r *http.Request) {
	if s.atproto == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "Bluesky public profile bridge is unavailable"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	profile, err := s.atproto.ResolveProfile(ctx, r.PathValue("handle"))
	if err != nil {
		writeATProtoError(w, err)
		return
	}
	w.Header().Set("Cache-Control", "public, max-age=60")
	writeJSON(w, http.StatusOK, map[string]any{"source": "bluesky-atproto", "actor": profile})
}

func (s *Server) atprotoAuthorFeed(w http.ResponseWriter, r *http.Request) {
	if s.atproto == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "Bluesky public feed bridge is unavailable"})
		return
	}
	limit := 20
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > 25 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "limit must be between 1 and 25"})
			return
		}
		limit = n
	}
	cursor := r.URL.Query().Get("cursor")
	if len(cursor) > 512 || strings.ContainsAny(cursor, "\r\n") {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid cursor"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 6*time.Second)
	defer cancel()
	page, err := s.atproto.AuthorFeed(ctx, r.PathValue("handle"), limit, cursor)
	if err != nil {
		writeATProtoError(w, err)
		return
	}
	w.Header().Set("Cache-Control", "public, max-age=30")
	writeJSON(w, http.StatusOK, page)
}

func writeATProtoError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, atproto.ErrInvalidHandle):
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_bluesky_handle"})
	case errors.Is(err, atproto.ErrNotFound):
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "bluesky_actor_not_found"})
	default:
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "bluesky_public_api_unavailable"})
	}
}
