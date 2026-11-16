package httpapi

import (
    "encoding/json"
    "errors"
    "io"
    "net/http"
    "strings"

    "github.com/Trendyzima/federated-content/internal/store"
)

type backendRemoteObjectsRequest struct {
    ActorURIs []string `json:"actor_uris"`
    Limit int `json:"limit"`
    Cursor string `json:"cursor,omitempty"`
}

// backendAuthorizedRemoteObjects is a private projection API. Its actor
// allowlist must come from Backend after viewer-specific authorization.
func (s *Server) backendAuthorizedRemoteObjects(w http.ResponseWriter, r *http.Request) {
    token := s.cfg.FederationIngressToken
    if token == "" || !validInternalBearer(r, token) {
        writeError(w, http.StatusUnauthorized, "internal_auth_required", "A valid Backend service credential is required")
        return
    }
    if s.state == nil {
        writeError(w, http.StatusServiceUnavailable, "federation_storage_unavailable", "Durable remote object storage is unavailable")
        return
    }
    r.Body = http.MaxBytesReader(w, r.Body, 32<<10)
    decoder := json.NewDecoder(r.Body)
    decoder.DisallowUnknownFields()
    var input backendRemoteObjectsRequest
    if err := decoder.Decode(&input); err != nil {
        var tooLarge *http.MaxBytesError
        if errors.As(err, &tooLarge) {
            writeError(w, http.StatusRequestEntityTooLarge, "request_too_large", "The actor allowlist exceeds 32 KiB")
        } else {
            writeError(w, http.StatusBadRequest, "invalid_request", "The remote object request is invalid")
        }
        return
    }
    if err := decoder.Decode(&struct{}{}); err != io.EOF {
        writeError(w, http.StatusBadRequest, "invalid_request", "The request must contain exactly one JSON object")
        return
    }
    if input.Limit == 0 { input.Limit = 50 }
    if input.Limit < 1 || input.Limit > 100 || len(input.ActorURIs) == 0 || len(input.ActorURIs) > 100 || len(input.Cursor) > 2048 {
        writeError(w, http.StatusBadRequest, "invalid_request_bounds", "Provide 1-100 actor URIs, a limit from 1-100 and a cursor no longer than 2048 characters")
        return
    }
    for i := range input.ActorURIs { input.ActorURIs[i] = strings.TrimSpace(input.ActorURIs[i]) }
    page, err := s.state.ListAuthorizedRemoteObjects(r.Context(), input.ActorURIs, input.Limit, input.Cursor)
    if err != nil {
        writeError(w, http.StatusBadRequest, "invalid_remote_object_query", "The actor allowlist or pagination cursor is invalid")
        return
    }
    w.Header().Set("Cache-Control", "no-store")
    writeJSON(w, http.StatusOK, map[string]any{
        "ok": true,
        "data": map[string]any{"source":"activitypub", "items":page.Items, "next_cursor":page.NextCursor, "has_more":page.NextCursor!=""},
        "error": nil,
        "request_id": internalRequestID(r),
    })
}

var _ = store.RemoteObjectPage{}
