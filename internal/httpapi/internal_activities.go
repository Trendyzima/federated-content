package httpapi

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

)

const maxInternalActivityBody = 1 << 20

type enqueueActivityRequest struct {
	ActivityID  string          `json:"activity_id"`
	ActorUserID string          `json:"actor_user_id"`
	Activity    json.RawMessage `json:"activity"`
}

type activityEnvelope struct {
	ID     string          `json:"id"`
	Type   string          `json:"type"`
	Actor  string          `json:"actor"`
	Object json.RawMessage `json:"object"`
}

func (s *Server) enqueueAuthorizedActivity(w http.ResponseWriter, r *http.Request) {
	if s.cfg.BackendInternalToken == "" || !validInternalBearer(r, s.cfg.BackendInternalToken) {
		writeError(w, http.StatusUnauthorized, "internal_auth_required", "A valid Backend service credential is required")
		return
	}
	if s.state == nil || s.backend == nil {
		writeError(w, http.StatusServiceUnavailable, "federation_not_ready", "Federation storage and Backend integration are required")
		return
	}
	if r.Header.Get("Content-Type") != "application/json" {
		writeError(w, http.StatusUnsupportedMediaType, "unsupported_media_type", "Content-Type must be application/json")
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxInternalActivityBody)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var input enqueueActivityRequest
	if err := decoder.Decode(&input); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeError(w, http.StatusRequestEntityTooLarge, "activity_too_large", "The activity request exceeds 1 MiB")
		} else {
			writeError(w, http.StatusBadRequest, "invalid_activity_request", "The activity request is invalid")
		}
		return
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		writeError(w, http.StatusBadRequest, "invalid_activity_request", "The request must contain exactly one JSON object")
		return
	}
	input.ActivityID = strings.TrimSpace(input.ActivityID)
	input.ActorUserID = strings.TrimSpace(input.ActorUserID)
	if input.ActivityID == "" || len(input.ActivityID) > 2048 || input.ActorUserID == "" || len(input.ActorUserID) > 128 || !json.Valid(input.Activity) {
		writeError(w, http.StatusBadRequest, "invalid_activity_request", "Activity ID, actor user ID and a valid ActivityPub object are required")
		return
	}
	var activity activityEnvelope
	if err := json.Unmarshal(input.Activity, &activity); err != nil || !validAuthorizedActivity(activity, input.ActivityID) {
		writeError(w, http.StatusBadRequest, "invalid_activity", "The ActivityPub activity is invalid or does not match its declared ID")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	profile, err := s.backend.ResolveActorByID(ctx, input.ActorUserID)
	if err != nil {
		writeError(w, http.StatusForbidden, "actor_not_authorized", "Backend did not authorize this local federation actor")
		return
	}
	if profile.UserID != input.ActorUserID || !profile.FederationEnabled || profile.Handle == "" {
		writeError(w, http.StatusForbidden, "actor_not_authorized", "The local account is not enabled for federation")
		return
	}
	actorURI := s.cfg.PublicBaseURL + "/users/" + url.PathEscape(profile.Handle)
	if activity.Actor != actorURI {
		writeError(w, http.StatusForbidden, "actor_mismatch", "The activity actor does not match the Backend-verified account")
		return
	}
	actor, err := s.state.EnsureActor(ctx, profile.UserID, profile.Handle, actorURI, profile.DisplayName, profile.Summary)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "actor_key_unavailable", "The local federation signing identity could not be prepared")
		return
	}
	if actor.ActorURI != activity.Actor || actor.PrivateKeyPEM == "" {
		writeError(w, http.StatusServiceUnavailable, "actor_key_unavailable", "The local federation signing identity is unavailable")
		return
	}
	inboxes, err := s.state.RemoteFollowerInboxes(ctx, profile.UserID, 1000)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "recipient_lookup_failed", "Federation recipients could not be loaded")
		return
	}
	if err := s.state.EnqueueOutbox(ctx, input.ActivityID, actor.ActorURI, input.Activity, inboxes); err != nil {
		writeError(w, http.StatusConflict, "activity_enqueue_conflict", "The activity could not be accepted idempotently")
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusAccepted, map[string]any{
		"ok": true,
		"data": map[string]any{"activity_id": input.ActivityID, "recipient_count": len(inboxes), "accepted": true},
		"error": nil,
		"request_id": internalRequestID(r),
	})
}

func validInternalBearer(r *http.Request, expected string) bool {
	value := strings.TrimSpace(r.Header.Get("Authorization"))
	if !strings.HasPrefix(value, "Bearer ") {
		return false
	}
	got := strings.TrimSpace(strings.TrimPrefix(value, "Bearer "))
	if got == "" || len(got) != len(expected) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(expected)) == 1
}

func validAuthorizedActivity(activity activityEnvelope, expectedID string) bool {
	if activity.ID != expectedID || activity.Actor == "" || len(activity.Actor) > 2048 {
		return false
	}
	idURL, err := url.Parse(activity.ID)
	if err != nil || idURL.Scheme != "https" || idURL.Host == "" || idURL.User != nil || idURL.Fragment != "" {
		return false
	}
	actorURL, err := url.Parse(activity.Actor)
	if err != nil || actorURL.Scheme != "https" || actorURL.Host == "" || actorURL.User != nil || actorURL.Fragment != "" {
		return false
	}
	switch activity.Type {
	case "Create", "Update", "Delete", "Like", "Announce":
	default:
		return false
	}
	if len(activity.Object) == 0 || !json.Valid(activity.Object) || bytes.Equal(bytes.TrimSpace(activity.Object), []byte("null")) {
		return false
	}
	return true
}


var internalRequestIDPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)

func internalRequestID(r *http.Request) string {
	value := strings.TrimSpace(r.Header.Get("X-Request-ID"))
	if internalRequestIDPattern.MatchString(value) {
		return value
	}
	return "federation-" + time.Now().UTC().Format("20060102T150405.000000000")
}
