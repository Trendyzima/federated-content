package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Trendyzima/federated-content/internal/activitypub"
	"github.com/Trendyzima/federated-content/internal/atproto"
	"github.com/Trendyzima/federated-content/internal/backend"
)

type remoteSubscriptionVerificationRequest struct {
	LocalUserID string `json:"local_user_id"`
	Protocol    string `json:"protocol"`
	Identity    string `json:"identity"`
}

type remoteActivityPubActorDocument struct {
	ID                string `json:"id"`
	Type              string `json:"type"`
	PreferredUsername string `json:"preferredUsername"`
	Name              string `json:"name"`
	Inbox             string `json:"inbox"`
	PublicKey         struct {
		ID    string `json:"id"`
		Owner string `json:"owner"`
		PEM   string `json:"publicKeyPem"`
	} `json:"publicKey"`
}

// verifyRemoteSubscription resolves an external identity using protocol-native
// rules and then asks Backend to promote the exact pending owner/identity pair.
// Client-supplied handles are never treated as identity proof.
func (s *Server) verifyRemoteSubscription(w http.ResponseWriter, r *http.Request) {
	token := s.cfg.FederationIngressToken
	if token == "" || !validInternalBearer(r, token) {
		writeError(w, http.StatusUnauthorized, "internal_auth_required", "A valid Backend service credential is required")
		return
	}
	if s.backend == nil {
		writeError(w, http.StatusServiceUnavailable, "backend_unavailable", "Private Backend verification is unavailable")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 16<<10)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var input remoteSubscriptionVerificationRequest
	if err := decoder.Decode(&input); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeError(w, http.StatusRequestEntityTooLarge, "request_too_large", "The remote identity request exceeds 16 KiB")
		} else {
			writeError(w, http.StatusBadRequest, "invalid_request", "A local owner, protocol and canonical remote identity are required")
		}
		return
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		writeError(w, http.StatusBadRequest, "invalid_request", "The request must contain exactly one JSON object")
		return
	}
	input.LocalUserID = strings.ToLower(strings.TrimSpace(input.LocalUserID))
	input.Protocol = strings.ToLower(strings.TrimSpace(input.Protocol))
	input.Identity = strings.TrimSpace(input.Identity)
	if !backend.ValidLocalUserID(input.LocalUserID) || len(input.Identity) < 12 || len(input.Identity) > 2048 {
		writeError(w, http.StatusBadRequest, "invalid_identity", "The local owner or canonical identity is invalid")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 18*time.Second)
	defer cancel()
	var handle string
	switch input.Protocol {
	case "activitypub":
		var err error
		handle, err = resolveRemoteActivityPubActor(ctx, input.Identity)
		if err != nil {
			s.logger.Warn("remote ActivityPub subscription verification failed", "reason", err.Error())
			writeError(w, http.StatusUnprocessableEntity, "activitypub_actor_unverified", "The remote ActivityPub actor could not be verified")
			return
		}
	case "bluesky":
		if !atproto.ValidDID(input.Identity) || s.atproto == nil {
			writeError(w, http.StatusBadRequest, "invalid_did", "A valid Bluesky DID is required")
			return
		}
		page, err := s.atproto.AuthorFeedByDID(ctx, input.Identity, 1, "")
		if err != nil || page.Profile.DID != input.Identity || page.Profile.Handle == "" {
			s.logger.Warn("remote Bluesky subscription verification failed", "reason", "DID profile resolution failed")
			writeError(w, http.StatusBadGateway, "bluesky_identity_unavailable", "The Bluesky DID could not be verified at this time")
			return
		}
		handle = page.Profile.Handle
	default:
		writeError(w, http.StatusBadRequest, "unsupported_protocol", "Protocol must be activitypub or bluesky")
		return
	}

	if err := s.backend.VerifyRemoteSubscription(ctx, backend.RemoteSubscriptionVerification{
		LocalUserID: input.LocalUserID,
		Protocol: input.Protocol,
		Identity: input.Identity,
		Handle: handle,
		Verified: true,
	}); err != nil {
		s.logger.Error("Backend rejected remote subscription verification callback", "error", err)
		writeError(w, http.StatusBadGateway, "backend_verification_unavailable", "Backend could not persist the verified remote identity")
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, map[string]any{
		"ok": true,
		"data": map[string]string{"protocol": input.Protocol, "identity": input.Identity, "handle": handle, "verification_status": "verified"},
		"error": nil,
		"request_id": internalRequestID(r),
	})
}

func resolveRemoteActivityPubActor(ctx context.Context, actorURI string) (string, error) {
	u, err := url.Parse(actorURI)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return "", errors.New("actor URI is not a canonical HTTPS URL")
	}
	client := activitypub.NewRemoteHTTPSClient()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Accept", "application/activity+json, application/ld+json")
	req.Header.Set("User-Agent", "Testagram-Federation/1.0")
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", errors.New("remote actor endpoint returned a non-success status")
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, (1<<20)+1))
	if err != nil || len(raw) > 1<<20 {
		return "", errors.New("remote actor document is unreadable or too large")
	}
	var actor remoteActivityPubActorDocument
	if err := json.Unmarshal(raw, &actor); err != nil {
		return "", errors.New("remote actor document is invalid JSON")
	}
	switch actor.Type {
	case "Person", "Service", "Organization", "Application":
	default:
		return "", errors.New("remote document is not an ActivityPub actor")
	}
	if actor.ID != u.String() || actor.PreferredUsername == "" || len(actor.PreferredUsername) > 128 || strings.ContainsAny(actor.PreferredUsername, " /?#@") {
		return "", errors.New("remote actor identity does not match requested URI")
	}
	inboxURL, err := url.Parse(actor.Inbox)
	if err != nil || inboxURL.Scheme != "https" || inboxURL.Host == "" || inboxURL.User != nil || inboxURL.Fragment != "" {
		return "", errors.New("remote actor inbox is not a valid HTTPS endpoint")
	}
	if actor.PublicKey.ID != "" && (actor.PublicKey.Owner != actor.ID || !strings.HasPrefix(actor.PublicKey.ID, actor.ID+"#") || actor.PublicKey.PEM == "") {
		return "", errors.New("remote actor public key owner does not match actor")
	}
	return actor.PreferredUsername + "@" + strings.ToLower(u.Hostname()), nil
}
