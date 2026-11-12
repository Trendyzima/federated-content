package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"time"

	"github.com/Trendyzima/federated-content/internal/backend"
	"github.com/Trendyzima/federated-content/internal/store"
)

type InboxStore interface {
	ClaimInbox(context.Context, int, time.Duration) ([]store.InboxActivity, error)
	AckInbox(context.Context, string, string) error
	FailInbox(context.Context, string, string, string, bool) error
	EnsureActor(context.Context, string, string, string, string, string) (store.Actor, error)
	AddRemoteFollower(context.Context, string, string) error
	RemoveRemoteFollower(context.Context, string, string) error
	RemoteActorInbox(context.Context, string) (string, error)
	EnqueueOutbox(context.Context, string, string, []byte, []string) error
}

type InboxBackend interface {
	ResolveActor(context.Context, string) (backend.ActorProfile, error)
	AcceptRemoteFollow(context.Context, string, string, string) error
	RemoveRemoteFollow(context.Context, string, string, string, string) error
}

type InboxWorker struct {
	store         InboxStore
	backend       InboxBackend
	publicBaseURL string
	logger        *slog.Logger
	interval      time.Duration
	batch         int
}

type permanentInboxError struct{ err error }

func (e *permanentInboxError) Error() string { return e.err.Error() }
func permanentInbox(message string) error    { return &permanentInboxError{err: errors.New(message)} }

func NewInboxWorker(state InboxStore, backendClient InboxBackend, publicBaseURL string, logger *slog.Logger) (*InboxWorker, error) {
	publicBaseURL = strings.TrimRight(strings.TrimSpace(publicBaseURL), "/")
	u, err := url.Parse(publicBaseURL)
	if state == nil || backendClient == nil || err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return nil, errors.New("signed inbox worker requires Federation storage, private Backend integration and an HTTPS public origin")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &InboxWorker{store: state, backend: backendClient, publicBaseURL: publicBaseURL, logger: logger, interval: 3 * time.Second, batch: 25}, nil
}

func (w *InboxWorker) Run(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		if _, err := w.ProcessBatch(ctx); err != nil && ctx.Err() == nil {
			w.logger.Error("federation inbox batch failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (w *InboxWorker) ProcessBatch(ctx context.Context) (int, error) {
	items, err := w.store.ClaimInbox(ctx, w.batch, 45*time.Second)
	if err != nil {
		return 0, fmt.Errorf("claim signed inbox activities: %w", err)
	}
	var firstErr error
	for _, item := range items {
		err := w.process(ctx, item)
		if err == nil {
			err = w.store.AckInbox(ctx, item.ActivityID, item.LeaseToken)
		} else {
			var permanent *permanentInboxError
			isPermanent := errors.As(err, &permanent)
			if failErr := w.store.FailInbox(ctx, item.ActivityID, item.LeaseToken, err.Error(), isPermanent); failErr != nil {
				err = fmt.Errorf("%v; persist inbox retry/dead-letter: %w", err, failErr)
			} else if isPermanent {
				w.logger.Warn("federation inbox activity moved to dead letter", "activity_id", item.ActivityID, "reason", err.Error())
				err = nil
			}
		}
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return len(items), firstErr
}

type inboxEnvelope struct {
	ID     string          `json:"id"`
	Type   string          `json:"type"`
	Actor  string          `json:"actor"`
	Object json.RawMessage `json:"object"`
}

type followEnvelope struct {
	ID     string          `json:"id"`
	Type   string          `json:"type"`
	Actor  string          `json:"actor"`
	Object json.RawMessage `json:"object"`
}

func (w *InboxWorker) process(ctx context.Context, item store.InboxActivity) error {
	if item.ActivityID == "" || item.ActorURI == "" || item.RecipientUsername == "" || item.LeaseToken == "" || !json.Valid(item.Payload) {
		return permanentInbox("claimed inbox activity is missing required fields")
	}
	var activity inboxEnvelope
	if err := json.Unmarshal(item.Payload, &activity); err != nil || activity.ID != item.ActivityID || activity.Actor != item.ActorURI || activity.Type != item.ActivityType || activity.Object == nil {
		return permanentInbox("stored signed ActivityPub envelope is inconsistent")
	}
	profile, err := w.backend.ResolveActor(ctx, item.RecipientUsername)
	if errors.Is(err, backend.ErrActorNotFound) {
		return permanentInbox("local actor is no longer eligible for federation")
	}
	if err != nil {
		return fmt.Errorf("recheck local actor eligibility: %w", err)
	}
	handle := strings.ToLower(strings.TrimSpace(profile.Handle))
	if profile.UserID == "" || handle != item.RecipientUsername || !profile.FederationEnabled {
		return permanentInbox("local actor is no longer eligible for federation")
	}
	actorURI := w.publicBaseURL + "/users/" + url.PathEscape(handle)
	localActor, err := w.store.EnsureActor(ctx, profile.UserID, handle, actorURI, profile.DisplayName, profile.Summary)
	if err != nil {
		return fmt.Errorf("ensure local signing actor: %w", err)
	}

	switch activity.Type {
	case "Follow":
		target, ok := activityObjectURI(activity.Object)
		if !ok || target != localActor.ActorURI {
			return permanentInbox("Follow activity targets a different local actor")
		}
		if err := w.backend.AcceptRemoteFollow(ctx, profile.UserID, item.ActorURI, item.ActivityID); err != nil {
			if errors.Is(err, backend.ErrRemoteFollowNotAuthorized) || errors.Is(err, backend.ErrRemoteFollowConflict) {
				return permanentInbox("Backend rejected the remote follow or local account is no longer eligible")
			}
			return fmt.Errorf("authorize remote follow in Backend: %w", err)
		}
		if err := w.store.AddRemoteFollower(ctx, profile.UserID, item.ActorURI); err != nil {
			return fmt.Errorf("persist authorized remote follower routing cache: %w", err)
		}
		inbox, err := w.store.RemoteActorInbox(ctx, item.ActorURI)
		if err != nil {
			return fmt.Errorf("resolve verified remote actor inbox: %w", err)
		}
		acceptID := deterministicAcceptID(w.publicBaseURL, item.ActivityID, localActor.ActorURI)
		payload, err := json.Marshal(map[string]any{
			"@context": "https://www.w3.org/ns/activitystreams",
			"id":       acceptID,
			"type":     "Accept",
			"actor":    localActor.ActorURI,
			"to":       []string{item.ActorURI},
			"object":   json.RawMessage(item.Payload),
		})
		if err != nil {
			return permanentInbox("could not encode Accept activity")
		}
		if err := w.store.EnqueueOutbox(ctx, acceptID, localActor.ActorURI, payload, []string{inbox}); err != nil {
			return fmt.Errorf("enqueue signed Accept delivery: %w", err)
		}
		return nil
	case "Undo":
		var originalID string
		var original followEnvelope
		if json.Unmarshal(activity.Object, &originalID) == nil {
			if !isHTTPSActivityID(originalID) {
				return permanentInbox("Undo activity references an invalid Follow ID")
			}
		} else {
			if err := json.Unmarshal(activity.Object, &original); err != nil || original.Type != "Follow" || original.Actor != item.ActorURI {
				return permanentInbox("Undo activity does not contain the matching original Follow")
			}
			target, ok := activityObjectURI(original.Object)
			if !ok || target != localActor.ActorURI || original.ID == "" || !isHTTPSActivityID(original.ID) {
				return permanentInbox("Undo activity references an invalid Follow")
			}
			originalID = original.ID
		}
		if err := w.backend.RemoveRemoteFollow(ctx, profile.UserID, item.ActorURI, activity.ID, originalID); err != nil {
			if errors.Is(err, backend.ErrRemoteFollowNotAuthorized) || errors.Is(err, backend.ErrRemoteFollowConflict) {
				return permanentInbox("Backend rejected an unmatched or unauthorized remote Undo Follow")
			}
			return fmt.Errorf("remove canonical remote follow in Backend: %w", err)
		}
		if err := w.store.RemoveRemoteFollower(ctx, profile.UserID, item.ActorURI); err != nil {
			return fmt.Errorf("remove remote follower routing cache: %w", err)
		}
		return nil
	default:
		return permanentInbox("inbound activity type is durably quarantined until its Backend projection is implemented: " + activity.Type)
	}
}

func activityObjectURI(raw json.RawMessage) (string, bool) {
	var uri string
	if json.Unmarshal(raw, &uri) == nil && isHTTPSActivityID(uri) {
		return uri, true
	}
	var object struct {
		ID string `json:"id"`
	}
	if json.Unmarshal(raw, &object) == nil && isHTTPSActivityID(object.ID) {
		return object.ID, true
	}
	return "", false
}

func isHTTPSActivityID(raw string) bool {
	u, err := url.Parse(strings.TrimSpace(raw))
	return err == nil && u.Scheme == "https" && u.Host != "" && u.User == nil && u.RawQuery == "" && u.Fragment == ""
}

func deterministicAcceptID(baseURL, followID, actorURI string) string {
	sum := sha256.Sum256([]byte(followID + "\x00" + actorURI))
	return strings.TrimRight(baseURL, "/") + "/activities/accept-" + hex.EncodeToString(sum[:])
}
