package worker

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Trendyzima/federated-content/internal/backend"
	"github.com/Trendyzima/federated-content/internal/store"
)

const backendOutboxCursorKey = "backend_outbox_v1"
const backendOutboxBatchSize = 50

type OutboxSource interface {
	ListOutbox(context.Context, string, int) (backend.OutboxPage, error)
	ResolveActorByID(context.Context, string) (backend.ActorProfile, error)
}

type OutboxStore interface {
	RecordOutboxDeadLetter(context.Context, string, string, string, []byte) error
	SyncCursor(context.Context, string) (string, error)
	SaveSyncCursor(context.Context, string, string) error
	EnsureActor(context.Context, string, string, string, string, string) (store.Actor, error)
	RemoteFollowerInboxes(context.Context, string, int) ([]string, error)
	EnqueueOutbox(context.Context, string, string, []byte, []string) error
}

type OutboxPoller struct {
	backend OutboxSource
	store OutboxStore
	publicBaseURL string
	instanceDomain string
	logger *slog.Logger
	interval time.Duration
}

type permanentOutboxError struct { code string; err error }
func (e *permanentOutboxError) Error() string { return e.err.Error() }
func (e *permanentOutboxError) Unwrap() error { return e.err }
func permanentOutbox(code, message string) error { return &permanentOutboxError{code: code, err: errors.New(message)} }

type publishedPostPayload struct {
	ID string `json:"id"`
	AuthorID string `json:"author_id"`
	ActorHandle string `json:"actor_handle"`
	Content string `json:"content"`
	Visibility string `json:"visibility"`
	Sensitive bool `json:"sensitive"`
	CreatedAt string `json:"created_at"`
}

func NewOutboxPoller(source OutboxSource, state OutboxStore, publicBaseURL, instanceDomain string, logger *slog.Logger) (*OutboxPoller, error) {
	publicBaseURL = strings.TrimRight(strings.TrimSpace(publicBaseURL), "/")
	instanceDomain = strings.ToLower(strings.TrimSpace(instanceDomain))
	base, err := url.Parse(publicBaseURL)
	if source == nil || state == nil || err != nil || base.Scheme != "https" || base.Host == "" || base.User != nil || base.RawQuery != "" || base.Fragment != "" ||
		(base.Path != "" && base.Path != "/") || instanceDomain == "" {
		return nil, errors.New("Backend outbox poller requires a private Backend client, durable store and valid public Federation origin")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &OutboxPoller{backend: source, store: state, publicBaseURL: publicBaseURL, instanceDomain: instanceDomain, logger: logger, interval: 5*time.Second}, nil
}

func (w *OutboxPoller) ProcessBatch(ctx context.Context) (int, error) {
	cursor, err := w.store.SyncCursor(ctx, backendOutboxCursorKey)
	if err != nil {
		return 0, err
	}
	page, err := w.backend.ListOutbox(ctx, cursor, backendOutboxBatchSize)
	if err != nil {
		return 0, fmt.Errorf("read Backend federation outbox: %w", err)
	}
	if len(page.Events) > backendOutboxBatchSize || (page.HasMore && len(page.Events) == 0) {
		return 0, errors.New("Backend returned invalid outbox page bounds")
	}
	processed := 0
	for _, event := range page.Events {
		if err := w.processEvent(ctx, event); err != nil {
			var permanent *permanentOutboxError
			if !errors.As(err, &permanent) { return processed, fmt.Errorf("process Backend outbox event %s: %w", event.ID, err) }
			quarantine, marshalErr := json.Marshal(map[string]any{"event_id": event.ID, "event_type": event.EventType, "actor_id": event.ActorID, "object_id": event.ObjectID, "payload_base64": base64.RawURLEncoding.EncodeToString(event.Payload)})
			if marshalErr != nil { return processed, marshalErr }
			if err := w.store.RecordOutboxDeadLetter(ctx, event.ID, event.EventType, permanent.code, quarantine); err != nil { return processed, err }
			w.logger.Warn("Backend federation outbox event quarantined", "event_id", event.ID, "reason", permanent.code)
		}
		next := base64.RawURLEncoding.EncodeToString([]byte(event.ID))
		if err := w.store.SaveSyncCursor(ctx, backendOutboxCursorKey, next); err != nil { return processed, err }
		processed++
	}
	return processed, nil
}

func (w *OutboxPoller) processEvent(ctx context.Context, event backend.OutboxEvent) error {
	if event.ID == "" || event.EventType != "testagram.post.published" || event.ActorID == "" || event.ObjectID == "" || len(event.Payload) == 0 {
		return permanentOutbox("invalid_event", "Backend outbox event is incomplete or unsupported")
	}
	var payload publishedPostPayload
	decoder := json.NewDecoder(strings.NewReader(string(event.Payload)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&payload); err != nil {
		return permanentOutbox("invalid_event", "Backend post event payload is invalid")
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return permanentOutbox("invalid_event", "Backend post event payload must contain exactly one JSON object")
	}
	if payload.ID != event.ObjectID || payload.AuthorID != event.ActorID || payload.Visibility != "public" || payload.Sensitive ||
		payload.ActorHandle == "" || len(payload.ActorHandle) > 30 || !utf8.ValidString(payload.Content) ||
		strings.TrimSpace(payload.Content) == "" || utf8.RuneCountInString(payload.Content) > 5000 {
		return permanentOutbox("invalid_event", "Backend post event failed federation eligibility checks")
	}
	if _, err := time.Parse(time.RFC3339, payload.CreatedAt); err != nil {
		return permanentOutbox("invalid_event", "Backend post event has an invalid creation timestamp")
	}
	if _, err := time.Parse(time.RFC3339, event.CreatedAt); err != nil {
		return permanentOutbox("invalid_event", "Backend outbox envelope has an invalid creation timestamp")
	}

	profile, err := w.backend.ResolveActorByID(ctx, event.ActorID)
	if err != nil {
		if errors.Is(err, backend.ErrActorNotFound) { return permanentOutbox("actor_no_longer_eligible", "Backend actor is no longer eligible for federation") }
		return fmt.Errorf("revalidate Backend actor eligibility: %w", err)
	}
	if profile.UserID != event.ActorID || !profile.FederationEnabled || profile.Handle != strings.ToLower(payload.ActorHandle) {
		return permanentOutbox("actor_no_longer_eligible", "Backend actor identity or federation consent no longer matches the event")
	}
	actorURI := w.publicBaseURL + "/users/" + url.PathEscape(profile.Handle)
	actor, err := w.store.EnsureActor(ctx, profile.UserID, profile.Handle, actorURI, profile.DisplayName, profile.Summary)
	if err != nil {
		return fmt.Errorf("ensure local ActivityPub actor: %w", err)
	}
	if actor.ActorURI != actorURI || actor.PrivateKeyPEM == "" {
		return permanentOutbox("actor_no_longer_eligible", "local ActivityPub signing identity is unavailable")
	}

	activityID := actorURI + "/activities/" + url.PathEscape(event.ID)
	objectURL := (&url.URL{Scheme: "https", Host: w.instanceDomain, Path: "/posts/" + event.ObjectID}).String()
	activity := map[string]any{
		"@context": "https://www.w3.org/ns/activitystreams",
		"id": activityID,
		"type": "Create",
		"actor": actor.ActorURI,
		"published": event.CreatedAt,
		"to": []string{"https://www.w3.org/ns/activitystreams#Public"},
		"cc": []string{actor.ActorURI + "/followers"},
		"object": map[string]any{
			"id": objectURL,
			"type": "Note",
			"attributedTo": actor.ActorURI,
			"content": payload.Content,
			"published": event.CreatedAt,
			"to": []string{"https://www.w3.org/ns/activitystreams#Public"},
			"cc": []string{actor.ActorURI + "/followers"},
		},
	}
	encoded, err := json.Marshal(activity)
	if err != nil {
		return fmt.Errorf("encode ActivityPub Create activity: %w", err)
	}
	inboxes, err := w.store.RemoteFollowerInboxes(ctx, event.ActorID, 1000)
	if err != nil {
		return fmt.Errorf("load remote follower inboxes: %w", err)
	}
	if err := w.store.EnqueueOutbox(ctx, activityID, actor.ActorURI, encoded, inboxes); err != nil {
		return fmt.Errorf("durably enqueue ActivityPub activity: %w", err)
	}
	return nil
}

func (w *OutboxPoller) Run(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		batchCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		count, err := w.ProcessBatch(batchCtx)
		cancel()
		if err != nil && ctx.Err() == nil {
			w.logger.Error("Backend federation outbox poll failed", "error", err)
		} else if count > 0 {
			w.logger.Info("Backend federation outbox events accepted", "count", count)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
