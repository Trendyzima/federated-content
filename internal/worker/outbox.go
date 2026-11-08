package worker

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"log/slog"
	"net/url"
	"strings"
	"time"

	"github.com/Trendyzima/federated-content/internal/backend"
	"github.com/Trendyzima/federated-content/internal/store"
)

const (
	backendOutboxCursorKey = "backend_federation_outbox_cursor_v1"
	publicAudience         = "https://www.w3.org/ns/activitystreams#Public"
)

type OutboxBackend interface {
	ListOutbox(context.Context, string, int) (backend.OutboxPage, error)
	ResolveActorByID(context.Context, string) (backend.ActorProfile, error)
}

type OutboxStore interface {
	GetState(context.Context, string) (string, error)
	SetState(context.Context, string, string) error
	EnsureActor(context.Context, string, string, string, string, string) (store.Actor, error)
	FollowerInboxes(context.Context, string) ([]string, error)
	EnqueueOutbox(context.Context, string, string, []byte, []string) error
}

type OutboxPoller struct {
	store      OutboxStore
	backend    OutboxBackend
	publicBase string
	logger     *slog.Logger
	interval   time.Duration
	batchSize  int
}

type publishedPostPayload struct {
	ID          string `json:"id"`
	AuthorID    string `json:"author_id"`
	ActorHandle string `json:"actor_handle"`
	DisplayName string `json:"actor_display_name"`
	Content     string `json:"content"`
	Visibility  string `json:"visibility"`
	Sensitive   bool   `json:"sensitive"`
	CreatedAt   string `json:"created_at"`
}

func NewOutboxPoller(storage OutboxStore, backendClient OutboxBackend, publicBase string, logger *slog.Logger) (*OutboxPoller, error) {
	publicBase = strings.TrimRight(strings.TrimSpace(publicBase), "/")
	if storage == nil || backendClient == nil || publicBase == "" {
		return nil, errors.New("Federation outbox poller requires storage, Backend client and public base URL")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &OutboxPoller{store: storage, backend: backendClient, publicBase: publicBase, logger: logger, interval: 5 * time.Second, batchSize: 50}, nil
}

func (p *OutboxPoller) Run(ctx context.Context) {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		for {
			hasMore, err := p.pollBatch(ctx)
			if err != nil {
				if ctx.Err() == nil {
					p.logger.Error("Federation Backend outbox poll failed", "error", err)
				}
				break
			}
			if !hasMore {
				break
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (p *OutboxPoller) pollBatch(ctx context.Context) (bool, error) {
	cursor, err := p.store.GetState(ctx, backendOutboxCursorKey)
	if err != nil {
		return false, fmt.Errorf("read Backend outbox cursor: %w", err)
	}
	page, err := p.backend.ListOutbox(ctx, cursor, p.batchSize)
	if err != nil {
		return false, fmt.Errorf("list Backend federation outbox: %w", err)
	}
	if page.HasMore && len(page.Events) == 0 {
		return false, errors.New("Backend outbox returned has_more without events")
	}
	for _, event := range page.Events {
		if err := p.processEvent(ctx, event); err != nil {
			return false, fmt.Errorf("process Backend outbox event %s: %w", event.ID, err)
		}
	}
	if len(page.Events) > 0 {
		next := page.NextCursor
		if next == "" {
			next = base64.RawURLEncoding.EncodeToString([]byte(page.Events[len(page.Events)-1].ID))
		}
		if err := p.store.SetState(ctx, backendOutboxCursorKey, next); err != nil {
			return false, fmt.Errorf("persist Backend outbox cursor: %w", err)
		}
	}
	return page.HasMore, nil
}

func (p *OutboxPoller) processEvent(ctx context.Context, event backend.OutboxEvent) error {
	if event.EventType != "testagram.post.published" || event.ID == "" || event.ActorID == "" || event.ObjectID == "" {
		p.logger.Warn("skipping unsupported Backend outbox event", "event_id", event.ID, "event_type", event.EventType)
		return nil
	}
	var payload publishedPostPayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		p.logger.Warn("skipping malformed Backend post event", "event_id", event.ID, "error", err)
		return nil
	}
	if payload.ID != event.ObjectID || payload.AuthorID != event.ActorID || strings.TrimSpace(payload.Content) == "" ||
		payload.Visibility != "public" || payload.Sensitive {
		p.logger.Warn("skipping ineligible Backend post event", "event_id", event.ID)
		return nil
	}
	published, err := time.Parse(time.RFC3339Nano, event.CreatedAt)
	if err != nil {
		return fmt.Errorf("invalid event timestamp: %w", err)
	}
	profile, err := p.backend.ResolveActorByID(ctx, event.ActorID)
	if errors.Is(err, backend.ErrActorNotFound) {
		// Federation may have been disabled after the post entered the outbox.
		// Advancing the cursor is safe: consent is checked again for every event.
		p.logger.Info("skipping event for actor without active federation consent", "event_id", event.ID, "actor_id", event.ActorID)
		return nil
	}
	if err != nil {
		return fmt.Errorf("verify current actor federation consent: %w", err)
	}
	if profile.UserID != event.ActorID || !profile.FederationEnabled || profile.Handle == "" {
		return errors.New("Backend returned an ineligible actor identity")
	}

	actorURI := p.publicBase + "/users/" + url.PathEscape(profile.Handle)
	actor, err := p.store.EnsureActor(ctx, profile.UserID, profile.Handle, actorURI, profile.DisplayName, profile.Summary)
	if err != nil {
		return fmt.Errorf("ensure local ActivityPub actor: %w", err)
	}
	inboxes, err := p.store.FollowerInboxes(ctx, actor.UserID)
	if err != nil {
		return fmt.Errorf("load remote follower inboxes: %w", err)
	}

	activityID := actor.ActorURI + "/activities/" + url.PathEscape(event.ID)
	objectID := actor.ActorURI + "/statuses/" + url.PathEscape(payload.ID)
	publishedText := published.UTC().Format(time.RFC3339Nano)
	to := []string{publicAudience}
	cc := []string{actor.ActorURI + "/followers"}
	object := map[string]any{
		"id": objectID, "type": "Note", "attributedTo": actor.ActorURI,
		"content": strings.ReplaceAll(html.EscapeString(payload.Content), "\n", "<br>"),
		"published": publishedText, "url": objectID, "to": to, "cc": cc, "sensitive": false,
	}
	activity := map[string]any{
		"@context": "https://www.w3.org/ns/activitystreams",
		"id": activityID, "type": "Create", "actor": actor.ActorURI,
		"published": publishedText, "to": to, "cc": cc, "object": object,
	}
	encoded, err := json.Marshal(activity)
	if err != nil {
		return fmt.Errorf("encode ActivityPub Create: %w", err)
	}
	if err := p.store.EnqueueOutbox(ctx, activityID, actor.ActorURI, encoded, inboxes); err != nil {
		return fmt.Errorf("persist ActivityPub outbox and fan-out jobs: %w", err)
	}
	p.logger.Info("federated local post", "event_id", event.ID, "actor", actor.ActorURI, "remote_inboxes", len(inboxes))
	return nil
}
