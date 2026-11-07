package worker

import (
	"context"
	"crypto/sha256"
	"database/sql"
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
	RemoteActorInbox(context.Context, string) (string, error)
	AddRemoteFollower(context.Context, string, string) error
	RemoveRemoteFollower(context.Context, string, string) error
	EnqueueOutbox(context.Context, string, string, []byte, []string) error
	UpsertRemoteObject(context.Context, store.RemoteObject) error
	TombstoneRemoteObject(context.Context, string, string) error
	AddRemoteObjectInteraction(context.Context, string, string, string, string) error
	RemoveRemoteObjectInteraction(context.Context, string, string, string) error
}

type InboxBackend interface {
	ResolveActor(context.Context, string) (backend.ActorProfile, error)
}

type InboxProcessor interface {
	Process(context.Context, store.InboxActivity) error
}

type PermanentInboxError struct{ Err error }

func (e PermanentInboxError) Error() string { return e.Err.Error() }
func (e PermanentInboxError) Unwrap() error { return e.Err }

func permanentInboxError(message string) error {
	return PermanentInboxError{Err: errors.New(message)}
}

type InboxWorker struct {
	store     InboxStore
	processor InboxProcessor
	logger    *slog.Logger
	interval  time.Duration
	batchSize int
}

func NewInboxWorker(storage InboxStore, processor InboxProcessor, logger *slog.Logger) (*InboxWorker, error) {
	if storage == nil || processor == nil {
		return nil, errors.New("Federation inbox worker requires storage and processor")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &InboxWorker{store: storage, processor: processor, logger: logger, interval: 5 * time.Second, batchSize: 25}, nil
}

func (w *InboxWorker) Run(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		for {
			count, err := w.processBatch(ctx)
			if err != nil {
				if ctx.Err() == nil {
					w.logger.Error("Federation inbox batch failed", "error", err)
				}
				break
			}
			if count < w.batchSize {
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

func (w *InboxWorker) processBatch(ctx context.Context) (int, error) {
	activities, err := w.store.ClaimInbox(ctx, w.batchSize, 45*time.Second)
	if err != nil {
		return 0, fmt.Errorf("claim inbox activities: %w", err)
	}
	for _, activity := range activities {
		processErr := w.processor.Process(ctx, activity)
		if processErr == nil {
			if err := w.store.AckInbox(ctx, activity.ActivityID, activity.LeaseToken); err != nil {
				return len(activities), fmt.Errorf("acknowledge inbox activity %s: %w", activity.ActivityID, err)
			}
			continue
		}
		var permanent PermanentInboxError
		isPermanent := errors.As(processErr, &permanent)
		if err := w.store.FailInbox(ctx, activity.ActivityID, activity.LeaseToken, processErr.Error(), isPermanent); err != nil {
			return len(activities), fmt.Errorf("record inbox failure %s: %w", activity.ActivityID, err)
		}
		if isPermanent {
			w.logger.Warn("rejected unsupported or invalid inbox activity", "activity_id", activity.ActivityID, "activity_type", activity.ActivityType, "error", processErr)
		} else {
			w.logger.Warn("scheduled inbox activity retry", "activity_id", activity.ActivityID, "activity_type", activity.ActivityType, "attempt", activity.Attempts, "error", processErr)
		}
	}
	return len(activities), nil
}

type FederationActivityProcessor struct {
	store      InboxStore
	backend    InboxBackend
	publicBase string
}

func NewFederationActivityProcessor(storage InboxStore, backendClient InboxBackend, publicBase string) (*FederationActivityProcessor, error) {
	publicBase = strings.TrimRight(strings.TrimSpace(publicBase), "/")
	if storage == nil || backendClient == nil || publicBase == "" {
		return nil, errors.New("Federation activity processor requires storage, Backend client and public base URL")
	}
	return &FederationActivityProcessor{store: storage, backend: backendClient, publicBase: publicBase}, nil
}

type activityEnvelope struct {
	ID     string          `json:"id"`
	Type   string          `json:"type"`
	Actor  string          `json:"actor"`
	Object json.RawMessage `json:"object"`
}

type activityObject struct {
	ID            string          `json:"id"`
	Type          string          `json:"type"`
	Actor         string          `json:"actor"`
	Object        json.RawMessage `json:"object"`
	AttributedTo  json.RawMessage `json:"attributedTo"`
	Content       string          `json:"content"`
	Summary       string          `json:"summary"`
	Published     string          `json:"published"`
	Updated       string          `json:"updated"`
	Sensitive     bool            `json:"sensitive"`
	To            []string        `json:"to"`
	CC            []string        `json:"cc"`
	InReplyTo     string          `json:"inReplyTo"`
	QuoteURI      string          `json:"quoteUri"`
	Language      string          `json:"language"`
	Attachment    json.RawMessage `json:"attachment"`
	Tag           json.RawMessage `json:"tag"`
}

func (p *FederationActivityProcessor) Process(ctx context.Context, item store.InboxActivity) error {
	var activity activityEnvelope
	if err := json.Unmarshal(item.Payload, &activity); err != nil {
		return PermanentInboxError{Err: fmt.Errorf("invalid ActivityPub JSON: %w", err)}
	}
	if activity.ID != item.ActivityID || activity.Actor != item.ActorURI || activity.Type != item.ActivityType || activity.Object == nil {
		return permanentInboxError("ActivityPub envelope does not match its verified inbox identity")
	}
	switch activity.Type {
	case "Follow":
		return p.processFollow(ctx, item, activity)
	case "Undo":
		return p.processUndo(ctx, item, activity)
	case "Create", "Update":
		return p.processObject(ctx, item, activity)
	case "Delete":
		return p.processDelete(ctx, item, activity)
	case "Like", "Announce":
		return p.processInteraction(ctx, item, activity, activity.Type)
	default:
		return PermanentInboxError{Err: fmt.Errorf("ActivityPub activity type %q is not supported by this processor", activity.Type)}
	}
}

func (p *FederationActivityProcessor) localActor(ctx context.Context, username string) (store.Actor, error) {
	profile, err := p.backend.ResolveActor(ctx, username)
	if errors.Is(err, backend.ErrActorNotFound) {
		return store.Actor{}, permanentInboxError("recipient local actor does not exist or federation consent is disabled")
	}
	if err != nil {
		return store.Actor{}, fmt.Errorf("verify local actor eligibility with Backend: %w", err)
	}
	if !profile.FederationEnabled || profile.UserID == "" || profile.Handle == "" {
		return store.Actor{}, permanentInboxError("Backend returned an ineligible local actor")
	}
	actorURI := p.publicBase + "/users/" + url.PathEscape(profile.Handle)
	actor, err := p.store.EnsureActor(ctx, profile.UserID, profile.Handle, actorURI, profile.DisplayName, profile.Summary)
	if err != nil {
		return store.Actor{}, fmt.Errorf("ensure local actor: %w", err)
	}
	return actor, nil
}

func (p *FederationActivityProcessor) processFollow(ctx context.Context, item store.InboxActivity, activity activityEnvelope) error {
	objectURI, err := activityObjectURI(activity.Object)
	if err != nil {
		return PermanentInboxError{Err: err}
	}
	localActor, err := p.localActor(ctx, item.RecipientUsername)
	if err != nil {
		return err
	}
	if objectURI != localActor.ActorURI {
		return permanentInboxError("Follow object does not identify the addressed local actor")
	}
	remoteInbox, err := p.store.RemoteActorInbox(ctx, item.ActorURI)
	if errors.Is(err, sql.ErrNoRows) {
		return permanentInboxError("verified remote actor inbox is unavailable")
	}
	if err != nil {
		return fmt.Errorf("load verified remote actor inbox: %w", err)
	}
	if err := p.store.AddRemoteFollower(ctx, localActor.UserID, item.ActorURI); err != nil {
		return fmt.Errorf("persist remote follower relationship: %w", err)
	}
	digest := sha256.Sum256([]byte(activity.ID))
	acceptID := localActor.ActorURI + "/activities/accept-" + hex.EncodeToString(digest[:16])
	accept := map[string]any{
		"@context": "https://www.w3.org/ns/activitystreams",
		"id": acceptID, "type": "Accept", "actor": localActor.ActorURI,
		"object": json.RawMessage(item.Payload),
	}
	encoded, err := json.Marshal(accept)
	if err != nil {
		return fmt.Errorf("encode ActivityPub Accept: %w", err)
	}
	if err := p.store.EnqueueOutbox(ctx, acceptID, localActor.ActorURI, encoded, []string{remoteInbox}); err != nil {
		return fmt.Errorf("persist ActivityPub Accept delivery: %w", err)
	}
	return nil
}

func (p *FederationActivityProcessor) processUndo(ctx context.Context, item store.InboxActivity, activity activityEnvelope) error {
	var undone activityEnvelope
	if err := json.Unmarshal(activity.Object, &undone); err != nil {
		return PermanentInboxError{Err: errors.New("Undo object must contain an ActivityPub activity")}
	}
	if undone.Actor != item.ActorURI {
		return permanentInboxError("Undo actor does not match verified inbox actor")
	}
	localActor, err := p.localActor(ctx, item.RecipientUsername)
	if err != nil {
		return err
	}
	objectURI, err := activityObjectURI(undone.Object)
	if err != nil {
		return PermanentInboxError{Err: err}
	}
	switch undone.Type {
	case "Follow":
		if objectURI != localActor.ActorURI {
			return permanentInboxError("Undo Follow does not identify the addressed local actor")
		}
		return p.store.RemoveRemoteFollower(ctx, localActor.UserID, item.ActorURI)
	case "Like", "Announce":
		if objectURI == "" {
			return permanentInboxError("Undo interaction has no object URI")
		}
		return p.store.RemoveRemoteObjectInteraction(ctx, item.ActorURI, objectURI, undone.Type)
	default:
		return PermanentInboxError{Err: fmt.Errorf("Undo of %q is not supported", undone.Type)}
	}
}

func (p *FederationActivityProcessor) processObject(ctx context.Context, item store.InboxActivity, activity activityEnvelope) error {
	var object activityObject
	if err := json.Unmarshal(activity.Object, &object); err != nil {
		return PermanentInboxError{Err: errors.New("Create/Update object must be an embedded ActivityPub object")}
	}
	if object.ID == "" || !sameActor(object.AttributedTo, item.ActorURI) {
		return permanentInboxError("remote object ID or attributedTo actor is invalid")
	}
	parsed, err := url.Parse(object.ID)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" {
		return permanentInboxError("remote object URI must be an absolute HTTPS URL")
	}
	visibility := "private"
	if containsAudience(object.To, publicAudience) {
		visibility = "public"
	} else if containsAudience(object.CC, publicAudience) {
		visibility = "unlisted"
	}
	if visibility == "private" {
		// Private/direct content is deliberately not copied into public discovery storage.
		return nil
	}
	if object.Type == "" {
		return permanentInboxError("remote object type is required")
	}
	published := parseActivityTime(object.Published)
	updated := parseActivityTime(object.Updated)
	if published.IsZero() {
		published = time.Now().UTC()
	}
	if updated.IsZero() {
		updated = published
	}
	attachments := normalizeJSONArray(object.Attachment)
	tags := normalizeJSONArray(object.Tag)
	record := store.RemoteObject{
		URI: object.ID, ActorURI: item.ActorURI, ObjectType: object.Type,
		URL: object.ID, Content: object.Content, Summary: object.Summary,
		PublishedAt: published, UpdatedAt: updated, Sensitive: object.Sensitive,
		Visibility: visibility, InReplyToURI: object.InReplyTo, QuoteURI: object.QuoteURI,
		LanguageCode: activityLanguage(object.Language), Attachments: attachments, Tags: tags,
		ContentWarning: object.Summary, RawObject: append(json.RawMessage(nil), activity.Object...),
	}
	if err := p.store.UpsertRemoteObject(ctx, record); err != nil {
		return fmt.Errorf("persist remote ActivityPub object: %w", err)
	}
	return nil
}

func (p *FederationActivityProcessor) processDelete(ctx context.Context, item store.InboxActivity, activity activityEnvelope) error {
	objectURI, err := activityObjectURI(activity.Object)
	if err != nil {
		return PermanentInboxError{Err: err}
	}
	err = p.store.TombstoneRemoteObject(ctx, item.ActorURI, objectURI)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	return err
}

func (p *FederationActivityProcessor) processInteraction(ctx context.Context, item store.InboxActivity, activity activityEnvelope, kind string) error {
	objectURI, err := activityObjectURI(activity.Object)
	if err != nil {
		return PermanentInboxError{Err: err}
	}
	err = p.store.AddRemoteObjectInteraction(ctx, activity.ID, item.ActorURI, objectURI, kind)
	if errors.Is(err, sql.ErrNoRows) {
		// Interactions may arrive before the corresponding Create; do not retry forever.
		return nil
	}
	return err
}

func activityObjectURI(raw json.RawMessage) (string, error) {
	var uri string
	if err := json.Unmarshal(raw, &uri); err == nil {
		if uri == "" {
			return "", errors.New("ActivityPub object URI is empty")
		}
		return uri, nil
	}
	var object struct { ID string `json:"id"` }
	if err := json.Unmarshal(raw, &object); err != nil || object.ID == "" {
		return "", errors.New("ActivityPub object must be a URI or object with an id")
	}
	return object.ID, nil
}

func sameActor(raw json.RawMessage, expected string) bool {
	var actor string
	if json.Unmarshal(raw, &actor) == nil {
		return actor == expected
	}
	var actors []string
	if json.Unmarshal(raw, &actors) == nil {
		for _, actor := range actors { if actor == expected { return true } }
	}
	return false
}

func containsAudience(values []string, audience string) bool {
	for _, value := range values { if value == audience { return true } }
	return false
}

func parseActivityTime(value string) time.Time {
	if strings.TrimSpace(value)=="" { return time.Time{} }
	parsed,err:=time.Parse(time.RFC3339Nano,value)
	if err!=nil{return time.Time{}}
	return parsed.UTC()
}

func normalizeJSONArray(raw json.RawMessage) json.RawMessage {
	if len(raw)==0 || string(raw)=="null" { return json.RawMessage("[]") }
	if !json.Valid(raw) { return json.RawMessage("[]") }
	return append(json.RawMessage(nil),raw...)
}

func activityLanguage(raw json.RawMessage) string {
	var value string
	if json.Unmarshal(raw,&value)==nil { return value }
	var languageMap map[string]string
	if json.Unmarshal(raw,&languageMap)==nil {
		if v:=languageMap["und"];v!=""{return v}
		for _,v:=range languageMap{return v}
	}
	return ""
}
