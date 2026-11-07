package worker

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"io"
	"testing"

	"github.com/Trendyzima/federated-content/internal/backend"
	"github.com/Trendyzima/federated-content/internal/store"
)

type fakeOutboxBackend struct {
	page       backend.OutboxPage
	profile    backend.ActorProfile
	resolveErr error
	listErr    error
	seenCursor string
}

func (f *fakeOutboxBackend) ListOutbox(_ context.Context, cursor string, _ int) (backend.OutboxPage, error) {
	f.seenCursor = cursor
	return f.page, f.listErr
}

func (f *fakeOutboxBackend) ResolveActorByID(context.Context, string) (backend.ActorProfile, error) {
	return f.profile, f.resolveErr
}

type queuedActivity struct {
	id      string
	actor   string
	payload []byte
	inboxes []string
}

type fakeOutboxStore struct {
	cursor      string
	actor       store.Actor
	inboxes     []string
	queued      []queuedActivity
	ensureCalls int
	enqueueErr  error
	stateErr    error
}

func (f *fakeOutboxStore) GetState(context.Context, string) (string, error) {
	return f.cursor, f.stateErr
}

func (f *fakeOutboxStore) SetState(_ context.Context, _ string, value string) error {
	if f.stateErr != nil { return f.stateErr }
	f.cursor = value
	return nil
}

func (f *fakeOutboxStore) EnsureActor(_ context.Context, userID, username, actorURI, displayName, summary string) (store.Actor, error) {
	f.ensureCalls++
	f.actor = store.Actor{UserID:userID, Username:username, ActorURI:actorURI, DisplayName:displayName, Summary:summary}
	return f.actor, nil
}

func (f *fakeOutboxStore) FollowerInboxes(context.Context, string) ([]string, error) {
	return f.inboxes, nil
}

func (f *fakeOutboxStore) EnqueueOutbox(_ context.Context, id, actor string, payload []byte, inboxes []string) error {
	if f.enqueueErr != nil { return f.enqueueErr }
	f.queued = append(f.queued, queuedActivity{id:id, actor:actor, payload:append([]byte(nil),payload...), inboxes:append([]string(nil),inboxes...)})
	return nil
}

func newOutboxFixture(t *testing.T) (*OutboxPoller, *fakeOutboxBackend, *fakeOutboxStore) {
	t.Helper()
	payload, err := json.Marshal(publishedPostPayload{
		ID:"p_123", AuthorID:"user_123", ActorHandle:"alice", DisplayName:"Alice",
		Content:"Hello <world>\nfrom Testagram", Visibility:"public", CreatedAt:"2026-11-07T20:00:00Z",
	})
	if err != nil { t.Fatal(err) }
	backendFake := &fakeOutboxBackend{
		page: backend.OutboxPage{Events:[]backend.OutboxEvent{{
			ID:"evt_12345678901234567890123456789012", EventType:"testagram.post.published",
			ActorID:"user_123", ObjectID:"p_123", Payload:payload, CreatedAt:"2026-11-07T20:00:00Z",
		}}, NextCursor:"Y3Vyc29yLW5leHQ", HasMore:false},
		profile:backend.ActorProfile{UserID:"user_123", Handle:"alice", DisplayName:"Alice", FederationEnabled:true},
	}
	storeFake := &fakeOutboxStore{inboxes:[]string{"https://remote.example/inbox"}}
	logger := slog.New(slog.NewTextHandler(io.Discard,nil))
	poller, err := NewOutboxPoller(storeFake, backendFake, "https://federation.testagram.site", logger)
	if err != nil { t.Fatal(err) }
	return poller, backendFake, storeFake
}

func TestOutboxPollerBuildsAndFansOutActivityPubCreate(t *testing.T) {
	poller, backendFake, storeFake := newOutboxFixture(t)
	hasMore, err := poller.pollBatch(context.Background())
	if err != nil { t.Fatal(err) }
	if hasMore { t.Fatal("single final outbox page unexpectedly reports more events") }
	if backendFake.seenCursor != "" { t.Fatalf("initial cursor=%q", backendFake.seenCursor) }
	if storeFake.cursor != "Y3Vyc29yLW5leHQ" { t.Fatalf("cursor=%q", storeFake.cursor) }
	if len(storeFake.queued) != 1 { t.Fatalf("queued activities=%d, want 1", len(storeFake.queued)) }
	queued := storeFake.queued[0]
	if queued.actor != "https://federation.testagram.site/users/alice" || len(queued.inboxes) != 1 || queued.inboxes[0] != "https://remote.example/inbox" {
		t.Fatalf("unexpected delivery fan-out: %+v", queued)
	}
	var activity map[string]any
	if err := json.Unmarshal(queued.payload, &activity); err != nil { t.Fatalf("decode ActivityPub Create: %v", err) }
	if activity["type"] != "Create" || activity["actor"] != queued.actor {
		t.Fatalf("unexpected ActivityPub activity: %+v", activity)
	}
	object, ok := activity["object"].(map[string]any)
	if !ok || object["type"] != "Note" || object["attributedTo"] != queued.actor {
		t.Fatalf("unexpected ActivityPub object: %#v", activity["object"])
	}
	content, _ := object["content"].(string)
	if !strings.Contains(content, "&lt;world&gt;") || !strings.Contains(content, "<br>") {
		t.Fatalf("user content was not safely rendered: %q", content)
	}
}

func TestOutboxPollerSkipsRevokedFederationConsentAndAdvancesCursor(t *testing.T) {
	poller, backendFake, storeFake := newOutboxFixture(t)
	backendFake.resolveErr = backend.ErrActorNotFound
	if _, err := poller.pollBatch(context.Background()); err != nil { t.Fatal(err) }
	if len(storeFake.queued) != 0 { t.Fatalf("revoked actor event was delivered: %+v", storeFake.queued) }
	if storeFake.cursor != "Y3Vyc29yLW5leHQ" { t.Fatalf("cursor did not advance past revoked actor: %q", storeFake.cursor) }
}

func TestOutboxPollerDoesNotAdvanceCursorWhenFanoutPersistenceFails(t *testing.T) {
	poller, _, storeFake := newOutboxFixture(t)
	storeFake.enqueueErr = errors.New("database unavailable")
	if _, err := poller.pollBatch(context.Background()); err == nil { t.Fatal("expected enqueue failure") }
	if storeFake.cursor != "" { t.Fatalf("cursor advanced after failed enqueue: %q", storeFake.cursor) }
}

func TestOutboxPollerSkipsSensitiveAndNonPublicEvents(t *testing.T) {
	for _, tc := range []struct{name string; visibility string; sensitive bool}{
		{"sensitive", "public", true},
		{"followers-only", "followers", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			poller, backendFake, storeFake := newOutboxFixture(t)
			var payload publishedPostPayload
			if err := json.Unmarshal(backendFake.page.Events[0].Payload, &payload); err != nil { t.Fatal(err) }
			payload.Visibility, payload.Sensitive = tc.visibility, tc.sensitive
			backendFake.page.Events[0].Payload, _ = json.Marshal(payload)
			if _, err := poller.pollBatch(context.Background()); err != nil { t.Fatal(err) }
			if backendFake.resolveErr != nil || storeFake.ensureCalls != 0 || len(storeFake.queued) != 0 {
				t.Fatal("ineligible post was passed to actor lookup or delivery")
			}
		})
	}
}
