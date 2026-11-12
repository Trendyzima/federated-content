package worker

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/Trendyzima/federated-content/internal/backend"
	"github.com/Trendyzima/federated-content/internal/store"
)

type fakeOutboxSource struct {
	page     backend.OutboxPage
	profile  backend.ActorProfile
	listErr  error
	actorErr error
	after    string
}

func (f *fakeOutboxSource) ListOutbox(_ context.Context, after string, limit int) (backend.OutboxPage, error) {
	f.after = after
	if limit != backendOutboxBatchSize {
		return backend.OutboxPage{}, errors.New("unexpected batch size")
	}
	return f.page, f.listErr
}

func (f *fakeOutboxSource) ResolveActorByID(context.Context, string) (backend.ActorProfile, error) {
	return f.profile, f.actorErr
}

type fakeOutboxStore struct {
	cursor          string
	saved           []string
	actor           store.Actor
	inboxes         []string
	inboxPages      [][]string
	pageCalls       int
	enqueuedPages   [][]string
	enqueuedID      string
	enqueuedActor   string
	enqueuedPayload []byte
	enqueuedInboxes []string
	enqueueErr      error
	deadLetters     []string
}

func (f *fakeOutboxStore) SyncCursor(context.Context, string) (string, error) {
	return f.cursor, nil
}

func (f *fakeOutboxStore) SaveSyncCursor(_ context.Context, _ string, value string) error {
	f.cursor = value
	f.saved = append(f.saved, value)
	return nil
}

func (f *fakeOutboxStore) RecordOutboxDeadLetter(_ context.Context, id, _ string, _ string, _ []byte) error {
	f.deadLetters = append(f.deadLetters, id)
	return nil
}

func (f *fakeOutboxStore) EnsureActor(_ context.Context, userID, username, actorURI, displayName, summary string) (store.Actor, error) {
	if f.actor.UserID != userID || f.actor.Username != username || f.actor.ActorURI != actorURI {
		return store.Actor{}, errors.New("unexpected actor identity")
	}
	return f.actor, nil
}

func (f *fakeOutboxStore) RemoteFollowerInboxesPage(_ context.Context, _ string, after string, limit int) ([]string, string, bool, error) {
	if limit != remoteFanoutPageSize {
		return nil, "", false, errors.New("unexpected fan-out page size")
	}
	pages := f.inboxPages
	if len(pages) == 0 {
		pages = [][]string{f.inboxes}
	}
	if f.pageCalls >= len(pages) {
		return nil, after, false, nil
	}
	page := pages[f.pageCalls]
	f.pageCalls++
	more := f.pageCalls < len(pages)
	next := after
	if more && len(page) > 0 {
		next = page[len(page)-1]
	}
	return page, next, more, nil
}

func (f *fakeOutboxStore) EnqueueOutbox(_ context.Context, id, actor string, payload []byte, inboxes []string) error {
	f.enqueuedID, f.enqueuedActor = id, actor
	f.enqueuedPayload = append([]byte(nil), payload...)
	f.enqueuedInboxes = append(f.enqueuedInboxes, inboxes...)
	f.enqueuedPages = append(f.enqueuedPages, append([]string(nil), inboxes...))
	return f.enqueueErr
}

func validBackendOutboxFixture(t *testing.T) (backend.OutboxEvent, backend.ActorProfile, store.Actor) {
	t.Helper()
	now := time.Date(2026, 11, 7, 12, 0, 0, 0, time.UTC).Format(time.RFC3339)
	payload, err := json.Marshal(publishedPostPayload{
		ID:               "post_123",
		AuthorID:         "user_123",
		ActorHandle:      "alice",
		ActorDisplayName: "Alice",
		Content:          "Hello from Testagram #Fediverse #ActivityPub #fediverse",
		Visibility:       "public",
		CreatedAt:        now,
	})
	if err != nil {
		t.Fatal(err)
	}
	event := backend.OutboxEvent{
		ID:        "evt_123",
		EventType: "testagram.post.published",
		ActorID:   "user_123",
		ObjectID:  "post_123",
		Payload:   payload,
		CreatedAt: now,
	}
	profile := backend.ActorProfile{
		UserID:            "user_123",
		Handle:            "alice",
		DisplayName:       "Alice",
		FederationEnabled: true,
	}
	actorURI := "https://federation.testagram.site/users/alice"
	actor := store.Actor{
		UserID:        "user_123",
		Username:      "alice",
		ActorURI:      actorURI,
		PrivateKeyPEM: "private-key",
	}
	return event, profile, actor
}

func newOutboxFixture(t *testing.T, event backend.OutboxEvent, profile backend.ActorProfile, actor store.Actor) (*OutboxPoller, *fakeOutboxSource, *fakeOutboxStore) {
	t.Helper()
	source := &fakeOutboxSource{
		page:    backend.OutboxPage{Events: []backend.OutboxEvent{event}},
		profile: profile,
	}
	state := &fakeOutboxStore{
		actor:   actor,
		inboxes: []string{"https://remote.example/inbox"},
	}
	poller, err := NewOutboxPoller(
		source,
		state,
		"https://federation.testagram.site",
		"testagram.site",
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err != nil {
		t.Fatal(err)
	}
	return poller, source, state
}

func TestActivityPubHashtagTagsNormalizeAndDeduplicate(t *testing.T) {
	tags := activityPubHashtagTags("Start #Mastodon #mastodon #Kiswahili #東京")
	if len(tags) != 3 {
		t.Fatalf("got %d hashtag tags, want 3: %#v", len(tags), tags)
	}
	names := map[string]bool{}
	for _, tag := range tags {
		if tag["type"] != "Hashtag" || !strings.HasPrefix(tag["href"], "https://testagram.site/hashtag/") {
			t.Fatalf("invalid ActivityPub hashtag: %#v", tag)
		}
		names[tag["name"]] = true
	}
	if !names["#Mastodon"] || !names["#Kiswahili"] || !names["#東京"] {
		t.Fatalf("Unicode or case-preserved tags missing: %#v", names)
	}
}

func TestOutboxPollerCreatesAndDurablyEnqueuesActivityPubPost(t *testing.T) {
	event, profile, actor := validBackendOutboxFixture(t)
	poller, source, state := newOutboxFixture(t, event, profile, actor)
	count, err := poller.ProcessBatch(context.Background())
	if err != nil || count != 1 {
		t.Fatalf("ProcessBatch count=%d err=%v", count, err)
	}
	if source.after != "" {
		t.Fatalf("first page cursor=%q, want empty", source.after)
	}
	if state.enqueuedID != "https://federation.testagram.site/users/alice/activities/evt_123" {
		t.Fatalf("unexpected ActivityPub ID: %q", state.enqueuedID)
	}
	if state.enqueuedActor != actor.ActorURI || len(state.enqueuedInboxes) != 1 {
		t.Fatalf("activity was not enqueued with actor and remote inbox: %+v", state)
	}
	var activity struct {
		ID     string `json:"id"`
		Type   string `json:"type"`
		Actor  string `json:"actor"`
		Object struct {
			ID      string `json:"id"`
			Type    string `json:"type"`
			Content string `json:"content"`
			Tag     []struct {
				Type string `json:"type"`
				Href string `json:"href"`
				Name string `json:"name"`
			} `json:"tag"`
		} `json:"object"`
	}
	if err := json.Unmarshal(state.enqueuedPayload, &activity); err != nil {
		t.Fatal(err)
	}
	if activity.Type != "Create" || activity.Actor != actor.ActorURI || activity.Object.Type != "Note" ||
		activity.Object.ID != "https://federation.testagram.site/objects/post_123" || activity.Object.Content != "Hello from Testagram #Fediverse #ActivityPub #fediverse" || len(activity.Object.Tag) != 2 {
		t.Fatalf("unexpected ActivityPub Create payload: %+v", activity)
	}
	seenTags := map[string]bool{}
	for _, tag := range activity.Object.Tag {
		if tag.Type != "Hashtag" || !strings.HasPrefix(tag.Href, "https://testagram.site/hashtag/") {
			t.Fatalf("invalid ActivityPub hashtag metadata: %+v", tag)
		}
		seenTags[tag.Name] = true
	}
	if !seenTags["#Fediverse"] || !seenTags["#ActivityPub"] {
		t.Fatalf("expected structured hashtag tags, got %+v", activity.Object.Tag)
	}
	wantCursor := base64.RawURLEncoding.EncodeToString([]byte(event.ID))
	if state.cursor != wantCursor || len(state.saved) != 1 {
		t.Fatalf("cursor not durably advanced after enqueue: %+v", state)
	}
}

func TestOutboxPollerQuarantinesPermanentlyIneligibleActorAndAdvances(t *testing.T) {
	event, profile, actor := validBackendOutboxFixture(t)
	poller, _, state := newOutboxFixture(t, event, profile, actor)
	// The account was deleted or federation consent was withdrawn after the
	// Backend event was written. Do not block every later event on this row.
	source := poller.backend.(*fakeOutboxSource)
	source.actorErr = backend.ErrActorNotFound
	count, err := poller.ProcessBatch(context.Background())
	if err != nil || count != 1 {
		t.Fatalf("ProcessBatch count=%d err=%v", count, err)
	}
	if len(state.deadLetters) != 1 || state.deadLetters[0] != event.ID || state.enqueuedID != "" {
		t.Fatalf("ineligible actor was not quarantined: %+v", state)
	}
	if state.cursor != base64.RawURLEncoding.EncodeToString([]byte(event.ID)) {
		t.Fatalf("cursor did not advance past durable dead letter: %q", state.cursor)
	}
}

func TestOutboxPollerDoesNotAdvanceCursorOnTransientBackendFailure(t *testing.T) {
	event, profile, actor := validBackendOutboxFixture(t)
	poller, source, state := newOutboxFixture(t, event, profile, actor)
	source.actorErr = errors.New("temporary Backend timeout")
	count, err := poller.ProcessBatch(context.Background())
	if err == nil || count != 0 {
		t.Fatalf("expected transient failure, count=%d err=%v", count, err)
	}
	if state.cursor != "" || len(state.saved) != 0 || len(state.deadLetters) != 0 {
		t.Fatalf("transient failure advanced or quarantined cursor: %+v", state)
	}
}

func TestOutboxPollerDoesNotAdvanceCursorBeforeDurableEnqueue(t *testing.T) {
	event, profile, actor := validBackendOutboxFixture(t)
	poller, _, state := newOutboxFixture(t, event, profile, actor)
	state.enqueueErr = errors.New("temporary federation database failure")
	count, err := poller.ProcessBatch(context.Background())
	if err == nil || count != 0 {
		t.Fatalf("expected enqueue failure, count=%d err=%v", count, err)
	}
	if state.cursor != "" || len(state.saved) != 0 {
		t.Fatalf("cursor advanced before durable enqueue: %+v", state)
	}
}

func TestOutboxPollerPaginatesAllRemoteFollowerInboxes(t *testing.T) {
	event, profile, actor := validBackendOutboxFixture(t)
	poller, _, state := newOutboxFixture(t, event, profile, actor)
	state.inboxPages = [][]string{
		{"https://remote-1.example/inbox", "https://remote-2.example/inbox"},
		{"https://remote-3.example/inbox"},
		{"https://remote-4.example/inbox"},
	}
	count, err := poller.ProcessBatch(context.Background())
	if err != nil || count != 1 {
		t.Fatalf("ProcessBatch count=%d err=%v", count, err)
	}
	if state.pageCalls != 3 || len(state.enqueuedPages) != 3 || len(state.enqueuedInboxes) != 4 {
		t.Fatalf("fan-out was truncated: calls=%d pages=%d inboxes=%d", state.pageCalls, len(state.enqueuedPages), len(state.enqueuedInboxes))
	}
	if state.enqueuedInboxes[3] != "https://remote-4.example/inbox" {
		t.Fatalf("last remote inbox was not enqueued: %#v", state.enqueuedInboxes)
	}
	if state.cursor != base64.RawURLEncoding.EncodeToString([]byte(event.ID)) {
		t.Fatalf("Backend cursor advanced incorrectly after paginated fan-out: %q", state.cursor)
	}
}

func TestOutboxPollerPersistsActivityWhenThereAreNoRemoteFollowers(t *testing.T) {
	event, profile, actor := validBackendOutboxFixture(t)
	poller, _, state := newOutboxFixture(t, event, profile, actor)
	state.inboxes = nil
	count, err := poller.ProcessBatch(context.Background())
	if err != nil || count != 1 {
		t.Fatalf("ProcessBatch count=%d err=%v", count, err)
	}
	if state.enqueuedID == "" || len(state.enqueuedPages) != 1 || len(state.enqueuedPages[0]) != 0 {
		t.Fatalf("actor outbox activity was not persisted without followers: %+v", state)
	}
}
