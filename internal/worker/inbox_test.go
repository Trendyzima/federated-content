package worker

import (
	"context"
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

type fakeInboxStore struct {
	items  []store.InboxActivity
	acked  []string
	failed []struct {
		id, reason string
		permanent  bool
	}
	added         []string
	removed       []string
	queuedID      string
	queuedActor   string
	queuedPayload []byte
	queuedInboxes []string
}

func (f *fakeInboxStore) ClaimInbox(context.Context, int, time.Duration) ([]store.InboxActivity, error) {
	return f.items, nil
}
func (f *fakeInboxStore) AckInbox(_ context.Context, id, token string) error {
	if token == "" {
		return errors.New("missing lease")
	}
	f.acked = append(f.acked, id)
	return nil
}
func (f *fakeInboxStore) FailInbox(_ context.Context, id, token, reason string, permanent bool) error {
	if token == "" {
		return errors.New("missing lease")
	}
	f.failed = append(f.failed, struct {
		id, reason string
		permanent  bool
	}{id, reason, permanent})
	return nil
}
func (f *fakeInboxStore) EnsureActor(_ context.Context, userID, username, actorURI, displayName, summary string) (store.Actor, error) {
	return store.Actor{UserID: userID, Username: username, ActorURI: actorURI, PublicKeyID: actorURI + "#main-key", DisplayName: displayName, Summary: summary}, nil
}
func (f *fakeInboxStore) AddRemoteFollower(_ context.Context, userID, actorURI string) error {
	f.added = append(f.added, userID+":"+actorURI)
	return nil
}
func (f *fakeInboxStore) RemoveRemoteFollower(_ context.Context, userID, actorURI string) error {
	f.removed = append(f.removed, userID+":"+actorURI)
	return nil
}
func (f *fakeInboxStore) RemoteActorInbox(context.Context, string) (string, error) {
	return "https://remote.example/inbox", nil
}
func (f *fakeInboxStore) EnqueueOutbox(_ context.Context, id, actor string, payload []byte, inboxes []string) error {
	f.queuedID = id
	f.queuedActor = actor
	f.queuedPayload = append([]byte(nil), payload...)
	f.queuedInboxes = append([]string(nil), inboxes...)
	return nil
}

type fakeInboxBackend struct {
	profile backend.ActorProfile
	err     error
}

func (f fakeInboxBackend) ResolveActor(context.Context, string) (backend.ActorProfile, error) {
	return f.profile, f.err
}

func testInboxWorker(state *fakeInboxStore, backendClient InboxBackend) *InboxWorker {
	return &InboxWorker{store: state, backend: backendClient, publicBaseURL: "https://federation.testagram.site", logger: slog.New(slog.NewTextHandler(io.Discard, nil)), interval: 1, batch: 10}
}

func TestInboxWorkerAcceptsFollowAndQueuesAccept(t *testing.T) {
	actorURI := "https://remote.example/users/bob"
	localURI := "https://federation.testagram.site/users/alice"
	payload, _ := json.Marshal(map[string]any{"id": "https://remote.example/activities/follow-1", "type": "Follow", "actor": actorURI, "object": localURI})
	state := &fakeInboxStore{items: []store.InboxActivity{{ActivityID: "https://remote.example/activities/follow-1", ActorURI: actorURI, RecipientUsername: "alice", ActivityType: "Follow", Payload: payload, LeaseToken: "lease-1"}}}
	backendClient := fakeInboxBackend{profile: backend.ActorProfile{UserID: "user_123", Handle: "alice", DisplayName: "Alice", FederationEnabled: true}}
	worker := testInboxWorker(state, backendClient)
	count, err := worker.ProcessBatch(context.Background())
	if err != nil || count != 1 {
		t.Fatalf("ProcessBatch()=(%d,%v)", count, err)
	}
	if len(state.added) != 1 || state.added[0] != "user_123:"+actorURI {
		t.Fatalf("remote follower was not stored: %#v", state.added)
	}
	if len(state.acked) != 1 || state.acked[0] != "https://remote.example/activities/follow-1" {
		t.Fatalf("Follow was not acknowledged: %#v", state.acked)
	}
	if len(state.failed) != 0 {
		t.Fatalf("valid Follow was failed: %#v", state.failed)
	}
	if state.queuedActor != localURI || len(state.queuedInboxes) != 1 || state.queuedInboxes[0] != "https://remote.example/inbox" {
		t.Fatalf("Accept delivery target is wrong: %#v", state)
	}
	var accept map[string]json.RawMessage
	if err := json.Unmarshal(state.queuedPayload, &accept); err != nil {
		t.Fatal(err)
	}
	var activityType string
	if err := json.Unmarshal(accept["type"], &activityType); err != nil || activityType != "Accept" {
		t.Fatalf("queued activity type = %q, err=%v", activityType, err)
	}
	if !strings.HasPrefix(state.queuedID, "https://federation.testagram.site/activities/accept-") {
		t.Fatalf("Accept ID is not deterministic local activity URI: %s", state.queuedID)
	}
}

func TestInboxWorkerProcessesUndoFollow(t *testing.T) {
	actorURI := "https://remote.example/users/bob"
	localURI := "https://federation.testagram.site/users/alice"
	original := map[string]any{"id": "https://remote.example/activities/follow-1", "type": "Follow", "actor": actorURI, "object": localURI}
	payload, _ := json.Marshal(map[string]any{"id": "https://remote.example/activities/undo-1", "type": "Undo", "actor": actorURI, "object": original})
	state := &fakeInboxStore{items: []store.InboxActivity{{ActivityID: "https://remote.example/activities/undo-1", ActorURI: actorURI, RecipientUsername: "alice", ActivityType: "Undo", Payload: payload, LeaseToken: "lease-2"}}}
	backendClient := fakeInboxBackend{profile: backend.ActorProfile{UserID: "user_123", Handle: "alice", FederationEnabled: true}}
	count, err := testInboxWorker(state, backendClient).ProcessBatch(context.Background())
	if err != nil || count != 1 {
		t.Fatalf("ProcessBatch()=(%d,%v)", count, err)
	}
	if len(state.removed) != 1 || state.removed[0] != "user_123:"+actorURI {
		t.Fatalf("Undo did not remove the remote follower: %#v", state.removed)
	}
	if len(state.acked) != 1 || len(state.failed) != 0 {
		t.Fatalf("Undo was not acknowledged cleanly: ack=%v failed=%v", state.acked, state.failed)
	}
}

func TestInboxWorkerQuarantinesUnsupportedActivity(t *testing.T) {
	actorURI := "https://remote.example/users/bob"
	payload, _ := json.Marshal(map[string]any{"id": "https://remote.example/activities/like-1", "type": "Like", "actor": actorURI, "object": "https://federation.testagram.site/posts/post_123"})
	state := &fakeInboxStore{items: []store.InboxActivity{{ActivityID: "https://remote.example/activities/like-1", ActorURI: actorURI, RecipientUsername: "alice", ActivityType: "Like", Payload: payload, LeaseToken: "lease-3"}}}
	backendClient := fakeInboxBackend{profile: backend.ActorProfile{UserID: "user_123", Handle: "alice", FederationEnabled: true}}
	count, err := testInboxWorker(state, backendClient).ProcessBatch(context.Background())
	if err != nil || count != 1 {
		t.Fatalf("ProcessBatch()=(%d,%v), want successful quarantine", count, err)
	}
	if len(state.failed) != 1 || !state.failed[0].permanent || !strings.Contains(state.failed[0].reason, "durably quarantined") {
		t.Fatalf("unsupported activity was not dead-lettered: %#v", state.failed)
	}
	if len(state.acked) != 0 {
		t.Fatalf("unsupported activity was incorrectly acknowledged: %#v", state.acked)
	}
}

func TestInboxWorkerRetriesBackendOutage(t *testing.T) {
	actorURI := "https://remote.example/users/bob"
	payload, _ := json.Marshal(map[string]any{"id": "https://remote.example/activities/follow-1", "type": "Follow", "actor": actorURI, "object": "https://federation.testagram.site/users/alice"})
	state := &fakeInboxStore{items: []store.InboxActivity{{ActivityID: "https://remote.example/activities/follow-1", ActorURI: actorURI, RecipientUsername: "alice", ActivityType: "Follow", Payload: payload, LeaseToken: "lease-4"}}}
	backendClient := fakeInboxBackend{err: errors.New("Backend temporarily unavailable")}
	count, err := testInboxWorker(state, backendClient).ProcessBatch(context.Background())
	if err == nil || count != 1 {
		t.Fatalf("ProcessBatch()=(%d,%v), want retry error", count, err)
	}
	if len(state.failed) != 1 || state.failed[0].permanent {
		t.Fatalf("Backend outage should be retryable: %#v", state.failed)
	}
}
