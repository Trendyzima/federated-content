package worker

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/Trendyzima/federated-content/internal/backend"
	"github.com/Trendyzima/federated-content/internal/store"
)

type recordingRemoteInboxStore struct {
	*fakeInboxStore
	appliedID, appliedType                 string
	applied                                store.RemoteObject
	deletedID, deletedObject, deletedActor string
	deletedAt                              time.Time
}

func (f *recordingRemoteInboxStore) ApplyRemoteObjectActivity(_ context.Context, id, typ string, obj store.RemoteObject) error {
	f.appliedID, f.appliedType, f.applied = id, typ, obj
	return nil
}
func (f *recordingRemoteInboxStore) ApplyRemoteObjectDeletion(_ context.Context, id, objectURI, actorURI string, at time.Time) error {
	f.deletedID, f.deletedObject, f.deletedActor, f.deletedAt = id, objectURI, actorURI, at
	return nil
}

func TestInboxWorkerProjectsSignedPublicCreateAndUpdate(t *testing.T) {
	actor := "https://remote.example/users/bob"
	objectURI := "https://remote.example/objects/post-1"
	for _, activityType := range []string{"Create", "Update"} {
		t.Run(activityType, func(t *testing.T) {
			object := map[string]any{
				"id": objectURI, "type": "Note", "attributedTo": actor,
				"content": "Hello from the Fediverse", "published": "2026-11-16T08:00:00Z",
				"to": []string{"https://www.w3.org/ns/activitystreams#Public"},
				"cc": []string{actor + "/followers"},
			}
			rawObject, _ := json.Marshal(object)
			activityID := "https://remote.example/activities/" + activityType + "-1"
			payload, _ := json.Marshal(map[string]any{
				"id": activityID, "type": activityType, "actor": actor,
				"published": "2026-11-16T08:01:00Z", "object": json.RawMessage(rawObject),
			})
			state := &recordingRemoteInboxStore{fakeInboxStore: &fakeInboxStore{items: []store.InboxActivity{{
				ActivityID: activityID, ActorURI: actor, RecipientUsername: "alice",
				ActivityType: activityType, Payload: payload, LeaseToken: "lease-1",
			}}}}
			backendClient := &fakeInboxBackend{profile: backend.ActorProfile{
				UserID: "native-user-1", Handle: "alice", FederationEnabled: true,
			}}
			worker := &InboxWorker{store: state, backend: backendClient, publicBaseURL: "https://federation.testagram.site", logger: slog.Default()}
			if _, err := worker.ProcessBatch(context.Background()); err != nil {
				t.Fatal(err)
			}
			if state.appliedID != activityID || state.appliedType != activityType {
				t.Fatalf("wrong activity persisted: %q %q", state.appliedID, state.appliedType)
			}
			if state.applied.ObjectURI != objectURI || state.applied.ActorURI != actor || state.applied.Visibility != "public" {
				t.Fatalf("wrong remote projection: %#v", state.applied)
			}
			if state.applied.ObjectType != "Note" || state.applied.Content != "Hello from the Fediverse" || state.applied.ActivityAt.IsZero() {
				t.Fatalf("projection fields missing: %#v", state.applied)
			}
			if len(state.acked) != 1 || len(state.failed) != 0 {
				t.Fatalf("activity not acknowledged: %#v", state)
			}
		})
	}
}

func TestInboxWorkerTombstonesOnlyAuthenticatedActorsObject(t *testing.T) {
	actor := "https://remote.example/users/bob"
	objectURI := "https://remote.example/objects/post-2"
	activityID := "https://remote.example/activities/delete-2"
	payload, _ := json.Marshal(map[string]any{
		"id": activityID, "type": "Delete", "actor": actor,
		"published": "2026-11-16T08:01:00Z", "object": objectURI,
	})
	state := &recordingRemoteInboxStore{fakeInboxStore: &fakeInboxStore{items: []store.InboxActivity{{
		ActivityID: activityID, ActorURI: actor, RecipientUsername: "alice",
		ActivityType: "Delete", Payload: payload, LeaseToken: "lease-2",
	}}}}
	backendClient := &fakeInboxBackend{profile: backend.ActorProfile{UserID: "native-user-1", Handle: "alice", FederationEnabled: true}}
	worker := &InboxWorker{store: state, backend: backendClient, publicBaseURL: "https://federation.testagram.site"}
	if _, err := worker.ProcessBatch(context.Background()); err != nil {
		t.Fatal(err)
	}
	if state.deletedID != activityID || state.deletedObject != objectURI || state.deletedActor != actor || state.deletedAt.IsZero() {
		t.Fatalf("delete tombstone not persisted with verified actor binding: %#v", state)
	}
	if len(state.acked) != 1 || len(state.failed) != 0 {
		t.Fatalf("Delete not acknowledged: %#v", state)
	}
}

func TestActivityVisibilityPreservesAudience(t *testing.T) {
	public := "https://www.w3.org/ns/activitystreams#Public"
	cases := []struct {
		name   string
		to, cc []string
		want   string
	}{
		{"public", []string{public}, nil, "public"},
		{"unlisted", nil, []string{public}, "unlisted"},
		{"followers", []string{"https://remote.example/users/bob/followers"}, nil, "followers"},
		{"direct", []string{"https://remote.example/users/alice"}, nil, "direct"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := activityVisibility(tc.to, tc.cc); got != tc.want {
				t.Fatalf("visibility=%q want=%q", got, tc.want)
			}
		})
	}
}
