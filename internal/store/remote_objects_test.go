package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestRemoteObjectProjectionReplayOrderingAndVisibility(t *testing.T) {
	s := integrationStore(t)
	ctx := context.Background()
	suffix := time.Now().UTC().Format("150405000000000")
	actorURI := "https://remote.example/users/projection-" + suffix
	keyID := actorURI + "#main-key"
	if err := s.StoreRemoteActor(ctx, actorURI, keyID, "-----BEGIN PUBLIC KEY-----\ninvalid-test-key\n-----END PUBLIC KEY-----", actorURI+"/inbox", ""); err != nil {
		t.Fatal(err)
	}
	published := time.Now().UTC().Add(-time.Minute)
	objectURI := "https://remote.example/objects/projection-" + suffix
	payload, _ := json.Marshal(map[string]any{"id": objectURI, "type": "Note", "attributedTo": actorURI})
	createdAt := time.Now().UTC()
	object := RemoteObject{
		ObjectURI: objectURI, ActorURI: actorURI, ObjectType: "Note", Content: "first version",
		PublishedAt: &published, Visibility: "public", Payload: payload, ActivityAt: createdAt,
	}
	createID := "https://remote.example/activities/create-" + suffix
	if err := s.ApplyRemoteObjectActivity(ctx, createID, "Create", object); err != nil {
		t.Fatal(err)
	}
	if err := s.ApplyRemoteObjectActivity(ctx, createID, "Create", object); err != nil {
		t.Fatalf("duplicate Create should be idempotent: %v", err)
	}

	newer := object
	newer.Content = "newer version"
	newer.ActivityAt = createdAt.Add(time.Minute)
	newerID := "https://remote.example/activities/update-newer-" + suffix
	if err := s.ApplyRemoteObjectActivity(ctx, newerID, "Update", newer); err != nil {
		t.Fatal(err)
	}
	stale := object
	stale.Content = "stale version"
	stale.ActivityAt = createdAt.Add(-time.Minute)
	staleID := "https://remote.example/activities/update-stale-" + suffix
	if err := s.ApplyRemoteObjectActivity(ctx, staleID, "Update", stale); err != nil {
		t.Fatal(err)
	}

	followerObject := object
	followerObject.ObjectURI = objectURI + "-followers"
	followerObject.Visibility = "followers"
	followerObject.ActivityAt = createdAt.Add(2 * time.Minute)
	followerID := "https://remote.example/activities/create-followers-" + suffix
	if err := s.ApplyRemoteObjectActivity(ctx, followerID, "Create", followerObject); err != nil {
		t.Fatal(err)
	}

	page, err := s.ListAuthorizedRemoteObjects(ctx, []string{actorURI}, 10, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || page.Items[0].ObjectURI != objectURI || page.Items[0].Content != "newer version" {
		t.Fatalf("public projection included private content or stale update: %#v", page.Items)
	}

	deleteID := "https://remote.example/activities/delete-" + suffix
	if err := s.ApplyRemoteObjectDeletion(ctx, deleteID, objectURI, actorURI, createdAt.Add(3*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := s.ApplyRemoteObjectActivity(ctx, "https://remote.example/activities/update-after-delete-"+suffix, "Update", newer); err != nil {
		t.Fatal(err)
	}
	page, err = s.ListAuthorizedRemoteObjects(ctx, []string{actorURI}, 10, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 0 {
		t.Fatalf("deleted object was resurrected: %#v", page.Items)
	}
}
