package store

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func integrationStore(t *testing.T) *Store {
	t.Helper()
	dsn := os.Getenv("FEDERATION_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("FEDERATION_TEST_DATABASE_URL is not configured")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s, err := Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestDurableActorInboxOutboxAndDeliveryLease(t *testing.T) {
	s := integrationStore(t)
	ctx := context.Background()
	suffix := time.Now().UTC().Format("150405000000")
	userID := "test_user_" + suffix
	actorURI := "https://federation.testagram.site/users/" + userID
	first, err := s.EnsureActor(ctx, userID, "tester_"+suffix, actorURI, "Test Actor", "Test summary")
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.EnsureActor(ctx, userID, first.Username, actorURI, "Updated Actor", "Updated summary")
	if err != nil {
		t.Fatal(err)
	}
	if first.PrivateKeyPEM == "" || first.PrivateKeyPEM != second.PrivateKeyPEM || first.PublicKeyPEM != second.PublicKeyPEM {
		t.Fatal("actor key material was not durably and consistently persisted")
	}
	activity := []byte("{\"id\":\"https://remote.example/activities/1\",\"type\":\"Follow\",\"actor\":\"https://remote.example/users/a\",\"object\":\"" + actorURI + "\"}")
	inserted, err := s.StoreInbox(ctx, "https://remote.example/activities/1", "https://remote.example/users/a", first.Username, "Follow", activity)
	if err != nil || !inserted {
		t.Fatalf("first inbox insert inserted=%v err=%v", inserted, err)
	}
	inserted, err = s.StoreInbox(ctx, "https://remote.example/activities/1", "https://remote.example/users/a", first.Username, "Follow", activity)
	if err != nil || inserted {
		t.Fatalf("duplicate inbox activity was not idempotent inserted=%v err=%v", inserted, err)
	}

	activityID := actorURI + "/activities/1"
	outbound := []byte("{\"id\":\"" + activityID + "\",\"type\":\"Create\"}")
	if err := s.EnqueueOutbox(ctx, activityID, actorURI, outbound, []string{"https://remote.example/inbox"}); err != nil {
		t.Fatal(err)
	}
	claimed, err := s.ClaimDeliveries(ctx, 10, 90*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var delivery *Delivery
	for i := range claimed {
		if claimed[i].ActivityID == activityID {
			delivery = &claimed[i]
			break
		}
	}
	if delivery == nil || delivery.LeaseToken == "" {
		t.Fatal("delivery was not claimed with a lease")
	}
	if delivery.ActorURI != actorURI {
		t.Fatalf("claimed delivery actor URI=%q want=%q", delivery.ActorURI, actorURI)
	}
	if err := s.AckDelivery(ctx, delivery.ID, delivery.LeaseToken); err != nil {
		t.Fatal(err)
	}
	if err := s.AckDelivery(ctx, delivery.ID, delivery.LeaseToken); err == nil {
		t.Fatal("stale delivery lease acknowledged twice")
	}
}

func TestDeliveryFailureCanBeDeadLettered(t *testing.T) {
	s := integrationStore(t)
	ctx := context.Background()
	suffix := time.Now().UTC().Format("150405000000")
	userID := "test_dlq_" + suffix
	actorURI := "https://federation.testagram.site/users/" + userID
	if _, err := s.EnsureActor(ctx, userID, "dlq_"+suffix, actorURI, "DLQ", ""); err != nil {
		t.Fatal(err)
	}
	activityID := actorURI + "/activities/dead-letter"
	outbound := []byte("{\"id\":\"" + activityID + "\",\"type\":\"Create\"}")
	if err := s.EnqueueOutbox(ctx, activityID, actorURI, outbound, []string{"https://remote.example/inbox"}); err != nil {
		t.Fatal(err)
	}
	claimed, err := s.ClaimDeliveries(ctx, 10, 90*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range claimed {
		if d.ActivityID == activityID {
			if err := s.FailDelivery(ctx, d.ID, d.LeaseToken, "remote rejected permanently", true); err != nil {
				t.Fatal(err)
			}
		}
	}
	var state string
	if err := s.db.QueryRowContext(ctx, "SELECT state FROM delivery_jobs WHERE activity_id=$1", activityID).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "dead_letter" {
		t.Fatalf("state=%q, want dead_letter", state)
	}
}

func TestConcurrentOpenSerializesSchemaMigration(t *testing.T) {
	dsn := os.Getenv("FEDERATION_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("FEDERATION_TEST_DATABASE_URL is not configured")
	}
	const workers = 8
	start := make(chan struct{})
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			s, err := Open(ctx, dsn)
			if err != nil {
				errs <- err
				return
			}
			if err := s.Ping(ctx); err != nil {
				errs <- err
			}
			if err := s.Close(); err != nil {
				errs <- err
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("concurrent schema initialization failed: %v", err)
	}
}

func TestRemoteFollowerFanoutAndActivityPubObjectDereference(t *testing.T) {
	s := integrationStore(t)
	ctx := context.Background()
	suffix := time.Now().UTC().Format("150405000000")
	userID := "test_followers_" + suffix
	actorURI := "https://federation.testagram.site/users/" + userID
	actor, err := s.EnsureActor(ctx, userID, "followers_"+suffix, actorURI, "Followers", "")
	if err != nil {
		t.Fatal(err)
	}
	remoteURI := "https://remote.example/users/follower"
	if err := s.StoreRemoteActor(ctx, remoteURI, remoteURI+"#main-key", "public-key", "https://remote.example/inbox", "https://remote.example/shared-inbox"); err != nil {
		t.Fatal(err)
	}
	if err := s.AddRemoteFollower(ctx, actor.UserID, remoteURI); err != nil {
		t.Fatal(err)
	}
	count, err := s.CountRemoteFollowers(ctx, actor.UserID)
	if err != nil || count != 1 {
		t.Fatalf("remote follower count=%d err=%v", count, err)
	}
	followers, next, err := s.ListRemoteFollowers(ctx, actor.UserID, "", 10)
	if err != nil || next != "" || len(followers) != 1 || followers[0] != remoteURI {
		t.Fatalf("remote followers=%v next=%q err=%v", followers, next, err)
	}
	inboxes, err := s.FollowerInboxes(ctx, actor.UserID)
	if err != nil || len(inboxes) != 1 || inboxes[0] != "https://remote.example/shared-inbox" {
		t.Fatalf("follower inboxes=%v err=%v", inboxes, err)
	}
	remoteInbox, err := s.RemoteActorInbox(ctx, remoteURI)
	if err != nil || remoteInbox != "https://remote.example/shared-inbox" {
		t.Fatalf("remote actor inbox=%q err=%v", remoteInbox, err)
	}

	objectURI := actorURI + "/statuses/post_" + suffix
	activityID := actorURI + "/activities/create_" + suffix
	activity := []byte(`{"id":"` + activityID + `","type":"Create","actor":"` + actorURI + `","object":{"id":"` + objectURI + `","type":"Note","content":"hello"}}`)
	if err := s.EnqueueOutbox(ctx, activityID, actorURI, activity, nil); err != nil {
		t.Fatal(err)
	}
	object, err := s.GetOutboxObject(ctx, actorURI, objectURI)
	if err != nil || !json.Valid(object) || !strings.Contains(string(object), `"content":"hello"`) {
		t.Fatalf("dereferenced object=%s err=%v", object, err)
	}
	if err := s.SetState(ctx, "test_cursor_"+suffix, "Y3Vyc29y"); err != nil {
		t.Fatal(err)
	}
	cursor, err := s.GetState(ctx, "test_cursor_"+suffix)
	if err != nil || cursor != "Y3Vyc29y" {
		t.Fatalf("persisted cursor=%q err=%v", cursor, err)
	}
	if err := s.RemoveRemoteFollower(ctx, actor.UserID, remoteURI); err != nil {
		t.Fatal(err)
	}
	count, err = s.CountRemoteFollowers(ctx, actor.UserID)
	if err != nil || count != 0 {
		t.Fatalf("remote follower count after removal=%d err=%v", count, err)
	}
}
