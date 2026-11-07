package store

import (
	"context"
	"os"
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
