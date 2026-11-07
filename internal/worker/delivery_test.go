package worker

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Trendyzima/federated-content/internal/activitypub"
	"github.com/Trendyzima/federated-content/internal/store"
)

type fakeDeliveryStore struct {
	deliveries []store.Delivery
	actor      store.Actor
	acked      []int64
	failed     []struct {
		id        int64
		permanent bool
		reason    string
	}
}

func (f *fakeDeliveryStore) ClaimDeliveries(context.Context, int, time.Duration) ([]store.Delivery, error) {
	items := f.deliveries
	f.deliveries = nil
	return items, nil
}
func (f *fakeDeliveryStore) ActorByURI(context.Context, string) (store.Actor, error) {
	return f.actor, nil
}
func (f *fakeDeliveryStore) AckDelivery(_ context.Context, id int64, _ string) error {
	f.acked = append(f.acked, id)
	return nil
}
func (f *fakeDeliveryStore) FailDelivery(_ context.Context, id int64, _ string, reason string, permanent bool) error {
	f.failed = append(f.failed, struct {
		id        int64
		permanent bool
		reason    string
	}{id: id, permanent: permanent, reason: reason})
	return nil
}

type deliveryKeyResolver struct{ key activitypub.PublicKey }

func (r deliveryKeyResolver) Resolve(context.Context, string) (activitypub.PublicKey, error) {
	return r.key, nil
}

func newDeliveryFixture(t *testing.T, responseStatus int) (*fakeDeliveryStore, *httptest.Server, *DeliveryWorker) {
	t.Helper()
	private, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	actorURI := "https://federation.testagram.site/users/alice"
	keyID := actorURI + "#main-key"
	privatePEM := string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(private)}))
	publicDER, err := x509.MarshalPKIXPublicKey(&private.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	publicPEM := string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: publicDER}))
	body, _ := json.Marshal(map[string]any{"id": actorURI + "/activities/1", "type": "Create", "actor": actorURI})
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read delivery body: %v", err)
			return
		}
		date, err := http.ParseTime(r.Header.Get("Date"))
		if err != nil {
			t.Errorf("invalid signed Date: %v", err)
			return
		}
		verified, err := activitypub.VerifyRequest(r.Context(), r, payload, deliveryKeyResolver{
			key: activitypub.PublicKey{ID: keyID, Owner: actorURI, PEM: publicPEM},
		}, date)
		if err != nil || verified.ID != keyID {
			t.Errorf("outbound signature did not verify: key=%#v err=%v", verified, err)
			return
		}
		if r.Header.Get("Content-Type") != "application/activity+json" {
			t.Errorf("Content-Type=%q", r.Header.Get("Content-Type"))
		}
		w.WriteHeader(responseStatus)
	}))
	t.Cleanup(server.Close)
	state := &fakeDeliveryStore{
		actor: store.Actor{UserID: "user_1", Username: "alice", ActorURI: actorURI, PublicKeyID: keyID, PrivateKeyPEM: privatePEM},
		deliveries: []store.Delivery{{
			ID: 7, ActivityID: actorURI + "/activities/1", ActorURI: actorURI,
			InboxURL: server.URL + "/inbox", Attempts: 0, LeaseToken: "lease-token", Payload: body,
		}},
	}
	worker, err := NewDeliveryWorker(state, server.Client(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	return state, server, worker
}

func TestDeliveryWorkerSignsAndAcknowledgesSuccessfulDelivery(t *testing.T) {
	state, _, worker := newDeliveryFixture(t, http.StatusAccepted)
	count, err := worker.ProcessBatch(context.Background())
	if count != 1 || err != nil {
		t.Fatalf("ProcessBatch count=%d err=%v", count, err)
	}
	if len(state.acked) != 1 || state.acked[0] != 7 || len(state.failed) != 0 {
		t.Fatalf("unexpected delivery state: acked=%v failed=%v", state.acked, state.failed)
	}
}

func TestDeliveryWorkerRetriesTemporaryFailuresAndDeadLettersPermanentFailures(t *testing.T) {
	for _, tc := range []struct {
		name      string
		status    int
		permanent bool
	}{
		{name: "server error retries", status: http.StatusServiceUnavailable, permanent: false},
		{name: "rate limit retries", status: http.StatusTooManyRequests, permanent: false},
		{name: "gone inbox dead letters", status: http.StatusGone, permanent: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state, _, worker := newDeliveryFixture(t, tc.status)
			count, err := worker.ProcessBatch(context.Background())
			if count != 1 || err == nil {
				t.Fatalf("ProcessBatch count=%d err=%v", count, err)
			}
			if len(state.acked) != 0 || len(state.failed) != 1 || state.failed[0].permanent != tc.permanent {
				t.Fatalf("unexpected retry/dead-letter state: acked=%v failed=%+v", state.acked, state.failed)
			}
		})
	}
}

func TestValidateInboxURLRejectsUnsafeTargets(t *testing.T) {
	for _, value := range []string{
		"http://remote.example/inbox",
		"https://user:password@remote.example/inbox",
		"https://remote.example/inbox#fragment",
		"file:///etc/passwd",
		"not a URL",
	} {
		if err := validateInboxURL(value); err == nil {
			t.Fatalf("accepted unsafe inbox URL %q", value)
		}
	}
	if err := validateInboxURL("https://remote.example/inbox?shared=true"); err != nil {
		t.Fatalf("rejected valid HTTPS inbox URL: %v", err)
	}
}

var _ = strings.Builder{}
