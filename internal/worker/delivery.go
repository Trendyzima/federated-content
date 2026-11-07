package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Trendyzima/federated-content/internal/activitypub"
	"github.com/Trendyzima/federated-content/internal/store"
)

type DeliveryStore interface {
	ClaimDeliveries(context.Context, int, time.Duration) ([]store.Delivery, error)
	ActorByURI(context.Context, string) (store.Actor, error)
	AckDelivery(context.Context, int64, string) error
	FailDelivery(context.Context, int64, string, string, bool) error
}

type DeliveryWorker struct {
	store    DeliveryStore
	client   *http.Client
	logger   *slog.Logger
	interval time.Duration
	batch    int
}

func NewDeliveryWorker(state DeliveryStore, client *http.Client, logger *slog.Logger) (*DeliveryWorker, error) {
	if state == nil {
		return nil, errors.New("delivery worker requires Federation storage")
	}
	if client == nil {
		client = activitypub.NewRemoteHTTPSClient()
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &DeliveryWorker{store: state, client: client, logger: logger, interval: 5 * time.Second, batch: 25}, nil
}

func (w *DeliveryWorker) Run(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		if _, err := w.ProcessBatch(ctx); err != nil && ctx.Err() == nil {
			w.logger.Error("federation delivery batch failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (w *DeliveryWorker) ProcessBatch(ctx context.Context) (int, error) {
	deliveries, err := w.store.ClaimDeliveries(ctx, w.batch, 45*time.Second)
	if err != nil {
		return 0, fmt.Errorf("claim federation deliveries: %w", err)
	}
	var firstErr error
	for _, delivery := range deliveries {
		if err := w.deliver(ctx, delivery); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return len(deliveries), firstErr
}

func (w *DeliveryWorker) deliver(ctx context.Context, delivery store.Delivery) error {
	fail := func(reason string, permanent bool) error {
		if err := w.store.FailDelivery(ctx, delivery.ID, delivery.LeaseToken, reason, permanent); err != nil {
			return fmt.Errorf("record failed delivery %d: %w", delivery.ID, err)
		}
		return nil
	}
	if delivery.ID <= 0 || delivery.LeaseToken == "" || delivery.ActorURI == "" || delivery.InboxURL == "" || len(delivery.Payload) == 0 {
		return fail("claimed delivery is missing required fields", true)
	}
	if err := validateInboxURL(delivery.InboxURL); err != nil {
		return fail(err.Error(), true)
	}
	actor, err := w.store.ActorByURI(ctx, delivery.ActorURI)
	if err != nil {
		return fail("local signing actor is unavailable: "+err.Error(), false)
	}
	if actor.ActorURI != delivery.ActorURI || actor.PublicKeyID == "" || actor.PrivateKeyPEM == "" {
		return fail("local signing actor key is unavailable or mismatched", true)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, delivery.InboxURL, bytes.NewReader(delivery.Payload))
	if err != nil {
		return fail("remote inbox request is invalid: "+err.Error(), true)
	}
	if err := activitypub.SignRequest(request, delivery.Payload, actor.PublicKeyID, actor.PrivateKeyPEM, time.Now().UTC()); err != nil {
		return fail("outbound HTTP signature failed: "+err.Error(), true)
	}
	response, err := w.client.Do(request)
	if err != nil {
		return fail("remote delivery transport failed: "+err.Error(), false)
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4<<10))
	if (response.StatusCode >= 200 && response.StatusCode < 300) || response.StatusCode == http.StatusConflict {
		if err := w.store.AckDelivery(ctx, delivery.ID, delivery.LeaseToken); err != nil {
			return fmt.Errorf("acknowledge delivery %d: %w", delivery.ID, err)
		}
		return nil
	}
	permanent := response.StatusCode >= 300 && response.StatusCode < 500 &&
		response.StatusCode != http.StatusRequestTimeout &&
		response.StatusCode != http.StatusTooEarly &&
		response.StatusCode != http.StatusTooManyRequests
	reason := fmt.Sprintf("remote inbox returned HTTP %d", response.StatusCode)
	if err := fail(reason, permanent); err != nil {
		return err
	}
	if permanent {
		w.logger.Warn("federation delivery moved to dead letter", "delivery_id", delivery.ID, "status", response.StatusCode)
	}
	return fmt.Errorf("delivery %d failed: %s", delivery.ID, reason)
}

func validateInboxURL(raw string) error {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.Fragment != "" {
		return errors.New("remote inbox must be an HTTPS URL without credentials or fragment")
	}
	return nil
}
