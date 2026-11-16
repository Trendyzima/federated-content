package backend

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

type RemoteSubscriptionVerification struct {
	LocalUserID string `json:"local_user_id"`
	Protocol    string `json:"protocol"`
	Identity    string `json:"identity"`
	Handle      string `json:"handle,omitempty"`
	Verified    bool   `json:"verified"`
	ErrorCode   string `json:"error_code,omitempty"`
}

// VerifyRemoteSubscription reports a protocol-resolved external identity to
// Backend. Backend remains authoritative for the local user's subscription
// state; Federation cannot create or mutate a native Testagram account.
func (c *Client) VerifyRemoteSubscription(ctx context.Context, input RemoteSubscriptionVerification) error {
	if c == nil {
		return errors.New("Backend client is not configured")
	}
	input.LocalUserID = strings.TrimSpace(input.LocalUserID)
	input.Protocol = strings.ToLower(strings.TrimSpace(input.Protocol))
	input.Identity = strings.TrimSpace(input.Identity)
	input.Handle = strings.TrimSpace(input.Handle)
	input.ErrorCode = strings.TrimSpace(input.ErrorCode)
	if !canonicalPostIDPattern.MatchString(input.LocalUserID) ||
		(input.Protocol != "activitypub" && input.Protocol != "bluesky") ||
		len(input.Identity) < 12 || len(input.Identity) > 2048 ||
		len(input.Handle) > 256 || len(input.ErrorCode) > 96 ||
		(input.Verified && input.ErrorCode != "") {
		return errors.New("remote subscription verification request is invalid")
	}
	body, err := json.Marshal(input)
	if err != nil {
		return fmt.Errorf("encode remote subscription verification: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/internal/v1/federation/subscriptions/verify", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-Testagram-API-Version", "1")
	req.Header.Set("X-Request-ID", newRequestID())
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("Backend remote subscription verification failed: %w", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, (64<<10)+1))
	if err != nil || len(raw) > 64<<10 {
		return errors.New("Backend remote subscription verification response is unreadable or too large")
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Backend remote subscription verification returned HTTP %d", resp.StatusCode)
	}
	var envelope struct {
		OK bool `json:"ok"`
		Data struct {
			VerificationStatus string `json:"verification_status"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil || !envelope.OK ||
		envelope.Data.VerificationStatus != "verified" && envelope.Data.VerificationStatus != "rejected" {
		return errors.New("Backend remote subscription verification response is invalid")
	}
	return nil
}

func validRemoteActorURL(raw string) bool {
	u, err := url.Parse(strings.TrimSpace(raw))
	return err == nil && u.Scheme == "https" && u.Host != "" && u.User == nil && u.RawQuery == "" && u.Fragment == ""
}

func ValidLocalUserID(value string) bool {
	return canonicalPostIDPattern.MatchString(strings.ToLower(strings.TrimSpace(value)))
}
