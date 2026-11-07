package backend

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type ActorProfile struct {
	UserID            string `json:"user_id"`
	Handle            string `json:"handle"`
	DisplayName       string `json:"display_name"`
	Summary           string `json:"summary"`
	FederationEnabled bool   `json:"federation_enabled"`
}

type Client struct {
	baseURL string
	token   string
	http    *http.Client
}

func New(baseURL, token string, client *http.Client) (*Client, error) {
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	token = strings.TrimSpace(token)
	u, err := url.Parse(baseURL)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Scheme != "https" && !(u.Scheme == "http" && (u.Hostname() == "api" || u.Hostname() == "localhost" || u.Hostname() == "127.0.0.1"))) {
		return nil, errors.New("Backend URL must be HTTPS (HTTP only for private api or localhost)")
	}
	if len(token) < 32 || strings.ContainsAny(token, "\r\n") {
		return nil, errors.New("Backend federation token must contain at least 32 characters")
	}
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	copyClient := *client
	if copyClient.Timeout <= 0 || copyClient.Timeout > 10*time.Second { copyClient.Timeout = 5*time.Second }
	copyClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &Client{baseURL: baseURL, token: token, http: &copyClient}, nil
}

func (c *Client) ResolveActor(ctx context.Context, handle string) (ActorProfile, error) {
	handle = strings.ToLower(strings.TrimSpace(handle))
	if handle == "" || len(handle) > 30 {
		return ActorProfile{}, errors.New("invalid local actor handle")
	}
	return c.resolveActorPath(ctx, "/internal/v1/federation/actors/"+url.PathEscape(handle), handle)
}

func (c *Client) ResolveActorByID(ctx context.Context, userID string) (ActorProfile, error) {
	userID = strings.TrimSpace(userID)
	if userID == "" || len(userID) > 36 {
		return ActorProfile{}, errors.New("invalid local actor user ID")
	}
	return c.resolveActorPath(ctx, "/internal/v1/federation/actors/by-id/"+url.PathEscape(userID), "")
}

func (c *Client) resolveActorPath(ctx context.Context, path, expectedHandle string) (ActorProfile, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return ActorProfile{}, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-Testagram-API-Version", "1")
	req.Header.Set("X-Request-ID", newRequestID())
	resp, err := c.http.Do(req)
	if err != nil {
		return ActorProfile{}, fmt.Errorf("Backend actor lookup failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return ActorProfile{}, ErrActorNotFound
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return ActorProfile{}, fmt.Errorf("Backend actor lookup returned HTTP %d", resp.StatusCode)
	}
	var envelope struct {
		OK   bool         `json:"ok"`
		Data ActorProfile `json:"data"`
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, (64<<10)+1))
	if err != nil || len(raw) > 64<<10 {
		return ActorProfile{}, errors.New("Backend actor lookup response exceeded 64 KiB")
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return ActorProfile{}, errors.New("Backend actor lookup returned invalid JSON")
	}
	if !envelope.OK || !envelope.Data.FederationEnabled || envelope.Data.UserID == "" || envelope.Data.Handle == "" || (expectedHandle != "" && envelope.Data.Handle != expectedHandle) {
		return ActorProfile{}, errors.New("Backend actor lookup returned an invalid or ineligible profile")
	}
	return envelope.Data, nil
}

func (c *Client) Ready(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/internal/v1/federation/readyz", nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-Testagram-API-Version", "1")
	req.Header.Set("X-Request-ID", newRequestID())
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("Backend readiness check failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Backend is not ready (HTTP %d)", resp.StatusCode)
	}
	return nil
}


func newRequestID() string {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err == nil { return hex.EncodeToString(raw[:]) }
	return fmt.Sprintf("federation-%d", time.Now().UnixNano())
}

var ErrActorNotFound = errors.New("local actor not found")
