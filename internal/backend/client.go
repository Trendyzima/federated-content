package backend

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

var canonicalPostIDPattern = regexp.MustCompile(`(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}package backend

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

)

type ActorProfile struct {
	UserID            string `json:"user_id"`
	Handle            string `json:"handle"`
	DisplayName       string `json:"display_name"`
	Summary           string `json:"summary"`
	FederationEnabled bool   `json:"federation_enabled"`
}

type OutboxEvent struct {
	ID        string          `json:"$id"`
	EventType string          `json:"event_type"`
	ActorID   string          `json:"actor_id"`
	ObjectID  string          `json:"object_id"`
	Payload   json.RawMessage `json:"payload"`
	CreatedAt string          `json:"created_at"`
}

type OutboxPage struct {
	Events     []OutboxEvent `json:"events"`
	NextCursor string        `json:"next_cursor"`
	HasMore    bool          `json:"has_more"`
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
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") || (u.Scheme != "https" && !(u.Scheme == "http" && isPrivateBackendHost(u.Hostname()))) {
		return nil, errors.New("Backend URL must be HTTPS (HTTP only for private api or localhost)")
	}
	if len(token) < 32 || strings.ContainsAny(token, "\r\n") {
		return nil, errors.New("Backend federation token must contain at least 32 characters")
	}
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	copyClient := *client
	if copyClient.Timeout <= 0 || copyClient.Timeout > 10*time.Second {
		copyClient.Timeout = 5 * time.Second
	}
	copyClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &Client{baseURL: baseURL, token: token, http: &copyClient}, nil
}


func (c *Client) GetPublicObject(ctx context.Context, postID string) (json.RawMessage, error) {
	postID = strings.ToLower(strings.TrimSpace(postID))
	if !canonicalPostIDPattern.MatchString(postID) {
		return nil, errors.New("invalid canonical public object ID")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/internal/v1/federation/objects/"+url.PathEscape(postID), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-Testagram-API-Version", "1")
	req.Header.Set("X-Request-ID", newRequestID())
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("Backend public object lookup failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrObjectNotFound
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("Backend public object lookup returned HTTP %d", resp.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, (1<<20)+1))
	if err != nil {
		return nil, fmt.Errorf("read Backend public object response: %w", err)
	}
	if len(raw) > 1<<20 {
		return nil, errors.New("Backend public object response exceeded 1 MiB")
	}
	var envelope struct {
		OK   bool
		Data json.RawMessage
	}
	if err := json.Unmarshal(raw, &envelope); err != nil || !envelope.OK || len(envelope.Data) == 0 || !json.Valid(envelope.Data) {
		return nil, errors.New("Backend public object response has an invalid envelope")
	}
	return envelope.Data, nil
}

func (c *Client) AcceptRemoteFollow(ctx context.Context, localUserID, remoteActorURI, activityID string) error {
	return c.changeRemoteFollow(ctx, "/internal/v1/federation/remote-follows", localUserID, remoteActorURI, activityID, "", true)
}

func (c *Client) RemoveRemoteFollow(ctx context.Context, localUserID, remoteActorURI, activityID, followActivityID string) error {
	return c.changeRemoteFollow(ctx, "/internal/v1/federation/remote-follows/undo", localUserID, remoteActorURI, activityID, followActivityID, false)
}

func (c *Client) changeRemoteFollow(ctx context.Context, path, localUserID, remoteActorURI, activityID, followActivityID string, accepting bool) error {
	localUserID = strings.TrimSpace(localUserID)
	remoteActorURI = strings.TrimSpace(remoteActorURI)
	activityID = strings.TrimSpace(activityID)
	followActivityID = strings.TrimSpace(followActivityID)
	if localUserID == "" || len(localUserID) > 36 || !isHTTPSActorURI(remoteActorURI) || !isHTTPSActorURI(activityID) || (!accepting && !isHTTPSActorURI(followActivityID)) {
		return errors.New("remote follow identity is invalid")
	}
	requestBody := map[string]string{
		"local_user_id": localUserID,
		"remote_actor_uri": remoteActorURI,
		"activity_id": activityID,
	}
	if followActivityID != "" {
		requestBody["follow_activity_id"] = followActivityID
	}
	body, err := json.Marshal(requestBody)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, strings.NewReader(string(body)))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Testagram-API-Version", "1")
	req.Header.Set("X-Request-ID", newRequestID())
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("Backend remote follow update failed: %w", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, (64<<10)+1))
	if err != nil {
		return fmt.Errorf("read Backend remote follow response: %w", err)
	}
	if len(raw) > 64<<10 {
		return errors.New("Backend remote follow response exceeded 64 KiB")
	}
	if resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusNotFound {
		return ErrRemoteFollowNotAuthorized
	}
	if resp.StatusCode == http.StatusConflict {
		return ErrRemoteFollowConflict
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("Backend remote follow update returned HTTP %d", resp.StatusCode)
	}
	var envelope struct {
		OK bool
		Data struct {
			Status string
			Removed bool
		}
	}
	if err := json.Unmarshal(raw, &envelope); err != nil || !envelope.OK {
		return errors.New("Backend remote follow response has an invalid envelope")
	}
	if accepting && envelope.Data.Status != "accepted" {
		return errors.New("Backend did not authorize the remote follow")
	}
	return nil
}

func isHTTPSActorURI(raw string) bool {
	u, err := url.Parse(strings.TrimSpace(raw))
	return err == nil && u.Scheme == "https" && u.Host != "" && u.User == nil && u.RawQuery == "" && u.Fragment == ""
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
	if err != nil {
		return ActorProfile{}, fmt.Errorf("read Backend actor lookup response: %w", err)
	}
	if len(raw) > 64<<10 {
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

func (c *Client) ListOutbox(ctx context.Context, after string, limit int) (OutboxPage, error) {
	if limit < 1 || limit > 99 {
		return OutboxPage{}, errors.New("outbox limit must be between 1 and 99")
	}
	if len(after) > 128 || strings.ContainsAny(after, "\r\n") {
		return OutboxPage{}, errors.New("outbox cursor is invalid")
	}
	if after != "" {
		decoded, err := base64.RawURLEncoding.DecodeString(after)
		if err != nil || len(decoded) == 0 || len(decoded) > 36 {
			return OutboxPage{}, errors.New("outbox cursor is invalid")
		}
	}
	query := url.Values{}
	query.Set("limit", fmt.Sprint(limit))
	if after != "" {
		query.Set("after", after)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/internal/v1/federation/outbox?"+query.Encode(), nil)
	if err != nil {
		return OutboxPage{}, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-Testagram-API-Version", "1")
	req.Header.Set("X-Request-ID", newRequestID())
	resp, err := c.http.Do(req)
	if err != nil {
		return OutboxPage{}, fmt.Errorf("Backend outbox request failed: %w", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, (1<<20)+1))
	if err != nil {
		return OutboxPage{}, fmt.Errorf("read Backend outbox response: %w", err)
	}
	if len(raw) > 1<<20 {
		return OutboxPage{}, errors.New("Backend outbox response exceeded 1 MiB")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return OutboxPage{}, fmt.Errorf("Backend outbox returned HTTP %d", resp.StatusCode)
	}
	var envelope struct {
		OK   bool       `json:"ok"`
		Data OutboxPage `json:"data"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil || !envelope.OK {
		return OutboxPage{}, errors.New("Backend outbox returned an invalid response envelope")
	}
	if len(envelope.Data.Events) > limit || (envelope.Data.HasMore && envelope.Data.NextCursor == "") {
		return OutboxPage{}, errors.New("Backend outbox returned invalid pagination metadata")
	}
	if envelope.Data.NextCursor != "" {
		decoded, err := base64.RawURLEncoding.DecodeString(envelope.Data.NextCursor)
		if err != nil || len(decoded) == 0 || len(decoded) > 36 {
			return OutboxPage{}, errors.New("Backend outbox returned an invalid next cursor")
		}
	}
	seen := make(map[string]struct{}, len(envelope.Data.Events))
	for _, event := range envelope.Data.Events {
		if _, duplicate := seen[event.ID]; duplicate {
			return OutboxPage{}, errors.New("Backend outbox returned duplicate event IDs")
		}
		seen[event.ID] = struct{}{}
		if _, err := time.Parse(time.RFC3339, event.CreatedAt); err != nil {
			return OutboxPage{}, errors.New("Backend outbox returned an invalid event timestamp")
		}
		if event.ID == "" || event.EventType != "testagram.post.published" || event.ActorID == "" || event.ObjectID == "" || len(event.Payload) == 0 {
			return OutboxPage{}, errors.New("Backend outbox returned an invalid or ineligible event")
		}
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

func isPrivateBackendHost(host string) bool {
	host = strings.Trim(strings.ToLower(host), "[]")
	if host == "api" || host == "backend" || host == "localhost" || host == "127.0.0.1" || strings.HasSuffix(host, ".local") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && (ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast())
}

func newRequestID() string {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err == nil {
		return hex.EncodeToString(raw[:])
	}
	return fmt.Sprintf("federation-%d", time.Now().UnixNano())
}

var ErrActorNotFound = errors.New("local actor not found")
var ErrObjectNotFound = errors.New("public ActivityPub object not found")
var ErrRemoteFollowNotAuthorized = errors.New("Backend did not authorize the remote follow")
var ErrRemoteFollowConflict = errors.New("Backend rejected conflicting remote follow activity identity")
