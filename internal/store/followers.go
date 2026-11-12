package store

import (
	"context"
	"errors"
	"net/url"
	"strings"
)

// RemoteFollowerInboxes returns a bounded, deduplicated list of remote inboxes
// for a Backend-verified local account. Shared inboxes reduce fan-out without
// changing the authoritative follower relationships stored by Federation.
func (s *Store) RemoteFollowerInboxes(ctx context.Context, localUserID string, limit int) ([]string, error) {
	localUserID = strings.TrimSpace(localUserID)
	if localUserID == "" || len(localUserID) > 128 || limit < 1 || limit > 5000 {
		return nil, errors.New("invalid remote follower inbox query")
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT DISTINCT COALESCE(NULLIF(ra.shared_inbox_url, ''), ra.inbox_url)
		FROM remote_followers rf
		JOIN remote_actors ra ON ra.actor_uri=rf.remote_actor_uri
		WHERE rf.local_user_id=$1
		ORDER BY 1
		LIMIT $2`, localUserID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	inboxes := make([]string, 0, min(limit, 128))
	seen := make(map[string]struct{})
	for rows.Next() {
		var inbox string
		if err := rows.Scan(&inbox); err != nil {
			return nil, err
		}
		u, parseErr := url.Parse(strings.TrimSpace(inbox))
		if parseErr != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.Fragment != "" {
			// Invalid persisted remote endpoints are skipped, never delivered to.
			continue
		}
		value := u.String()
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		inboxes = append(inboxes, value)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return inboxes, nil
}

func validateStoredInboxURL(raw string) error {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.Fragment != "" {
		return errors.New("remote delivery inbox must be an HTTPS URL without credentials or fragment")
	}
	return nil
}

// RemoteFollowerInboxesPage returns one stable lexical page of deduplicated
// delivery endpoints. The cursor advances over raw destinations, including
// invalid persisted URLs, so a bad row cannot stall or truncate fan-out.
func (s *Store) RemoteFollowerInboxesPage(ctx context.Context, localUserID, after string, limit int) ([]string, string, bool, error) {
	localUserID = strings.TrimSpace(localUserID)
	if localUserID == "" || len(localUserID) > 128 || len(after) > 2048 || limit < 1 || limit > 1000 {
		return nil, "", false, errors.New("invalid remote follower inbox page query")
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT inbox_url FROM (
			SELECT DISTINCT COALESCE(NULLIF(ra.shared_inbox_url, ''), ra.inbox_url) AS inbox_url
			FROM remote_followers rf
			JOIN remote_actors ra ON ra.actor_uri=rf.remote_actor_uri
			WHERE rf.local_user_id=$1
		) destinations
		WHERE inbox_url > $2
		ORDER BY inbox_url
		LIMIT $3`, localUserID, after, limit+1)
	if err != nil {
		return nil, "", false, err
	}
	defer rows.Close()
	raw := make([]string, 0, limit+1)
	for rows.Next() {
		var inbox string
		if err := rows.Scan(&inbox); err != nil {
			return nil, "", false, err
		}
		raw = append(raw, inbox)
	}
	if err := rows.Err(); err != nil {
		return nil, "", false, err
	}
	hasMore := len(raw) > limit
	if hasMore {
		raw = raw[:limit]
	}
	if len(raw) == 0 {
		return nil, after, false, nil
	}
	next := raw[len(raw)-1]
	inboxes := make([]string, 0, len(raw))
	for _, value := range raw {
		u, parseErr := url.Parse(strings.TrimSpace(value))
		if parseErr != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.Fragment != "" {
			continue
		}
		inboxes = append(inboxes, u.String())
	}
	return inboxes, next, hasMore, nil
}

// RemoteFollowerCount returns the public remote-follower count for one
// Backend-verified local actor.
func (s *Store) RemoteFollowerCount(ctx context.Context, localUserID string) (int, error) {
	localUserID = strings.TrimSpace(localUserID)
	if localUserID == "" || len(localUserID) > 128 {
		return 0, errors.New("invalid remote follower count query")
	}
	var count int
	if err := s.db.QueryRowContext(ctx, "SELECT COUNT(DISTINCT remote_actor_uri) FROM remote_followers WHERE local_user_id=$1", localUserID).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

// RemoteFollowersPage returns stable, bounded pages of remote actor URIs for
// an actor's public ActivityPub followers collection.
func (s *Store) RemoteFollowersPage(ctx context.Context, localUserID, after string, limit int) ([]string, string, bool, error) {
	localUserID = strings.TrimSpace(localUserID)
	if localUserID == "" || len(localUserID) > 128 || len(after) > 2048 || limit < 1 || limit > 100 {
		return nil, "", false, errors.New("invalid remote followers page query")
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT remote_actor_uri
		FROM remote_followers
		WHERE local_user_id=$1 AND remote_actor_uri>$2
		ORDER BY remote_actor_uri
		LIMIT $3`, localUserID, after, limit+1)
	if err != nil {
		return nil, "", false, err
	}
	defer rows.Close()
	raw := make([]string, 0, limit+1)
	for rows.Next() {
		var actorURI string
		if err := rows.Scan(&actorURI); err != nil {
			return nil, "", false, err
		}
		raw = append(raw, actorURI)
	}
	if err := rows.Err(); err != nil {
		return nil, "", false, err
	}
	hasMore := len(raw) > limit
	if hasMore {
		raw = raw[:limit]
	}
	if len(raw) == 0 {
		return nil, after, false, nil
	}
	// Advance over every selected row, including malformed legacy rows, so a
	// corrupt persisted URI cannot pin the collection cursor indefinitely.
	next := raw[len(raw)-1]
	actors := make([]string, 0, len(raw))
	for _, actorURI := range raw {
		parsed, parseErr := url.Parse(strings.TrimSpace(actorURI))
		if parseErr != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" {
			continue
		}
		actors = append(actors, parsed.String())
	}
	return actors, next, hasMore, nil
}
