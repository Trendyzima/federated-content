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
