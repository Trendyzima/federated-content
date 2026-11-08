package store

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
)

const maxInboxAttempts = 12

type InboxActivity struct {
	ActivityID        string
	ActorURI          string
	RecipientUsername string
	ActivityType      string
	Payload           []byte
	Attempts          int
	LeaseToken        string
}

// ClaimInbox leases a bounded batch. Expired leases are reclaimable after a
// worker crash; SKIP LOCKED allows multiple Federation replicas to share work.
func (s *Store) ClaimInbox(ctx context.Context, limit int, lease time.Duration) ([]InboxActivity, error) {
	if limit < 1 || limit > 100 || lease < 15*time.Second || lease > 5*time.Minute {
		return nil, errors.New("invalid inbox claim bounds")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `UPDATE inbox_activities
		SET state='dead_letter',lease_token=NULL,lease_until=NULL,
		    last_error=CASE WHEN last_error='' THEN 'maximum processing attempts exceeded' ELSE last_error END,
		    processed_at=COALESCE(processed_at,NOW())
		WHERE attempts >= $1 AND (
		    state IN ('accepted','retry') OR (state='processing' AND lease_until < NOW())
		)`, maxInboxAttempts); err != nil {
		return nil, fmt.Errorf("expire exhausted inbox activities: %w", err)
	}

	rows, err := tx.QueryContext(ctx, `SELECT activity_id,actor_uri,recipient_username,activity_type,payload,attempts
		FROM inbox_activities
		WHERE attempts < $1 AND (
		    (state IN ('accepted','retry') AND available_at <= NOW()) OR
		    (state='processing' AND lease_until < NOW())
		)
		ORDER BY available_at,received_at,activity_id
		FOR UPDATE SKIP LOCKED LIMIT $2`, maxInboxAttempts, limit)
	if err != nil {
		return nil, err
	}
	activities := make([]InboxActivity, 0, limit)
	for rows.Next() {
		var item InboxActivity
		if err := rows.Scan(&item.ActivityID, &item.ActorURI, &item.RecipientUsername, &item.ActivityType, &item.Payload, &item.Attempts); err != nil {
			rows.Close()
			return nil, err
		}
		activities = append(activities, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()

	for i := range activities {
		token := make([]byte, 24)
		if _, err := rand.Read(token); err != nil {
			return nil, err
		}
		activities[i].LeaseToken = hex.EncodeToString(token)
		_, err := tx.ExecContext(ctx, `UPDATE inbox_activities
			SET state='processing',attempts=attempts+1,lease_token=$2,
			    lease_until=NOW()+($3 * INTERVAL '1 second')
			WHERE activity_id=$1`, activities[i].ActivityID, activities[i].LeaseToken, int(lease.Seconds()))
		if err != nil {
			return nil, err
		}
		activities[i].Attempts++
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return activities, nil
}

func (s *Store) AckInbox(ctx context.Context, activityID, leaseToken string) error {
	result, err := s.db.ExecContext(ctx, `UPDATE inbox_activities
		SET state='processed',processed_at=NOW(),lease_token=NULL,lease_until=NULL,last_error=''
		WHERE activity_id=$1 AND state='processing' AND lease_token=$2`, activityID, leaseToken)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err == nil && n != 1 {
		return errors.New("inbox lease is no longer owned")
	}
	return err
}

func (s *Store) FailInbox(ctx context.Context, activityID, leaseToken, reason string, permanent bool) error {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "inbox processing failed"
	}
	if len(reason) > 1000 {
		reason = reason[:1000]
	}
	result, err := s.db.ExecContext(ctx, `UPDATE inbox_activities SET
		state=CASE WHEN $3 OR attempts >= $4 THEN 'dead_letter' ELSE 'retry' END,
		available_at=CASE WHEN $3 OR attempts >= $4 THEN available_at ELSE NOW()+((1 << LEAST(attempts,10)) * INTERVAL '1 second') END,
		lease_token=NULL,lease_until=NULL,last_error=$5
		WHERE activity_id=$1 AND state='processing' AND lease_token=$2`,
		activityID, leaseToken, permanent, maxInboxAttempts, reason)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err == nil && n != 1 {
		return errors.New("inbox lease is no longer owned")
	}
	return err
}

func (s *Store) RemoteActorInbox(ctx context.Context, actorURI string) (string, error) {
	var inbox string
	err := s.db.QueryRowContext(ctx, `SELECT COALESCE(NULLIF(shared_inbox_url,''),inbox_url)
		FROM remote_actors WHERE actor_uri=$1`, strings.TrimSpace(actorURI)).Scan(&inbox)
	if errors.Is(err, sql.ErrNoRows) {
		return "", errors.New("remote actor inbox is unavailable")
	}
	if err != nil {
		return "", fmt.Errorf("read remote actor inbox: %w", err)
	}
	if err := validateStoredInboxURL(inbox); err != nil {
		return "", err
	}
	return inbox, nil
}

func (s *Store) AddRemoteFollower(ctx context.Context, localUserID, remoteActorURI string) error {
	localUserID, remoteActorURI = strings.TrimSpace(localUserID), strings.TrimSpace(remoteActorURI)
	if localUserID == "" || len(localUserID) > 128 || remoteActorURI == "" {
		return errors.New("invalid remote follower identity")
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO remote_followers (local_user_id,remote_actor_uri)
		VALUES ($1,$2) ON CONFLICT (local_user_id,remote_actor_uri) DO NOTHING`, localUserID, remoteActorURI)
	if err != nil {
		return fmt.Errorf("persist remote follower: %w", err)
	}
	return nil
}

func (s *Store) RemoveRemoteFollower(ctx context.Context, localUserID, remoteActorURI string) error {
	localUserID, remoteActorURI = strings.TrimSpace(localUserID), strings.TrimSpace(remoteActorURI)
	if localUserID == "" || len(localUserID) > 128 || remoteActorURI == "" {
		return errors.New("invalid remote follower identity")
	}
	_, err := s.db.ExecContext(ctx, `DELETE FROM remote_followers WHERE local_user_id=$1 AND remote_actor_uri=$2`, localUserID, remoteActorURI)
	if err != nil {
		return fmt.Errorf("remove remote follower: %w", err)
	}
	return nil
}
