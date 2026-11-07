package store

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"database/sql"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

//go:embed schema.sql
var schema string

const maxDeliveryAttempts = 12
const maxInboxAttempts = 12

// schemaMigrationLock serializes embedded schema application across processes.
// Multiple service replicas may start against an empty database simultaneously.
const schemaMigrationLock int64 = 0x5445535441475241

type Store struct {
	db *sql.DB
}

type Actor struct {
	UserID        string
	Username      string
	ActorURI      string
	PublicKeyID   string
	PublicKeyPEM  string
	PrivateKeyPEM string
	DisplayName   string
	Summary       string
}


type InboxActivity struct {
	ActivityID       string
	ActorURI         string
	RecipientUsername string
	ActivityType     string
	Payload          json.RawMessage
	Attempts         int
	LeaseToken       string
}

type Delivery struct {
	ID         int64
	ActivityID string
	ActorURI   string
	InboxURL   string
	Attempts   int
	LeaseToken string
	Payload    json.RawMessage
}

func Open(ctx context.Context, databaseURL string) (*Store, error) {
	databaseURL, err := validateDatabaseURL(databaseURL)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return nil, fmt.Errorf("open federation database: %w", err)
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(15 * time.Minute)
	db.SetConnMaxIdleTime(2 * time.Minute)
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("connect to federation database: %w", err)
	}
	if err := applySchema(ctx, db); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("migrate federation database: %w", err)
	}
	return &Store{db: db}, nil
}

func applySchema(ctx context.Context, db *sql.DB) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire federation migration connection: %w", err)
	}
	defer conn.Close()

	// Use a transaction-scoped advisory lock. Session-scoped advisory locks do
	// not work reliably through Neon/PgBouncer transaction pooling.
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin federation schema migration: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock($1)", schemaMigrationLock); err != nil {
		return fmt.Errorf("lock federation schema migration: %w", err)
	}
	if _, err := tx.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("execute federation schema: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit federation schema migration: %w", err)
	}
	return nil
}

func (s *Store) Close() error                   { return s.db.Close() }
func (s *Store) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }

func (s *Store) EnsureActor(ctx context.Context, userID, username, actorURI, displayName, summary string) (Actor, error) {
	userID, username, actorURI = strings.TrimSpace(userID), strings.TrimSpace(username), strings.TrimSpace(actorURI)
	if userID == "" || username == "" || actorURI == "" {
		return Actor{}, errors.New("actor identity fields are required")
	}
	existing, err := s.getActor(ctx, "local_user_id", userID)
	if err == nil {
		_, err = s.db.ExecContext(ctx, `UPDATE local_actors SET username=$2, actor_uri=$3, public_key_id=$4, display_name=$5, summary=$6, updated_at=NOW() WHERE local_user_id=$1`,
			userID, username, actorURI, actorURI+"#main-key", displayName, summary)
		if err != nil {
			return Actor{}, fmt.Errorf("update local actor: %w", err)
		}
		existing.Username, existing.ActorURI, existing.PublicKeyID = username, actorURI, actorURI+"#main-key"
		existing.DisplayName, existing.Summary = displayName, summary
		return existing, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Actor{}, err
	}

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return Actor{}, fmt.Errorf("generate actor signing key: %w", err)
	}
	privateDER := x509.MarshalPKCS1PrivateKey(key)
	publicDER, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		return Actor{}, fmt.Errorf("encode actor public key: %w", err)
	}
	actor := Actor{
		UserID: userID, Username: username, ActorURI: actorURI,
		PublicKeyID:   actorURI + "#main-key",
		PublicKeyPEM:  string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: publicDER})),
		PrivateKeyPEM: string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: privateDER})),
		DisplayName:   displayName, Summary: summary,
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO local_actors (local_user_id, username, actor_uri, public_key_id, public_key_pem, private_key_pem, display_name, summary)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (local_user_id) DO NOTHING`,
		actor.UserID, actor.Username, actor.ActorURI, actor.PublicKeyID, actor.PublicKeyPEM, actor.PrivateKeyPEM, actor.DisplayName, actor.Summary)
	if err != nil {
		return Actor{}, fmt.Errorf("persist local actor key: %w", err)
	}
	// Another request may have won the insert race. Always return the persisted key.
	return s.getActor(ctx, "local_user_id", userID)
}

func (s *Store) getActor(ctx context.Context, column, value string) (Actor, error) {
	if column != "local_user_id" && column != "username" && column != "actor_uri" {
		return Actor{}, errors.New("invalid actor lookup")
	}
	query := "SELECT local_user_id, username, actor_uri, public_key_id, public_key_pem, private_key_pem, display_name, summary FROM local_actors WHERE " + column + "=$1"
	var a Actor
	err := s.db.QueryRowContext(ctx, query, value).Scan(&a.UserID, &a.Username, &a.ActorURI, &a.PublicKeyID, &a.PublicKeyPEM, &a.PrivateKeyPEM, &a.DisplayName, &a.Summary)
	return a, err
}
func (s *Store) ActorByUserID(ctx context.Context, id string) (Actor, error) {
	return s.getActor(ctx, "local_user_id", id)
}
func (s *Store) ActorByUsername(ctx context.Context, username string) (Actor, error) {
	return s.getActor(ctx, "username", username)
}
func (s *Store) ActorByURI(ctx context.Context, uri string) (Actor, error) {
	return s.getActor(ctx, "actor_uri", uri)
}

func (s *Store) StoreRemoteActor(ctx context.Context, actorURI, keyID, keyPEM, inboxURL, sharedInboxURL string) error {
	if actorURI == "" || keyID == "" || keyPEM == "" || inboxURL == "" {
		return errors.New("remote actor signing identity and inbox are required")
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO remote_actors (actor_uri,public_key_id,public_key_pem,inbox_url,shared_inbox_url,fetched_at)
		VALUES ($1,$2,$3,$4,$5,NOW()) ON CONFLICT (actor_uri) DO UPDATE SET
		public_key_id=EXCLUDED.public_key_id,public_key_pem=EXCLUDED.public_key_pem,
		inbox_url=EXCLUDED.inbox_url,shared_inbox_url=EXCLUDED.shared_inbox_url,fetched_at=NOW()`,
		actorURI, keyID, keyPEM, inboxURL, sharedInboxURL)
	return err
}

func (s *Store) StoreInbox(ctx context.Context, activityID, actorURI, recipientUsername, activityType string, payload []byte) (bool, error) {
	if activityID == "" || actorURI == "" || recipientUsername == "" || activityType == "" || !json.Valid(payload) {
		return false, errors.New("invalid inbox activity")
	}
	result, err := s.db.ExecContext(ctx, `INSERT INTO inbox_activities (activity_id, actor_uri, recipient_username, activity_type, payload)
		VALUES ($1,$2,$3,$4,$5::jsonb) ON CONFLICT (activity_id) DO NOTHING`, activityID, actorURI, recipientUsername, activityType, string(payload))
	if err != nil {
		return false, fmt.Errorf("persist inbox activity: %w", err)
	}
	n, err := result.RowsAffected()
	return n == 1, err
}

func (s *Store) EnqueueOutbox(ctx context.Context, activityID, actorURI string, payload []byte, inboxes []string) error {
	if activityID == "" || actorURI == "" || !json.Valid(payload) {
		return errors.New("invalid outbox activity")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `INSERT INTO outbox_activities (activity_id,actor_uri,payload) VALUES ($1,$2,$3::jsonb) ON CONFLICT (activity_id) DO NOTHING`, activityID, actorURI, string(payload)); err != nil {
		return fmt.Errorf("persist outbox activity: %w", err)
	}
	for _, inbox := range inboxes {
		inbox = strings.TrimSpace(inbox)
		if inbox == "" {
			continue
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO delivery_jobs (activity_id,inbox_url) VALUES ($1,$2) ON CONFLICT (activity_id,inbox_url) DO NOTHING`, activityID, inbox); err != nil {
			return fmt.Errorf("enqueue remote delivery: %w", err)
		}
	}
	return tx.Commit()
}

func (s *Store) ListOutbox(ctx context.Context, actorURI string, limit int) ([]json.RawMessage, int, error) {
	if actorURI == "" || limit < 1 || limit > 100 {
		return nil, 0, errors.New("invalid outbox query")
	}
	var total int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM outbox_activities WHERE actor_uri=$1`, actorURI).Scan(&total); err != nil {
		return nil, 0, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT payload FROM outbox_activities WHERE actor_uri=$1 ORDER BY created_at DESC,activity_id DESC LIMIT $2`, actorURI, limit)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	items := make([]json.RawMessage, 0, limit)
	for rows.Next() {
		var payload []byte
		if err := rows.Scan(&payload); err != nil {
			return nil, 0, err
		}
		items = append(items, json.RawMessage(payload))
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return items, total, nil
}

func (s *Store) ClaimDeliveries(ctx context.Context, limit int, lease time.Duration) ([]Delivery, error) {
	if limit < 1 || limit > 100 || lease < 15*time.Second || lease > 5*time.Minute {
		return nil, errors.New("invalid delivery claim bounds")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT d.id,d.activity_id,o.actor_uri,d.inbox_url,d.attempts,o.payload
		FROM delivery_jobs d JOIN outbox_activities o ON o.activity_id=d.activity_id
		WHERE d.attempts < $1 AND ((d.state IN ('pending','retry') AND d.available_at<=NOW()) OR (d.state='delivering' AND d.lease_until<NOW()))
		ORDER BY d.available_at,d.id FOR UPDATE OF d SKIP LOCKED LIMIT $2`, maxDeliveryAttempts, limit)
	if err != nil {
		return nil, err
	}
	var candidates []Delivery
	for rows.Next() {
		var d Delivery
		if err := rows.Scan(&d.ID, &d.ActivityID, &d.ActorURI, &d.InboxURL, &d.Attempts, &d.Payload); err != nil {
			rows.Close()
			return nil, err
		}
		candidates = append(candidates, d)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	for i := range candidates {
		token := make([]byte, 24)
		if _, err := rand.Read(token); err != nil {
			return nil, err
		}
		candidates[i].LeaseToken = hex.EncodeToString(token)
		_, err = tx.ExecContext(ctx, `UPDATE delivery_jobs SET state='delivering',lease_token=$2,lease_until=NOW()+($3 * INTERVAL '1 second') WHERE id=$1`, candidates[i].ID, candidates[i].LeaseToken, int(lease.Seconds()))
		if err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return candidates, nil
}

func (s *Store) AckDelivery(ctx context.Context, id int64, token string) error {
	result, err := s.db.ExecContext(ctx, `UPDATE delivery_jobs SET state='delivered',delivered_at=NOW(),lease_token=NULL,lease_until=NULL,last_error='' WHERE id=$1 AND state='delivering' AND lease_token=$2`, id, token)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err == nil && n != 1 {
		return errors.New("delivery lease is no longer owned")
	}
	return err
}

func (s *Store) FailDelivery(ctx context.Context, id int64, token, reason string, permanent bool) error {
	reason = strings.TrimSpace(reason)
	if len(reason) > 1000 {
		reason = reason[:1000]
	}
	result, err := s.db.ExecContext(ctx, `UPDATE delivery_jobs SET
		attempts=attempts+1,
		state=CASE WHEN $3 OR attempts+1 >= $4 THEN 'dead_letter' ELSE 'retry' END,
		available_at=CASE WHEN $3 OR attempts+1 >= $4 THEN available_at ELSE NOW()+((1 << LEAST(attempts,10)) * INTERVAL '1 second') END,
		lease_token=NULL,lease_until=NULL,last_error=$5
		WHERE id=$1 AND state='delivering' AND lease_token=$2`, id, token, permanent, maxDeliveryAttempts, reason)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err == nil && n != 1 {
		return errors.New("delivery lease is no longer owned")
	}
	return err
}

func (s *Store) MarkInboxProcessed(ctx context.Context, activityID string, processingErr error) error {
	if processingErr == nil {
		_, err := s.db.ExecContext(ctx, `UPDATE inbox_activities SET state='processed',processed_at=NOW(),last_error='' WHERE activity_id=$1`, activityID)
		return err
	}
	reason := processingErr.Error()
	if len(reason) > 1000 {
		reason = reason[:1000]
	}
	_, err := s.db.ExecContext(ctx, `UPDATE inbox_activities SET state='rejected',processed_at=NOW(),last_error=$2 WHERE activity_id=$1`, activityID, reason)
	return err
}


func (s *Store) GetState(ctx context.Context, key string) (string, error) {
	key = strings.TrimSpace(key)
	if key == "" || len(key) > 128 {
		return "", errors.New("invalid Federation state key")
	}
	var value string
	err := s.db.QueryRowContext(ctx, `SELECT state_value FROM service_state WHERE state_key=$1`, key).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return value, err
}

func (s *Store) SetState(ctx context.Context, key, value string) error {
	key, value = strings.TrimSpace(key), strings.TrimSpace(value)
	if key == "" || len(key) > 128 || len(value) > 256 {
		return errors.New("invalid Federation state value")
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO service_state(state_key,state_value,updated_at)
		VALUES($1,$2,NOW()) ON CONFLICT(state_key) DO UPDATE
		SET state_value=EXCLUDED.state_value,updated_at=NOW()`, key, value)
	return err
}

func (s *Store) FollowerInboxes(ctx context.Context, localUserID string) ([]string, error) {
	localUserID = strings.TrimSpace(localUserID)
	if localUserID == "" || len(localUserID) > 128 {
		return nil, errors.New("invalid local follower owner")
	}
	rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT COALESCE(NULLIF(a.shared_inbox_url,''),a.inbox_url)
		FROM remote_followers f
		JOIN remote_actors a ON a.actor_uri=f.remote_actor_uri
		WHERE f.local_user_id=$1 AND a.inbox_url <> ''
		ORDER BY 1`, localUserID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	inboxes := make([]string, 0, 16)
	for rows.Next() {
		var inbox string
		if err := rows.Scan(&inbox); err != nil {
			return nil, err
		}
		inboxes = append(inboxes, inbox)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return inboxes, nil
}



func (s *Store) AddRemoteFollower(ctx context.Context, localUserID, remoteActorURI string) error {
	localUserID, remoteActorURI = strings.TrimSpace(localUserID), strings.TrimSpace(remoteActorURI)
	if localUserID == "" || remoteActorURI == "" {
		return errors.New("local user and remote actor are required")
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO remote_followers(local_user_id,remote_actor_uri)
		VALUES($1,$2) ON CONFLICT(local_user_id,remote_actor_uri) DO NOTHING`, localUserID, remoteActorURI)
	return err
}

func (s *Store) RemoveRemoteFollower(ctx context.Context, localUserID, remoteActorURI string) error {
	localUserID, remoteActorURI = strings.TrimSpace(localUserID), strings.TrimSpace(remoteActorURI)
	if localUserID == "" || remoteActorURI == "" {
		return errors.New("local user and remote actor are required")
	}
	_, err := s.db.ExecContext(ctx, `DELETE FROM remote_followers WHERE local_user_id=$1 AND remote_actor_uri=$2`, localUserID, remoteActorURI)
	return err
}

func (s *Store) RemoteActorInbox(ctx context.Context, actorURI string) (string, error) {
	actorURI = strings.TrimSpace(actorURI)
	if actorURI == "" {
		return "", errors.New("remote actor URI is required")
	}
	var inbox string
	err := s.db.QueryRowContext(ctx, `SELECT COALESCE(NULLIF(shared_inbox_url,''),inbox_url)
		FROM remote_actors WHERE actor_uri=$1 AND inbox_url <> ''`, actorURI).Scan(&inbox)
	return inbox, err
}

func (s *Store) CountRemoteFollowers(ctx context.Context, localUserID string) (int, error) {
	localUserID = strings.TrimSpace(localUserID)
	if localUserID == "" {
		return 0, errors.New("local user ID is required")
	}
	var count int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM remote_followers WHERE local_user_id=$1`, localUserID).Scan(&count)
	return count, err
}

func (s *Store) ListRemoteFollowers(ctx context.Context, localUserID, after string, limit int) ([]string, string, error) {
	localUserID, after = strings.TrimSpace(localUserID), strings.TrimSpace(after)
	if localUserID == "" || limit < 1 || limit > 100 || len(after) > 2048 {
		return nil, "", errors.New("invalid remote followers query")
	}
	rows, err := s.db.QueryContext(ctx, `SELECT remote_actor_uri FROM remote_followers
		WHERE local_user_id=$1 AND remote_actor_uri > $2
		ORDER BY remote_actor_uri LIMIT $3`, localUserID, after, limit+1)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	actors := make([]string, 0, limit)
	for rows.Next() {
		var actorURI string
		if err := rows.Scan(&actorURI); err != nil {
			return nil, "", err
		}
		actors = append(actors, actorURI)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	var next string
	if len(actors) > limit {
		actors = actors[:limit]
		next = actors[len(actors)-1]
	}
	return actors, next, nil
}

func (s *Store) GetOutboxObject(ctx context.Context, actorURI, objectURI string) (json.RawMessage, error) {
	actorURI, objectURI = strings.TrimSpace(actorURI), strings.TrimSpace(objectURI)
	if actorURI == "" || objectURI == "" {
		return nil, errors.New("actor and object URI are required")
	}
	var object []byte
	err := s.db.QueryRowContext(ctx, `SELECT payload->'object' FROM outbox_activities
		WHERE actor_uri=$1 AND payload->'object'->>'id'=$2`, actorURI, objectURI).Scan(&object)
	return json.RawMessage(object), err
}



func (s *Store) ClaimInbox(ctx context.Context, limit int, lease time.Duration) ([]InboxActivity, error) {
	if limit < 1 || limit > 100 || lease < 15*time.Second || lease > 5*time.Minute {
		return nil, errors.New("invalid inbox claim bounds")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT activity_id,actor_uri,recipient_username,activity_type,payload,attempts
		FROM inbox_activities
		WHERE attempts < $1 AND (
			(state IN ('accepted','retry') AND available_at<=NOW()) OR
			(state='processing' AND lease_until<NOW())
		)
		ORDER BY available_at,received_at,activity_id
		FOR UPDATE SKIP LOCKED LIMIT $2`, maxInboxAttempts, limit)
	if err != nil {
		return nil, err
	}
	var activities []InboxActivity
	for rows.Next() {
		var activity InboxActivity
		if err := rows.Scan(&activity.ActivityID, &activity.ActorURI, &activity.RecipientUsername, &activity.ActivityType, &activity.Payload, &activity.Attempts); err != nil {
			rows.Close()
			return nil, err
		}
		activities = append(activities, activity)
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
		_, err := tx.ExecContext(ctx, `UPDATE inbox_activities SET state='processing',attempts=attempts+1,
			lease_token=$2,lease_until=NOW()+($3 * INTERVAL '1 second')
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

func (s *Store) AckInbox(ctx context.Context, activityID, token string) error {
	result, err := s.db.ExecContext(ctx, `UPDATE inbox_activities SET state='processed',processed_at=NOW(),
		lease_token=NULL,lease_until=NULL,last_error=''
		WHERE activity_id=$1 AND state='processing' AND lease_token=$2`, activityID, token)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err == nil && n != 1 {
		return errors.New("inbox lease is no longer owned")
	}
	return err
}

func (s *Store) FailInbox(ctx context.Context, activityID, token, reason string, permanent bool) error {
	reason = strings.TrimSpace(reason)
	if len(reason) > 1000 {
		reason = reason[:1000]
	}
	result, err := s.db.ExecContext(ctx, `UPDATE inbox_activities SET
		state=CASE WHEN $3 THEN 'rejected' WHEN attempts >= $4 THEN 'dead_letter' ELSE 'retry' END,
		available_at=CASE WHEN $3 OR attempts >= $4 THEN available_at ELSE NOW()+((1 << LEAST(attempts,10)) * INTERVAL '1 second') END,
		lease_token=NULL,lease_until=NULL,last_error=$5
		WHERE activity_id=$1 AND state='processing' AND lease_token=$2`,
		activityID, token, permanent, maxInboxAttempts, reason)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err == nil && n != 1 {
		return errors.New("inbox lease is no longer owned")
	}
	return err
}

