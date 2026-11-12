package store

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"database/sql"
	_ "embed"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

//go:embed schema.sql
var schema string

const maxDeliveryAttempts = 12

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
	if err != nil || n == 1 {
		return n == 1, err
	}
	var storedActor, storedRecipient, storedType string
	var storedPayload []byte
	if err := s.db.QueryRowContext(ctx, `SELECT actor_uri,recipient_username,activity_type,payload FROM inbox_activities WHERE activity_id=$1`, activityID).Scan(&storedActor, &storedRecipient, &storedType, &storedPayload); err != nil {
		return false, fmt.Errorf("verify duplicate inbox activity: %w", err)
	}
	if storedActor != actorURI || storedRecipient != recipientUsername || storedType != activityType || !jsonEquivalent(storedPayload, payload) {
		return false, errors.New("ActivityPub activity ID was replayed with a different actor, recipient, type or payload")
	}
	return false, nil
}

func jsonEquivalent(left, right []byte) bool {
	var leftValue, rightValue any
	if json.Unmarshal(left, &leftValue) != nil || json.Unmarshal(right, &rightValue) != nil {
		return false
	}
	leftJSON, leftErr := json.Marshal(leftValue)
	rightJSON, rightErr := json.Marshal(rightValue)
	return leftErr == nil && rightErr == nil && string(leftJSON) == string(rightJSON)
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
	result, err := tx.ExecContext(ctx, `INSERT INTO outbox_activities (activity_id,actor_uri,payload)
		VALUES ($1,$2,$3::jsonb)
		ON CONFLICT (activity_id) DO UPDATE SET activity_id=EXCLUDED.activity_id
		WHERE outbox_activities.actor_uri=EXCLUDED.actor_uri AND outbox_activities.payload=EXCLUDED.payload`,
		activityID, actorURI, string(payload))
	if err != nil {
		return fmt.Errorf("persist outbox activity: %w", err)
	}
	if affected, rowsErr := result.RowsAffected(); rowsErr != nil || affected != 1 {
		return errors.New("activity ID already exists with different actor or payload")
	}
	for _, inbox := range inboxes {
		inbox = strings.TrimSpace(inbox)
		if inbox == "" {
			continue
		}
		if err := validateStoredInboxURL(inbox); err != nil {
			return err
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
		available_at=CASE WHEN $3 OR attempts+1 >= $4 THEN available_at ELSE NOW()+(LEAST(900.0,(1 << LEAST(attempts,10))::double precision*(0.5+random())) * INTERVAL '1 second') END,
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

// SyncCursor reads a durable integration cursor. A missing row means the
// consumer has not yet processed any events.
var syncStateKeyPattern = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,63}$`)

// SyncCursor reads a durable integration cursor. A missing row means the consumer has not yet processed any events.
func (s *Store) SyncCursor(ctx context.Context, key string) (string, error) {
	if !syncStateKeyPattern.MatchString(key) {
		return "", errors.New("invalid federation sync-state key")
	}
	var cursor string
	err := s.db.QueryRowContext(ctx, `SELECT cursor_value FROM federation_sync_state WHERE state_key=$1`, key).Scan(&cursor)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("read federation sync cursor: %w", err)
	}
	return cursor, nil
}

// SaveSyncCursor advances only after the corresponding activity or dead letter is durable.
func (s *Store) SaveSyncCursor(ctx context.Context, key, cursor string) error {
	if !syncStateKeyPattern.MatchString(key) || len(cursor) > 128 || strings.ContainsAny(cursor, "\r\n") {
		return errors.New("invalid federation sync cursor")
	}
	if cursor != "" {
		decoded, err := base64.RawURLEncoding.DecodeString(cursor)
		if err != nil || len(decoded) == 0 || len(decoded) > 36 {
			return errors.New("invalid federation sync cursor")
		}
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO federation_sync_state (state_key,cursor_value,updated_at) VALUES ($1,$2,NOW()) ON CONFLICT (state_key) DO UPDATE SET cursor_value=EXCLUDED.cursor_value,updated_at=NOW()`, key, cursor)
	if err != nil {
		return fmt.Errorf("save federation sync cursor: %w", err)
	}
	return nil
}

func (s *Store) RecordOutboxDeadLetter(ctx context.Context, eventID, eventType, reason string, payload []byte) error {
	if eventID == "" || len(eventID) > 36 || eventType == "" || len(eventType) > 96 || !json.Valid(payload) || (reason != "invalid_event" && reason != "actor_no_longer_eligible") {
		return errors.New("invalid Backend outbox dead-letter record")
	}
	var accepted string
	err := s.db.QueryRowContext(ctx, `INSERT INTO federation_outbox_dead_letters (event_id,event_type,reason_code,payload) VALUES ($1,$2,$3,$4::jsonb) ON CONFLICT (event_id) DO UPDATE SET event_id=EXCLUDED.event_id WHERE federation_outbox_dead_letters.event_type=EXCLUDED.event_type AND federation_outbox_dead_letters.reason_code=EXCLUDED.reason_code AND federation_outbox_dead_letters.payload=EXCLUDED.payload RETURNING event_id`, eventID, eventType, reason, string(payload)).Scan(&accepted)
	if err != nil {
		return fmt.Errorf("persist Backend outbox dead letter or detect conflicting replay: %w", err)
	}
	return nil
}
