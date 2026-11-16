package store

import (
    "context"
    "database/sql"
    "encoding/base64"
    "errors"
    "fmt"
    "net/url"
    "strings"
    "time"
)

type RemoteObject struct {
    ObjectURI string
    ActorURI string
    ObjectType string
    Content string
    PublishedAt *time.Time
    Visibility string
    Payload []byte
    ActivityAt time.Time
}

func validRemoteObjectURI(raw string) bool {
    u, err := url.Parse(strings.TrimSpace(raw))
    return err == nil && u.Scheme == "https" && u.Host != "" && u.User == nil && u.RawQuery == "" && u.Fragment == ""
}

// ApplyRemoteObjectActivity atomically records the activity ID and projection.
// Duplicate deliveries are no-ops; older updates cannot overwrite newer state;
// tombstoned object IDs can never be resurrected by delayed Create/Update events.
func (s *Store) ApplyRemoteObjectActivity(ctx context.Context, activityID, activityType string, object RemoteObject) error {
    activityID = strings.TrimSpace(activityID)
    object.ObjectURI = strings.TrimSpace(object.ObjectURI)
    object.ActorURI = strings.TrimSpace(object.ActorURI)
    object.Content = strings.TrimSpace(object.Content)
    if !validRemoteObjectURI(activityID) || !validRemoteObjectURI(object.ObjectURI) ||
        !validRemoteObjectURI(object.ActorURI) || len(object.Content) == 0 || len(object.Content) > 20000 ||
        len(object.Payload) == 0 || len(object.Payload) > 1<<20 ||
        (object.ObjectType != "Note" && object.ObjectType != "Article") ||
        (object.Visibility != "public" && object.Visibility != "unlisted" && object.Visibility != "followers" && object.Visibility != "direct") ||
        (activityType != "Create" && activityType != "Update") {
        return errors.New("invalid remote object activity")
    }
    if object.ActivityAt.IsZero() { object.ActivityAt = time.Now().UTC() }
    tx, err := s.db.BeginTx(ctx, nil)
    if err != nil { return err }
    defer tx.Rollback()
    var existingActor string
    err = tx.QueryRowContext(ctx, `SELECT actor_uri FROM remote_objects WHERE object_uri=$1`, object.ObjectURI).Scan(&existingActor)
    if err == nil && existingActor != object.ActorURI { return errors.New("remote object URI is already owned by a different actor") }
    if err != nil && !errors.Is(err, sql.ErrNoRows) { return err }
    result, err := tx.ExecContext(ctx, `INSERT INTO remote_object_events(activity_id,object_uri,actor_uri,activity_type,activity_at)
        VALUES($1,$2,$3,$4,$5) ON CONFLICT(activity_id) DO NOTHING`,
        activityID, object.ObjectURI, object.ActorURI, activityType, object.ActivityAt)
    if err != nil { return err }
    inserted, err := result.RowsAffected()
    if err != nil { return err }
    if inserted == 0 { return tx.Commit() }
    var tombstoned bool
    if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM remote_object_tombstones WHERE object_uri=$1)`, object.ObjectURI).Scan(&tombstoned); err != nil { return err }
    if tombstoned { return tx.Commit() }
    _, err = tx.ExecContext(ctx, `INSERT INTO remote_objects
        (object_uri,actor_uri,object_type,content,published_at,visibility,payload,last_activity_at,source_activity_id,updated_at)
        VALUES($1,$2,$3,$4,$5,$6,$7::jsonb,$8,$9,NOW())
        ON CONFLICT(object_uri) DO UPDATE SET
            object_type=EXCLUDED.object_type,content=EXCLUDED.content,published_at=EXCLUDED.published_at,
            visibility=EXCLUDED.visibility,payload=EXCLUDED.payload,last_activity_at=EXCLUDED.last_activity_at,
            source_activity_id=EXCLUDED.source_activity_id,updated_at=NOW()
        WHERE remote_objects.actor_uri=EXCLUDED.actor_uri
          AND remote_objects.deleted_at IS NULL
          AND remote_objects.last_activity_at <= EXCLUDED.last_activity_at`,
        object.ObjectURI, object.ActorURI, object.ObjectType, object.Content, object.PublishedAt,
        object.Visibility, string(object.Payload), object.ActivityAt, activityID)
    if err != nil { return err }
    return tx.Commit()
}

// ApplyRemoteObjectDeletion records a durable tombstone, including when Delete
// arrives before Create. The actor binding prevents one server deleting another's object.
func (s *Store) ApplyRemoteObjectDeletion(ctx context.Context, activityID, objectURI, actorURI string, activityAt time.Time) error {
    activityID, objectURI, actorURI = strings.TrimSpace(activityID), strings.TrimSpace(objectURI), strings.TrimSpace(actorURI)
    if !validRemoteObjectURI(activityID) || !validRemoteObjectURI(objectURI) || !validRemoteObjectURI(actorURI) {
        return errors.New("invalid remote object deletion identity")
    }
    if activityAt.IsZero() { activityAt = time.Now().UTC() }
    tx, err := s.db.BeginTx(ctx, nil)
    if err != nil { return err }
    defer tx.Rollback()
    var existingActor string
    err = tx.QueryRowContext(ctx, `SELECT actor_uri FROM remote_object_tombstones WHERE object_uri=$1`, objectURI).Scan(&existingActor)
    if err == nil && existingActor != actorURI { return errors.New("remote object tombstone belongs to a different actor") }
    if err != nil && !errors.Is(err, sql.ErrNoRows) { return err }
    err = tx.QueryRowContext(ctx, `SELECT actor_uri FROM remote_objects WHERE object_uri=$1`, objectURI).Scan(&existingActor)
    if err == nil && existingActor != actorURI { return errors.New("remote object URI is owned by a different actor") }
    if err != nil && !errors.Is(err, sql.ErrNoRows) { return err }
    result, err := tx.ExecContext(ctx, `INSERT INTO remote_object_events(activity_id,object_uri,actor_uri,activity_type,activity_at)
        VALUES($1,$2,$3,'Delete',$4) ON CONFLICT(activity_id) DO NOTHING`, activityID, objectURI, actorURI, activityAt)
    if err != nil { return err }
    inserted, err := result.RowsAffected()
    if err != nil { return err }
    if inserted == 0 { return tx.Commit() }
    _, err = tx.ExecContext(ctx, `INSERT INTO remote_object_tombstones(object_uri,actor_uri,deleted_at,activity_id)
        VALUES($1,$2,$3,$4) ON CONFLICT(object_uri) DO UPDATE SET
          deleted_at=GREATEST(remote_object_tombstones.deleted_at,EXCLUDED.deleted_at)
        WHERE remote_object_tombstones.actor_uri=EXCLUDED.actor_uri`,
        objectURI, actorURI, activityAt, activityID)
    if err != nil { return err }
    _, err = tx.ExecContext(ctx, `UPDATE remote_objects SET deleted_at=COALESCE(deleted_at,$3),updated_at=NOW()
        WHERE object_uri=$1 AND actor_uri=$2`, objectURI, actorURI, activityAt)
    if err != nil { return err }
    return tx.Commit()
}

type RemoteObjectCandidate struct {
    ObjectURI string `json:"object_uri"`
    ActorURI string `json:"actor_uri"`
    ObjectType string `json:"object_type"`
    Content string `json:"content"`
    PublishedAt *time.Time `json:"published_at,omitempty"`
    Visibility string `json:"visibility"`
    CreatedAt time.Time `json:"created_at"`
}

type RemoteObjectPage struct {
    Items []RemoteObjectCandidate `json:"items"`
    NextCursor string `json:"next_cursor,omitempty"`
}

func encodeRemoteObjectCursor(at time.Time, objectURI string) string {
    return base64.RawURLEncoding.EncodeToString([]byte(at.UTC().Format(time.RFC3339Nano)+"\n"+objectURI))
}

func decodeRemoteObjectCursor(raw string) (time.Time, string, error) {
    if raw == "" { return time.Time{}, "", nil }
    if len(raw) > 2048 { return time.Time{}, "", errors.New("remote object cursor is too large") }
    decoded, err := base64.RawURLEncoding.DecodeString(raw)
    if err != nil { return time.Time{}, "", errors.New("remote object cursor is malformed") }
    parts := strings.SplitN(string(decoded), "\n", 2)
    if len(parts) != 2 || !validRemoteObjectURI(parts[1]) { return time.Time{}, "", errors.New("remote object cursor is invalid") }
    at, err := time.Parse(time.RFC3339Nano, parts[0])
    if err != nil { return time.Time{}, "", errors.New("remote object cursor timestamp is invalid") }
    return at.UTC(), parts[1], nil
}

// ListAuthorizedRemoteObjects only returns public/unlisted projections for the
// exact actor allowlist supplied by Backend. It does not perform viewer auth;
// Backend must compute this allowlist after applying its own social-graph and
// privacy policy. Followers-only and direct objects are never returned here.
func (s *Store) ListAuthorizedRemoteObjects(ctx context.Context, actorURIs []string, limit int, cursor string) (RemoteObjectPage, error) {
    if len(actorURIs) == 0 || len(actorURIs) > 100 { return RemoteObjectPage{}, errors.New("remote actor allowlist must contain 1-100 entries") }
    if limit < 1 || limit > 100 { return RemoteObjectPage{}, errors.New("remote object limit must be between 1 and 100") }
    seen := make(map[string]struct{}, len(actorURIs))
    args := make([]any, 0, len(actorURIs)+3)
    placeholders := make([]string, 0, len(actorURIs))
    for _, actorURI := range actorURIs {
        actorURI = strings.TrimSpace(actorURI)
        if !validRemoteObjectURI(actorURI) { return RemoteObjectPage{}, errors.New("remote actor allowlist contains an invalid HTTPS URI") }
        if _, ok := seen[actorURI]; ok { continue }
        seen[actorURI] = struct{}{}
        args = append(args, actorURI)
        placeholders = append(placeholders, fmt.Sprintf("$%d", len(args)))
    }
    if len(args) == 0 { return RemoteObjectPage{}, errors.New("remote actor allowlist is empty") }
    cursorAt, cursorURI, err := decodeRemoteObjectCursor(cursor)
    if err != nil { return RemoteObjectPage{}, err }
    query := `SELECT object_uri,actor_uri,object_type,content,published_at,visibility,created_at
        FROM remote_objects
        WHERE actor_uri IN (`+strings.Join(placeholders, ",")+`)
          AND visibility IN ('public','unlisted') AND deleted_at IS NULL`
    if cursor != "" {
        args = append(args, cursorAt, cursorURI)
        query += fmt.Sprintf(" AND (COALESCE(published_at,created_at),object_uri) < ($%d,$%d)", len(args)-1, len(args))
    }
    args = append(args, limit+1)
    query += fmt.Sprintf(" ORDER BY COALESCE(published_at,created_at) DESC,object_uri DESC LIMIT $%d", len(args))
    rows, err := s.db.QueryContext(ctx, query, args...)
    if err != nil { return RemoteObjectPage{}, err }
    defer rows.Close()
    items := make([]RemoteObjectCandidate, 0, limit+1)
    for rows.Next() {
        var item RemoteObjectCandidate
        if err := rows.Scan(&item.ObjectURI,&item.ActorURI,&item.ObjectType,&item.Content,&item.PublishedAt,&item.Visibility,&item.CreatedAt); err != nil {
            return RemoteObjectPage{}, err
        }
        items = append(items,item)
    }
    if err := rows.Err(); err != nil { return RemoteObjectPage{}, err }
    page := RemoteObjectPage{Items: items}
    if len(page.Items) > limit {
        last := page.Items[limit-1]
        sortAt := last.CreatedAt
        if last.PublishedAt != nil { sortAt = *last.PublishedAt }
        page.NextCursor = encodeRemoteObjectCursor(sortAt,last.ObjectURI)
        page.Items = page.Items[:limit]
    }
    return page,nil
}
