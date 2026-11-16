package store

import (
    "context"
    "errors"
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
