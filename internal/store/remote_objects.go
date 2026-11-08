package store

import (
	"context"
	"database/sql"
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
}

func validRemoteHTTPSURI(raw string) bool {
	u, err := url.Parse(strings.TrimSpace(raw))
	return err == nil && u.Scheme == "https" && u.Host != "" && u.User == nil && u.Fragment == ""
}

func (s *Store) UpsertRemoteObject(ctx context.Context, object RemoteObject) error {
	object.ObjectURI = strings.TrimSpace(object.ObjectURI)
	object.ActorURI = strings.TrimSpace(object.ActorURI)
	object.ObjectType = strings.TrimSpace(object.ObjectType)
	object.Content = strings.TrimSpace(object.Content)
	if !validRemoteHTTPSURI(object.ObjectURI) || !validRemoteHTTPSURI(object.ActorURI) ||
		(object.ObjectType != "Note" && object.ObjectType != "Article") ||
		len(object.Content) == 0 || len(object.Content) > 20000 ||
		len(object.Payload) == 0 || len(object.Payload) > 1<<20 {
		return errors.New("invalid remote ActivityPub object")
	}
	if object.Visibility != "public" && object.Visibility != "unlisted" && object.Visibility != "followers" && object.Visibility != "direct" {
		return errors.New("invalid remote object visibility")
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO remote_objects
		(object_uri,actor_uri,object_type,content,published_at,visibility,payload,updated_at)
		VALUES($1,$2,$3,$4,$5,$6,$7::jsonb,NOW())
		ON CONFLICT(object_uri) DO UPDATE SET
			object_type=EXCLUDED.object_type,content=EXCLUDED.content,
			published_at=EXCLUDED.published_at,visibility=EXCLUDED.visibility,
			payload=EXCLUDED.payload,updated_at=NOW(),deleted_at=NULL
		WHERE remote_objects.actor_uri=EXCLUDED.actor_uri`,
		object.ObjectURI, object.ActorURI, object.ObjectType, object.Content, object.PublishedAt, object.Visibility, string(object.Payload))
	if err != nil { return err }
	return nil
}

func (s *Store) DeleteRemoteObject(ctx context.Context, objectURI, actorURI string) error {
	if !validRemoteHTTPSURI(objectURI) || !validRemoteHTTPSURI(actorURI) {
		return errors.New("invalid remote object deletion identity")
	}
	_, err := s.db.ExecContext(ctx, `UPDATE remote_objects SET deleted_at=COALESCE(deleted_at,NOW()),updated_at=NOW()
		WHERE object_uri=$1 AND actor_uri=$2`, strings.TrimSpace(objectURI), strings.TrimSpace(actorURI))
	return err
}

func (s *Store) AddRemoteLike(ctx context.Context, objectURI, actorURI string) error {
	if !validRemoteHTTPSURI(objectURI) || !validRemoteHTTPSURI(actorURI) { return errors.New("invalid remote Like identity") }
	_, err := s.db.ExecContext(ctx, `INSERT INTO remote_likes(object_uri,actor_uri) VALUES($1,$2) ON CONFLICT DO NOTHING`,
		strings.TrimSpace(objectURI), strings.TrimSpace(actorURI))
	return err
}
func (s *Store) RemoveRemoteLike(ctx context.Context, objectURI, actorURI string) error {
	if !validRemoteHTTPSURI(objectURI) || !validRemoteHTTPSURI(actorURI) { return errors.New("invalid remote Like identity") }
	_, err := s.db.ExecContext(ctx, `DELETE FROM remote_likes WHERE object_uri=$1 AND actor_uri=$2`,
		strings.TrimSpace(objectURI), strings.TrimSpace(actorURI))
	return err
}
func (s *Store) AddRemoteAnnounce(ctx context.Context, objectURI, actorURI string) error {
	if !validRemoteHTTPSURI(objectURI) || !validRemoteHTTPSURI(actorURI) { return errors.New("invalid remote Announce identity") }
	_, err := s.db.ExecContext(ctx, `INSERT INTO remote_announces(object_uri,actor_uri) VALUES($1,$2) ON CONFLICT DO NOTHING`,
		strings.TrimSpace(objectURI), strings.TrimSpace(actorURI))
	return err
}
func (s *Store) RemoveRemoteAnnounce(ctx context.Context, objectURI, actorURI string) error {
	if !validRemoteHTTPSURI(objectURI) || !validRemoteHTTPSURI(actorURI) { return errors.New("invalid remote Announce identity") }
	_, err := s.db.ExecContext(ctx, `DELETE FROM remote_announces WHERE object_uri=$1 AND actor_uri=$2`,
		strings.TrimSpace(objectURI), strings.TrimSpace(actorURI))
	return err
}

var _ = sql.ErrNoRows
