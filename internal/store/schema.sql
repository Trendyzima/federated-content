CREATE TABLE IF NOT EXISTS local_actors (
    local_user_id TEXT PRIMARY KEY,
    username TEXT NOT NULL,
    actor_uri TEXT NOT NULL UNIQUE,
    public_key_id TEXT NOT NULL UNIQUE,
    public_key_pem TEXT NOT NULL,
    private_key_pem TEXT NOT NULL,
    display_name TEXT NOT NULL DEFAULT '',
    summary TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS local_actors_username_ci ON local_actors (LOWER(username));

CREATE TABLE IF NOT EXISTS remote_actors (
    actor_uri TEXT PRIMARY KEY,
    public_key_id TEXT NOT NULL,
    public_key_pem TEXT NOT NULL,
    inbox_url TEXT NOT NULL,
    shared_inbox_url TEXT NOT NULL DEFAULT '',
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS inbox_activities (
    activity_id TEXT PRIMARY KEY,
    actor_uri TEXT NOT NULL,
    recipient_username TEXT NOT NULL,
    activity_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    state TEXT NOT NULL DEFAULT 'accepted' CHECK (state IN ('accepted','processed','rejected')),
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMPTZ,
    last_error TEXT NOT NULL DEFAULT ''
);
ALTER TABLE inbox_activities ADD COLUMN IF NOT EXISTS attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0);
ALTER TABLE inbox_activities ADD COLUMN IF NOT EXISTS available_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
ALTER TABLE inbox_activities ADD COLUMN IF NOT EXISTS lease_token TEXT;
ALTER TABLE inbox_activities ADD COLUMN IF NOT EXISTS lease_until TIMESTAMPTZ;
ALTER TABLE inbox_activities DROP CONSTRAINT IF EXISTS inbox_activities_state_check;
ALTER TABLE inbox_activities ADD CONSTRAINT inbox_activities_state_check
    CHECK (state IN ('accepted','processing','retry','processed','rejected','dead_letter'));
DROP INDEX IF EXISTS inbox_activities_pending_idx;
CREATE INDEX IF NOT EXISTS inbox_activities_pending_idx
    ON inbox_activities (available_at,received_at,activity_id)
    WHERE state IN ('accepted','retry','processing');

CREATE TABLE IF NOT EXISTS outbox_activities (
    activity_id TEXT PRIMARY KEY,
    actor_uri TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS delivery_jobs (
    id BIGSERIAL PRIMARY KEY,
    activity_id TEXT NOT NULL REFERENCES outbox_activities(activity_id) ON DELETE CASCADE,
    inbox_url TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending' CHECK (state IN ('pending','delivering','retry','delivered','dead_letter')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    lease_token TEXT,
    lease_until TIMESTAMPTZ,
    last_error TEXT NOT NULL DEFAULT '',
    delivered_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(activity_id, inbox_url)
);
CREATE INDEX IF NOT EXISTS delivery_jobs_claim_idx ON delivery_jobs (available_at, id) WHERE state IN ('pending','retry','delivering');
CREATE INDEX IF NOT EXISTS delivery_jobs_dead_letter_idx ON delivery_jobs (created_at) WHERE state = 'dead_letter';

CREATE TABLE IF NOT EXISTS remote_followers (
    local_user_id TEXT NOT NULL,
    remote_actor_uri TEXT NOT NULL REFERENCES remote_actors(actor_uri) ON DELETE CASCADE,
    accepted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (local_user_id, remote_actor_uri)
);

-- Durable cursor for the Backend -> Federation outbox poller. The cursor is
-- advanced only after the corresponding activity is durably enqueued locally.
CREATE TABLE IF NOT EXISTS federation_sync_state (
    state_key TEXT PRIMARY KEY,
    cursor_value TEXT NOT NULL DEFAULT '',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Quarantine permanently invalid Backend events so one poison record cannot pin the cursor.
CREATE TABLE IF NOT EXISTS federation_outbox_dead_letters (
 event_id TEXT PRIMARY KEY,
 event_type TEXT NOT NULL,
 reason_code TEXT NOT NULL CHECK (reason_code IN ('invalid_event','actor_no_longer_eligible')),
 payload JSONB NOT NULL,
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


-- Durable remote object projections produced only by verified inbox workers.
CREATE TABLE IF NOT EXISTS remote_objects (
    object_uri TEXT PRIMARY KEY,
    actor_uri TEXT NOT NULL REFERENCES remote_actors(actor_uri) ON DELETE CASCADE,
    object_type TEXT NOT NULL CHECK (object_type IN ('Note','Article')),
    content TEXT NOT NULL CHECK (length(content) BETWEEN 1 AND 20000),
    published_at TIMESTAMPTZ,
    visibility TEXT NOT NULL CHECK (visibility IN ('public','unlisted','followers','direct')),
    payload JSONB NOT NULL CHECK (jsonb_typeof(payload)='object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS remote_objects_public_timeline_idx
    ON remote_objects(published_at DESC,object_uri DESC)
    WHERE deleted_at IS NULL AND visibility='public';
CREATE INDEX IF NOT EXISTS remote_objects_actor_timeline_idx
    ON remote_objects(actor_uri,published_at DESC,object_uri DESC)
    WHERE deleted_at IS NULL;

CREATE TABLE IF NOT EXISTS remote_likes (
    object_uri TEXT NOT NULL,
    actor_uri TEXT NOT NULL REFERENCES remote_actors(actor_uri) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY(object_uri,actor_uri)
);
CREATE INDEX IF NOT EXISTS remote_likes_actor_idx ON remote_likes(actor_uri,created_at DESC);

CREATE TABLE IF NOT EXISTS remote_announces (
    object_uri TEXT NOT NULL,
    actor_uri TEXT NOT NULL REFERENCES remote_actors(actor_uri) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY(object_uri,actor_uri)
);
CREATE INDEX IF NOT EXISTS remote_announces_actor_idx ON remote_announces(actor_uri,created_at DESC);
