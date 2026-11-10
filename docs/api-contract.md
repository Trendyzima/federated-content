# Testagram Federation API contract

Federated Content owns ActivityPub protocol state and public protocol endpoints. It is not the client-facing product API. XClone must never call this service directly.

## Public protocol surface
- `GET /.well-known/webfinger`
- `GET /users/{username}` (ActivityPub representation negotiated by Accept)
- `GET /users/{username}/outbox`
- `POST /users/{username}/inbox` verifies HTTP Signatures, rechecks local federation consent, persists remote actor keys, and durably deduplicates accepted activities before returning `202`.

## Private Backend ingress
`POST /internal/v1/activities` accepts only Backend-authorized activities from a private network with a dedicated high-entropy bearer token. Backend uses `Idempotency-Key` equal to the stable activity ID. A successful response means durable acceptance by Federation storage, not delivery to every remote peer.

Required headers: `Authorization`, `Content-Type: application/json`, `X-Testagram-API-Version: 1`, `X-Request-ID`, and `Idempotency-Key`.

## Backend dependencies
Federation resolves local actors and reads public/non-sensitive post events through Backend private APIs. It never reads Appwrite/Neon directly, creates canonical local users/posts, or overrides Backend blocks, visibility, suspension, deletion or retention decisions.

Inbox acceptance is asynchronous: `202` confirms durable queue acceptance only. It does not claim that the activity has already been processed or delivered. The worker applies bounded retries and dead-letter handling.

## Delivery semantics
- Backend publishes only committed, federation-eligible events through its transactional outbox.
- Federation deduplicates by stable event/activity ID and acknowledges only after durable acceptance.
- Remote delivery is asynchronous, bounded and retried with backoff; permanent policy/authorization failures are not retried forever.
- Direct/private content is never sent to public federation delivery.
- Keep inbox/outbox, actor keys and remote delivery state here; local account authority stays in Backend.

## Activation gate
The signed inbox, durable inbox persistence, asynchronous processing, and delivery retry/dead-letter mechanisms are implemented. Keep public federation activation gated on staging interoperability, replay/deduplication verification, key-rotation drills, backend authorization and operational monitoring. Health/readiness alone is not permission to enable public federation.
