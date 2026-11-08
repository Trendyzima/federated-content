# Testagram Federation API contract

Federated Content owns ActivityPub protocol state and public protocol endpoints. It is not the client-facing product API. XClone must never call this service directly.

## Public protocol surface
- `GET /.well-known/webfinger`
- `GET /users/{username}` (ActivityPub representation negotiated by Accept)
- `GET /users/{username}/outbox`
- `POST /users/{username}/inbox` (must verify HTTP Signatures, replay protection and durable inbox acceptance before public activation)

## Private Backend ingress
`POST /internal/v1/activities` accepts only Backend-authorized activities from a private network with a dedicated high-entropy bearer token. Backend uses `Idempotency-Key` equal to the stable activity ID. A successful response means durable acceptance by Federation storage, not delivery to every remote peer.

Required headers: `Authorization`, `Content-Type: application/json`, `X-Testagram-API-Version: 1`, `X-Request-ID`, and `Idempotency-Key`.

## Backend dependencies
Federation resolves local actors and reads public/non-sensitive post events through Backend private APIs. It never reads Appwrite/Neon directly, creates canonical local users/posts, or overrides Backend blocks, visibility, suspension, deletion or retention decisions.

## Delivery semantics
- Backend publishes only committed, federation-eligible events through its transactional outbox.
- Federation deduplicates by stable event/activity ID and acknowledges only after durable acceptance.
- Remote delivery is asynchronous, bounded and retried with backoff; permanent policy/authorization failures are not retried forever.
- Direct/private content is never sent to public federation delivery.
- Keep inbox/outbox, actor keys and remote delivery state here; local account authority stays in Backend.

## Activation gate
Remain private/preview until signature verification, replay protection, durable inbox/outbox processing, delivery retry/dead-letter behavior, backend authorization, and staging interoperability tests pass. Health/readiness alone is not permission to enable public federation.
