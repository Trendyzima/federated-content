# Federation service architecture

## Ownership boundaries

The Testagram Backend remains the source of truth for local user identity, profile data, local posts, blocks, and authorization decisions. This service owns protocol-specific concerns: ActivityPub actor documents and keys, remote actor/object cache, inbound activity receipts, outbound delivery attempts, and federation worker state.

Do not duplicate local user records or invent an independent authentication system here. The backend integration must use a versioned service-to-service contract and a durable event/outbox mechanism.

## First vertical slice

The initial service exposes health/readiness, a configured instance actor, and WebFinger for that actor. It intentionally does not claim arbitrary user handles because a canonical-user resolver has not yet been connected. It also refuses inbox POSTs until signature verification and durable processing exist.

## Planned components

- HTTP API: WebFinger, actor/object resolution, inbox and outbox.
- Identity adapter: resolves local users and checks canonical account state through the core backend.
- Key store: stores encrypted actor signing keys; private material never appears in source control or logs.
- Ingress pipeline: bounded body read, signature verification, actor/key binding, replay defense, authorization, deduplication, persistence.
- Delivery pipeline: durable outbox, per-host concurrency limits, retries with jitter, dead-letter queue, remote response classification.
- Remote fetcher: HTTPS-only allowlisted schemes, DNS/IP validation on every connection, redirect limits, response-size and timeout limits.
- Persistence: migrations for actors, remote actors, inbox receipts, delivery jobs, and cursor state.

## Operational rules

- Liveness must not depend on PostgreSQL or queue availability.
- Readiness must become dependency-aware once those dependencies are configured.
- No unbounded goroutines or fan-out loops.
- Do not hold database transactions open during remote HTTP requests.
- Persist an outbound delivery job in the same transaction as the event/outbox record.
- Deduplicate incoming activities by canonical activity URI and actor.
- Retry only transient failures; treat 4xx policy/authentication failures separately.
- Keep public and private activity visibility explicit in every event contract.
