# Testagram Federated Content

The ActivityPub protocol service for the five-repository Testagram platform.

## Five-repository ownership

| Repository | Owns | Must not own |
| --- | --- | --- |
| [XClone](https://github.com/Trendyzima/XClone-9b4dz1-9b4dz1-8936-20260809tr) | Web/mobile UI, client-side presentation and user interactions | Canonical identity, permissions, federation delivery or media durability |
| [Testagram Backend](https://github.com/Trendyzima/Testagram-backend-) | Appwrite identity verification and account mapping; canonical profiles, posts, follows, blocks, visibility, authorization, transactional events and API orchestration in Neon PostgreSQL | ActivityPub protocol execution or ranking policy |
| **Federated Content (this repository)** | WebFinger, ActivityPub actor documents, remote actor/key discovery, signed inbox ingress, protocol outbox, delivery state and federation workers | Canonical Testagram users/posts, account permissions or media ownership |
| [the-algorithm](https://github.com/Trendyzima/the-algorithm) | Candidate ranking, recommendation and discovery scoring | Canonical feed authorization, visibility or social graph |
| [Testagram Data Centre](https://github.com/Trendyzima/Testagram-data-centre-) | Deployment, private networking, PostgreSQL operations, B2/R2 media durability, storage nodes, backups and recovery | Product identity or social application policy |

The request path is:

`XClone → Testagram Backend → private service APIs/events → Federation / Algorithm / Data Centre`

Only XClone talks to users. Backend remains the authority for identity, post state, authorization, visibility, social graph and media ownership. The ranker returns ordering only; Backend must re-check access and hydrate canonical post data. Federation stores protocol state and must resolve local actors through Backend. Data Centre supplies infrastructure and durable media storage.

## Current implementation status

This branch implements the durable ActivityPub identity and signed-ingress foundation:

- WebFinger resolves local handles through the private Backend API. Only active, explicitly opted-in profiles are eligible.
- Per-user actor documents and RSA key pairs are persisted in Federation PostgreSQL; actor keys survive service restarts.
- Actor documents and persisted outbox collections are served with ActivityPub content negotiation.
- Inbound requests are checked using RSA-SHA256 HTTP Signatures, Date and Digest validation, actor/key ownership checks, bounded request bodies and safe remote-key resolution.
- Verified remote actor metadata and inbox activities are persisted idempotently before the service returns HTTP 202.
- PostgreSQL delivery records support atomic claims, leases, acknowledgements, bounded retries and dead-letter state.
- An opt-in outbound worker claims leased delivery jobs, signs HTTP POST requests with the local actor RSA key, uses a redirect-disabled public-IP-only HTTPS client, acknowledges successful/duplicate responses, retries transient failures and dead-letters permanent rejections. It remains disabled unless `FEDERATION_DELIVERY_WORKER_ENABLED=true`.
- Readiness requires both durable Federation storage and the private Backend dependency. A missing dependency fails closed with HTTP 503.

**This is not yet a complete or production-ready federation service.** The following work remains before public federation can be enabled:

1. Start and supervise the durable inbox processor; apply Follow/Undo and supported Create/Update/Delete/Like/Announce activities to Backend through authenticated, idempotent private APIs.
2. Poll Backend's transactional federation outbox with a durable cursor; recover safely after restarts and handle pagination.
3. Connect a producer to the outbound delivery queue and operationalize the signed delivery worker. The worker implementation is opt-in and remains disabled by default until Follow/Undo processing, follower fan-out and outbox event production are ready.
4. Build follower fan-out and recipient deduplication, with bounded concurrency and backpressure.
5. Complete remote post projection and local canonical visibility/authorization checks.
6. Add end-to-end tests across Backend, Federation and Data Centre, plus Mastodon/GoToSocial interoperability and load tests.
7. Finish public domain routing, TLS, observability, operational recovery and production rollout controls.

The database stores accepted inbox activities and delivery jobs. The signed delivery worker and Backend outbox poller are implemented but deliberately disabled by default. The poller consumes eligible public-post events, rechecks current Backend consent, durably creates ActivityPub activities and quarantines permanent poison events before advancing its cursor. The private Backend activity-ingress endpoint is an additional authenticated idempotent producer. The signed inbox now has leased claiming, crash recovery, exponential retry and dead-letter quarantine. Its first processor rechecks local federation consent, accepts Follow, queues a signed Accept through the durable delivery queue, and handles Undo. Create/Update/Delete/Like/Announce and other unsupported inbound activity types are explicitly dead-lettered rather than silently marked processed; their Backend-owned projections and block-policy checks remain a production gate. Do not enable either worker in production or public ActivityPub routing until inbox projection, follower fan-out limits, observability and end-to-end interoperability tests are complete.

## Service contract

- Public ActivityPub ingress and discovery terminate at this service, not at XClone or the public Backend API.
- Federation-to-Backend calls use `BACKEND_INTERNAL_URL` and `BACKEND_INTERNAL_TOKEN` on a private network. The token must be at least 32 characters and must match the Backend's Federation service token. Readiness calls the private `GET /internal/v1/federation/readyz` endpoint with that token; it deliberately does not depend on Algorithm's public home-feed readiness.
- Backend owns the local-user lookup and eligibility decision. Federation must not create or infer local accounts from usernames.
- Backend owns post visibility and permissions. Federation may persist remote protocol records but cannot grant local access.
- Activity delivery is asynchronous. Local publishing must not fail merely because a remote instance is unavailable.
- Media bytes remain under Data Centre's B2/R2 and replica/recovery lifecycle; ActivityPub payloads may reference only approved, durable media URLs.

## Run locally

Requirements: Go 1.27.3+ and PostgreSQL.

```sh
cp .env.example .env
go test -race ./...
go run ./cmd/federation-api
```

The HTTP service listens on `:8081` by default.

| Endpoint | Purpose |
| --- | --- |
| `GET /healthz` | Process liveness; does not prove dependencies are ready |
| `GET /readyz` | Requires PostgreSQL and the private Backend dependency; otherwise returns 503 |
| `GET /.well-known/webfinger?resource=acct:alice@testagram.site` | Resolve the service or an eligible local account |
| `GET /users/{username}` | ActivityPub actor document; requires an ActivityPub-compatible `Accept` header |
| `GET /users/{username}/outbox` | Persisted ActivityPub outbox collection |
| `POST /users/{username}/inbox` | Verify and durably accept a signed activity; returns 202 only after persistence |

For local development, configure `PUBLIC_BASE_URL`, `INSTANCE_DOMAIN`, `FEDERATION_DATABASE_URL`, `BACKEND_INTERNAL_URL` and `BACKEND_INTERNAL_TOKEN`. The outbound delivery worker is disabled by default; only set `FEDERATION_DELIVERY_WORKER_ENABLED=true` in an isolated environment after creating test delivery jobs. Do not put the private service token in a frontend, mobile bundle, public workflow log or browser request.

## Security and reliability requirements

- Require HTTPS for public federation endpoints and remote actor/key URLs.
- Verify the HTTP signature, signed request target, Date and Digest; ensure the signing key belongs to the claimed actor.
- Reject loopback, private, link-local, metadata-service and special-purpose IP ranges. Revalidate resolved addresses to mitigate DNS rebinding.
- Bound body sizes, timeouts, redirects, pagination, recursion and outbound concurrency.
- Persist idempotency and queue state before acknowledging work. Use leases so crashed workers can be recovered without duplicate side effects.
- Retry transient remote failures with bounded exponential backoff; permanently rejected or exhausted jobs must be visible in dead-letter state.
- Never log credentials, private keys, signature material or private activity payloads.
- Treat remote content as untrusted; sanitize it before display in XClone.
- Preserve Backend visibility, blocking and account eligibility decisions. Never turn an ActivityPub receipt into authorization to view a local post.

## Next milestones

1. Durable inbox processing and Backend activity-application contract.
2. Backend transactional outbox poller with persistent cursor and replay protection.
3. Connect the signed delivery worker to the Backend event outbox and remote follower fan-out; add dead-letter inspection/replay.
4. Remote post projection, pagination and canonical visibility enforcement.
5. Cross-repository end-to-end, interoperability, security and load tests.
6. Production routing, telemetry, incident response and controlled rollout.

## Related repositories

- [XClone frontend](https://github.com/Trendyzima/XClone-9b4dz1-9b4dz1-8936-20260809tr)
- [Testagram Backend](https://github.com/Trendyzima/Testagram-backend-)
- [the-algorithm](https://github.com/Trendyzima/the-algorithm)
- [Testagram Data Centre](https://github.com/Trendyzima/Testagram-data-centre-)
