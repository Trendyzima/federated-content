# Testagram six-repository runtime integration contract

Status: implementation and staging contract; not production cutover authorization.
Reviewed: 2026-11-11.

This is the shared communication contract for the six retained Testagram repositories. Each repository owns its domain; no repository may bypass the Backend's product authorization or become a second source of truth.

## Service ownership

| Repository | Runtime responsibility | Allowed communication |
|---|---|---|
| `testagram-flutter-client-` | Native Flutter UI, navigation, local presentation state, Appwrite session acquisition | Public authenticated Backend API only |
| `Testagram-backend-` | Canonical product authorization, account identity bridge, social API, PostgreSQL product state, media lifecycle, orchestration and outboxes | Appwrite Auth; private Algorithm, Data Centre and Federation contracts; public CDN URLs |
| `Testagram-data-centre-` | Private infrastructure API, storage nodes, video processing, notifications, VPS/runtime deployment, backup and operations | Private Backend/platform network; storage providers and node agents |
| `cdn-` | Media/HLS/DASH delivery, cache, origin/shield routing, range handling and playback-token validation | Public media requests and configured private/public origins; no product authorization |
| `federated-content` | ActivityPub actors, keys, inbox/outbox, signature verification, remote delivery and federation state | Private Backend actor/outbox APIs; remote ActivityPub peers |
| `the-algorithm` | Stateless feed ordering over an already-authorized bounded candidate set | Private Backend ranking API only |

## Canonical request paths

### Flutter -> Backend
- Flutter obtains an Appwrite session JWT and sends it as `Authorization: Bearer <JWT>`.
- Backend validates the session and derives the canonical user identity server-side.
- Flutter must not call Algorithm, Federation, Data Centre platform APIs, PostgreSQL, or storage-provider credentials directly.
- The current client API base is configured through `TESTAGRAM_API_URL`; no private service URL or secret belongs in a Flutter build.

### Backend -> Algorithm
- Internal endpoint: `POST /v1/feed/rank`; readiness: `GET /readyz`.
- Backend sends `Authorization: Bearer <ALGORITHM_INTERNAL_TOKEN>`, `X-Testagram-API-Version: 1`, and `X-Request-ID`.
- Candidate request is bounded to 200 candidates; requested result limit is at most 50.
- Algorithm may return only IDs from the submitted candidate set. Backend validates the response and rechecks current visibility, blocks, follows and moderation before hydrating canonical posts.
- Deployment contract for the current `cmd/api` runtime: `ALGORITHM_URL=http://algorithm:8092` and `ALGORITHM_INTERNAL_TOKEN` must match Algorithm's `INTERNAL_TOKEN`. The `cmd/fresh-api` runtime uses `ALGORITHM_ENDPOINT` instead; do not mix the two runtime configurations.

### Backend <-> Federated Content
- Backend actor-resolution endpoints: `GET /internal/v1/federation/actors/{handle}`, `GET /internal/v1/federation/actors/by-id/{user_id}`, and readiness `GET /internal/v1/federation/readyz`.
- Backend publishes authorized activities to Federation at `POST /internal/v1/activities`.
- Publish request fields are `activity_id`, `actor_user_id`, and `activity`; duplicate activity IDs must be idempotent.
- Backend post transaction and outbox event must commit atomically. The dispatcher retries bounded batches and acknowledges only after durable Federation acceptance.
- Use separate credentials in each direction: `FEDERATION_INTERNAL_TOKEN` authenticates Backend -> Federation; `FEDERATION_BACKEND_INTERNAL_TOKEN` authenticates Federation -> Backend. Never reuse Appwrite user JWTs as service credentials.
- Federation delivery, inbox and outbox polling remain disabled until signature, consent, replay, durability, idempotency and interoperability gates pass.

### Backend -> Data Centre
- Data Centre's platform listener is private (default port 8095). Current implemented routes include health/readiness/metrics, feature-flag configuration, event intake and audit intake.
- The Data Centre platform service is not a product API and does not own user identity, post visibility, media ownership, quota or retention decisions.
- The current Backend mainline does not yet have a verified typed Data Centre platform client. Do not claim Backend <-> Data Centre cache, queue, rate-limit or media-placement integration is active until the client, matching routes, authentication and contract tests are implemented together.
- Data Centre media placement/replica evidence is not durable until byte count and SHA-256 are independently read back and verified.

### Backend -> CDN -> object origin
- Backend owns upload intent, authorization, visibility, quota, retention and deletion. It emits public URLs using `MEDIA_PUBLIC_BASE_URL`.
- The Go CDN supports the stable `/v1/` media path and HLS/DASH paths. Configure `MEDIA_PUBLIC_BASE_URL=https://media.testagram.site/v1` only when DNS, TLS, CDN routing and the CDN origin are actually configured for that hostname and path.
- CDN is a delivery plane, not an authorization authority. Private objects require a key-bound, expiring playback token or an authenticated origin path; never mark private objects public or cache private responses as public.
- HLS playlists are short-lived; segments need cache lifetimes consistent with the playlist window. Range requests and content type must be preserved.

## Shared reliability and security rules
- JSON over HTTP(S); use HTTPS for public endpoints and private-network HTTP only for explicitly private service hostnames.
- Use distinct high-entropy service credentials per direction and audience; keep them in server-side secret stores only.
- Propagate a request ID across each service hop. Never log JWTs, service tokens, signed URLs, private content, or raw authorization headers.
- Enforce strict JSON schemas, body and batch limits, request deadlines, bounded concurrency and bounded retries.
- Retry only operations that are idempotent or protected by an idempotency key. A 2xx acknowledgement means durable acceptance only where the receiving contract explicitly guarantees it.
- Preserve privacy, blocks, moderation and deletion tombstones across every projection and delivery path.
- Readiness must include required dependencies, not merely process liveness. Optional ranking may fall back only according to explicit Backend policy.
- Use additive/versioned changes. Update both producer and consumer plus shared fixtures and tests before emitting a new contract.

## Known integration gates (not to be represented as completed)
- Backend -> Data Centre typed platform client and matching route coverage are not verified as implemented.
- TV catalogue API `GET /api/tv/catalog` is not registered in the Backend mainline; the health-monitor workflow cannot run successfully until a catalogue source and endpoint are implemented.
- CDN production hostname/origin wiring and private-media end-to-end verification must be checked in the deployed environment; repository configuration alone is not proof.
- Data Centre's documented API route inventory must match the actual platform server handlers before clients are generated.
- Full cross-repository contract tests and deployment smoke tests must pass against the same immutable image versions before production promotion.

## Release acceptance
1. Verify each repository's own CI on the proposed integration branch.
2. Run producer/consumer contract tests for Flutter/Backend, Backend/Algorithm, Backend/Federation, Backend/Data Centre and Backend/CDN media URL behavior.
3. Verify private-network DNS, ports, TLS, secrets, readiness and timeouts in staging.
4. Exercise authorization failures, malformed/oversized payloads, duplicate requests, upstream outage, retry and recovery.
5. Verify media upload -> completion -> CDN retrieval, including private-media denial and byte-range behavior.
6. Keep all changes in reviewable PRs. Merge and deploy only after the release owner explicitly approves the verified set.
