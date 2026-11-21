# Testagram Federation boundary

The canonical Go Backend issues and verifies Testagram-native user sessions against PostgreSQL session/account state. Backend-owned PostgreSQL is authoritative for local accounts, credentials, profiles, posts, follows, blocks, visibility, permissions and media ownership. No external identity or document-database provider is part of the Federation trust path.

Federated Content is a protocol service, not an identity provider or product-data authority. It owns remote actor/key cache, signed ActivityPub ingress, protocol inbox/outbox records and delivery state in its own Federation PostgreSQL database. Local actors and eligibility must be resolved through the private Backend API; Federation must never create local accounts, infer account ownership from a handle, or grant local post access.

Federation remains disabled for public delivery by default. In the production Docker overlay, Federation-to-Backend calls use the canonical Backend alias `testagram-backend-api:8082` over the private, Backend-owned `testagram-platform` network. The legacy Data Centre `api:8081` container is not the federation authority and must not be started as a dependency of the Federation service. The public Backend listener does not serve Federation internal routes. The API's `/healthz` is public liveness only, while dependency-aware `/readyz` and Federation internal routes are private runtime checks. Do not expose them through public routing. A public 404 on `federation.testagram.site/healthz` is not evidence of a healthy deployment; it means the public route is not currently established. Do not expose the service merely to make a health URL return 200.

Before enabling public federation, verify durable inbox processing, supported Follow/Undo semantics, explicit handling/quarantine for unsupported activities, Backend transactional outbox cursor recovery, follower fan-out and recipient deduplication, signed delivery retries/dead letters, SSRF protections, observability, TLS/domain routing and end-to-end Mastodon/GoToSocial interoperability.

Outbound delivery remains disabled by default. Keep `FEDERATION_DELIVERY_WORKER_ENABLED=false` until the Backend event producer, inbox projection, follower fan-out limits, private authorization checks and operational recovery gates have passed. Do not enable public routing or delivery as part of a documentation or CI change.

No Supabase connection, legacy actor import or legacy user-ID mapping. Remote actors never gain local account privileges.
