# Bluesky / AT Protocol public-read bridge

This service remains Testagram's ActivityPub Federation service. This addition provides a separate, read-only adapter for public Bluesky records through the official AT Protocol public API.

## Routes

- `GET /v1/atproto/actors/{handle}` resolves a public handle to its DID and public profile.
- `GET /v1/atproto/actors/{handle}/feed?limit=20&cursor=...` returns the public author feed in a normalized Testagram response.

The adapter calls only the fixed official origin `https://public.api.bsky.app` and the XRPC methods:

- `com.atproto.identity.resolveHandle`
- `app.bsky.actor.getProfile`
- `app.bsky.feed.getAuthorFeed`

It does not fetch caller-supplied URLs, accept a caller-supplied PDS host, forward cookies, or attach user credentials. Handles are validated before requests; DIDs and AT URIs are validated before records are returned. Requests have bounded timeouts and response sizes, and redirects are disabled. Public profile responses are cached for 60 seconds and feed responses for 30 seconds.

## Scope and limitations

This is public read-only interoperability, not an AT Protocol Personal Data Server (PDS), not a Bluesky login integration, and not a write/cross-post feature. Testagram does not ask users for Bluesky passwords or app passwords. Posting on behalf of users would require the AT Protocol OAuth flow with DPoP-bound tokens, account consent, secure refresh-token storage, revocation and audit support; it must not be implemented by collecting passwords.

Mastodon compatibility remains based on ActivityPub actors, WebFinger, inbox/outbox, signatures, remote follows and server-to-server delivery. The AT Protocol adapter does not alter ActivityPub actor identity or federation readiness gates.

## Release gates

1. Run unit and HTTP handler tests.
2. Validate public API response compatibility against the official Bluesky API.
3. Add bounded per-client rate limiting and cache instrumentation before high-volume public exposure.
4. Keep the bridge read-only until a separate OAuth/DPoP security review is complete.
