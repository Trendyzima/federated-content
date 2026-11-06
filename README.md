# Testagram Federated Content

Independent ActivityPub federation service for the Testagram platform.

This repository is the federation component in Testagram's five-repository architecture:

- **XClone** is the only web/mobile frontend and presentation layer.
- **Testagram Backend** owns canonical accounts, authentication, local posts, social graph, authorization, and application APIs.
- **The Algorithm** ranks authorized feed candidates supplied by Testagram Backend; it is a private backend service, never a browser-facing API.
- **Testagram Data Centre** owns VPS/runtime orchestration, edge routing, node/host operations, and storage infrastructure; it does not become the canonical owner of social records.
- **Federated Content** owns ActivityPub protocol endpoints, remote actor discovery, inbox/outbox processing, and outbound delivery.

Federation is asynchronous. Local publishing must continue when remote delivery is unavailable.

## Current implementation status

The first foundation release provides a Go HTTP service with health/readiness endpoints, WebFinger discovery for one configured service actor, an ActivityPub actor representation, environment-based configuration, Docker build, Compose development setup, and CI.

This first slice does not yet resolve arbitrary Testagram user handles. That requires the canonical identity adapter from Testagram Backend. Inbox processing is intentionally disabled until HTTP Signature verification and durable idempotent processing are implemented. This is an integration foundation, not a production-ready federation server.

## Architecture

XClone (single frontend) -> Testagram Backend (canonical identity, local content and authorization) -> Federated Content (protocol boundary) -> ActivityPub peers.\n\nFor the home feed, XClone calls Testagram Backend; Backend sends only authorized candidate batches to The Algorithm and owns pagination and final authorization. Testagram Data Centre deploys and routes these services on the private runtime network.

The service must not create a second canonical Testagram account store. It may own protocol-specific actor keys, remote actor cache, delivery receipts, inbox deduplication, and federation job state.

## Run locally

Requirements: Go 1.27.3+ and Docker (optional).

    cp .env.example .env
    go test ./...
    go run ./cmd/federation-api

The service listens on :8081 by default.

- GET /healthz — process liveness.
- GET /readyz — configuration/dependency readiness.
- GET /.well-known/webfinger?resource=acct:testagram@testagram.site — configured service actor discovery.
- GET /users/testagram — ActivityPub actor (requires ActivityPub Accept header).
- POST /users/testagram/inbox — deliberately returns 501 until secure inbox processing is enabled.

## Security policy

- HTTPS is mandatory for public federation deployments.
- Incoming requests must be authenticated with interoperable HTTP Signatures before side effects.
- Validate that the signing key belongs to the claimed actor and that the actor is authorized for the activity.
- Enforce request-body, collection-page, recursion, and network time limits.
- Block loopback, private, link-local, and metadata-service IPs on remote fetches; revalidate DNS results to mitigate DNS rebinding.
- Use idempotency keys and durable delivery state; retries must be bounded with exponential backoff and jitter.
- Never trust remote HTML; sanitize before rendering in XClone.
- Preserve public, unlisted, followers-only, and direct visibility semantics.
- Never log authorization headers, private keys, signatures, or private content.

## Planned milestones

1. Protocol foundation: WebFinger, actor documents, content negotiation, key management.
2. Secure ingress: HTTP Signature verification, inbox validation, replay protection, deduplication.
3. Outbound delivery: signed requests, follower fanout, retries, dead-letter handling.
4. Remote discovery: actor resolution, safe fetching, caching and SSRF controls.
5. Backend integration: versioned internal API and durable event contract with Testagram Backend.
6. Interoperability: Mastodon and GoToSocial integration tests, federation conformance, load tests.

## Related repositories

- [XClone frontend](https://github.com/Trendyzima/XClone-9b4dz1-9b4dz1-8936-20260809tr)
- [Testagram Backend](https://github.com/Trendyzima/Testagram-backend-)\n- [The Algorithm](https://github.com/Trendyzima/the-algorithm)\n- [Testagram Data Centre](https://github.com/Trendyzima/Testagram-data-centre-)

## Testagram platform integration

This service is the federation protocol component in the five-repository Testagram platform. XClone remains the only frontend. Browser/mobile clients call the public Testagram Backend API; they never call this service directly. Backend owns canonical local accounts, posts, permissions and media ownership. This service is intended to own ActivityPub protocol state and remote actor/delivery state only.

The current release is a protocol foundation, not a production federation server: WebFinger and one configured service actor are available, the outbox is empty, and inbox POST deliberately returns HTTP 501 until signature verification and durable inbox/outbox storage are implemented. Keep this container private and do not route public ActivityPub traffic to it yet.

The Data Centre repository can build/run the container under its optional federation-preview Compose profile. The container image is published as ghcr.io/trendyzima/testagram-federation after a successful push to main. Do not enable public DNS/routing based solely on image availability.
