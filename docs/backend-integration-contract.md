# Testagram Backend integration contract

## Ownership

The Testagram Backend is authoritative for local account identity, profile state, blocks, local posts, visibility, and user authorization. Federated Content is authoritative only for protocol-specific state: actor key material, remote actor/object cache, inbox receipts, delivery attempts, and federation cursors.

Do not create local copies of the backend's user table. Every local actor must resolve to a stable backend user ID and a canonical actor URI.

## Local actor resolution

The future identity adapter must resolve a requested local handle through a private backend endpoint. The response must include:

- immutable local user ID;
- canonical username and account status;
- display name, summary, avatar and profile URL;
- actor URI;
- public-key identifier and public-key material;
- inbox/outbox policy;
- current federation eligibility.

A deleted, suspended, blocked, or federation-disabled account must not be advertised as an active actor. Never accept a client-supplied actor URI or key as authoritative.

## Event contract: local post published

The backend should publish a durable, versioned event after the local post transaction commits. Recommended envelope:

    {
      "event_id": "uuid",
      "event_type": "testagram.post.published",
      "schema_version": 1,
      "occurred_at": "RFC3339 timestamp",
      "actor_id": "canonical backend user ID",
      "object_id": "canonical backend post ID",
      "visibility": "public | unlisted | followers | direct",
      "federation_eligible": true
    }

The federation service must fetch the canonical post and recipient/follower information through an authenticated backend contract rather than trusting arbitrary event payload content. Direct content must never enter public federation delivery. Private and followers-only visibility require explicit recipient authorization and protocol handling.

## Delivery semantics

- Backend event publication must use a transactional outbox so a committed post cannot lose its federation event.
- Delivery is at-least-once; every event and remote activity must be idempotent.
- Federation workers acknowledge an event only after durable acceptance into federation-owned storage.
- Remote HTTP delivery must happen outside database transactions.
- Retry transient network errors, 408, 429 and selected 5xx responses with bounded exponential backoff and jitter.
- Do not retry permanent authorization or policy 4xx responses indefinitely.
- Dead-letter exhausted deliveries with a reason and operator replay capability.

## Service authentication

Use private network routing plus mTLS where available. If signed service tokens are used, validate issuer, audience, expiry, key ID, and replay protection. Never place internal service credentials in XClone or browser-visible environment variables.

## Contract rollout

1. Agree on schema and identity semantics in both repositories.
2. Add backend transactional outbox and service-authenticated read endpoints.
3. Add federation event consumer and durable inbox/outbox tables.
4. Add contract tests using shared fixtures.
5. Enable a staging-only actor and test account.
6. Enable public federation only after signature, authorization, idempotency, and interoperability gates pass.

This document defines a contract proposal; it does not claim that the backend endpoints or event publisher already exist.


## Private API client usage

The Federation service uses the Backend private API for actor discovery, readiness and durable published-post outbox polling. Every request carries the internal bearer, API version `1` and a unique request ID. Outbox polling uses bounded pages (`limit <= 99`) and persists `next_cursor` even when `has_more=false`, so the next poll can resume without replaying the entire event history. The Federation service never reads Appwrite/Neon directly.
