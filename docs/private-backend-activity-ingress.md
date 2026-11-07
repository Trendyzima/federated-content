# Private Backend Activity Ingress

## Ownership

The central Testagram Backend is the authority for local account identity, federation preference, post visibility, moderation, blocks, privacy and permission to publish. This service owns ActivityPub actor keys, remote follower relationships, protocol outbox persistence and signed delivery to remote inboxes.

XClone and other user-facing clients must never call this endpoint. Only the Backend may call it over the private service network.

## Endpoint

`POST /internal/v1/activities`

Required headers:
- `Authorization: Bearer <BACKEND_INTERNAL_TOKEN>`
- `Content-Type: application/json`
- `X-Request-ID: <opaque request id>`
- `Idempotency-Key: <activity id>` (recommended; activity ID is the durable idempotency key)

Request:
```json
{
  "activity_id": "https://testagram.site/activities/opaque-id",
  "actor_user_id": "canonical-backend-user-id",
  "activity": {
    "id": "https://testagram.site/activities/opaque-id",
    "type": "Create",
    "actor": "https://federation.testagram.site/users/handle",
    "object": {"id": "https://testagram.site/posts/opaque-id"}
  }
}
```

Accepted activity types in this first slice are `Create`, `Update`, `Delete`, `Like` and `Announce`. The request body is limited to 1 MiB and rejects unknown top-level fields and trailing JSON values.

## Acceptance sequence

1. Authenticate the distinct Backend service credential using constant-time comparison.
2. Require durable Federation storage and a configured private Backend client.
3. Resolve `actor_user_id` through Backend; require the canonical ID to match and federation to be enabled.
4. Derive the actor URI from the configured Federation origin and Backend-verified handle; reject an activity actor mismatch.
5. Ensure the local signing actor/key exists.
6. Load a bounded, deduplicated set of remote follower inboxes (shared inbox preferred).
7. Persist the outbox activity and delivery jobs in one Federation database transaction.
8. Return HTTP 202 only after durable acceptance.

The outbox insert is idempotent by ActivityPub activity ID. Reuse of an activity ID with a different actor or payload is rejected. Delivery jobs are unique per activity/inbox and are processed by the existing leased outbound worker. Remote HTTPS delivery uses the redirect-disabled, public-IP-only dialer.

## Current implementation status

Implemented in this branch:
- private route registration and handler;
- constant-time service bearer verification;
- strict bounded JSON decoding and request ID propagation;
- Backend actor resolution and federation-enabled check;
- local actor/key provisioning;
- bounded remote follower inbox selection;
- atomic outbox + delivery-job enqueue;
- activity-ID collision protection;
- tests for missing credentials, unavailable runtime and activity identity/type validation;
- OpenAPI path documentation.

Not yet complete:
- Backend transactional-outbox table/producer/dispatcher wiring;
- inbound HTTP Signature verification and durable inbox processing;
- inbox processor that applies accepted remote activities through Backend authorization;
- recipient fan-out beyond the current bounded set of stored remote followers;
- delivery status callback/metrics and operational dead-letter UI;
- end-to-end staging interoperability against Mastodon-compatible peers.

## Safety and rollout

Do not enable public inbox processing until signature verification, Digest/Date validation, replay protection, durable acceptance and Backend-authorized inbox processing pass tests. Do not call the new endpoint directly from a browser, mobile client or untrusted worker. Keep `FEDERATION_DELIVERY_WORKER_ENABLED=false` until staging verifies remote delivery, retries, lease recovery and dead-letter handling.
