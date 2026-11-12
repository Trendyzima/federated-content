# Live external ActivityPub peer validation

This is a staging-only release gate. Unit tests and the PostgreSQL-backed CI integration suite use local test servers; they do not prove interoperability with an independently operated ActivityPub server.

## Required staging fixtures

Use two independently administered staging identities:

- A disposable Testagram actor with Federation explicitly enabled and a linked canonical Backend profile.
- A disposable remote ActivityPub actor on a separate instance, with a working inbox and a follow relationship to the Testagram actor.

The remote instance must be controlled by the Testagram operator or an explicitly approved test partner. Do not use an unrelated user's account or send unsolicited test activities.

Record these non-secret values in the protected GitHub `staging` environment:

- `FEDERATION_STAGING_PUBLIC_BASE_URL`
- `FEDERATION_STAGING_INSTANCE_DOMAIN`
- `FEDERATION_STAGING_TEST_ACTOR_HANDLE`
- `FEDERATION_STAGING_REMOTE_ACTOR`
- `FEDERATION_STAGING_REMOTE_INBOX`

Keep Backend session tokens, Federation internal tokens, private keys and remote peer credentials in environment secrets. Never place them in workflow inputs, logs, artifacts or this document.

## Required live test sequence

1. **Discovery:** request Testagram WebFinger for the exact local actor. Verify the subject, canonical actor URL and HTTPS host.
2. **Actor document:** fetch the actor with `Accept: application/activity+json`. Verify actor ID, inbox, outbox, followers URL, key ID and public-key PEM. Confirm the key ID belongs to the actor.
3. **Remote follow:** have the controlled remote actor follow the Testagram actor. Confirm the signed Follow activity is accepted, persisted once and reflected in the local follower relationship.
4. **Outbound delivery:** publish a unique public, non-sensitive test post from the disposable Testagram actor. Confirm a Create activity is durably queued, signed with the local actor key, delivered to the remote inbox and visible to the remote test actor exactly once.
5. **Inbound delivery:** publish a reply or Like from the controlled remote actor. Confirm the local inbox validates its HTTP Signature and Digest, resolves the key owner, checks the Date window and persists the activity once.
6. **Replay protection:** resend the identical inbound activity ID. The endpoint may return an accepted/idempotent response, but the inbox must not create a second activity or duplicate notification.
7. **Tamper rejection:** send requests with a modified body digest, modified signature, wrong key owner and stale Date. Each must be rejected before side effects.
8. **Retry behavior:** configure the controlled peer to return HTTP 503 for the first two delivery attempts, then 202. Verify retry scheduling, bounded backoff, eventual acknowledgement and exactly one remote activity.
9. **Permanent failure:** configure a separate test inbox to return HTTP 410. Verify the delivery reaches the dead-letter state and does not block later deliveries.
10. **Consent revocation:** disable Federation for the disposable local actor. Verify WebFinger, actor, inbox and outbox access stop resolving and queued deliveries are not newly emitted for that actor.

## Evidence to retain

For each run, retain only redacted evidence:

- GitHub Actions run URL and commit SHA.
- Remote instance software/version and test actor handles.
- WebFinger and actor HTTP status codes.
- Activity IDs and delivery attempt counts (never signatures or private keys).
- Inbox accepted/rejected counts, duplicate suppression result and dead-letter reason.
- Remote timeline evidence for the single outbound test post.
- Cleanup confirmation for disposable accounts and test activities.

A passing local test suite is not a substitute for this external peer exercise. Do not mark the Federation release gate complete until both directions, replay rejection, signature failures, retry recovery and permanent-failure handling have been observed against the controlled remote instance.
