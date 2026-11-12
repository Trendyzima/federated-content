# Federation production security and reliability gate

This checklist is the release boundary for public ActivityPub federation. A green unit/CI suite is necessary but is not proof of production interoperability.

## Current protocol support boundary

| Area | Implemented behavior | Release evidence still required |
|---|---|---|
| Actor discovery | WebFinger and actor documents resolve local identities through the private Backend contract | Controlled external peer verifies canonical actor, inbox, outbox and key identity |
| Outbound signing | RSA HTTP Signatures cover request-target, host, date and SHA-256 Digest | Verify delivery against at least two independently operated ActivityPub implementations |
| Inbound verification | Resolve remote key, verify RSA signature, Digest and five-minute Date window; require activity actor to match key owner | Verify tampered signature, digest, wrong key owner, stale Date, key rotation and remote outage against controlled peer |
| Inbound durability | Unique ActivityPub IDs, durable inbox rows, worker leases, retries and dead-letter state | Verify crash recovery, replay and poison-message behavior on staging PostgreSQL |
| Follow lifecycle | Follow is persisted and Accept is queued; Undo removes the sender's follower relationship | Verify Follow/Accept/Undo round trip with remote peer, including duplicate delivery |
| Other inbound activities | Create, Update, Delete, Like, Announce, Accept, Reject and Block are accepted into durable inbox storage but not all have a Backend projection; unsupported processing is quarantined/dead-lettered | Do not claim full support or enable these activity types until the corresponding Backend-authorized projection, policy and tests exist |
| Outbound delivery | Durable jobs, leases, bounded attempts, dead-lettering and redirect-disabled public-IP-only HTTPS client | Verify 503/429 retry, permanent 4xx handling, lease expiry and no duplicate remote effects |
| Remote network boundary | HTTPS-only, port 443, redirects disabled, private/reserved IP ranges rejected and DNS resolved at connection time | Re-run SSRF regression suite and observe controlled peer DNS/TLS behavior |
| Local consent | Backend is authoritative; actor resolution and inbox processing recheck federation eligibility | Verify opt-out, suspension, deletion and handle change while jobs are queued |
| Operations | Readiness checks Backend and Federation database | Add alerting, dead-letter inspection/replay procedures, key rotation runbook and backup/restore evidence |

## Hard security invariants

1. Backend owns local account identity, blocks, privacy, moderation and permission to federate. Federation must never infer consent from a client payload.
2. Every inbound activity must have a valid HTTPS actor ID equal to the owner of the verified signing key.
3. The signed request must cover request-target, host, date and digest. Ambiguous duplicate signature, Date or Digest headers are rejected.
4. ActivityPub activity IDs are idempotency keys. Reusing an ID with a different actor, recipient, type or payload is a conflict, not a duplicate.
5. Inbound activities are acknowledged only after durable storage. Processing uses leases and bounded retries; permanent or exhausted failures go to dead-letter state.
6. Outbound HTTP requests may not follow redirects and may connect only to public IP addresses on HTTPS port 443.
7. No private/direct Backend post may be emitted to public or shared inboxes. Recipient fan-out must be authorized from canonical Backend policy.
8. Federation opt-out must stop new actor discovery and delivery. Already queued work must be revalidated before delivery.
9. Never log private keys, bearer tokens, full signed request headers or private activity bodies.
10. Public federation stays disabled until the staging peer exercise and operational recovery drill are signed off.

## Staging interoperability exercise

Use disposable actors on a Testagram staging instance and a separately controlled Mastodon-compatible peer. Do not use an unrelated account or send unsolicited activities.

- Discover the local actor using WebFinger and fetch its ActivityPub actor document.
- Follow from the remote peer; confirm signature validation, one durable inbox row, follower relationship and one signed Accept delivery.
- Undo the Follow; confirm the follower relationship is removed and duplicate Undo does not create side effects.
- Send one harmless public test Create from each side only after a Backend projection exists for that activity type.
- Replay the same activity ID; verify idempotent acceptance with no duplicate effects.
- Tamper with body, digest, signature, key owner and Date; verify rejection before side effects.
- Force remote HTTP 503/429 twice and then 202; verify jittered bounded retry, lease ownership and eventual acknowledgement.
- Force permanent HTTP 410; verify dead-letter state and that other deliveries continue.
- Disable Federation for the local actor while work is queued; verify no further delivery is sent.
- Restart the API and workers during a claimed job; verify expired leases are reclaimed and no activity is processed twice.

## Evidence required for release

Retain redacted evidence only:

- Commit SHA and GitHub Actions run URLs.
- Staging environment and remote software/version.
- WebFinger/actor response status and canonical actor identifiers.
- Activity IDs, inbox row states, attempt counts and dead-letter reasons.
- Remote timeline evidence for the one controlled public test post.
- Opt-out result and cleanup confirmation.
- Restore drill: backup timestamp, restore target, schema migration result, row-count/integrity checks and measured recovery time.

Do not store private keys, signed Authorization headers, session tokens or private activity payloads in artifacts.

## Restore and key management

- Back up Federation PostgreSQL data and actor signing-key material as one recoverable set. A database-only restore that loses local private keys breaks actor identity; key-only restore without matching actor rows is also unsafe.
- Encrypt backups, restrict access, record retention and verify checksums.
- Perform a restore into an isolated staging database, run schema migrations, verify actor key pairs against published public keys, inspect inbox/outbox/delivery state, then run a controlled signed delivery.
- Rotate keys with an overlap window: publish the new key, retain the old public key long enough for in-flight signatures, then revoke the old key only after peer verification. Never overwrite a key without a recoverable encrypted backup.
- Document operator approval, rollback and incident contacts before enabling workers in production.

## Release decision

The current implementation is not a complete ActivityPub implementation: Follow and Undo are the only inbound activity types with active domain processing in the inbox worker. Other accepted types are deliberately quarantined until Backend-authorized projection handlers exist. External peer interoperability, production restore and key rotation remain separate release gates. Keep worker flags disabled until all gates pass.
