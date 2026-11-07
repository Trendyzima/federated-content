# ActivityPub media retention and expiry contract

Testagram Backend remains the canonical owner of local posts, stories, media lifecycle and deletion deadlines. Federated Content owns protocol delivery and remote object bookkeeping only.

## Stories
- Do not federate a local story unless the product explicitly defines a federation policy for stories.
- If a story is federated in a future release, Backend supplies an immutable expiry timestamp. Federated Content schedules an idempotent ActivityPub `Delete` at that deadline and records remote delivery outcomes.
- Expiry delivery failure must not keep the local story visible or block local expiry. Retry remote delivery independently with bounded backoff and dead-letter/operator visibility.
- Never extend expiry based on remote receipt time or retries.

## User-deleted media and posts
- Backend emits signed/authorized lifecycle events with stable event ID, local object ID, policy version, deletion timestamp and reason class (without leaking private content).
- Federated Content deduplicates events, maps them to protocol object IDs, and emits ActivityPub `Delete` where required by federation semantics and user visibility settings.
- Local access revocation is immediate and does not wait for remote acknowledgements. Track remote delivery separately; remote servers may retain lawful caches or copies outside Testagram's control.
- A 30-day local recovery window must not silently republish remote content. If restoration is allowed, Backend must issue an explicit restore/republish event and federation policy must decide whether an `Undo` or a new `Create` is semantically correct.
- Never claim that Testagram can erase copies already delivered to independent remote servers.

## Reliability and security
- Verify event authenticity; use durable inbox/outbox, deduplication, replay protection and retry scheduling.
- Store delivery state per remote actor/server; one failing server must not block others or local posting.
- Do not accept unsigned browser requests as lifecycle authority.
- Federation remains disabled until HTTP Signature verification, durable idempotent inbox/outbox processing and delivery safety are complete.

This document defines the integration contract only. It does not activate federation or destructive retention.
