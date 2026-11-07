# Federated Content <-> centralized Backend contract

The Backend remains canonical for Testagram users, posts, visibility, blocks, deletion and retention. Federated Content owns ActivityPub protocol state: remote actors, inbox/outbox, signature verification, delivery attempts and remote retry state. It is not a second product API or canonical user database.

The Backend will call a private `/internal/v1` Federation API using a dedicated rotatable service credential and private network. Planned operations: idempotently enqueue a Backend-authorized activity; query delivery state; resolve/cache a remote actor. Federation must not publish an activity without Backend authorization and must not treat a remote Delete as proof that every remote copy was erased.

Public WebFinger/actor/inbox/outbox protocol routes are distinct from the private Backend integration API. Current readiness intentionally remains unavailable and inbox processing rejects requests until HTTP Signature verification, replay protection, durable inbox/outbox, idempotent delivery and abuse controls pass tests. Do not expose service credentials to XClone or Data Centre nodes. Keep Federation code/contracts here; no sixth API repository.
