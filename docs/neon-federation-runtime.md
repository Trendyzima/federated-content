# Neon PostgreSQL Federation runtime

Federation protocol state is durable PostgreSQL state and must not be stored in Redis or a Durable Object. The service can use Neon PostgreSQL by setting FEDERATION_DATABASE_URL to a dedicated database named federation on a Neon pooled endpoint.

## Required setup

1. Provision a staging Neon project/database and a dedicated role for Federated Content.
2. Use the pooled connection string, TLS with sslmode=require (or verify-full), and a bounded connect_timeout.
3. Keep the role restricted to the Federation database. Do not reuse the Backend application role or Data Centre control-plane role.
4. Run the existing embedded Federation schema migration in staging and verify all indexes and unique constraints.
5. Import existing local actor keys, remote actors, inbox activities, outbox activities, delivery jobs and remote followers with stable IDs. Reconcile counts and checksums before switching.
6. Test advisory-lock schema initialization, concurrent worker claims, lease recovery, acknowledgements, retries and dead-letter behavior against Neon.
7. Only after migration verification, change the production FEDERATION_DATABASE_URL. Keep FEDERATION_DELIVERY_WORKER_ENABLED=false until the producer, inbox processor, fan-out and end-to-end release gates are complete.

## Runtime safeguards

- Remote database URLs must use TLS. The application rejects remote PostgreSQL URLs without sslmode=require or stronger.
- Local development may use localhost or the federation-db Compose service without TLS.
- The service pool defaults to 8 open / 2 idle connections, with a 15-minute maximum connection lifetime and 2-minute idle lifetime. Keep aggregate connections across replicas within the Neon compute's connection budget.
- Use the Neon pooled endpoint for horizontally scaled services; do not point every replica at a direct endpoint.
- Neon Free quotas can suspend compute when CU-hours or network transfer are exhausted. A database URL change does not migrate data and does not make the free plan a production SLA.

The local Compose database remains the development default. This branch does not change production secrets, copy data, activate delivery, or enable public federation routing.
