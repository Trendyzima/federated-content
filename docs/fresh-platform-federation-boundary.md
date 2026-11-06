# Fresh platform — Federation boundary

Local identity and canonical data are Appwrite Auth and Appwrite Database. Federated Content is not an identity provider and cannot create/authenticate local accounts.

Federation remains disabled by default. Remote actors and inbox/outbox state are isolated from local profiles/posts. Treat remote input as untrusted. Verify HTTP signatures, replay windows, actor ownership, content limits, SSRF protections, rate limits and delivery retries before activation.

No Supabase connection, legacy actor import or legacy user-ID mapping. Remote actors never gain local account privileges.
