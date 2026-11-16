package httpapi

import (
	"context"
	"testing"
)

func TestResolveRemoteActivityPubActorRejectsUnsafeIdentitiesBeforeNetwork(t *testing.T) {
	for _, identity := range []string{
		"http://mastodon.example/users/alice",
		"https://user:pass@mastodon.example/users/alice",
		"https://mastodon.example/users/alice?redirect=https://evil.example",
		"https://mastodon.example/users/alice#fragment",
		"file:///etc/passwd",
		"not-a-url",
	} {
		if _, err := resolveRemoteActivityPubActor(context.Background(), identity); err == nil {
			t.Errorf("expected unsafe actor URI %q to be rejected", identity)
		}
	}
}
