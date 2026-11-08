package store

import "testing"

func TestValidateStoredInboxURL(t *testing.T) {
	valid := []string{
		"https://remote.example/inbox",
		"https://remote.example/shared/inbox",
	}
	for _, value := range valid {
		if err := validateStoredInboxURL(value); err != nil {
			t.Errorf("expected %q to be valid: %v", value, err)
		}
	}
	invalid := []string{
		"http://remote.example/inbox",
		"//remote.example/inbox",
		"https://user:pass@remote.example/inbox",
		"https://remote.example/inbox#fragment",
		"javascript:alert(1)",
		"",
	}
	for _, value := range invalid {
		if err := validateStoredInboxURL(value); err == nil {
			t.Errorf("expected %q to be rejected", value)
		}
	}
}
