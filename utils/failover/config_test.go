package failover

import "testing"

func TestDefaultAutoConnectGroupUsesPoolGroupWhenAvailable(t *testing.T) {
	got := defaultAutoConnectGroup("digitalocean", "sg-prod", "Token 1")
	if got != "digitalocean/sg-prod" {
		t.Fatalf("expected pool-group based auto-connect group, got %q", got)
	}
}

func TestDefaultAutoConnectGroupFallsBackToCredentialName(t *testing.T) {
	got := defaultAutoConnectGroup("aws", "", "Credential A")
	if got != "aws/Credential A" {
		t.Fatalf("expected credential-name fallback, got %q", got)
	}
}
