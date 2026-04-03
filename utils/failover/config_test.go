package failover

import (
	"strings"
	"testing"
)

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

func TestBuildAWSIPv6RefreshUserDataWrapsShellScriptWhenEmpty(t *testing.T) {
	got, err := buildAWSIPv6RefreshUserData("")
	if err != nil {
		t.Fatalf("expected helper to build shell user_data, got %v", err)
	}
	if !strings.HasPrefix(got, "#!/bin/bash\nset -eu\n\n# Komari AWS IPv6 refresh\n") {
		t.Fatalf("unexpected IPv6 refresh user_data: %q", got)
	}
	if !strings.Contains(got, "networkctl reconfigure") {
		t.Fatalf("expected IPv6 refresh commands in user_data, got %q", got)
	}
}

func TestBuildAWSIPv6RefreshUserDataRejectsCloudConfig(t *testing.T) {
	_, err := buildAWSIPv6RefreshUserData("#cloud-config\nusers: []")
	if err == nil {
		t.Fatal("expected #cloud-config user_data to be rejected")
	}
	if !strings.Contains(err.Error(), "AWS IPv6 refresh cannot be combined with #cloud-config") {
		t.Fatalf("unexpected error: %v", err)
	}
}
