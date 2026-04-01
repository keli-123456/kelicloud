package cloudflared

import (
	"strings"
	"testing"
)

func TestRunCloudflaredRequiresToken(t *testing.T) {
	t.Setenv("KOMARI_CLOUDFLARED_TOKEN", "")

	err := RunCloudflared()
	if err == nil {
		t.Fatal("expected missing token error")
	}
	if !strings.Contains(err.Error(), "KOMARI_CLOUDFLARED_TOKEN") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestResolveCloudflaredBinarySupportsCurrentPlatform(t *testing.T) {
	fileName, downloadURL, err := resolveCloudflaredBinary()
	if err != nil {
		t.Fatalf("expected current platform to be supported: %v", err)
	}
	if fileName == "" || downloadURL == "" {
		t.Fatalf("expected non-empty binary spec, got file=%q url=%q", fileName, downloadURL)
	}
}
