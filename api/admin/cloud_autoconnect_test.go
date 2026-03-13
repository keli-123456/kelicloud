package admin

import (
	"encoding/base64"
	"strings"
	"testing"
)

func TestResolveCloudAutoConnectOrigin(t *testing.T) {
	t.Run("uses script domain without scheme", func(t *testing.T) {
		origin, err := resolveCloudAutoConnectOrigin("panel.example.com/", "https", "ignored.example.com")
		if err != nil {
			t.Fatalf("resolveCloudAutoConnectOrigin returned error: %v", err)
		}
		if origin != "http://panel.example.com" {
			t.Fatalf("expected script domain origin, got %q", origin)
		}
	})

	t.Run("uses script domain with scheme", func(t *testing.T) {
		origin, err := resolveCloudAutoConnectOrigin("https://panel.example.com/", "http", "ignored.example.com")
		if err != nil {
			t.Fatalf("resolveCloudAutoConnectOrigin returned error: %v", err)
		}
		if origin != "https://panel.example.com" {
			t.Fatalf("expected https origin, got %q", origin)
		}
	})

	t.Run("falls back to request origin", func(t *testing.T) {
		origin, err := resolveCloudAutoConnectOrigin("", "https", "panel.internal:23333")
		if err != nil {
			t.Fatalf("resolveCloudAutoConnectOrigin returned error: %v", err)
		}
		if origin != "https://panel.internal:23333" {
			t.Fatalf("expected request origin, got %q", origin)
		}
	})

	t.Run("rejects empty host", func(t *testing.T) {
		if _, err := resolveCloudAutoConnectOrigin("", "https", ""); err == nil {
			t.Fatal("expected error for empty host")
		}
	})
}

func TestBuildScopedAutoDiscoveryKey(t *testing.T) {
	baseKey := "abcdefghijklmnop"
	group := "aws/Prod Team"
	expectedGroup := base64.RawURLEncoding.EncodeToString([]byte(group))

	got := buildScopedAutoDiscoveryKey(baseKey, group)
	want := baseKey + "::group-b64=" + expectedGroup
	if got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}
}

func TestDefaultCloudAutoConnectGroup(t *testing.T) {
	if got := defaultCloudAutoConnectGroup("digitalocean", "Primary Token"); got != "digitalocean/Primary Token" {
		t.Fatalf("unexpected group: %q", got)
	}
	if got := defaultCloudAutoConnectGroup("aws", ""); got != "aws/default" {
		t.Fatalf("unexpected fallback group: %q", got)
	}
}

func TestMergeCloudAutoConnectUserData(t *testing.T) {
	const snippet = "# Komari auto-connect\necho ready"

	t.Run("wraps empty shell data", func(t *testing.T) {
		merged, err := mergeCloudAutoConnectUserData("", snippet, true)
		if err != nil {
			t.Fatalf("mergeCloudAutoConnectUserData returned error: %v", err)
		}
		if !strings.HasPrefix(merged, "#!/bin/bash\nset -eu\n\n") {
			t.Fatalf("expected shell wrapper, got %q", merged)
		}
		if !strings.Contains(merged, snippet) {
			t.Fatalf("expected snippet in merged user_data: %q", merged)
		}
	})

	t.Run("keeps bare commands when wrapper disabled", func(t *testing.T) {
		merged, err := mergeCloudAutoConnectUserData("", snippet, false)
		if err != nil {
			t.Fatalf("mergeCloudAutoConnectUserData returned error: %v", err)
		}
		if merged != snippet+"\n" {
			t.Fatalf("expected bare commands, got %q", merged)
		}
	})

	t.Run("appends to existing shell data", func(t *testing.T) {
		existing := "#!/bin/bash\necho custom"
		merged, err := mergeCloudAutoConnectUserData(existing, snippet, true)
		if err != nil {
			t.Fatalf("mergeCloudAutoConnectUserData returned error: %v", err)
		}
		if !strings.Contains(merged, existing) || !strings.Contains(merged, snippet) {
			t.Fatalf("expected existing script and snippet, got %q", merged)
		}
	})

	t.Run("rejects cloud config", func(t *testing.T) {
		if _, err := mergeCloudAutoConnectUserData("#cloud-config\nusers: []", snippet, true); err == nil {
			t.Fatal("expected error for #cloud-config")
		}
	})
}
