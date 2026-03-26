package outboundproxy

import (
	"context"
	"net/http"
	"testing"
	"time"
)

func withProbeFetcher(t *testing.T, fetcher func(context.Context, *http.Client) (string, string, string, string, error)) {
	t.Helper()

	previous := fetchExitIPsFunc
	fetchExitIPsFunc = fetcher
	t.Cleanup(func() {
		fetchExitIPsFunc = previous
	})
}

func TestProbeWithSettingsUsesProvidedHTTPProxy(t *testing.T) {
	withProbeFetcher(t, func(ctx context.Context, client *http.Client) (string, string, string, string, error) {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://probe.invalid/ipv4", nil)
		if err != nil {
			t.Fatalf("create probe request: %v", err)
		}

		transport, ok := client.Transport.(*http.Transport)
		if !ok {
			t.Fatalf("expected http transport, got %T", client.Transport)
		}

		proxyURL, err := transport.Proxy(request)
		if err != nil {
			t.Fatalf("resolve proxy url: %v", err)
		}
		if proxyURL == nil {
			t.Fatal("expected proxy url to be configured")
		}
		if proxyURL.Scheme != "http" {
			t.Fatalf("unexpected proxy scheme: %q", proxyURL.Scheme)
		}
		if proxyURL.Host != "proxy.example.com:8080" {
			t.Fatalf("unexpected proxy host: %q", proxyURL.Host)
		}

		return "198.51.100.10", "http://probe.invalid/ipv4", "", "", nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result, err := ProbeWithSettings(ctx, &Settings{
		Enabled:  true,
		Protocol: "http",
		Host:     "proxy.example.com",
		Port:     8080,
	})
	if err != nil {
		t.Fatalf("probe with provided proxy settings: %v", err)
	}

	if result.Mode != "proxy" {
		t.Fatalf("expected proxy mode, got %q", result.Mode)
	}
	if result.IPv4 != "198.51.100.10" {
		t.Fatalf("unexpected ipv4 result: %q", result.IPv4)
	}
	if result.IPv4URL != "http://probe.invalid/ipv4" {
		t.Fatalf("unexpected ipv4 probe url: %q", result.IPv4URL)
	}
}

func TestProbeWithSettingsParsesCredentialLinePayload(t *testing.T) {
	withProbeFetcher(t, func(ctx context.Context, client *http.Client) (string, string, string, string, error) {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://probe.invalid/ipv4", nil)
		if err != nil {
			t.Fatalf("create probe request: %v", err)
		}

		transport, ok := client.Transport.(*http.Transport)
		if !ok {
			t.Fatalf("expected http transport, got %T", client.Transport)
		}

		proxyURL, err := transport.Proxy(request)
		if err != nil {
			t.Fatalf("resolve proxy url: %v", err)
		}
		if proxyURL == nil {
			t.Fatal("expected proxy url to be configured")
		}
		if proxyURL.Host != "proxy.example.com:8080" {
			t.Fatalf("unexpected proxy host: %q", proxyURL.Host)
		}
		if proxyURL.User == nil || proxyURL.User.Username() != "alice" {
			t.Fatalf("unexpected proxy username: %#v", proxyURL.User)
		}
		password, ok := proxyURL.User.Password()
		if !ok || password != "secret" {
			t.Fatalf("unexpected proxy password: %q", password)
		}

		return "203.0.113.7", "http://probe.invalid/ipv4", "", "", nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result, err := ProbeWithSettings(ctx, &Settings{
		Enabled:  true,
		Protocol: "http",
		Host:     "proxy.example.com:8080:alice:secret",
		Port:     1080,
	})
	if err != nil {
		t.Fatalf("probe with credential line payload: %v", err)
	}

	if result.Mode != "proxy" {
		t.Fatalf("expected proxy mode, got %q", result.Mode)
	}
	if result.IPv4 != "203.0.113.7" {
		t.Fatalf("unexpected ipv4 result: %q", result.IPv4)
	}
}
