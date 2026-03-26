package outboundproxy

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

type stubRoundTripper func(*http.Request) (*http.Response, error)

func (s stubRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return s(request)
}

func TestFallbackRoundTripperUsesProxyResponseWhenHealthy(t *testing.T) {
	proxyCalls := 0
	directCalls := 0
	transport := &fallbackRoundTripper{
		proxied: stubRoundTripper(func(request *http.Request) (*http.Response, error) {
			proxyCalls++
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader("proxied")),
			}, nil
		}),
		direct: stubRoundTripper(func(request *http.Request) (*http.Response, error) {
			directCalls++
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader("direct")),
			}, nil
		}),
	}

	request, err := http.NewRequest(http.MethodGet, "https://example.com", nil)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}

	response, err := transport.RoundTrip(request)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	if proxyCalls != 1 {
		t.Fatalf("expected proxy to be called once, got %d", proxyCalls)
	}
	if directCalls != 0 {
		t.Fatalf("expected direct fallback to stay unused, got %d calls", directCalls)
	}
	if string(body) != "proxied" {
		t.Fatalf("expected proxied response body, got %q", string(body))
	}
}

func TestFallbackRoundTripperFallsBackToDirectOnProxyError(t *testing.T) {
	transport := &fallbackRoundTripper{
		proxied: stubRoundTripper(func(request *http.Request) (*http.Response, error) {
			return nil, errors.New("proxy dial failed")
		}),
		direct: stubRoundTripper(func(request *http.Request) (*http.Response, error) {
			payload, err := io.ReadAll(request.Body)
			if err != nil {
				t.Fatalf("read retry body: %v", err)
			}
			if string(payload) != "{\"hello\":\"world\"}" {
				t.Fatalf("unexpected retried body: %q", string(payload))
			}
			return &http.Response{
				StatusCode: http.StatusCreated,
				Body:       io.NopCloser(strings.NewReader("direct")),
			}, nil
		}),
	}

	request, err := http.NewRequest(http.MethodPost, "https://example.com", strings.NewReader("{\"hello\":\"world\"}"))
	if err != nil {
		t.Fatalf("create request: %v", err)
	}

	response, err := transport.RoundTrip(request)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusCreated {
		t.Fatalf("expected direct fallback status, got %d", response.StatusCode)
	}
}

func TestFallbackRoundTripperFallsBackOnProxyAuthResponse(t *testing.T) {
	directCalls := 0
	transport := &fallbackRoundTripper{
		proxied: stubRoundTripper(func(request *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusProxyAuthRequired,
				Body:       io.NopCloser(strings.NewReader("proxy auth required")),
			}, nil
		}),
		direct: stubRoundTripper(func(request *http.Request) (*http.Response, error) {
			directCalls++
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader("direct")),
			}, nil
		}),
	}

	request, err := http.NewRequest(http.MethodGet, "https://example.com", nil)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}

	response, err := transport.RoundTrip(request)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer response.Body.Close()

	if directCalls != 1 {
		t.Fatalf("expected direct fallback to be used once, got %d", directCalls)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("expected direct fallback response, got %d", response.StatusCode)
	}
}

func TestNewProxyTransportDisablesConnectionReuse(t *testing.T) {
	transport, err := newProxyTransport(&Settings{
		Enabled:  true,
		Protocol: "socks5",
		Host:     "proxy.example.com",
		Port:     1080,
	})
	if err != nil {
		t.Fatalf("build proxy transport: %v", err)
	}

	if !transport.DisableKeepAlives {
		t.Fatalf("expected proxy transport to disable keep-alives")
	}
	if transport.ForceAttemptHTTP2 {
		t.Fatalf("expected proxy transport to disable http2 reuse")
	}
}

func TestNewProxyTransportUsesDedicatedSocks5Dialer(t *testing.T) {
	transport, err := newProxyTransport(&Settings{
		Enabled:  true,
		Protocol: "socks5",
		Host:     "proxy.example.com",
		Port:     1080,
		Username: "alice",
		Password: "secret",
	})
	if err != nil {
		t.Fatalf("build socks5 proxy transport: %v", err)
	}

	if transport.Proxy != nil {
		t.Fatal("expected socks5 transport to avoid http proxy hook")
	}
	if transport.DialContext == nil {
		t.Fatal("expected socks5 transport to install a custom dialer")
	}
	if transport.DialTLSContext == nil {
		t.Fatal("expected socks5 transport to install a custom TLS dialer")
	}
}

func TestLoadSettingsParsesCredentialLine(t *testing.T) {
	settings := &Settings{
		Host: "prem.country.iprocket.io:9595:com94499845-res-any:RqT26IE0U72JxLy",
		Port: 1080,
	}

	settings.normalizeCredentialLine()

	if settings.Host != "prem.country.iprocket.io" {
		t.Fatalf("unexpected host: %q", settings.Host)
	}
	if settings.Port != 9595 {
		t.Fatalf("unexpected port: %d", settings.Port)
	}
	if settings.Username != "com94499845-res-any" {
		t.Fatalf("unexpected username: %q", settings.Username)
	}
	if settings.Password != "RqT26IE0U72JxLy" {
		t.Fatalf("unexpected password: %q", settings.Password)
	}
}
