package ws

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func requestWithOrigin(host, origin string) *http.Request {
	req := httptest.NewRequest(http.MethodGet, "https://"+host+"/api/ws", nil)
	req.Host = host
	if origin != "" {
		req.Header.Set("Origin", origin)
	}
	return req
}

func TestCheckOriginAllowsSameHost(t *testing.T) {
	t.Setenv(wsDisableOriginEnv, "")
	t.Setenv(corsAllowedOriginsEnv, "")
	t.Setenv(wsAllowedOriginsEnv, "")

	req := requestWithOrigin("monitor.example.com", "https://monitor.example.com")

	if !CheckOrigin(req) {
		t.Fatal("expected same-host websocket origin to be allowed")
	}
}

func TestCheckOriginRejectsMissingOrigin(t *testing.T) {
	t.Setenv(wsDisableOriginEnv, "")
	t.Setenv(corsAllowedOriginsEnv, "")
	t.Setenv(wsAllowedOriginsEnv, "")

	if CheckOrigin(requestWithOrigin("monitor.example.com", "")) {
		t.Fatal("expected missing websocket origin to be rejected")
	}
}

func TestCheckOriginAllowsConfiguredCORSOrigin(t *testing.T) {
	t.Setenv(wsDisableOriginEnv, "")
	t.Setenv(corsAllowedOriginsEnv, "https://admin.example.com")
	t.Setenv(wsAllowedOriginsEnv, "")

	req := requestWithOrigin("monitor.example.com", "https://admin.example.com")

	if !CheckOrigin(req) {
		t.Fatal("expected CORS allowlist origin to be allowed for websocket")
	}
}

func TestCheckOriginAllowsConfiguredWSOrigin(t *testing.T) {
	t.Setenv(wsDisableOriginEnv, "")
	t.Setenv(corsAllowedOriginsEnv, "")
	t.Setenv(wsAllowedOriginsEnv, "https://terminal.example.com")

	req := requestWithOrigin("monitor.example.com", "https://terminal.example.com")

	if !CheckOrigin(req) {
		t.Fatal("expected websocket allowlist origin to be allowed")
	}
}

func TestCheckOriginRejectsUnlistedCrossOrigin(t *testing.T) {
	t.Setenv(wsDisableOriginEnv, "")
	t.Setenv(corsAllowedOriginsEnv, "https://admin.example.com")
	t.Setenv(wsAllowedOriginsEnv, "")

	req := requestWithOrigin("monitor.example.com", "https://evil.example.com")

	if CheckOrigin(req) {
		t.Fatal("expected unlisted websocket origin to be rejected")
	}
}

func TestCheckOriginCanBeDisabled(t *testing.T) {
	t.Setenv(wsDisableOriginEnv, "true")
	t.Setenv(corsAllowedOriginsEnv, "")
	t.Setenv(wsAllowedOriginsEnv, "")

	req := requestWithOrigin("monitor.example.com", "https://evil.example.com")

	if !CheckOrigin(req) {
		t.Fatal("expected disabled websocket origin check to allow request")
	}
}
