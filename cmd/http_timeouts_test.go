package cmd

import (
	"net/http"
	"testing"
	"time"
)

func TestApplyHTTPTimeoutsUsesEnvironmentValues(t *testing.T) {
	t.Setenv("KOMARI_HTTP_READ_HEADER_TIMEOUT", "3s")
	t.Setenv("KOMARI_HTTP_READ_TIMEOUT", "4s")
	t.Setenv("KOMARI_HTTP_WRITE_TIMEOUT", "5s")
	t.Setenv("KOMARI_HTTP_IDLE_TIMEOUT", "6s")

	srv := &http.Server{}
	applyHTTPTimeouts(srv)

	if srv.ReadHeaderTimeout != 3*time.Second {
		t.Fatalf("expected read header timeout 3s, got %s", srv.ReadHeaderTimeout)
	}
	if srv.ReadTimeout != 4*time.Second {
		t.Fatalf("expected read timeout 4s, got %s", srv.ReadTimeout)
	}
	if srv.WriteTimeout != 5*time.Second {
		t.Fatalf("expected write timeout 5s, got %s", srv.WriteTimeout)
	}
	if srv.IdleTimeout != 6*time.Second {
		t.Fatalf("expected idle timeout 6s, got %s", srv.IdleTimeout)
	}
}
