package cmd

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestSecurityHeadersAreApplied(t *testing.T) {
	t.Setenv(securityHSTSEnv, "")
	gin.SetMode(gin.TestMode)

	router := gin.New()
	router.Use(applySecurityHeaders())
	router.GET("/ping", func(c *gin.Context) {
		c.Status(http.StatusNoContent)
	})

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/ping", nil)
	router.ServeHTTP(recorder, req)

	if got := recorder.Header().Get("X-Content-Type-Options"); got != "nosniff" {
		t.Fatalf("expected X-Content-Type-Options nosniff, got %q", got)
	}
	if got := recorder.Header().Get("X-Frame-Options"); got != "SAMEORIGIN" {
		t.Fatalf("expected X-Frame-Options SAMEORIGIN, got %q", got)
	}
	if got := recorder.Header().Get("Referrer-Policy"); got != "no-referrer" {
		t.Fatalf("expected Referrer-Policy no-referrer, got %q", got)
	}
	if got := recorder.Header().Get("Permissions-Policy"); got == "" {
		t.Fatal("expected Permissions-Policy to be set")
	}
	if got := recorder.Header().Get("Strict-Transport-Security"); got != "" {
		t.Fatalf("expected HSTS to stay disabled by default, got %q", got)
	}
}

func TestSecurityHeadersApplyHSTSForForwardedHTTPS(t *testing.T) {
	t.Setenv(securityHSTSEnv, "true")
	gin.SetMode(gin.TestMode)

	router := gin.New()
	router.Use(applySecurityHeaders())
	router.GET("/ping", func(c *gin.Context) {
		c.Status(http.StatusNoContent)
	})

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/ping", nil)
	req.Header.Set("X-Forwarded-Proto", "https")
	router.ServeHTTP(recorder, req)

	if got := recorder.Header().Get("Strict-Transport-Security"); got == "" {
		t.Fatal("expected HSTS to be set for forwarded HTTPS request")
	}
}
