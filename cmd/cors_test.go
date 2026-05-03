package cmd

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestCORSAllowsSameHostOrigin(t *testing.T) {
	t.Setenv(corsAllowedOriginsEnv, "")
	gin.SetMode(gin.TestMode)

	router := gin.New()
	router.Use(func(c *gin.Context) {
		if handleDynamicCORS(c) {
			return
		}
		c.Status(http.StatusNoContent)
	})
	router.GET("/api/public", func(c *gin.Context) {
		c.Status(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "https://monitor.example.com/api/public", nil)
	req.Host = "monitor.example.com"
	req.Header.Set("Origin", "https://monitor.example.com")
	recorder := httptest.NewRecorder()

	router.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200, got %d", recorder.Code)
	}
	if got := recorder.Header().Get("Access-Control-Allow-Origin"); got != "https://monitor.example.com" {
		t.Fatalf("expected same-host origin to be allowed, got %q", got)
	}
}

func TestCORSRejectsUnlistedCrossOriginPreflight(t *testing.T) {
	t.Setenv(corsAllowedOriginsEnv, "")
	gin.SetMode(gin.TestMode)

	router := gin.New()
	router.Use(func(c *gin.Context) {
		if handleDynamicCORS(c) {
			return
		}
		c.Status(http.StatusNoContent)
	})

	req := httptest.NewRequest(http.MethodOptions, "https://monitor.example.com/api/public", nil)
	req.Host = "monitor.example.com"
	req.Header.Set("Origin", "https://evil.example.com")
	recorder := httptest.NewRecorder()

	router.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusForbidden {
		t.Fatalf("expected HTTP 403, got %d", recorder.Code)
	}
	if got := recorder.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Fatalf("expected unlisted origin to receive no CORS allow header, got %q", got)
	}
}

func TestCORSRejectsUnlistedCrossOriginRequest(t *testing.T) {
	t.Setenv(corsAllowedOriginsEnv, "")
	gin.SetMode(gin.TestMode)

	router := gin.New()
	router.Use(func(c *gin.Context) {
		if handleDynamicCORS(c) {
			return
		}
		c.Status(http.StatusNoContent)
	})
	router.POST("/api/admin/action", func(c *gin.Context) {
		c.Status(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodPost, "https://monitor.example.com/api/admin/action", nil)
	req.Host = "monitor.example.com"
	req.Header.Set("Origin", "https://evil.example.com")
	recorder := httptest.NewRecorder()

	router.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusForbidden {
		t.Fatalf("expected HTTP 403, got %d", recorder.Code)
	}
}

func TestCORSAllowsConfiguredCrossOrigin(t *testing.T) {
	t.Setenv(corsAllowedOriginsEnv, "https://admin.example.com, https://app.example.com")
	gin.SetMode(gin.TestMode)

	router := gin.New()
	router.Use(func(c *gin.Context) {
		if handleDynamicCORS(c) {
			return
		}
		c.Status(http.StatusNoContent)
	})
	router.GET("/api/public", func(c *gin.Context) {
		c.Status(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "https://monitor.example.com/api/public", nil)
	req.Host = "monitor.example.com"
	req.Header.Set("Origin", "https://app.example.com")
	recorder := httptest.NewRecorder()

	router.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200, got %d", recorder.Code)
	}
	if got := recorder.Header().Get("Access-Control-Allow-Origin"); got != "https://app.example.com" {
		t.Fatalf("expected configured origin to be allowed, got %q", got)
	}
}
