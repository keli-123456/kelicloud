package cmd

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestRequestBodyLimitRejectsOversizedBody(t *testing.T) {
	t.Setenv(httpMaxBodyBytesEnv, "16384")
	gin.SetMode(gin.TestMode)

	router := gin.New()
	router.Use(applyRequestBodyLimit())
	router.POST("/api/admin/settings", func(c *gin.Context) {
		c.Status(http.StatusNoContent)
	})

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/admin/settings", strings.NewReader(strings.Repeat("a", 16385)))
	router.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected HTTP 413, got %d", recorder.Code)
	}
}

func TestRequestBodyLimitAllowsBodyAtLimit(t *testing.T) {
	t.Setenv(httpMaxBodyBytesEnv, "16384")
	gin.SetMode(gin.TestMode)

	router := gin.New()
	router.Use(applyRequestBodyLimit())
	router.POST("/api/admin/settings", func(c *gin.Context) {
		c.Status(http.StatusNoContent)
	})

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/admin/settings", strings.NewReader(strings.Repeat("a", 16384)))
	router.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusNoContent {
		t.Fatalf("expected HTTP 204, got %d", recorder.Code)
	}
}

func TestRequestBodyLimitSkipsBackupUpload(t *testing.T) {
	t.Setenv(httpMaxBodyBytesEnv, "32")
	gin.SetMode(gin.TestMode)

	router := gin.New()
	router.Use(applyRequestBodyLimit())
	router.POST("/api/admin/upload/backup", func(c *gin.Context) {
		c.Status(http.StatusNoContent)
	})

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/admin/upload/backup", strings.NewReader(strings.Repeat("a", 33)))
	router.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusNoContent {
		t.Fatalf("expected backup upload to bypass global body limit, got %d", recorder.Code)
	}
}

func TestCurrentMaxBodyBytesUsesSafeBounds(t *testing.T) {
	t.Setenv(httpMaxBodyBytesEnv, "8")
	if got := currentMaxBodyBytes(); got != defaultHTTPMaxBodyBytes {
		t.Fatalf("expected too-small limit to fall back to default, got %d", got)
	}

	t.Setenv(httpMaxBodyBytesEnv, "268435456")
	if got := currentMaxBodyBytes(); got != 128<<20 {
		t.Fatalf("expected too-large limit to clamp to max, got %d", got)
	}
}
