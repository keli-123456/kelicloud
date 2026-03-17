package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/komari-monitor/komari/database/dbcore"
)

func TestRequireLoginForPanelDataMiddlewareRejectsGuestPanelData(t *testing.T) {
	gin.SetMode(gin.TestMode)
	configureAPITestDB()
	_ = dbcore.GetDBInstance()

	router := gin.New()
	router.Use(RequireLoginForPanelDataMiddleware())
	router.GET("/api/records/load", func(c *gin.Context) {
		c.Status(http.StatusNoContent)
	})

	req := httptest.NewRequest(http.MethodGet, "/api/records/load", nil)
	recorder := httptest.NewRecorder()

	router.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("expected HTTP 401, got %d", recorder.Code)
	}
}

func TestRequireLoginForPanelDataMiddlewareAllowsLoginRoute(t *testing.T) {
	gin.SetMode(gin.TestMode)
	configureAPITestDB()
	_ = dbcore.GetDBInstance()

	router := gin.New()
	router.Use(RequireLoginForPanelDataMiddleware())
	router.GET("/api/login", func(c *gin.Context) {
		c.Status(http.StatusNoContent)
	})

	req := httptest.NewRequest(http.MethodGet, "/api/login", nil)
	recorder := httptest.NewRecorder()

	router.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusNoContent {
		t.Fatalf("expected HTTP 204, got %d", recorder.Code)
	}
}
