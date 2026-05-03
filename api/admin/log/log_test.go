package log

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/komari-monitor/komari/database/accounts"
)

func newLogTestContext(method, target string) (*gin.Context, *httptest.ResponseRecorder) {
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(method, target, nil)
	return c, recorder
}

func TestGetLogsRequiresUserContext(t *testing.T) {
	gin.SetMode(gin.TestMode)

	c, recorder := newLogTestContext(http.MethodGet, "/admin/logs?limit=10&page=1")

	GetLogs(c)

	if recorder.Code != http.StatusForbidden {
		t.Fatalf("expected 403 when user context is missing, got %d", recorder.Code)
	}
}

func TestGetLogsRejectsAllScopeForRegularUser(t *testing.T) {
	gin.SetMode(gin.TestMode)

	c, recorder := newLogTestContext(http.MethodGet, "/admin/logs?limit=10&page=1&scope=all")
	c.Set("uuid", "user-a")
	c.Set("user_role", accounts.RoleUser)

	GetLogs(c)

	if recorder.Code != http.StatusForbidden {
		t.Fatalf("expected 403 when a regular user requests all logs, got %d", recorder.Code)
	}
}

func TestWantsAllLogsAcceptsScopeAndAllFlag(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tests := []struct {
		name     string
		target   string
		expected bool
	}{
		{name: "scope all", target: "/admin/logs?scope=all", expected: true},
		{name: "all true", target: "/admin/logs?all=true", expected: true},
		{name: "all one", target: "/admin/logs?all=1", expected: true},
		{name: "default self", target: "/admin/logs", expected: false},
		{name: "user scope", target: "/admin/logs?scope=user", expected: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, _ := newLogTestContext(http.MethodGet, tt.target)
			if got := wantsAllLogs(c); got != tt.expected {
				t.Fatalf("expected wantsAllLogs=%v, got %v", tt.expected, got)
			}
		})
	}
}

func TestIsPlatformAdminRequestRecognizesRoleAndAPIKey(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tests := []struct {
		name     string
		setup    func(*gin.Context)
		expected bool
	}{
		{
			name: "admin role",
			setup: func(c *gin.Context) {
				c.Set("user_role", accounts.RoleAdmin)
			},
			expected: true,
		},
		{
			name: "api key",
			setup: func(c *gin.Context) {
				c.Set("api_key", "secret")
			},
			expected: true,
		},
		{
			name: "regular user",
			setup: func(c *gin.Context) {
				c.Set("user_role", accounts.RoleUser)
			},
			expected: false,
		},
		{
			name:     "missing context",
			setup:    func(*gin.Context) {},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, _ := newLogTestContext(http.MethodGet, "/admin/logs")
			tt.setup(c)
			if got := isPlatformAdminRequest(c); got != tt.expected {
				t.Fatalf("expected isPlatformAdminRequest=%v, got %v", tt.expected, got)
			}
		})
	}
}
