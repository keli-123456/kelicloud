package admin

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestRequireCurrentOwnerScopeRejectsMissingUserContext(t *testing.T) {
	gin.SetMode(gin.TestMode)

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet, "/", nil)

	scope, ok := requireCurrentOwnerScope(c)
	if ok {
		t.Fatalf("expected missing user context to be rejected, got %+v", scope)
	}
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for missing user context, got %d", recorder.Code)
	}
}

func TestRequireCurrentOwnerScopeAcceptsUserContext(t *testing.T) {
	gin.SetMode(gin.TestMode)

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet, "/", nil)
	c.Set("uuid", "user-a")

	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		t.Fatal("expected user context to be accepted")
	}
	if scope.UserUUID != "user-a" {
		t.Fatalf("unexpected owner scope: %+v", scope)
	}
}
