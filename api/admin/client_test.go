package admin

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestListClientsRequiresUserContext(t *testing.T) {
	gin.SetMode(gin.TestMode)

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet, "/admin/clients", nil)

	ListClients(c)

	if recorder.Code != http.StatusForbidden {
		t.Fatalf("expected 403 when user context is missing, got %d", recorder.Code)
	}
}
