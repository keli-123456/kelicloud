package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func newAPITestContext(t *testing.T) (*gin.Context, *httptest.ResponseRecorder) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	configureAPITestDB()
	recorder := httptest.NewRecorder()
	context, _ := gin.CreateTestContext(recorder)
	context.Request = httptest.NewRequest(http.MethodGet, "/", nil)
	return context, recorder
}

func TestRequireUserScopeFromSessionRejectsGuest(t *testing.T) {
	context, recorder := newAPITestContext(t)

	userUUID, ok := RequireUserScopeFromSession(context)
	if ok {
		t.Fatalf("expected missing session to be rejected, got user %q", userUUID)
	}
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("expected HTTP 401, got %d", recorder.Code)
	}
}

func TestGetNodesInformationRejectsGuest(t *testing.T) {
	context, recorder := newAPITestContext(t)

	GetNodesInformation(context)

	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("expected HTTP 401, got %d", recorder.Code)
	}
}
