package update

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestUpdateUserRejectsSelfPolicyUpdate(t *testing.T) {
	gin.SetMode(gin.TestMode)

	recorder := httptest.NewRecorder()
	context, _ := gin.CreateTestContext(recorder)
	context.Request = httptest.NewRequest(
		http.MethodPost,
		"/api/admin/update/user",
		strings.NewReader(`{"uuid":"user-a","server_quota":99,"plan_name":"Business"}`),
	)
	context.Request.Header.Set("Content-Type", "application/json")
	context.Set("uuid", "user-a")

	UpdateUser(context)

	if recorder.Code != http.StatusForbidden {
		t.Fatalf("expected 403 when a user updates their own policy, got %d", recorder.Code)
	}
}
