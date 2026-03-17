package notification

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/komari-monitor/komari/api"
)

type ownerScope struct {
	UserUUID string
}

func (scope ownerScope) HasUser() bool {
	return strings.TrimSpace(scope.UserUUID) != ""
}

func currentOwnerScope(c *gin.Context) (ownerScope, bool) {
	userUUID, _ := c.Get("uuid")

	userValue, _ := userUUID.(string)
	if strings.TrimSpace(userValue) == "" {
		return ownerScope{}, false
	}

	return ownerScope{
		UserUUID: strings.TrimSpace(userValue),
	}, true
}

func requireCurrentOwnerScope(c *gin.Context) (ownerScope, bool) {
	scope, ok := currentOwnerScope(c)
	if !ok {
		api.RespondError(c, http.StatusForbidden, "User context is required")
		return ownerScope{}, false
	}
	return scope, true
}
