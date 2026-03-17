package admin

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/komari-monitor/komari/api"
	"github.com/komari-monitor/komari/database"
	"github.com/komari-monitor/komari/database/models"
	"github.com/komari-monitor/komari/utils/cloudshare"
)

type ownerScope struct {
	UserUUID string
}

func (scope ownerScope) HasUser() bool {
	return strings.TrimSpace(scope.UserUUID) != ""
}

func currentOwnerScope(c *gin.Context) (ownerScope, bool) {
	userUUID, _ := currentUserUUID(c)
	if userUUID == "" {
		return ownerScope{}, false
	}
	return ownerScope{
		UserUUID: strings.TrimSpace(userUUID),
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

func getCloudProviderConfigForScope(scope ownerScope, providerName string) (*models.CloudProvider, error) {
	return database.GetCloudProviderConfigByUserAndName(scope.UserUUID, providerName)
}

func saveCloudProviderConfigForScope(scope ownerScope, providerName, addition string) error {
	config := &models.CloudProvider{
		UserID:   scope.UserUUID,
		Name:     providerName,
		Addition: addition,
	}
	return database.SaveCloudProviderConfigForUser(config)
}

func getCloudInstanceShareForScope(scope ownerScope, provider, resourceType, resourceID string) (*models.CloudInstanceShare, error) {
	return database.GetCloudInstanceShareByUser(scope.UserUUID, provider, resourceType, resourceID)
}

func resolveCloudResourceForScope(scope ownerScope, provider, resourceType, resourceID string) (*cloudshare.AdminResourceState, error) {
	return cloudshare.ResolveActiveResourceForUser(scope.UserUUID, provider, resourceType, resourceID)
}
