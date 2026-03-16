package client

import (
	"encoding/base64"
	"errors"
	"net/url"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/komari-monitor/komari/api"
	"github.com/komari-monitor/komari/config"
	"github.com/komari-monitor/komari/database"
	"github.com/komari-monitor/komari/database/clients"
	"github.com/komari-monitor/komari/utils"
	"gorm.io/gorm"
)

func parseAutoDiscoveryAuthorization(authHeader string) (string, string, bool) {
	if !strings.HasPrefix(authHeader, "Bearer ") {
		return "", "", false
	}

	rawKey := strings.TrimSpace(strings.TrimPrefix(authHeader, "Bearer "))
	if rawKey == "" {
		return "", "", false
	}

	const groupMarker = "::group="
	const groupBase64Marker = "::group-b64="
	baseKey := rawKey
	group := ""
	switch {
	case strings.Contains(rawKey, groupBase64Marker):
		parts := strings.SplitN(rawKey, groupBase64Marker, 2)
		baseKey = strings.TrimSpace(parts[0])
		if len(parts) == 2 {
			decoded, err := base64.RawURLEncoding.DecodeString(parts[1])
			if err == nil {
				group = strings.TrimSpace(string(decoded))
			}
		}
	case strings.Contains(rawKey, groupMarker):
		parts := strings.SplitN(rawKey, groupMarker, 2)
		baseKey = strings.TrimSpace(parts[0])
		if len(parts) == 2 {
			decoded, err := url.QueryUnescape(parts[1])
			if err == nil {
				group = strings.TrimSpace(decoded)
			}
		}
	}

	if len(group) > 100 {
		group = group[:100]
	}

	return baseKey, group, true
}

func RegisterClient(c *gin.Context) {
	auth := c.GetHeader("Authorization")
	if auth == "" {
		api.RespondError(c, 403, "Invalid AutoDiscovery Key")
		return
	}
	if !strings.HasPrefix(auth, "Bearer ") {
		api.RespondError(c, 403, "Invalid AutoDiscovery Key")
		return
	}
	requestKey, group, ok := parseAutoDiscoveryAuthorization(auth)
	if !ok || len(strings.TrimSpace(requestKey)) < 12 {
		api.RespondError(c, 403, "Invalid AutoDiscovery Key")
		return
	}

	tenantID, err := config.FindTenantIDByConfigValue(config.AutoDiscoveryKeyKey, requestKey)
	if err != nil {
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			api.RespondError(c, 500, "Failed to resolve auto discovery tenant: "+err.Error())
			return
		}
		legacyKey, legacyErr := config.GetAs[string](config.AutoDiscoveryKeyKey, "")
		if legacyErr != nil {
			api.RespondError(c, 500, "Failed to get AutoDiscovery Key: "+legacyErr.Error())
			return
		}
		if requestKey != legacyKey || len(strings.TrimSpace(legacyKey)) < 12 {
			api.RespondError(c, 403, "Invalid AutoDiscovery Key")
			return
		}

		defaultTenantID, defaultErr := database.GetDefaultTenantID()
		if defaultErr != nil {
			api.RespondError(c, 500, "Failed to resolve auto discovery tenant: "+defaultErr.Error())
			return
		}
		tenantID = defaultTenantID
	}

	name := c.Query("name")
	if name == "" {
		name = utils.GenerateRandomString(8)
	}
	name = "Auto-" + name

	uuid, token, err := clients.CreateClientWithNameAndGroupForTenant(tenantID, name, group)
	if err != nil {
		api.RespondError(c, 500, "Failed to create client: "+err.Error())
		return
	}
	api.RespondSuccess(c, gin.H{"uuid": uuid, "token": token})
}
