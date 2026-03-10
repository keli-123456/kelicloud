package client

import (
	"net/url"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/komari-monitor/komari/api"
	"github.com/komari-monitor/komari/config"
	"github.com/komari-monitor/komari/database/clients"
	"github.com/komari-monitor/komari/utils"
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
	baseKey := rawKey
	group := ""
	if strings.Contains(rawKey, groupMarker) {
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
	AutoDiscoveryKey, err := config.GetAs[string](config.AutoDiscoveryKeyKey, "")
	if err != nil {
		api.RespondError(c, 500, "Failed to get AutoDiscovery Key: "+err.Error())
		return
	}
	if AutoDiscoveryKey == "" ||
		len(AutoDiscoveryKey) < 12 ||
		!strings.HasPrefix(auth, "Bearer ") {

		api.RespondError(c, 403, "Invalid AutoDiscovery Key")
		return
	}
	requestKey, group, ok := parseAutoDiscoveryAuthorization(auth)
	if !ok || requestKey != AutoDiscoveryKey {
		api.RespondError(c, 403, "Invalid AutoDiscovery Key")
		return
	}
	name := c.Query("name")
	if name == "" {
		name = utils.GenerateRandomString(8)
	}
	name = "Auto-" + name
	uuid, token, err := clients.CreateClientWithNameAndGroup(name, group)
	if err != nil {
		api.RespondError(c, 500, "Failed to create client: "+err.Error())
		return
	}
	api.RespondSuccess(c, gin.H{"uuid": uuid, "token": token})
}
