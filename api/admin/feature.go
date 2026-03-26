package admin

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/komari-monitor/komari/api"
	"github.com/komari-monitor/komari/config"
)

func IsCurrentUserFeatureAllowed(c *gin.Context, feature string) (bool, error) {
	if _, ok := c.Get("api_key"); ok {
		return true, nil
	}
	if allowed, err := isPlatformAdmin(c); err == nil && allowed {
		return true, nil
	}

	userUUID, ok := currentUserUUID(c)
	if !ok {
		return false, nil
	}

	return config.IsUserFeatureAllowed(userUUID, feature)
}

func RequireUserFeatureMiddleware(feature string) gin.HandlerFunc {
	return RequireAnyUserFeatureMiddleware(feature)
}

func RequireAnyUserFeatureMiddleware(features ...string) gin.HandlerFunc {
	normalized := make([]string, 0, len(features))
	for _, feature := range features {
		value := strings.TrimSpace(feature)
		if value == "" {
			continue
		}
		normalized = append(normalized, value)
	}

	return func(c *gin.Context) {
		if len(normalized) == 0 {
			c.Next()
			return
		}

		for _, feature := range normalized {
			allowed, err := IsCurrentUserFeatureAllowed(c, feature)
			if err != nil {
				api.RespondError(c, http.StatusInternalServerError, "Failed to resolve user feature access: "+err.Error())
				c.Abort()
				return
			}
			if allowed {
				c.Next()
				return
			}
		}

		if _, ok := currentUserUUID(c); !ok {
			api.RespondError(c, http.StatusForbidden, "User context is required")
			c.Abort()
			return
		}

		api.RespondError(c, http.StatusForbidden, "Feature is disabled for this user")
		c.Abort()
	}
}
