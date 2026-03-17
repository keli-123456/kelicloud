package admin

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/komari-monitor/komari/api"
	"github.com/komari-monitor/komari/config"
)

func RequireUserFeatureMiddleware(feature string) gin.HandlerFunc {
	return func(c *gin.Context) {
		if _, ok := c.Get("api_key"); ok {
			c.Next()
			return
		}
		if allowed, err := isPlatformAdmin(c); err == nil && allowed {
			c.Next()
			return
		}

		userUUID, ok := currentUserUUID(c)
		if !ok {
			api.RespondError(c, http.StatusForbidden, "User context is required")
			c.Abort()
			return
		}

		allowed, err := config.IsUserFeatureAllowed(userUUID, feature)
		if err != nil {
			api.RespondError(c, http.StatusInternalServerError, "Failed to resolve user feature access: "+err.Error())
			c.Abort()
			return
		}
		if !allowed {
			api.RespondError(c, http.StatusForbidden, "Feature is disabled for this user")
			c.Abort()
			return
		}

		c.Next()
	}
}
