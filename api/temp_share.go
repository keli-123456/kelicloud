package api

import (
	"time"

	"github.com/gin-gonic/gin"
	"github.com/komari-monitor/komari/config"
)

func hasValidTempShareAccess(c *gin.Context) bool {
	tempKey, err := c.Cookie("temp_key")
	if err != nil {
		return false
	}

	_, ok, err := config.ResolveValidTempShareUserUUID(tempKey, time.Now())
	if err != nil {
		return false
	}
	return ok
}
