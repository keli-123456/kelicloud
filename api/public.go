package api

import (
	"time"

	"github.com/gin-gonic/gin"
	"github.com/komari-monitor/komari/config"
	"github.com/komari-monitor/komari/database"
)

func GetPublicSettings(c *gin.Context) {
	tenantID, _, resolveErr := ResolveTenantScopeFromSession(c)
	if resolveErr != nil {
		RespondError(c, 500, resolveErr.Error())
		return
	}

	p, e := database.GetPublicInfo(tenantID)
	if e != nil {
		RespondError(c, 500, e.Error())
		return
	}
	// 临时访问许可
	if func() bool {
		tempKey, err := c.Cookie("temp_key")
		if err != nil {
			return false
		}

		tempKeyExpireTime, err := config.GetAsForTenant[int64](tenantID, config.TempShareTokenExpireAtKey, 0)
		if err != nil {
			return false
		}
		allowTempKey, err := config.GetAsForTenant[string](tenantID, config.TempShareTokenKey, "")
		if err != nil {
			return false
		}

		if allowTempKey == "" || tempKey != allowTempKey {
			return false
		}
		now := time.Now().Unix()
		if tempKeyExpireTime < now {
			return false
		}

		return true
	}() {
		p["private_site"] = false
	}
	RespondSuccess(c, p)
}
