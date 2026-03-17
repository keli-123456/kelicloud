package api

import (
	"net/http"
	"strings"

	"github.com/komari-monitor/komari/database/accounts"

	"github.com/gin-gonic/gin"
)

var (
	publicExactPaths = map[string]struct{}{
		"/ping":               {},
		"/api/login":          {},
		"/api/me":             {},
		"/api/oauth":          {},
		"/api/oauth_callback": {},
		"/api/logout":         {},
		"/api/version":        {},
		"/api/public":         {},
	}
	publicPrefixPaths = []string{
		"/api/admin",    // 由AdminAuthMiddleware处理
		"/api/clients/", // 由TokenAuthMiddleware处理
		"/api/public/cloud/shares/",
	}
)

func isPublicPath(path string) bool {
	if _, ok := publicExactPaths[path]; ok {
		return true
	}
	for _, prefix := range publicPrefixPaths {
		if strings.HasPrefix(path, prefix) {
			return true
		}
	}
	return false
}

func RequireLoginForPanelDataMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// API key authentication
		apiKey := c.GetHeader("Authorization")
		if isApiKeyValid(apiKey) {
			c.Set("api_key", apiKey)
			c.Next()
			return
		}
		// 如果是公开的路径，直接放行
		if isPublicPath(c.Request.URL.Path) {
			c.Next()
			return
		}
		// 如果不是 /api，直接放行
		if !strings.HasPrefix(c.Request.URL.Path, "/api") {
			c.Next()
			return
		}
		session, err := c.Cookie("session_token")
		if err != nil {
			RespondError(c, http.StatusUnauthorized, "Login required")
			c.Abort()
			return
		}
		_, err = accounts.GetSession(session)
		if err != nil {
			RespondError(c, http.StatusUnauthorized, "Unauthorized.")
			c.Abort()
			return
		}

		c.Next()
	}
}
