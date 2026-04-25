package api

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

func SetSecureCookie(c *gin.Context, name, value string, maxAge int) {
	c.SetSameSite(http.SameSiteLaxMode)
	c.SetCookie(name, value, maxAge, "/", "", requestIsHTTPS(c), true)
}

func ClearSecureCookie(c *gin.Context, name string) {
	SetSecureCookie(c, name, "", -1)
}

func requestIsHTTPS(c *gin.Context) bool {
	if c.Request != nil && c.Request.TLS != nil {
		return true
	}
	return firstForwardedHeaderValue(c.GetHeader("X-Forwarded-Proto")) == "https" ||
		firstForwardedHeaderValue(c.GetHeader("X-Forwarded-Ssl")) == "on"
}

func firstForwardedHeaderValue(value string) string {
	if before, _, ok := strings.Cut(value, ","); ok {
		value = before
	}
	return strings.ToLower(strings.TrimSpace(value))
}
