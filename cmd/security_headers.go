package cmd

import (
	"net/http"
	"os"
	"strings"

	"github.com/gin-gonic/gin"
)

const securityHSTSEnv = "KOMARI_SECURITY_HSTS"

func applySecurityHeaders() gin.HandlerFunc {
	return func(c *gin.Context) {
		setSecurityHeaders(c)
		c.Next()
	}
}

func setSecurityHeaders(c *gin.Context) {
	c.Header("X-Content-Type-Options", "nosniff")
	c.Header("X-Frame-Options", "SAMEORIGIN")
	c.Header("Referrer-Policy", "no-referrer")
	c.Header("Permissions-Policy", "camera=(), microphone=(), geolocation=()")

	if securityHSTSEnabled() && requestIsHTTPS(c.Request) {
		c.Header("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
	}
}

func securityHSTSEnabled() bool {
	return strings.EqualFold(strings.TrimSpace(os.Getenv(securityHSTSEnv)), "true")
}

func requestIsHTTPS(r *http.Request) bool {
	if r == nil {
		return false
	}
	if r.TLS != nil {
		return true
	}
	return firstForwardedHeaderValue(r.Header.Get("X-Forwarded-Proto")) == "https" ||
		firstForwardedHeaderValue(r.Header.Get("X-Forwarded-Ssl")) == "on"
}

func firstForwardedHeaderValue(value string) string {
	if before, _, ok := strings.Cut(value, ","); ok {
		value = before
	}
	return strings.ToLower(strings.TrimSpace(value))
}
