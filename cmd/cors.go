package cmd

import (
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/gin-gonic/gin"
)

const corsAllowedOriginsEnv = "KOMARI_CORS_ALLOWED_ORIGINS"

func handleDynamicCORS(c *gin.Context) bool {
	origin := strings.TrimSpace(c.GetHeader("Origin"))
	if origin == "" {
		if c.Request.Method == http.MethodOptions {
			c.AbortWithStatus(http.StatusNoContent)
			return true
		}
		return false
	}

	if !isCORSOriginAllowed(c.Request, origin) {
		c.AbortWithStatus(http.StatusForbidden)
		return true
	}

	c.Header("Access-Control-Allow-Origin", normalizedOrigin(origin))
	c.Header("Vary", "Origin")
	c.Header("Access-Control-Allow-Credentials", "true")
	c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, HEAD, OPTIONS")
	c.Header("Access-Control-Allow-Headers", "Origin, Content-Length, Content-Type, Authorization, Accept, X-CSRF-Token, X-Requested-With, Set-Cookie")
	c.Header("Access-Control-Expose-Headers", "Content-Length, Authorization, Set-Cookie")
	c.Header("Access-Control-Max-Age", "43200")

	if c.Request.Method == http.MethodOptions {
		c.AbortWithStatus(http.StatusNoContent)
		return true
	}
	return false
}

func isCORSOriginAllowed(r *http.Request, origin string) bool {
	origin = normalizedOrigin(origin)
	if origin == "" {
		return false
	}
	if isSameHostOrigin(r, origin) {
		return true
	}

	for _, allowed := range configuredCORSOrigins() {
		if allowed == "*" || allowed == origin {
			return true
		}
	}
	return false
}

func configuredCORSOrigins() []string {
	raw := strings.TrimSpace(os.Getenv(corsAllowedOriginsEnv))
	if raw == "" {
		return nil
	}

	parts := strings.Split(raw, ",")
	origins := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if part == "*" {
			origins = append(origins, part)
			continue
		}
		if normalized := normalizedOrigin(part); normalized != "" {
			origins = append(origins, normalized)
		}
	}
	return origins
}

func isSameHostOrigin(r *http.Request, origin string) bool {
	parsed, err := url.Parse(origin)
	if err != nil || parsed.Host == "" {
		return false
	}
	return strings.EqualFold(parsed.Host, r.Host)
}

func normalizedOrigin(origin string) string {
	parsed, err := url.Parse(strings.TrimSpace(origin))
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return ""
	}
	scheme := strings.ToLower(parsed.Scheme)
	if scheme != "http" && scheme != "https" {
		return ""
	}
	return scheme + "://" + strings.ToLower(parsed.Host)
}
