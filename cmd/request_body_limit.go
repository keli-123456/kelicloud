package cmd

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/komari-monitor/komari/utils"
)

const (
	httpMaxBodyBytesEnv         = "KOMARI_HTTP_MAX_BODY_BYTES"
	defaultHTTPMaxBodyBytes int = 4 << 20
)

func applyRequestBodyLimit() gin.HandlerFunc {
	return func(c *gin.Context) {
		if shouldSkipRequestBodyLimit(c.Request) {
			c.Next()
			return
		}

		limit := currentMaxBodyBytes()
		if c.Request.ContentLength > int64(limit) {
			c.AbortWithStatusJSON(http.StatusRequestEntityTooLarge, gin.H{
				"status":  "error",
				"message": "Request body is too large",
			})
			return
		}

		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, int64(limit))
		c.Next()
	}
}

func currentMaxBodyBytes() int {
	return utils.IntFromEnv(httpMaxBodyBytesEnv, defaultHTTPMaxBodyBytes, 16*1024, 128<<20)
}

func shouldSkipRequestBodyLimit(r *http.Request) bool {
	if r == nil || r.Body == nil {
		return true
	}
	switch r.Method {
	case http.MethodPost, http.MethodPut, http.MethodPatch:
	default:
		return true
	}
	switch r.URL.Path {
	case "/api/admin/upload/backup", "/api/admin/update/favicon":
		return true
	default:
		return false
	}
}
