package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/komari-monitor/komari/database/clients"
	"gorm.io/gorm"

	"github.com/gin-gonic/gin"
)

// TokenAuthMiddleware creates a Gin middleware that validates a token from query parameters or request body.
func TokenAuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// API key authentication
		apiKey := c.GetHeader("Authorization")
		if isApiKeyValid(apiKey) {
			c.Set("api_key", apiKey)
			c.Next()
			return
		}

		var token string

		// Step 1: Check query parameter for token
		token = c.Query("token")

		// Step 2: If no token in query, check request body for non-GET requests
		if token == "" && c.Request.Method != http.MethodGet {
			// Read the body
			bodyBytes, err := io.ReadAll(c.Request.Body)
			if err != nil {
				c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"status": "error", "error": "failed to read request body"})
				return
			}

			// Restore the body for downstream handlers
			c.Request.Body = io.NopCloser(bytes.NewReader(bodyBytes))

			// Try to extract token from JSON body
			var bodyMap map[string]interface{}
			if len(bodyBytes) > 0 {
				if err := json.Unmarshal(bodyBytes, &bodyMap); err == nil {
					if tokenVal, exists := bodyMap["token"]; exists {
						if str, ok := tokenVal.(string); ok && str != "" {
							token = str
						}
					}
				}
			}
		}

		if token == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"status": "error", "error": "token is required"})
			return
		}

		client, err := clients.GetClientByToken(token)
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"status": "error", "error": "invalid token"})
				return
			}
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"status": "error", "error": "failed to validate token"})
			return
		}

		c.Set("client_uuid", client.UUID)
		c.Set("user_id", client.UserID)
		c.Next()
	}
}
