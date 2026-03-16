package api

import (
	"errors"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/komari-monitor/komari/database"
	"github.com/komari-monitor/komari/database/accounts"
	"gorm.io/gorm"
)

func GetTenantInvite(c *gin.Context) {
	token := strings.TrimSpace(c.Param("token"))
	if token == "" {
		RespondError(c, http.StatusBadRequest, "Invite token is required")
		return
	}

	invite, err := database.GetTenantInviteByToken(token)
	if err != nil {
		switch {
		case errors.Is(err, gorm.ErrRecordNotFound):
			RespondError(c, http.StatusNotFound, "Tenant invite not found")
		case errors.Is(err, database.ErrTenantInviteExpired):
			RespondError(c, http.StatusGone, "Tenant invite expired")
		case errors.Is(err, database.ErrTenantInviteUnavailable):
			RespondError(c, http.StatusGone, "Tenant invite is no longer available")
		default:
			RespondError(c, http.StatusInternalServerError, "Failed to load tenant invite: "+err.Error())
		}
		return
	}

	RespondSuccess(c, invite)
}

func AcceptTenantInvite(c *gin.Context) {
	token := strings.TrimSpace(c.Param("token"))
	if token == "" {
		RespondError(c, http.StatusBadRequest, "Invite token is required")
		return
	}

	session, err := c.Cookie("session_token")
	if err != nil {
		RespondError(c, http.StatusUnauthorized, "Login is required")
		return
	}
	sessionRecord, err := accounts.GetSessionRecord(session)
	if err != nil {
		RespondError(c, http.StatusUnauthorized, "Unauthorized.")
		return
	}

	tenant, err := database.AcceptTenantInvite(token, sessionRecord.UUID)
	if err != nil {
		switch {
		case errors.Is(err, gorm.ErrRecordNotFound):
			RespondError(c, http.StatusNotFound, "Tenant invite not found")
		case errors.Is(err, database.ErrTenantInviteExpired):
			RespondError(c, http.StatusGone, "Tenant invite expired")
		case errors.Is(err, database.ErrTenantInviteUnavailable):
			RespondError(c, http.StatusGone, "Tenant invite is no longer available")
		default:
			RespondError(c, http.StatusInternalServerError, "Failed to accept tenant invite: "+err.Error())
		}
		return
	}

	if tenant != nil {
		_ = accounts.SetSessionCurrentTenant(session, tenant.ID)
	}

	RespondSuccess(c, gin.H{"current": tenant})
}
