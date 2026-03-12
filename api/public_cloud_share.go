package api

import (
	"errors"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/komari-monitor/komari/database"
	"github.com/komari-monitor/komari/utils/cloudshare"
	"gorm.io/gorm"
)

func GetPublicCloudInstanceShare(c *gin.Context) {
	token := strings.TrimSpace(c.Param("token"))
	if token == "" {
		RespondError(c, http.StatusBadRequest, "Invalid cloud share token")
		return
	}

	share, err := database.GetCloudInstanceShareByToken(token)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			RespondError(c, http.StatusNotFound, "Cloud share not found")
			return
		}
		RespondError(c, http.StatusInternalServerError, "Failed to load cloud share")
		return
	}

	view, err := cloudshare.ResolvePublicShare(share)
	if err != nil {
		switch {
		case errors.Is(err, cloudshare.ErrInvalidReference):
			RespondError(c, http.StatusBadRequest, err.Error())
		case errors.Is(err, cloudshare.ErrInstanceNotFound), errors.Is(err, cloudshare.ErrCredentialNotFound):
			RespondError(c, http.StatusNotFound, err.Error())
		default:
			RespondError(c, http.StatusInternalServerError, err.Error())
		}
		return
	}

	RespondSuccess(c, view)
}
