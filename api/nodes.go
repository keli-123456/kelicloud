package api

import (
	"github.com/gin-gonic/gin"
	"github.com/komari-monitor/komari/database/accounts"
	"github.com/komari-monitor/komari/database/clients"
	"github.com/komari-monitor/komari/database/models"
)

func GetNodesInformation(c *gin.Context) {
	user, ok := RequireSessionUser(c)
	if !ok {
		return
	}

	var (
		clientList []models.Client
		err        error
	)
	if accounts.IsUserRoleAtLeast(user.Role, accounts.RoleAdmin) {
		clientList, err = clients.GetAllClientBasicInfo()
	} else {
		clientList, err = clients.GetAllClientBasicInfoByUser(user.UUID)
	}
	if err != nil {
		RespondError(c, 500, "Failed to retrieve client information: "+err.Error())
		return
	}

	for index := range clientList {
		clientList[index].Token = ""
	}

	RespondSuccess(c, clientList)
}
