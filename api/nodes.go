package api

import (
	"github.com/gin-gonic/gin"
	"github.com/komari-monitor/komari/database/clients"
)

func GetNodesInformation(c *gin.Context) {
	userUUID, ok := RequireUserScopeFromSession(c)
	if !ok {
		return
	}

	clientList, err := clients.GetAllClientBasicInfoByUser(userUUID)
	if err != nil {
		RespondError(c, 500, "Failed to retrieve client information: "+err.Error())
		return
	}

	for index := range clientList {
		clientList[index].Token = ""
	}

	RespondSuccess(c, clientList)
}
