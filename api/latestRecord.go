package api

import (
	"time"

	"github.com/gin-gonic/gin"
	"github.com/komari-monitor/komari/common"
	"github.com/komari-monitor/komari/database/accounts"
	"github.com/komari-monitor/komari/database/clients"
)

type clientRecentRecordDTO struct {
	UUID           string                       `json:"uuid,omitempty"`
	CPU            common.CPUReport             `json:"cpu"`
	Ram            common.RamReport             `json:"ram"`
	Swap           common.RamReport             `json:"swap"`
	Load           common.LoadReport            `json:"load"`
	Disk           common.DiskReport            `json:"disk"`
	Network        common.NetworkReport         `json:"network"`
	Connections    common.ConnectionsReport     `json:"connections"`
	GPU            *common.GPUDetailReport      `json:"gpu,omitempty"`
	Uptime         int64                        `json:"uptime"`
	Process        int                          `json:"process"`
	Message        string                       `json:"message"`
	CNConnectivity *common.CNConnectivityReport `json:"cn_connectivity,omitempty"`
	Method         string                       `json:"method,omitempty"`
	Time           time.Time                    `json:"time"`
}

func buildClientRecentRecordsDTO(records []common.Report) []clientRecentRecordDTO {
	result := make([]clientRecentRecordDTO, 0, len(records))
	for _, record := range records {
		result = append(result, clientRecentRecordDTO{
			UUID:           record.UUID,
			CPU:            record.CPU,
			Ram:            record.Ram,
			Swap:           record.Swap,
			Load:           record.Load,
			Disk:           record.Disk,
			Network:        record.Network,
			Connections:    record.Connections,
			GPU:            record.GPU,
			Uptime:         record.Uptime,
			Process:        record.Process,
			Message:        record.Message,
			CNConnectivity: record.CNConnectivity,
			Method:         record.Method,
			Time:           record.UpdatedAt,
		})
	}
	return result
}

func GetClientRecentRecords(c *gin.Context) {
	uuid := c.Param("uuid")

	if uuid == "" {
		RespondError(c, 400, "UUID is required")
		return
	}

	user, ok := RequireSessionUser(c)
	if !ok {
		return
	}

	isAdmin := accounts.IsUserRoleAtLeast(user.Role, accounts.RoleAdmin)
	if !isAdmin {
		if _, err := clients.GetClientByUUIDForUser(uuid, user.UUID); err != nil {
			RespondError(c, 404, "Client not found")
			return
		}
	} else if _, err := clients.GetClientByUUID(uuid); err != nil {
		RespondError(c, 404, "Client not found")
		return
	}
	cached, _ := Records.Get(uuid)
	reports, _ := cached.([]common.Report)
	RespondSuccess(c, buildClientRecentRecordsDTO(reports))
}
