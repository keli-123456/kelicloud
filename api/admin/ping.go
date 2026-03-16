package admin

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/komari-monitor/komari/api"
	"github.com/komari-monitor/komari/database/models"
	"github.com/komari-monitor/komari/database/tasks"
)

// POST body: clients []string, target, task_type string, interval int
func AddPingTask(c *gin.Context) {
	var req struct {
		Clients  []string `json:"clients" binding:"required"`
		Name     string   `json:"name" binding:"required"`
		Target   string   `json:"target" binding:"required"`
		TaskType string   `json:"type" binding:"required"`     // icmp, tcp, http
		Interval int      `json:"interval" binding:"required"` // 间隔时间，单位秒
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}

	tenantID, ok := currentTenantID(c)
	if !ok {
		api.RespondError(c, http.StatusForbidden, "Tenant context is required")
		return
	}

	if taskID, err := tasks.AddPingTaskForTenant(tenantID, req.Clients, req.Name, req.Target, req.TaskType, req.Interval); err != nil {
		api.RespondError(c, http.StatusInternalServerError, err.Error())
	} else {
		api.RespondSuccess(c, gin.H{"task_id": taskID})
	}
}

// POST body: id []uint
func DeletePingTask(c *gin.Context) {
	var req struct {
		ID []uint `json:"id" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}

	tenantID, ok := currentTenantID(c)
	if !ok {
		api.RespondError(c, http.StatusForbidden, "Tenant context is required")
		return
	}

	if err := tasks.DeletePingTaskForTenant(tenantID, req.ID); err != nil {
		api.RespondError(c, http.StatusInternalServerError, err.Error())
	} else {
		api.RespondSuccess(c, nil)
	}
}

// POST body: id []uint, updates map[string]interface{}
func EditPingTask(c *gin.Context) {
	var req struct {
		Tasks []*models.PingTask `json:"tasks" binding:"required"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid request data")
		return
	}

	tenantID, ok := currentTenantID(c)
	if !ok {
		api.RespondError(c, http.StatusForbidden, "Tenant context is required")
		return
	}

	if err := tasks.EditPingTaskForTenant(tenantID, req.Tasks); err != nil {
		api.RespondError(c, http.StatusInternalServerError, err.Error())
	} else {
		// for _, task := range req.Tasks {
		// 	tasks.DeletePingRecords([]uint{task.Id})
		// }
		api.RespondSuccess(c, nil)
	}
}

func GetAllPingTasks(c *gin.Context) {
	tenantID, ok := currentTenantID(c)
	if !ok {
		api.RespondError(c, http.StatusForbidden, "Tenant context is required")
		return
	}

	tasks, err := tasks.GetAllPingTasksByTenant(tenantID)
	if err != nil {
		api.RespondError(c, http.StatusInternalServerError, err.Error())
		return
	}

	api.RespondSuccess(c, tasks)
}
