package client

import (
	"time"

	"github.com/gin-gonic/gin"
	"github.com/komari-monitor/komari/database/models"
	"github.com/komari-monitor/komari/database/tasks"
)

func TaskResult(c *gin.Context) {
	clientValue, ok := c.Get("client_uuid")
	clientId, _ := clientValue.(string)
	if !ok || clientId == "" {
		c.JSON(400, gin.H{"status": "error", "message": "Invalid or missing token"})
		return
	}
	tenantValue, ok := c.Get("tenant_id")
	tenantID, _ := tenantValue.(string)
	if !ok || tenantID == "" {
		c.JSON(400, gin.H{"status": "error", "message": "Invalid or missing tenant context"})
		return
	}
	var req struct {
		TaskId     string    `json:"task_id" binding:"required"`
		Result     string    `json:"result" binding:"required"`
		ExitCode   int       `json:"exit_code"`
		FinishedAt time.Time `json:"finished_at" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"status": "error", "message": "Invalid request"})
		return
	}

	if err := tasks.SaveTaskResultForTenant(tenantID, req.TaskId, clientId, req.Result, req.ExitCode, models.FromTime(req.FinishedAt)); err != nil {
		c.JSON(500, gin.H{"status": "error", "message": "Failed to update task result: " + err.Error()})
		return
	}

	c.JSON(200, gin.H{"status": "success", "message": "Task result updated successfully"})
}
