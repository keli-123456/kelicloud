package client

import (
	"errors"
	"log"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/komari-monitor/komari/database/models"
	"github.com/komari-monitor/komari/database/tasks"
	"gorm.io/gorm"
)

func TaskResult(c *gin.Context) {
	clientValue, ok := c.Get("client_uuid")
	clientId, _ := clientValue.(string)
	if !ok || clientId == "" {
		c.JSON(400, gin.H{"status": "error", "message": "Invalid or missing token"})
		return
	}
	userValue, _ := c.Get("user_id")
	userUUID, _ := userValue.(string)
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

	var err error
	if userUUID == "" {
		c.JSON(400, gin.H{"status": "error", "message": "Missing user context"})
		return
	}
	existingResult, lookupErr := tasks.GetSpecificTaskResultForUser(userUUID, req.TaskId, clientId)
	if lookupErr != nil && !errors.Is(lookupErr, gorm.ErrRecordNotFound) {
		log.Printf("Task result lookup failed before save: task=%s client=%s user=%s err=%v", req.TaskId, clientId, userUUID, lookupErr)
	}
	wasAlreadyFinished := lookupErr == nil && existingResult != nil && existingResult.FinishedAt != nil
	previousFinishedAt := ""
	if wasAlreadyFinished {
		previousFinishedAt = existingResult.FinishedAt.ToTime().UTC().Format(time.RFC3339)
	}

	err = tasks.SaveTaskResultForUser(userUUID, req.TaskId, clientId, req.Result, req.ExitCode, models.FromTime(req.FinishedAt))
	if err != nil {
		c.JSON(500, gin.H{"status": "error", "message": "Failed to update task result: " + err.Error()})
		return
	}

	if wasAlreadyFinished {
		log.Printf(
			"Task result overwrite detected: task=%s client=%s user=%s previous_finished_at=%s new_finished_at=%s",
			req.TaskId,
			clientId,
			userUUID,
			previousFinishedAt,
			req.FinishedAt.UTC().Format(time.RFC3339),
		)
	} else {
		log.Printf(
			"Task result stored: task=%s client=%s user=%s finished_at=%s",
			req.TaskId,
			clientId,
			userUUID,
			req.FinishedAt.UTC().Format(time.RFC3339),
		)
	}

	c.JSON(200, gin.H{"status": "success", "message": "Task result updated successfully"})
}
