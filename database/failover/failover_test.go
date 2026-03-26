package failover

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/komari-monitor/komari/database/models"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func openFailoverTestDB(t *testing.T) *gorm.DB {
	t.Helper()

	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared", strings.ReplaceAll(t.Name(), "/", "_"))
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}
	return db
}

func seedActiveExecution(t *testing.T, db *gorm.DB, taskName string, executionStatus string) (*models.FailoverTask, *models.FailoverExecution, *models.FailoverExecutionStep) {
	t.Helper()

	now := models.FromTime(time.Now())
	task := &models.FailoverTask{
		UserID:          "user-a",
		Name:            taskName,
		Enabled:         true,
		WatchClientUUID: taskName + "-client",
		LastStatus:      models.FailoverTaskStatusRunning,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	if err := db.Create(task).Error; err != nil {
		t.Fatalf("failed to create task: %v", err)
	}

	execution := &models.FailoverExecution{
		TaskID:          task.ID,
		Status:          executionStatus,
		TriggerReason:   "manual run",
		WatchClientUUID: task.WatchClientUUID,
		StartedAt:       now,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	if err := db.Create(execution).Error; err != nil {
		t.Fatalf("failed to create execution: %v", err)
	}

	if err := db.Model(&models.FailoverTask{}).
		Where("id = ?", task.ID).
		Updates(map[string]interface{}{
			"last_execution_id": execution.ID,
			"last_triggered_at": now,
		}).Error; err != nil {
		t.Fatalf("failed to link latest execution: %v", err)
	}

	step := &models.FailoverExecutionStep{
		ExecutionID: execution.ID,
		Sort:        1,
		StepKey:     "detect",
		StepLabel:   "Detect Trigger",
		Status:      models.FailoverStepStatusRunning,
		StartedAt:   &now,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	if err := db.Create(step).Error; err != nil {
		t.Fatalf("failed to create running step: %v", err)
	}

	return task, execution, step
}

func TestRecoverInterruptedExecutionsWithDBMarksActiveExecutionFailed(t *testing.T) {
	db := openFailoverTestDB(t)
	if err := db.AutoMigrate(
		&models.FailoverTask{},
		&models.FailoverExecution{},
		&models.FailoverExecutionStep{},
	); err != nil {
		t.Fatalf("failed to migrate test schema: %v", err)
	}

	task, execution, step := seedActiveExecution(t, db, "task-a", models.FailoverExecutionStatusQueued)
	reason := "failover execution was interrupted before completion"

	recovered, err := recoverInterruptedExecutionsWithDB(db, 0, reason, time.Now())
	if err != nil {
		t.Fatalf("recoverInterruptedExecutionsWithDB returned error: %v", err)
	}
	if recovered != 1 {
		t.Fatalf("expected 1 recovered execution, got %d", recovered)
	}

	var updatedExecution models.FailoverExecution
	if err := db.First(&updatedExecution, execution.ID).Error; err != nil {
		t.Fatalf("failed to reload execution: %v", err)
	}
	if updatedExecution.Status != models.FailoverExecutionStatusFailed {
		t.Fatalf("expected execution status %q, got %q", models.FailoverExecutionStatusFailed, updatedExecution.Status)
	}
	if updatedExecution.ErrorMessage != reason {
		t.Fatalf("expected execution error %q, got %q", reason, updatedExecution.ErrorMessage)
	}
	if updatedExecution.FinishedAt == nil || updatedExecution.FinishedAt.ToTime().IsZero() {
		t.Fatal("expected execution finished_at to be set")
	}

	var updatedStep models.FailoverExecutionStep
	if err := db.First(&updatedStep, step.ID).Error; err != nil {
		t.Fatalf("failed to reload step: %v", err)
	}
	if updatedStep.Status != models.FailoverStepStatusFailed {
		t.Fatalf("expected step status %q, got %q", models.FailoverStepStatusFailed, updatedStep.Status)
	}
	if updatedStep.Message != reason {
		t.Fatalf("expected step message %q, got %q", reason, updatedStep.Message)
	}
	if updatedStep.FinishedAt == nil || updatedStep.FinishedAt.ToTime().IsZero() {
		t.Fatal("expected step finished_at to be set")
	}

	var updatedTask models.FailoverTask
	if err := db.First(&updatedTask, task.ID).Error; err != nil {
		t.Fatalf("failed to reload task: %v", err)
	}
	if updatedTask.LastStatus != models.FailoverTaskStatusFailed {
		t.Fatalf("expected task last_status %q, got %q", models.FailoverTaskStatusFailed, updatedTask.LastStatus)
	}
	if updatedTask.LastMessage != reason {
		t.Fatalf("expected task last_message %q, got %q", reason, updatedTask.LastMessage)
	}
	if updatedTask.LastFailedAt == nil || updatedTask.LastFailedAt.ToTime().IsZero() {
		t.Fatal("expected task last_failed_at to be set")
	}
}

func TestRecoverInterruptedExecutionsWithDBScopesToTask(t *testing.T) {
	db := openFailoverTestDB(t)
	if err := db.AutoMigrate(
		&models.FailoverTask{},
		&models.FailoverExecution{},
		&models.FailoverExecutionStep{},
	); err != nil {
		t.Fatalf("failed to migrate test schema: %v", err)
	}

	taskA, executionA, _ := seedActiveExecution(t, db, "task-a", models.FailoverExecutionStatusQueued)
	_, executionB, _ := seedActiveExecution(t, db, "task-b", models.FailoverExecutionStatusProvisioning)

	recovered, err := recoverInterruptedExecutionsWithDB(db, taskA.ID, "interrupted", time.Now())
	if err != nil {
		t.Fatalf("recoverInterruptedExecutionsWithDB returned error: %v", err)
	}
	if recovered != 1 {
		t.Fatalf("expected 1 recovered execution, got %d", recovered)
	}

	var updatedExecutionA models.FailoverExecution
	if err := db.First(&updatedExecutionA, executionA.ID).Error; err != nil {
		t.Fatalf("failed to reload execution A: %v", err)
	}
	if updatedExecutionA.Status != models.FailoverExecutionStatusFailed {
		t.Fatalf("expected execution A status %q, got %q", models.FailoverExecutionStatusFailed, updatedExecutionA.Status)
	}

	var updatedExecutionB models.FailoverExecution
	if err := db.First(&updatedExecutionB, executionB.ID).Error; err != nil {
		t.Fatalf("failed to reload execution B: %v", err)
	}
	if updatedExecutionB.Status != models.FailoverExecutionStatusProvisioning {
		t.Fatalf("expected execution B to remain %q, got %q", models.FailoverExecutionStatusProvisioning, updatedExecutionB.Status)
	}
}
