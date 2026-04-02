package awsfollowup

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/komari-monitor/komari/database/models"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func openAWSFollowUpTestDB(t *testing.T) *gorm.DB {
	t.Helper()

	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared", strings.ReplaceAll(t.Name(), "/", "_"))
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}
	return db
}

func TestClaimRetryAndCompleteAWSFollowUpTask(t *testing.T) {
	db := openAWSFollowUpTestDB(t)
	if err := db.AutoMigrate(&models.AWSFollowUpTask{}); err != nil {
		t.Fatalf("failed to migrate schema: %v", err)
	}

	now := time.Now()
	if err := enqueueTaskWithDB(db, &models.AWSFollowUpTask{
		UserID:       "user-a",
		CredentialID: "cred-a",
		Region:       "us-east-1",
		TaskType:     models.AWSFollowUpTaskTypeEC2AssignIPv6,
		ResourceID:   "i-123",
		NextRunAt:    models.FromTime(now),
	}); err != nil {
		t.Fatalf("failed to enqueue task: %v", err)
	}

	claimed, err := claimDueTasksWithDB(db, 5, now, time.Minute)
	if err != nil {
		t.Fatalf("failed to claim tasks: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("expected 1 claimed task, got %d", len(claimed))
	}

	reclaimed, err := claimDueTasksWithDB(db, 5, now, time.Minute)
	if err != nil {
		t.Fatalf("failed to re-claim tasks: %v", err)
	}
	if len(reclaimed) != 0 {
		t.Fatalf("expected claimed task lease to block immediate re-claim, got %d tasks", len(reclaimed))
	}

	nextRunAt := now.Add(15 * time.Second)
	if err := markTaskAttemptWithDB(db, claimed[0], now, nextRunAt, errors.New("temporary failure")); err != nil {
		t.Fatalf("failed to mark retry attempt: %v", err)
	}

	var reloaded models.AWSFollowUpTask
	if err := db.First(&reloaded, claimed[0].ID).Error; err != nil {
		t.Fatalf("failed to reload task: %v", err)
	}
	if reloaded.Attempts != 1 {
		t.Fatalf("expected attempts to equal 1, got %d", reloaded.Attempts)
	}
	if reloaded.Status != models.AWSFollowUpTaskStatusPending {
		t.Fatalf("expected task to stay pending, got %q", reloaded.Status)
	}
	if reloaded.LeaseUntil != nil {
		t.Fatalf("expected lease to be cleared after retry scheduling")
	}

	claimedAgain, err := claimDueTasksWithDB(db, 5, nextRunAt.Add(time.Second), time.Minute)
	if err != nil {
		t.Fatalf("failed to claim retried task: %v", err)
	}
	if len(claimedAgain) != 1 {
		t.Fatalf("expected 1 claimed task after retry delay, got %d", len(claimedAgain))
	}

	if err := markTaskSucceededWithDB(db, claimedAgain[0].ID, nextRunAt.Add(2*time.Second)); err != nil {
		t.Fatalf("failed to mark task successful: %v", err)
	}

	if err := db.First(&reloaded, claimedAgain[0].ID).Error; err != nil {
		t.Fatalf("failed to reload completed task: %v", err)
	}
	if reloaded.Status != models.AWSFollowUpTaskStatusSuccess {
		t.Fatalf("expected task success status, got %q", reloaded.Status)
	}
	if reloaded.CompletedAt == nil || reloaded.CompletedAt.ToTime().IsZero() {
		t.Fatal("expected completed_at to be set")
	}
}

func TestClaimDueTasksAllowsExpiredLease(t *testing.T) {
	db := openAWSFollowUpTestDB(t)
	if err := db.AutoMigrate(&models.AWSFollowUpTask{}); err != nil {
		t.Fatalf("failed to migrate schema: %v", err)
	}

	now := time.Now()
	leaseUntil := models.FromTime(now.Add(-time.Minute))
	task := &models.AWSFollowUpTask{
		UserID:       "user-a",
		CredentialID: "cred-a",
		Region:       "us-east-1",
		TaskType:     models.AWSFollowUpTaskTypeEC2AllowAllTraffic,
		ResourceID:   "i-123",
		Status:       models.AWSFollowUpTaskStatusPending,
		LeaseUntil:   &leaseUntil,
		NextRunAt:    models.FromTime(now.Add(-time.Second)),
	}
	if err := db.Create(task).Error; err != nil {
		t.Fatalf("failed to seed leased task: %v", err)
	}

	claimed, err := claimDueTasksWithDB(db, 5, now, time.Minute)
	if err != nil {
		t.Fatalf("failed to claim task with expired lease: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("expected expired lease task to be claimable, got %d tasks", len(claimed))
	}
}

func TestCancelPendingTasksByCredential(t *testing.T) {
	db := openAWSFollowUpTestDB(t)
	if err := db.AutoMigrate(&models.AWSFollowUpTask{}); err != nil {
		t.Fatalf("failed to migrate schema: %v", err)
	}

	now := time.Now()
	seed := []*models.AWSFollowUpTask{
		{
			UserID:       "user-a",
			CredentialID: "cred-a",
			Region:       "us-east-1",
			TaskType:     models.AWSFollowUpTaskTypeEC2AssignIPv6,
			ResourceID:   "i-1",
			Status:       models.AWSFollowUpTaskStatusPending,
			NextRunAt:    models.FromTime(now),
		},
		{
			UserID:       "user-a",
			CredentialID: "cred-a",
			Region:       "us-east-1",
			TaskType:     models.AWSFollowUpTaskTypeEC2AllowAllTraffic,
			ResourceID:   "i-2",
			Status:       models.AWSFollowUpTaskStatusSuccess,
			NextRunAt:    models.FromTime(now),
		},
		{
			UserID:       "user-a",
			CredentialID: "cred-b",
			Region:       "us-east-1",
			TaskType:     models.AWSFollowUpTaskTypeEC2AllowAllTraffic,
			ResourceID:   "i-3",
			Status:       models.AWSFollowUpTaskStatusPending,
			NextRunAt:    models.FromTime(now),
		},
	}
	for _, task := range seed {
		if err := db.Create(task).Error; err != nil {
			t.Fatalf("failed to seed task: %v", err)
		}
	}

	cancelled, err := cancelPendingTasksByCredentialWithDB(db, "user-a", "cred-a", now, "credential removed")
	if err != nil {
		t.Fatalf("failed to cancel pending tasks: %v", err)
	}
	if cancelled != 1 {
		t.Fatalf("expected to cancel 1 task, got %d", cancelled)
	}

	var cancelledTask models.AWSFollowUpTask
	if err := db.First(&cancelledTask, seed[0].ID).Error; err != nil {
		t.Fatalf("failed to reload cancelled task: %v", err)
	}
	if cancelledTask.Status != models.AWSFollowUpTaskStatusCancelled {
		t.Fatalf("expected cancelled status, got %q", cancelledTask.Status)
	}
	if cancelledTask.CompletedAt == nil || cancelledTask.CompletedAt.ToTime().IsZero() {
		t.Fatal("expected completed_at to be set for cancelled task")
	}
	if cancelledTask.LastError != "credential removed" {
		t.Fatalf("expected cancellation reason to be preserved, got %q", cancelledTask.LastError)
	}

	var untouchedTask models.AWSFollowUpTask
	if err := db.First(&untouchedTask, seed[2].ID).Error; err != nil {
		t.Fatalf("failed to reload untouched task: %v", err)
	}
	if untouchedTask.Status != models.AWSFollowUpTaskStatusPending {
		t.Fatalf("expected other credential task to stay pending, got %q", untouchedTask.Status)
	}
}

func TestCancelledTaskIsNotOverwrittenBySuccessUpdate(t *testing.T) {
	db := openAWSFollowUpTestDB(t)
	if err := db.AutoMigrate(&models.AWSFollowUpTask{}); err != nil {
		t.Fatalf("failed to migrate schema: %v", err)
	}

	now := time.Now()
	task := &models.AWSFollowUpTask{
		UserID:       "user-a",
		CredentialID: "cred-a",
		Region:       "us-east-1",
		TaskType:     models.AWSFollowUpTaskTypeEC2AssignIPv6,
		ResourceID:   "i-123",
		Status:       models.AWSFollowUpTaskStatusPending,
		NextRunAt:    models.FromTime(now),
	}
	if err := db.Create(task).Error; err != nil {
		t.Fatalf("failed to seed task: %v", err)
	}

	if err := markTaskTerminalWithDB(db, task.ID, models.AWSFollowUpTaskStatusCancelled, now, errors.New("credential removed")); err != nil {
		t.Fatalf("failed to mark task cancelled: %v", err)
	}
	if err := markTaskSucceededWithDB(db, task.ID, now.Add(time.Second)); err != nil {
		t.Fatalf("failed to attempt success update: %v", err)
	}

	var reloaded models.AWSFollowUpTask
	if err := db.First(&reloaded, task.ID).Error; err != nil {
		t.Fatalf("failed to reload task: %v", err)
	}
	if reloaded.Status != models.AWSFollowUpTaskStatusCancelled {
		t.Fatalf("expected task to remain cancelled, got %q", reloaded.Status)
	}
}

func TestRetryTaskByIDResetsTerminalTask(t *testing.T) {
	db := openAWSFollowUpTestDB(t)
	if err := db.AutoMigrate(&models.AWSFollowUpTask{}); err != nil {
		t.Fatalf("failed to migrate schema: %v", err)
	}

	now := time.Now()
	completedAt := models.FromTime(now.Add(-time.Minute))
	lastAttemptAt := models.FromTime(now.Add(-2 * time.Minute))
	task := &models.AWSFollowUpTask{
		UserID:        "user-a",
		CredentialID:  "cred-a",
		Region:        "us-east-1",
		TaskType:      models.AWSFollowUpTaskTypeEC2AssignIPv6,
		ResourceID:    "i-123",
		Status:        models.AWSFollowUpTaskStatusFailed,
		Attempts:      7,
		MaxAttempts:   60,
		LastError:     "timed out",
		LastAttemptAt: &lastAttemptAt,
		CompletedAt:   &completedAt,
		NextRunAt:     models.FromTime(now.Add(time.Hour)),
	}
	if err := db.Create(task).Error; err != nil {
		t.Fatalf("failed to seed task: %v", err)
	}

	retryAt := now.Add(15 * time.Second)
	if err := retryTaskByIDWithDB(db, "user-a", task.ID, retryAt); err != nil {
		t.Fatalf("failed to retry task: %v", err)
	}

	var reloaded models.AWSFollowUpTask
	if err := db.First(&reloaded, task.ID).Error; err != nil {
		t.Fatalf("failed to reload task: %v", err)
	}
	if reloaded.Status != models.AWSFollowUpTaskStatusPending {
		t.Fatalf("expected task to be pending again, got %q", reloaded.Status)
	}
	if reloaded.Attempts != 0 {
		t.Fatalf("expected attempts to reset to 0, got %d", reloaded.Attempts)
	}
	if reloaded.LastError != "" {
		t.Fatalf("expected last error to be cleared, got %q", reloaded.LastError)
	}
	if reloaded.LastAttemptAt != nil {
		t.Fatal("expected last_attempt_at to be cleared")
	}
	if reloaded.CompletedAt != nil {
		t.Fatal("expected completed_at to be cleared")
	}
	nextRunAt := reloaded.NextRunAt.ToTime()
	if diff := nextRunAt.Sub(retryAt); diff < -time.Second || diff > time.Second {
		t.Fatalf("expected next_run_at to be close to retry time, got %v (diff %v)", nextRunAt, diff)
	}
}

func TestDeleteTerminalTasksByUserRemovesOnlyTerminalStates(t *testing.T) {
	db := openAWSFollowUpTestDB(t)
	if err := db.AutoMigrate(&models.AWSFollowUpTask{}); err != nil {
		t.Fatalf("failed to migrate schema: %v", err)
	}

	now := time.Now()
	seed := []*models.AWSFollowUpTask{
		{UserID: "user-a", CredentialID: "cred-a", Region: "us-east-1", TaskType: models.AWSFollowUpTaskTypeEC2AssignIPv6, ResourceID: "i-1", Status: models.AWSFollowUpTaskStatusPending, NextRunAt: models.FromTime(now)},
		{UserID: "user-a", CredentialID: "cred-a", Region: "us-east-1", TaskType: models.AWSFollowUpTaskTypeEC2AssignIPv6, ResourceID: "i-2", Status: models.AWSFollowUpTaskStatusFailed, NextRunAt: models.FromTime(now)},
		{UserID: "user-a", CredentialID: "cred-a", Region: "us-east-1", TaskType: models.AWSFollowUpTaskTypeEC2AssignIPv6, ResourceID: "i-3", Status: models.AWSFollowUpTaskStatusCancelled, NextRunAt: models.FromTime(now)},
		{UserID: "user-a", CredentialID: "cred-a", Region: "us-east-1", TaskType: models.AWSFollowUpTaskTypeEC2AssignIPv6, ResourceID: "i-4", Status: models.AWSFollowUpTaskStatusSkipped, NextRunAt: models.FromTime(now)},
		{UserID: "user-a", CredentialID: "cred-a", Region: "us-east-1", TaskType: models.AWSFollowUpTaskTypeEC2AssignIPv6, ResourceID: "i-5", Status: models.AWSFollowUpTaskStatusSuccess, NextRunAt: models.FromTime(now)},
	}
	for _, task := range seed {
		if err := db.Create(task).Error; err != nil {
			t.Fatalf("failed to seed task: %v", err)
		}
	}

	deleted, err := deleteTerminalTasksByUserWithDB(db, "user-a")
	if err != nil {
		t.Fatalf("failed to delete terminal tasks: %v", err)
	}
	if deleted != 3 {
		t.Fatalf("expected to delete 3 terminal tasks, got %d", deleted)
	}

	var tasks []models.AWSFollowUpTask
	if err := db.Order("resource_id ASC").Find(&tasks).Error; err != nil {
		t.Fatalf("failed to reload tasks: %v", err)
	}
	if len(tasks) != 2 {
		t.Fatalf("expected 2 tasks to remain, got %d", len(tasks))
	}
	if tasks[0].Status != models.AWSFollowUpTaskStatusPending || tasks[1].Status != models.AWSFollowUpTaskStatusSuccess {
		t.Fatalf("expected pending and success to remain, got %q and %q", tasks[0].Status, tasks[1].Status)
	}
}
