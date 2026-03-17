package tasks

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

func openTaskTestDB(t *testing.T) *gorm.DB {
	t.Helper()

	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared", strings.ReplaceAll(t.Name(), "/", "_"))
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}
	return db
}

func TestTaskQueriesAreStrictlyUserScoped(t *testing.T) {
	db := openTaskTestDB(t)

	if err := db.AutoMigrate(&models.Client{}, &models.Task{}, &models.TaskResult{}); err != nil {
		t.Fatalf("failed to migrate test schema: %v", err)
	}

	now := models.FromTime(time.Now())
	legacyClient := models.Client{
		UUID:      "legacy-client",
		Token:     "legacy-token",
		Name:      "Legacy Client",
		CreatedAt: now,
		UpdatedAt: now,
	}
	userClient := models.Client{
		UUID:      "user-client",
		Token:     "user-token",
		UserID:    "user-a",
		Name:      "User Client",
		CreatedAt: now,
		UpdatedAt: now,
	}
	otherClient := models.Client{
		UUID:      "other-client",
		Token:     "other-token",
		UserID:    "user-b",
		Name:      "Other Client",
		CreatedAt: now,
		UpdatedAt: now,
	}
	userSecondClient := models.Client{
		UUID:      "user-client-b",
		Token:     "user-token-b",
		UserID:    "user-a",
		Name:      "User Client B",
		CreatedAt: now,
		UpdatedAt: now,
	}
	for _, client := range []models.Client{legacyClient, userClient, otherClient, userSecondClient} {
		if err := db.Create(&client).Error; err != nil {
			t.Fatalf("failed to create client %s: %v", client.UUID, err)
		}
	}

	if err := createTaskWithDB(db, "", "legacy", "legacy-task", []string{legacyClient.UUID}); err != nil {
		t.Fatalf("failed to create legacy task: %v", err)
	}
	if err := createTaskWithDB(db, "user-a", "tenant-a", "user-task", []string{userClient.UUID}); err != nil {
		t.Fatalf("failed to create user task: %v", err)
	}
	if err := createTaskWithDB(db, "user-a", "tenant-b", "user-task-b", []string{userSecondClient.UUID}); err != nil {
		t.Fatalf("failed to create cross-tenant user task: %v", err)
	}
	if err := createTaskWithDB(db, "user-b", "tenant-a", "other-task", []string{otherClient.UUID}); err != nil {
		t.Fatalf("failed to create other user task: %v", err)
	}

	if err := saveTaskResultWithDB(db, "", "legacy", "legacy-task", legacyClient.UUID, "legacy-ok", 0, now); err != nil {
		t.Fatalf("failed to save legacy task result: %v", err)
	}
	if err := saveTaskResultWithDB(db, "user-a", "tenant-a", "user-task", userClient.UUID, "user-ok", 0, now); err != nil {
		t.Fatalf("failed to save user task result: %v", err)
	}
	if err := saveTaskResultWithDB(db, "user-a", "tenant-b", "user-task-b", userSecondClient.UUID, "user-ok-b", 0, now); err != nil {
		t.Fatalf("failed to save cross-tenant user task result: %v", err)
	}

	userTasks, err := getAllTasksByUserWithDB(db, "user-a")
	if err != nil {
		t.Fatalf("failed to load user-scoped tasks: %v", err)
	}
	if len(userTasks) != 2 {
		t.Fatalf("expected user to see both personal tasks across tenants, got %+v", userTasks)
	}

	if _, err := getTaskByTaskIdForUserWithDB(db, "user-a", "other-task"); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("expected cross-user task lookup to fail, got %v", err)
	}
	if _, err := getTaskByTaskIdForUserWithDB(db, "user-a", "user-task-b"); err != nil {
		t.Fatalf("expected cross-tenant personal task lookup to succeed, got %v", err)
	}

	if _, err := getSpecificTaskResultForUserWithDB(db, "user-a", "legacy-task", legacyClient.UUID); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("expected legacy task result lookup to fail, got %v", err)
	}
	if _, err := getSpecificTaskResultForUserWithDB(db, "user-a", "user-task-b", userSecondClient.UUID); err != nil {
		t.Fatalf("expected cross-tenant personal task result lookup to succeed, got %v", err)
	}

	if err := saveTaskResultWithDB(db, "user-a", "tenant-a", "legacy-task", legacyClient.UUID, "legacy-updated", 0, now); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("expected legacy task result update to fail, got %v", err)
	}
	if err := saveTaskResultWithDB(db, "user-a", "tenant-a", "other-task", otherClient.UUID, "blocked", 1, now); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("expected cross-user task result update to fail, got %v", err)
	}

	userTaskRecord, err := getTaskByTaskIdForUserWithDB(db, "user-a", "user-task")
	if err != nil {
		t.Fatalf("failed to reload user task: %v", err)
	}
	if userTaskRecord.UserID != "user-a" {
		t.Fatalf("expected user task owner to remain user-a, got %q", userTaskRecord.UserID)
	}

	userResults, err := getTaskResultsByTaskIDForUserWithDB(db, "user-a", "user-task")
	if err != nil {
		t.Fatalf("failed to reload user task results: %v", err)
	}
	if len(userResults) != 1 || userResults[0].UserID != "user-a" {
		t.Fatalf("expected user task result owner to remain user-a, got %+v", userResults)
	}
}

func TestNormalizeTaskOwnerScopePrefersUser(t *testing.T) {
	userUUID, tenantID, err := normalizeTaskOwnerScope("user-a", "tenant-a")
	if err != nil {
		t.Fatalf("normalizeTaskOwnerScope returned error: %v", err)
	}
	if userUUID != "user-a" || tenantID != "" {
		t.Fatalf("expected user scope to clear tenant, got user=%q tenant=%q", userUUID, tenantID)
	}
}
