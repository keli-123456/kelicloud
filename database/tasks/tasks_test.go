package tasks

import (
	"errors"
	"testing"
	"time"

	"github.com/komari-monitor/komari/database/models"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestTaskQueriesAreTenantScoped(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}

	if err := db.AutoMigrate(&models.Client{}, &models.Task{}, &models.TaskResult{}); err != nil {
		t.Fatalf("failed to migrate test schema: %v", err)
	}

	now := models.FromTime(time.Now())
	clientA := models.Client{
		UUID:      "tenant-a-client",
		Token:     "tenant-a-token",
		TenantID:  "tenant-a",
		Name:      "Tenant A Client",
		CreatedAt: now,
		UpdatedAt: now,
	}
	clientB := models.Client{
		UUID:      "tenant-b-client",
		Token:     "tenant-b-token",
		TenantID:  "tenant-b",
		Name:      "Tenant B Client",
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := db.Create(&clientA).Error; err != nil {
		t.Fatalf("failed to create tenant A client: %v", err)
	}
	if err := db.Create(&clientB).Error; err != nil {
		t.Fatalf("failed to create tenant B client: %v", err)
	}

	if err := createTaskWithDB(db, "tenant-a", "task-a", []string{clientA.UUID}); err != nil {
		t.Fatalf("failed to create tenant A task: %v", err)
	}
	if err := createTaskWithDB(db, "tenant-b", "task-b", []string{clientB.UUID}); err != nil {
		t.Fatalf("failed to create tenant B task: %v", err)
	}

	if err := saveTaskResultWithDB(db, "tenant-a", "task-a", clientA.UUID, "ok", 0, now); err != nil {
		t.Fatalf("failed to save tenant A task result: %v", err)
	}

	tenantATasks, err := getAllTasksWithDB(db, "tenant-a")
	if err != nil {
		t.Fatalf("failed to load tenant A tasks: %v", err)
	}
	if len(tenantATasks) != 1 || tenantATasks[0].TaskId != "task-a" {
		t.Fatalf("expected only tenant A task, got %+v", tenantATasks)
	}

	tenantBTasks, err := getAllTasksWithDB(db, "tenant-b")
	if err != nil {
		t.Fatalf("failed to load tenant B tasks: %v", err)
	}
	if len(tenantBTasks) != 1 || tenantBTasks[0].TaskId != "task-b" {
		t.Fatalf("expected only tenant B task, got %+v", tenantBTasks)
	}

	if _, err := getTaskByTaskIdWithDB(db, "tenant-b", "task-a"); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("expected cross-tenant task lookup to fail, got %v", err)
	}

	if _, err := getSpecificTaskResultWithDB(db, "tenant-b", "task-a", clientA.UUID); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("expected cross-tenant task result lookup to fail, got %v", err)
	}

	if err := saveTaskResultWithDB(db, "tenant-b", "task-a", clientA.UUID, "blocked", 1, now); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("expected cross-tenant task result update to fail, got %v", err)
	}
}
