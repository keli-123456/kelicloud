package auditlog

import (
	"testing"
	"time"

	"github.com/komari-monitor/komari/database/models"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestListLogsByTenantScopesResults(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}

	if err := db.AutoMigrate(&models.Log{}); err != nil {
		t.Fatalf("failed to migrate test schema: %v", err)
	}

	older := models.FromTime(time.Now().Add(-time.Minute))
	newer := models.FromTime(time.Now())

	if err := createLogEntryWithDB(db, "tenant-a", "127.0.0.1", "user-a", "older entry", "info", older); err != nil {
		t.Fatalf("failed to create older tenant A log: %v", err)
	}
	if err := createLogEntryWithDB(db, "tenant-a", "127.0.0.1", "user-a", "newer entry", "warn", newer); err != nil {
		t.Fatalf("failed to create newer tenant A log: %v", err)
	}
	if err := createLogEntryWithDB(db, "tenant-b", "127.0.0.1", "user-b", "other tenant entry", "info", newer); err != nil {
		t.Fatalf("failed to create tenant B log: %v", err)
	}

	logs, total, err := listLogsByTenantWithDB(db, "tenant-a", 10, 0)
	if err != nil {
		t.Fatalf("failed to list tenant A logs: %v", err)
	}
	if total != 2 {
		t.Fatalf("expected total 2 for tenant A, got %d", total)
	}
	if len(logs) != 2 {
		t.Fatalf("expected 2 logs for tenant A, got %d", len(logs))
	}
	if logs[0].Message != "newer entry" || logs[1].Message != "older entry" {
		t.Fatalf("expected tenant A logs ordered by time desc, got %+v", logs)
	}
}
