package auditlog

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/komari-monitor/komari/database/models"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func openAuditLogTestDB(t *testing.T) *gorm.DB {
	t.Helper()

	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared", strings.ReplaceAll(t.Name(), "/", "_"))
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}
	return db
}

func TestListLogsByUserUsesUserScope(t *testing.T) {
	db := openAuditLogTestDB(t)

	if err := db.AutoMigrate(&models.Log{}); err != nil {
		t.Fatalf("failed to migrate test schema: %v", err)
	}

	older := models.FromTime(time.Now().Add(-time.Minute))
	newer := models.FromTime(time.Now())

	if err := createLogEntryWithDB(db, "", "127.0.0.1", "system", "system entry", "info", older); err != nil {
		t.Fatalf("failed to create system log: %v", err)
	}
	if err := createLogEntryWithDB(db, "user-a", "127.0.0.1", "user-a", "older entry", "warn", older); err != nil {
		t.Fatalf("failed to create older user log: %v", err)
	}
	if err := createLogEntryWithDB(db, "user-a", "127.0.0.1", "user-a", "newer entry", "warn", newer); err != nil {
		t.Fatalf("failed to create newer user log: %v", err)
	}
	if err := createLogEntryWithDB(db, "user-b", "127.0.0.1", "user-b", "other user entry", "warn", newer); err != nil {
		t.Fatalf("failed to create other user log: %v", err)
	}

	logs, total, err := listLogsByUserWithDB(db, "user-a", 10, 0)
	if err != nil {
		t.Fatalf("failed to list user logs: %v", err)
	}
	if total != 2 {
		t.Fatalf("expected total 2 for user-a, got %d", total)
	}
	if len(logs) != 2 {
		t.Fatalf("expected two personal log entries, got %+v", logs)
	}
	if logs[0].Message != "newer entry" || logs[1].Message != "older entry" {
		t.Fatalf("expected user logs ordered by time desc, got %+v", logs)
	}
}

func TestListLogsIncludesAllUsersAndSystemLogs(t *testing.T) {
	db := openAuditLogTestDB(t)

	if err := db.AutoMigrate(&models.Log{}); err != nil {
		t.Fatalf("failed to migrate test schema: %v", err)
	}

	oldest := models.FromTime(time.Now().Add(-2 * time.Minute))
	middle := models.FromTime(time.Now().Add(-time.Minute))
	newest := models.FromTime(time.Now())

	if err := createLogEntryWithDB(db, "", "127.0.0.1", "system", "system entry", "info", oldest); err != nil {
		t.Fatalf("failed to create system log: %v", err)
	}
	if err := createLogEntryWithDB(db, "user-a", "127.0.0.1", "user-a", "user entry", "warn", middle); err != nil {
		t.Fatalf("failed to create user log: %v", err)
	}
	if err := createLogEntryWithDB(db, "user-b", "127.0.0.1", "user-b", "other user entry", "info", newest); err != nil {
		t.Fatalf("failed to create other user log: %v", err)
	}

	logs, total, err := listLogsWithDB(db, 10, 0)
	if err != nil {
		t.Fatalf("failed to list all logs: %v", err)
	}
	if total != 3 {
		t.Fatalf("expected total 3, got %d", total)
	}
	if len(logs) != 3 {
		t.Fatalf("expected three log entries, got %+v", logs)
	}
	if logs[0].Message != "other user entry" || logs[1].Message != "user entry" || logs[2].Message != "system entry" {
		t.Fatalf("expected all logs ordered by time desc, got %+v", logs)
	}

	paged, total, err := listLogsWithDB(db, 1, 1)
	if err != nil {
		t.Fatalf("failed to list paged logs: %v", err)
	}
	if total != 3 || len(paged) != 1 || paged[0].Message != "user entry" {
		t.Fatalf("expected second row with total 3, got total=%d logs=%+v", total, paged)
	}
}
