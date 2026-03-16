package accounts

import (
	"testing"
	"time"

	"github.com/komari-monitor/komari/database/models"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestSessionQueriesAndDeletesAreUserScoped(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}

	if err := db.AutoMigrate(&models.Session{}); err != nil {
		t.Fatalf("failed to migrate test schema: %v", err)
	}

	now := models.FromTime(time.Now().Add(time.Hour))
	sessionA := models.Session{
		UUID:      "user-a",
		Session:   "session-a",
		Expires:   now,
		CreatedAt: now,
	}
	sessionB := models.Session{
		UUID:      "user-b",
		Session:   "session-b",
		Expires:   now,
		CreatedAt: now,
	}
	if err := db.Create(&sessionA).Error; err != nil {
		t.Fatalf("failed to create session A: %v", err)
	}
	if err := db.Create(&sessionB).Error; err != nil {
		t.Fatalf("failed to create session B: %v", err)
	}

	userASessions, err := getSessionsByUserWithDB(db, "user-a")
	if err != nil {
		t.Fatalf("failed to query user A sessions: %v", err)
	}
	if len(userASessions) != 1 || userASessions[0].Session != "session-a" {
		t.Fatalf("expected only user A session, got %+v", userASessions)
	}

	if err := deleteSessionByUserWithDB(db, "user-a", "session-b"); err != nil {
		t.Fatalf("failed to attempt cross-user delete: %v", err)
	}
	var count int64
	if err := db.Model(&models.Session{}).Where("session = ?", "session-b").Count(&count).Error; err != nil {
		t.Fatalf("failed to count session B: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected user B session to remain, got count %d", count)
	}

	if err := deleteAllSessionsByUserWithDB(db, "user-a"); err != nil {
		t.Fatalf("failed to delete all user A sessions: %v", err)
	}
	if err := db.Model(&models.Session{}).Where("uuid = ?", "user-a").Count(&count).Error; err != nil {
		t.Fatalf("failed to count user A sessions after delete: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected user A sessions to be deleted, got count %d", count)
	}
	if err := db.Model(&models.Session{}).Where("uuid = ?", "user-b").Count(&count).Error; err != nil {
		t.Fatalf("failed to count user B sessions after delete: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected user B session to remain after user A delete-all, got count %d", count)
	}
}
