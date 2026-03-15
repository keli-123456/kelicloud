package clients

import (
	"testing"
	"time"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"

	"github.com/komari-monitor/komari/database/models"
)

func TestDeleteClientWithDBClearsOfflineNotifications(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}

	if err := db.Exec("PRAGMA foreign_keys = ON").Error; err != nil {
		t.Fatalf("failed to enable foreign keys: %v", err)
	}
	if err := db.AutoMigrate(&models.Client{}, &models.OfflineNotification{}); err != nil {
		t.Fatalf("failed to migrate test schema: %v", err)
	}

	now := models.FromTime(time.Now())
	client := models.Client{
		UUID:      "test-client-1",
		Token:     "test-token-1",
		Name:      "Test Client",
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := db.Create(&client).Error; err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	notification := models.OfflineNotification{
		Client: client.UUID,
		Enable: true,
	}
	if err := db.Create(&notification).Error; err != nil {
		t.Fatalf("failed to create offline notification: %v", err)
	}

	if err := deleteClientWithDB(db, client.UUID); err != nil {
		t.Fatalf("deleteClientWithDB returned error: %v", err)
	}

	var clientCount int64
	if err := db.Model(&models.Client{}).Where("uuid = ?", client.UUID).Count(&clientCount).Error; err != nil {
		t.Fatalf("failed to count clients: %v", err)
	}
	if clientCount != 0 {
		t.Fatalf("expected client to be deleted, found %d rows", clientCount)
	}

	var notificationCount int64
	if err := db.Model(&models.OfflineNotification{}).Where("client = ?", client.UUID).Count(&notificationCount).Error; err != nil {
		t.Fatalf("failed to count offline notifications: %v", err)
	}
	if notificationCount != 0 {
		t.Fatalf("expected offline notification to be deleted, found %d rows", notificationCount)
	}
}
