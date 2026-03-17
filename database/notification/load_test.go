package notification

import (
	"fmt"
	"strings"
	"testing"

	"github.com/komari-monitor/komari/database/models"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func openLoadNotificationTestDB(t *testing.T) *gorm.DB {
	t.Helper()

	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared", strings.ReplaceAll(t.Name(), "/", "_"))
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}
	return db
}

func TestLoadNotificationsAreStrictlyUserScoped(t *testing.T) {
	db := openLoadNotificationTestDB(t)

	if err := db.AutoMigrate(&models.LoadNotification{}); err != nil {
		t.Fatalf("failed to migrate test schema: %v", err)
	}

	fixtures := []models.LoadNotification{
		{Name: "Legacy", Clients: models.StringArray{"legacy-client"}, Metric: "cpu", Threshold: 80, Ratio: 0.8, Interval: 15},
		{UserID: "user-a", Name: "Personal", Clients: models.StringArray{"user-client"}, Metric: "cpu", Threshold: 80, Ratio: 0.8, Interval: 15},
		{UserID: "user-a", Name: "Personal B", Clients: models.StringArray{"user-client-b"}, Metric: "cpu", Threshold: 80, Ratio: 0.8, Interval: 15},
		{UserID: "user-b", Name: "Other", Clients: models.StringArray{"other-client"}, Metric: "cpu", Threshold: 80, Ratio: 0.8, Interval: 15},
	}
	for _, item := range fixtures {
		if err := db.Create(&item).Error; err != nil {
			t.Fatalf("failed to create load notification %s: %v", item.Name, err)
		}
	}

	userNotifications, err := getAllLoadNotificationsByUserWithDB(db, "user-a")
	if err != nil {
		t.Fatalf("failed to load user-scoped notifications: %v", err)
	}
	if len(userNotifications) != 2 {
		t.Fatalf("expected personal notifications across tenants, got %+v", userNotifications)
	}

	var legacyNotifications []models.LoadNotification
	if err := db.Where("COALESCE(user_id, '') = ''").Find(&legacyNotifications).Error; err != nil {
		t.Fatalf("failed to load legacy notifications: %v", err)
	}
	if len(legacyNotifications) != 1 || legacyNotifications[0].Name != "Legacy" {
		t.Fatalf("expected legacy lookup to return only unowned rows, got %+v", legacyNotifications)
	}
}

func TestNormalizeLoadNotificationOwnerScopePrefersUser(t *testing.T) {
	userUUID, tenantID, err := normalizeLoadNotificationOwnerScope("user-a", "tenant-a")
	if err != nil {
		t.Fatalf("normalizeLoadNotificationOwnerScope returned error: %v", err)
	}
	if userUUID != "user-a" || tenantID != "" {
		t.Fatalf("expected user scope to clear tenant, got user=%q tenant=%q", userUUID, tenantID)
	}
}
