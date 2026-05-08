package accounts

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/komari-monitor/komari/database/models"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func openLegacyOwnerTestDB(t *testing.T) *gorm.DB {
	t.Helper()

	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared", strings.ReplaceAll(t.Name(), "/", "_"))
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}
	return db
}

func TestBackfillLegacyUserScopedDataAssignsBlankOwners(t *testing.T) {
	db := openLegacyOwnerTestDB(t)
	if err := db.AutoMigrate(
		&models.Client{},
		&models.Clipboard{},
		&models.Task{},
		&models.TaskResult{},
		&models.FailoverTask{},
		&models.FailoverV2Service{},
		&models.CloudProvider{},
		&models.CloudInstanceShare{},
	); err != nil {
		t.Fatalf("failed to migrate test schema: %v", err)
	}

	now := models.FromTime(time.Now())
	if err := db.Create(&models.Client{
		UUID:      "legacy-client",
		Token:     "legacy-token",
		Name:      "Legacy",
		CreatedAt: now,
		UpdatedAt: now,
	}).Error; err != nil {
		t.Fatalf("failed to create legacy client: %v", err)
	}
	if err := db.Create(&models.Clipboard{Name: "legacy-script", Text: "whoami"}).Error; err != nil {
		t.Fatalf("failed to create legacy clipboard: %v", err)
	}
	if err := db.Create(&models.Task{TaskId: "legacy-task", Clients: models.StringArray{"legacy-client"}}).Error; err != nil {
		t.Fatalf("failed to create legacy task: %v", err)
	}
	if err := db.Create(&models.TaskResult{TaskId: "legacy-task", Client: "legacy-client"}).Error; err != nil {
		t.Fatalf("failed to create legacy task result: %v", err)
	}
	if err := db.Create(&models.FailoverTask{Name: "legacy-failover", WatchClientUUID: "legacy-client"}).Error; err != nil {
		t.Fatalf("failed to create legacy failover task: %v", err)
	}
	if err := db.Create(&models.FailoverV2Service{Name: "legacy-v2", DNSProvider: "aliyun"}).Error; err != nil {
		t.Fatalf("failed to create legacy failover v2 service: %v", err)
	}
	if err := db.Create(&models.CloudProvider{Name: "digitalocean", Addition: "{}"}).Error; err != nil {
		t.Fatalf("failed to create legacy cloud provider: %v", err)
	}
	if err := db.Create(&models.CloudInstanceShare{
		ShareToken:   "legacy-share-token",
		Provider:     "digitalocean",
		ResourceType: "droplet",
		ResourceID:   "1001",
	}).Error; err != nil {
		t.Fatalf("failed to create legacy cloud share: %v", err)
	}

	summary, err := backfillLegacyUserScopedDataWithDB(db, "admin-user")
	if err != nil {
		t.Fatalf("backfillLegacyUserScopedDataWithDB returned error: %v", err)
	}
	if summary.TotalUpdated() < 8 {
		t.Fatalf("expected at least 8 rows to be assigned, got summary %+v", summary)
	}

	assertUserID := func(model interface{}, where string, args ...interface{}) {
		t.Helper()
		var userID string
		if err := db.Model(model).Select("user_id").Where(where, args...).Scan(&userID).Error; err != nil {
			t.Fatalf("failed to load user_id for %T: %v", model, err)
		}
		if userID != "admin-user" {
			t.Fatalf("expected %T to be assigned to admin-user, got %q", model, userID)
		}
	}

	assertUserID(&models.Client{}, "uuid = ?", "legacy-client")
	assertUserID(&models.Clipboard{}, "name = ?", "legacy-script")
	assertUserID(&models.Task{}, "task_id = ?", "legacy-task")
	assertUserID(&models.TaskResult{}, "task_id = ? AND client = ?", "legacy-task", "legacy-client")
	assertUserID(&models.FailoverTask{}, "name = ?", "legacy-failover")
	assertUserID(&models.FailoverV2Service{}, "name = ?", "legacy-v2")
	assertUserID(&models.CloudProvider{}, "name = ?", "digitalocean")
	assertUserID(&models.CloudInstanceShare{}, "share_token = ?", "legacy-share-token")
}

func TestBackfillLegacyUserScopedDataSkipsUniqueScopeConflicts(t *testing.T) {
	db := openLegacyOwnerTestDB(t)
	if err := db.AutoMigrate(&models.CloudProvider{}, &models.CloudInstanceShare{}); err != nil {
		t.Fatalf("failed to migrate test schema: %v", err)
	}

	if err := db.Create(&models.CloudProvider{UserID: "admin-user", Name: "linode", Addition: `{"new":true}`}).Error; err != nil {
		t.Fatalf("failed to create owned cloud provider: %v", err)
	}
	if err := db.Create(&models.CloudProvider{Name: "linode", Addition: `{"legacy":true}`}).Error; err != nil {
		t.Fatalf("failed to create legacy cloud provider: %v", err)
	}
	if err := db.Create(&models.CloudInstanceShare{
		UserID:       "admin-user",
		ShareToken:   "owned-token",
		Provider:     "vultr",
		ResourceType: "instance",
		ResourceID:   "abc",
	}).Error; err != nil {
		t.Fatalf("failed to create owned cloud share: %v", err)
	}
	if err := db.Create(&models.CloudInstanceShare{
		ShareToken:   "legacy-token",
		Provider:     "vultr",
		ResourceType: "instance",
		ResourceID:   "abc",
	}).Error; err != nil {
		t.Fatalf("failed to create legacy cloud share: %v", err)
	}

	summary, err := backfillLegacyUserScopedDataWithDB(db, "admin-user")
	if err != nil {
		t.Fatalf("backfillLegacyUserScopedDataWithDB returned error: %v", err)
	}
	if summary.Skipped["cloud_providers"] != 1 {
		t.Fatalf("expected cloud provider conflict to be skipped, got %+v", summary)
	}
	if summary.Skipped["cloud_instance_shares"] != 1 {
		t.Fatalf("expected cloud share conflict to be skipped, got %+v", summary)
	}

	var legacyProvider models.CloudProvider
	if err := db.Where("user_id = ? AND name = ?", "", "linode").First(&legacyProvider).Error; err != nil {
		t.Fatalf("expected legacy cloud provider to remain ownerless after conflict skip: %v", err)
	}
	var legacyShare models.CloudInstanceShare
	if err := db.Where("share_token = ?", "legacy-token").First(&legacyShare).Error; err != nil {
		t.Fatalf("failed to reload legacy share: %v", err)
	}
	if legacyShare.UserID != "" {
		t.Fatalf("expected conflicting legacy share to remain ownerless, got %q", legacyShare.UserID)
	}
}
