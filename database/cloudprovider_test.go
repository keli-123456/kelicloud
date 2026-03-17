package database

import (
	"fmt"
	"strings"
	"testing"

	"github.com/komari-monitor/komari/database/models"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func openCloudProviderTestDB(t *testing.T) *gorm.DB {
	t.Helper()

	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared", strings.ReplaceAll(t.Name(), "/", "_"))
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}
	return db
}

func TestCloudProviderConfigIsUserScoped(t *testing.T) {
	db := openCloudProviderTestDB(t)

	if err := db.AutoMigrate(&models.CloudProvider{}); err != nil {
		t.Fatalf("failed to migrate test schema: %v", err)
	}

	configA := &models.CloudProvider{
		UserID:   "user-a",
		Name:     "cloudflare",
		Addition: `{"api_token":"user-a-token"}`,
	}
	configB := &models.CloudProvider{
		UserID:   "user-b",
		Name:     "cloudflare",
		Addition: `{"api_token":"user-b-token"}`,
	}

	if err := saveCloudProviderConfigForUserWithDB(db, configA); err != nil {
		t.Fatalf("failed to save user A config: %v", err)
	}
	if err := saveCloudProviderConfigForUserWithDB(db, configB); err != nil {
		t.Fatalf("failed to save user B config: %v", err)
	}

	loadedA, err := getCloudProviderConfigByUserWithDB(db, "user-a", "cloudflare")
	if err != nil {
		t.Fatalf("failed to load user A config: %v", err)
	}
	if loadedA.Addition != configA.Addition {
		t.Fatalf("expected user A addition %q, got %q", configA.Addition, loadedA.Addition)
	}

	loadedB, err := getCloudProviderConfigByUserWithDB(db, "user-b", "cloudflare")
	if err != nil {
		t.Fatalf("failed to load user B config: %v", err)
	}
	if loadedB.Addition != configB.Addition {
		t.Fatalf("expected user B addition %q, got %q", configB.Addition, loadedB.Addition)
	}
}

func TestCloudProviderConfigUpsertsByUserAndName(t *testing.T) {
	db := openCloudProviderTestDB(t)

	if err := db.AutoMigrate(&models.CloudProvider{}); err != nil {
		t.Fatalf("failed to migrate test schema: %v", err)
	}

	if err := saveCloudProviderConfigForUserWithDB(db, &models.CloudProvider{
		UserID:   "user-a",
		Name:     "digitalocean",
		Addition: `{"api_token":"initial-token"}`,
	}); err != nil {
		t.Fatalf("failed to save initial config: %v", err)
	}
	if err := saveCloudProviderConfigForUserWithDB(db, &models.CloudProvider{
		UserID:   "user-a",
		Name:     "digitalocean",
		Addition: `{"api_token":"updated-token"}`,
	}); err != nil {
		t.Fatalf("failed to update config: %v", err)
	}

	updated, err := getCloudProviderConfigByUserWithDB(db, "user-a", "digitalocean")
	if err != nil {
		t.Fatalf("failed to reload updated config: %v", err)
	}
	if updated.Addition != `{"api_token":"updated-token"}` {
		t.Fatalf("expected updated config, got %q", updated.Addition)
	}

	var count int64
	if err := db.Model(&models.CloudProvider{}).
		Where("user_id = ? AND name = ?", "user-a", "digitalocean").
		Count(&count).Error; err != nil {
		t.Fatalf("failed to count configs: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected single config row after upsert, got %d", count)
	}
}

func TestCloudProviderConfigRequiresUser(t *testing.T) {
	db := openCloudProviderTestDB(t)

	if err := db.AutoMigrate(&models.CloudProvider{}); err != nil {
		t.Fatalf("failed to migrate test schema: %v", err)
	}

	err := saveCloudProviderConfigForUserWithDB(db, &models.CloudProvider{
		Name:     "aws",
		Addition: `{"credentials":[]}`,
	})
	if err == nil {
		t.Fatal("expected user-scoped cloud provider config to require user id")
	}
}
