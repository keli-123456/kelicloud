package database

import (
	"testing"

	"github.com/komari-monitor/komari/database/models"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestCloudProviderConfigIsTenantScoped(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}

	if err := db.AutoMigrate(&models.CloudProvider{}); err != nil {
		t.Fatalf("failed to migrate test schema: %v", err)
	}

	configA := &models.CloudProvider{
		TenantID: "tenant-a",
		Name:     "cloudflare",
		Addition: `{"api_token":"tenant-a-token"}`,
	}
	configB := &models.CloudProvider{
		TenantID: "tenant-b",
		Name:     "cloudflare",
		Addition: `{"api_token":"tenant-b-token"}`,
	}

	if err := saveCloudProviderConfigWithDB(db, configA); err != nil {
		t.Fatalf("failed to save tenant A config: %v", err)
	}
	if err := saveCloudProviderConfigWithDB(db, configB); err != nil {
		t.Fatalf("failed to save tenant B config: %v", err)
	}

	loadedA, err := getCloudProviderConfigWithDB(db, "tenant-a", "cloudflare")
	if err != nil {
		t.Fatalf("failed to load tenant A config: %v", err)
	}
	if loadedA.Addition != configA.Addition {
		t.Fatalf("expected tenant A addition %q, got %q", configA.Addition, loadedA.Addition)
	}

	loadedB, err := getCloudProviderConfigWithDB(db, "tenant-b", "cloudflare")
	if err != nil {
		t.Fatalf("failed to load tenant B config: %v", err)
	}
	if loadedB.Addition != configB.Addition {
		t.Fatalf("expected tenant B addition %q, got %q", configB.Addition, loadedB.Addition)
	}
}
