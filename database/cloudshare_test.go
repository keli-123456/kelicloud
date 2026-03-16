package database

import (
	"testing"

	"github.com/komari-monitor/komari/database/models"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestCloudInstanceShareIsTenantScoped(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}

	if err := db.AutoMigrate(&models.CloudInstanceShare{}); err != nil {
		t.Fatalf("failed to migrate test schema: %v", err)
	}

	shareA := &models.CloudInstanceShare{
		TenantID:     "tenant-a",
		ShareToken:   "token-a",
		Provider:     "digitalocean",
		ResourceType: "droplet",
		ResourceID:   "1001",
		ResourceName: "Tenant A Droplet",
	}
	shareB := &models.CloudInstanceShare{
		TenantID:     "tenant-b",
		ShareToken:   "token-b",
		Provider:     "digitalocean",
		ResourceType: "droplet",
		ResourceID:   "1001",
		ResourceName: "Tenant B Droplet",
	}

	if err := saveCloudInstanceShareWithDB(db, shareA); err != nil {
		t.Fatalf("failed to save tenant A share: %v", err)
	}
	if err := saveCloudInstanceShareWithDB(db, shareB); err != nil {
		t.Fatalf("failed to save tenant B share: %v", err)
	}

	loadedA, err := getCloudInstanceShareWithDB(db, "tenant-a", "digitalocean", "droplet", "1001")
	if err != nil {
		t.Fatalf("failed to load tenant A share: %v", err)
	}
	if loadedA.ShareToken != "token-a" {
		t.Fatalf("expected tenant A token %q, got %q", "token-a", loadedA.ShareToken)
	}

	loadedB, err := getCloudInstanceShareWithDB(db, "tenant-b", "digitalocean", "droplet", "1001")
	if err != nil {
		t.Fatalf("failed to load tenant B share: %v", err)
	}
	if loadedB.ShareToken != "token-b" {
		t.Fatalf("expected tenant B token %q, got %q", "token-b", loadedB.ShareToken)
	}

	tokenLookup, err := getCloudInstanceShareByTokenWithDB(db, "token-b")
	if err != nil {
		t.Fatalf("failed to load share by token: %v", err)
	}
	if tokenLookup.TenantID != "tenant-b" {
		t.Fatalf("expected token lookup to return tenant-b share, got %q", tokenLookup.TenantID)
	}
}
