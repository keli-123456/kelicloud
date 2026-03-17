package database

import (
	"fmt"
	"strings"
	"testing"

	"github.com/komari-monitor/komari/database/models"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func openCloudShareTestDB(t *testing.T) *gorm.DB {
	t.Helper()

	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared", strings.ReplaceAll(t.Name(), "/", "_"))
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}
	return db
}

func TestCloudInstanceShareIsUserScoped(t *testing.T) {
	db := openCloudShareTestDB(t)

	if err := db.AutoMigrate(&models.CloudInstanceShare{}); err != nil {
		t.Fatalf("failed to migrate test schema: %v", err)
	}

	shareA := &models.CloudInstanceShare{
		UserID:       "user-a",
		ShareToken:   "token-a",
		Provider:     "digitalocean",
		ResourceType: "droplet",
		ResourceID:   "1001",
		ResourceName: "User A Droplet",
	}
	shareB := &models.CloudInstanceShare{
		UserID:       "user-b",
		ShareToken:   "token-b",
		Provider:     "digitalocean",
		ResourceType: "droplet",
		ResourceID:   "1001",
		ResourceName: "User B Droplet",
	}

	if err := saveCloudInstanceShareWithDB(db, shareA); err != nil {
		t.Fatalf("failed to save user A share: %v", err)
	}
	if err := saveCloudInstanceShareWithDB(db, shareB); err != nil {
		t.Fatalf("failed to save user B share: %v", err)
	}

	loadedA, err := getCloudInstanceShareByUserWithDB(db, "user-a", "digitalocean", "droplet", "1001")
	if err != nil {
		t.Fatalf("failed to load user A share: %v", err)
	}
	if loadedA.ShareToken != "token-a" {
		t.Fatalf("expected user A token %q, got %q", "token-a", loadedA.ShareToken)
	}

	loadedB, err := getCloudInstanceShareByUserWithDB(db, "user-b", "digitalocean", "droplet", "1001")
	if err != nil {
		t.Fatalf("failed to load user B share: %v", err)
	}
	if loadedB.ShareToken != "token-b" {
		t.Fatalf("expected user B token %q, got %q", "token-b", loadedB.ShareToken)
	}

	tokenLookup, err := getCloudInstanceShareByTokenWithDB(db, "token-b")
	if err != nil {
		t.Fatalf("failed to load share by token: %v", err)
	}
	if tokenLookup.UserID != "user-b" {
		t.Fatalf("expected token lookup to return user-b share, got %q", tokenLookup.UserID)
	}
}

func TestCloudInstanceShareRequiresUser(t *testing.T) {
	db := openCloudShareTestDB(t)

	if err := db.AutoMigrate(&models.CloudInstanceShare{}); err != nil {
		t.Fatalf("failed to migrate test schema: %v", err)
	}

	err := saveCloudInstanceShareWithDB(db, &models.CloudInstanceShare{
		ShareToken:   "token-a",
		Provider:     "digitalocean",
		ResourceType: "droplet",
		ResourceID:   "1001",
	})
	if err == nil {
		t.Fatal("expected user-scoped cloud share to require user id")
	}
}
