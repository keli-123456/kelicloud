package clientddns

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/komari-monitor/komari/database/models"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func openClientDDNSTestDB(t *testing.T) *gorm.DB {
	t.Helper()

	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared", strings.ReplaceAll(t.Name(), "/", "_"))
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}
	return db
}

func TestSaveBindingForUserWithDBResetsSyncStateOnTargetChange(t *testing.T) {
	db := openClientDDNSTestDB(t)

	if err := db.AutoMigrate(&models.ClientDDNSBinding{}); err != nil {
		t.Fatalf("failed to migrate client ddns schema: %v", err)
	}

	now := models.FromTime(time.Now())
	if err := db.Create(&models.ClientDDNSBinding{
		UserID:       "user-a",
		ClientUUID:   "node-a",
		Enabled:      true,
		Provider:     "cloudflare",
		EntryID:      "entry-a",
		AddressMode:  models.ClientDDNSAddressModeIPv4,
		Payload:      `{"record_name":"edge.example.com","ttl":60}`,
		RecordKey:    "cloudflare:example.com:edge.example.com",
		LastIPv4:     "1.1.1.1",
		LastIPv6:     "2001:db8::1",
		LastSyncedAt: &now,
		LastError:    "",
		LastResult:   `[{"provider":"cloudflare"}]`,
	}).Error; err != nil {
		t.Fatalf("failed to seed binding: %v", err)
	}

	updated, err := saveBindingForUserWithDB(db, "user-a", "node-a", &models.ClientDDNSBinding{
		Enabled:     true,
		Provider:    "cloudflare",
		EntryID:     "entry-a",
		AddressMode: models.ClientDDNSAddressModeIPv4,
		Payload:     `{"record_name":"new-edge.example.com","ttl":60}`,
		RecordKey:   "cloudflare:example.com:new-edge.example.com",
	})
	if err != nil {
		t.Fatalf("saveBindingForUserWithDB returned error: %v", err)
	}

	if updated.LastIPv4 != "" || updated.LastIPv6 != "" {
		t.Fatalf("expected synced IPs to reset after target change, got ipv4=%q ipv6=%q", updated.LastIPv4, updated.LastIPv6)
	}
	if updated.LastSyncedAt != nil {
		t.Fatalf("expected last synced timestamp to reset after target change, got %v", updated.LastSyncedAt)
	}
	if updated.LastError != "" || updated.LastResult != "" {
		t.Fatalf("expected sync status to reset after target change, got error=%q result=%q", updated.LastError, updated.LastResult)
	}
}

func TestSaveBindingForUserWithDBPreservesSyncStateWhenUnchanged(t *testing.T) {
	db := openClientDDNSTestDB(t)

	if err := db.AutoMigrate(&models.ClientDDNSBinding{}); err != nil {
		t.Fatalf("failed to migrate client ddns schema: %v", err)
	}

	now := models.FromTime(time.Now())
	if err := db.Create(&models.ClientDDNSBinding{
		UserID:       "user-a",
		ClientUUID:   "node-a",
		Enabled:      true,
		Provider:     "cloudflare",
		EntryID:      "entry-a",
		AddressMode:  models.ClientDDNSAddressModeIPv4,
		Payload:      `{"record_name":"edge.example.com","ttl":60}`,
		RecordKey:    "cloudflare:example.com:edge.example.com",
		LastIPv4:     "1.1.1.1",
		LastSyncedAt: &now,
		LastResult:   `[{"provider":"cloudflare"}]`,
	}).Error; err != nil {
		t.Fatalf("failed to seed binding: %v", err)
	}

	updated, err := saveBindingForUserWithDB(db, "user-a", "node-a", &models.ClientDDNSBinding{
		Enabled:     true,
		Provider:    "cloudflare",
		EntryID:     "entry-a",
		AddressMode: models.ClientDDNSAddressModeIPv4,
		Payload:     `{"record_name":"edge.example.com","ttl":60}`,
		RecordKey:   "cloudflare:example.com:edge.example.com",
	})
	if err != nil {
		t.Fatalf("saveBindingForUserWithDB returned error: %v", err)
	}

	if updated.LastIPv4 != "1.1.1.1" {
		t.Fatalf("expected synced IPv4 to be preserved, got %q", updated.LastIPv4)
	}
	if updated.LastSyncedAt == nil {
		t.Fatal("expected last synced timestamp to be preserved")
	}
	if updated.LastResult == "" {
		t.Fatal("expected last sync result to be preserved")
	}
}
