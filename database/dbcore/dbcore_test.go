package dbcore

import (
	"testing"

	"github.com/komari-monitor/komari/database/models"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestPrepareFailoverV2MemberSchemaCompatibilityRebuildsLegacyUniqueIndex(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}
	if err := db.AutoMigrate(&models.FailoverV2Service{}, &models.FailoverV2Member{}); err != nil {
		t.Fatalf("failed to migrate failover v2 schema: %v", err)
	}

	migrator := db.Migrator()
	if migrator.HasIndex(&models.FailoverV2Member{}, "idx_failover_v2_service_client") {
		if err := migrator.DropIndex(&models.FailoverV2Member{}, "idx_failover_v2_service_client"); err != nil {
			t.Fatalf("failed to drop current failover v2 member index: %v", err)
		}
	}
	if err := db.Exec(
		"CREATE UNIQUE INDEX idx_failover_v2_service_client ON failover_v2_members(service_id, watch_client_uuid)",
	).Error; err != nil {
		t.Fatalf("failed to create legacy unique failover v2 member index: %v", err)
	}

	prepareFailoverV2MemberSchemaCompatibility(db)

	indexes, err := migrator.GetIndexes(&models.FailoverV2Member{})
	if err != nil {
		t.Fatalf("failed to inspect failover v2 member indexes: %v", err)
	}
	unique := true
	found := false
	for _, index := range indexes {
		if index.Name() != "idx_failover_v2_service_client" {
			continue
		}
		found = true
		if value, ok := index.Unique(); ok {
			unique = value
		} else {
			t.Fatal("expected rebuilt failover v2 member index uniqueness to be inspectable")
		}
		break
	}
	if !found {
		t.Fatal("expected rebuilt failover v2 member index to exist")
	}
	if unique {
		t.Fatal("expected rebuilt failover v2 member index to be non-unique")
	}

	service := models.FailoverV2Service{
		UserID:      "user-a",
		Name:        "edge-service",
		Enabled:     true,
		DNSProvider: models.FailoverDNSProviderAliyun,
		DNSEntryID:  "dns-entry-1",
		DNSPayload:  `{"domain_name":"example.com","rr":"@"}`,
	}
	if err := db.Create(&service).Error; err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	members := []models.FailoverV2Member{
		{
			ServiceID:       service.ID,
			Name:            "telecom",
			Enabled:         true,
			DNSLine:         "telecom",
			Provider:        "digitalocean",
			ProviderEntryID: "token-1",
		},
		{
			ServiceID:       service.ID,
			Name:            "unicom",
			Enabled:         true,
			DNSLine:         "unicom",
			Provider:        "digitalocean",
			ProviderEntryID: "token-1",
		},
	}
	if err := db.Create(&members).Error; err != nil {
		t.Fatalf("expected multiple uninitialized members after index rebuild, got %v", err)
	}
}
