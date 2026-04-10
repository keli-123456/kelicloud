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
	if err := db.AutoMigrate(&models.FailoverV2Service{}, &models.FailoverV2Member{}, &models.FailoverV2MemberLine{}); err != nil {
		t.Fatalf("failed to migrate failover v2 schema: %v", err)
	}

	migrator := db.Migrator()
	for _, indexName := range []string{
		"idx_failover_v2_service_client",
		"idx_failover_v2_service_line",
	} {
		if migrator.HasIndex(&models.FailoverV2Member{}, indexName) {
			if err := migrator.DropIndex(&models.FailoverV2Member{}, indexName); err != nil {
				t.Fatalf("failed to drop current failover v2 member index %s: %v", indexName, err)
			}
		}
	}
	if migrator.HasIndex(&models.FailoverV2MemberLine{}, "idx_failover_v2_service_line_code") {
		if err := migrator.DropIndex(&models.FailoverV2MemberLine{}, "idx_failover_v2_service_line_code"); err != nil {
			t.Fatalf("failed to drop current failover v2 member line index: %v", err)
		}
	}

	for _, statement := range []string{
		"CREATE UNIQUE INDEX idx_failover_v2_service_client ON failover_v2_members(service_id, watch_client_uuid)",
		"CREATE UNIQUE INDEX idx_failover_v2_service_line ON failover_v2_members(service_id, dns_line)",
		"CREATE UNIQUE INDEX idx_failover_v2_service_line_code ON failover_v2_member_lines(service_id, line_code)",
	} {
		if err := db.Exec(statement).Error; err != nil {
			t.Fatalf("failed to create legacy unique failover v2 index with %q: %v", statement, err)
		}
	}

	prepareFailoverV2MemberSchemaCompatibility(db)

	indexes, err := migrator.GetIndexes(&models.FailoverV2Member{})
	if err != nil {
		t.Fatalf("failed to inspect failover v2 member indexes: %v", err)
	}

	assertIndexNonUnique := func(indexes []gorm.Index, indexName string) {
		t.Helper()

		found := false
		unique := true
		for _, index := range indexes {
			if index.Name() != indexName {
				continue
			}
			found = true
			if value, ok := index.Unique(); ok {
				unique = value
			} else {
				t.Fatalf("expected rebuilt failover v2 index %s uniqueness to be inspectable", indexName)
			}
			break
		}
		if !found {
			t.Fatalf("expected rebuilt failover v2 index %s to exist", indexName)
		}
		if unique {
			t.Fatalf("expected rebuilt failover v2 index %s to be non-unique", indexName)
		}
	}

	assertIndexNonUnique(indexes, "idx_failover_v2_service_client")
	assertIndexNonUnique(indexes, "idx_failover_v2_service_line")

	lineIndexes, err := migrator.GetIndexes(&models.FailoverV2MemberLine{})
	if err != nil {
		t.Fatalf("failed to inspect failover v2 member line indexes: %v", err)
	}
	assertIndexNonUnique(lineIndexes, "idx_failover_v2_service_line_code")

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
			Name:            "telecom-a",
			Enabled:         true,
			DNSLine:         "telecom",
			Provider:        "digitalocean",
			ProviderEntryID: "token-1",
		},
		{
			ServiceID:       service.ID,
			Name:            "telecom-b",
			Enabled:         true,
			DNSLine:         "telecom",
			Provider:        "digitalocean",
			ProviderEntryID: "token-1",
		},
	}
	if err := db.Create(&members).Error; err != nil {
		t.Fatalf("expected shared dns line members after index rebuild, got %v", err)
	}

	memberLines := []models.FailoverV2MemberLine{
		{
			ServiceID:     service.ID,
			MemberID:      members[0].ID,
			LineCode:      "telecom",
			DNSRecordRefs: `{}`,
		},
		{
			ServiceID:     service.ID,
			MemberID:      members[1].ID,
			LineCode:      "telecom",
			DNSRecordRefs: `{}`,
		},
	}
	if err := db.Create(&memberLines).Error; err != nil {
		t.Fatalf("expected shared member line codes after index rebuild, got %v", err)
	}
}
