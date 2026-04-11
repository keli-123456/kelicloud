package dbcore

import (
	"testing"

	"github.com/komari-monitor/komari/database/models"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func testPrepareFailoverV2MemberSchemaCompatibilityRebuildsLegacyUniqueIndexWithLegacyLineIndex(t *testing.T, legacyLineIndex string) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}
	if err := db.AutoMigrate(&models.FailoverV2Service{}, &models.FailoverV2Member{}, &models.FailoverV2MemberLine{}); err != nil {
		t.Fatalf("failed to migrate failover v2 schema: %v", err)
	}

	migrator := db.Migrator()
	indexNamesToDrop := []string{
		"idx_failover_v2_service_client",
		"idx_failover_v2_service_line",
		"idx_failover_v2_service_line_code",
		legacyLineIndex,
	}
	uniqueIndexNames := make(map[string]struct{}, len(indexNamesToDrop)*2)
	for _, indexName := range indexNamesToDrop {
		uniqueIndexNames[indexName] = struct{}{}
	}

	for indexName := range uniqueIndexNames {
		if migrator.HasIndex(&models.FailoverV2Member{}, indexName) {
			if err := migrator.DropIndex(&models.FailoverV2Member{}, indexName); err != nil {
				t.Fatalf("failed to drop current failover v2 member index %s: %v", indexName, err)
			}
		}
		if migrator.HasIndex(&models.FailoverV2MemberLine{}, indexName) {
			if err := migrator.DropIndex(&models.FailoverV2MemberLine{}, indexName); err != nil {
				t.Fatalf("failed to drop current failover v2 member line index %s: %v", indexName, err)
			}
		}
	}

	for _, statement := range []string{
		"CREATE UNIQUE INDEX idx_failover_v2_service_client ON failover_v2_members(service_id, watch_client_uuid)",
		"CREATE UNIQUE INDEX idx_failover_v2_service_line ON failover_v2_members(service_id, dns_line)",
		"CREATE UNIQUE INDEX " + legacyLineIndex + " ON failover_v2_member_lines(service_id, line_code)",
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

func TestPrepareFailoverV2MemberSchemaCompatibilityRebuildsLegacyUniqueIndex(t *testing.T) {
	t.Run("default legacy line index name", func(t *testing.T) {
		testPrepareFailoverV2MemberSchemaCompatibilityRebuildsLegacyUniqueIndexWithLegacyLineIndex(t, "idx_failover_v2_service_line_code")
	})

	t.Run("renamed legacy line index name", func(t *testing.T) {
		testPrepareFailoverV2MemberSchemaCompatibilityRebuildsLegacyUniqueIndexWithLegacyLineIndex(t, "idx_failover_v2_service_line_code_legacy")
	})
}

func TestApplyFailoverCooldownDefaultZeroMigration(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}
	if err := db.AutoMigrate(
		&models.FailoverTask{},
		&models.FailoverV2Service{},
		&models.FailoverV2Member{},
		&models.DBMigrationMarker{},
	); err != nil {
		t.Fatalf("failed to migrate failover schema: %v", err)
	}

	legacyTask := models.FailoverTask{
		UserID:          "user-a",
		Name:            "legacy-task",
		WatchClientUUID: "client-legacy",
		DNSProvider:     models.FailoverDNSProviderAliyun,
		DNSEntryID:      "entry-legacy",
		DNSPayload:      "{}",
		CooldownSeconds: 1800,
	}
	customTask := models.FailoverTask{
		UserID:          "user-a",
		Name:            "custom-task",
		WatchClientUUID: "client-custom",
		DNSProvider:     models.FailoverDNSProviderAliyun,
		DNSEntryID:      "entry-custom",
		DNSPayload:      "{}",
		CooldownSeconds: 900,
	}
	if err := db.Create(&legacyTask).Error; err != nil {
		t.Fatalf("failed to create legacy failover task: %v", err)
	}
	if err := db.Create(&customTask).Error; err != nil {
		t.Fatalf("failed to create custom failover task: %v", err)
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
		t.Fatalf("failed to create failover v2 service: %v", err)
	}

	legacyMember := models.FailoverV2Member{
		ServiceID:       service.ID,
		Name:            "telecom-legacy",
		Enabled:         true,
		DNSLine:         "telecom",
		Provider:        "digitalocean",
		ProviderEntryID: "token-legacy",
		CooldownSeconds: 1800,
	}
	customMember := models.FailoverV2Member{
		ServiceID:       service.ID,
		Name:            "telecom-custom",
		Enabled:         true,
		DNSLine:         "telecom2",
		Provider:        "digitalocean",
		ProviderEntryID: "token-custom",
		CooldownSeconds: 900,
	}
	if err := db.Create(&legacyMember).Error; err != nil {
		t.Fatalf("failed to create legacy failover v2 member: %v", err)
	}
	if err := db.Create(&customMember).Error; err != nil {
		t.Fatalf("failed to create custom failover v2 member: %v", err)
	}

	applyFailoverCooldownDefaultZeroMigration(db)

	var reloadedLegacyTask models.FailoverTask
	if err := db.First(&reloadedLegacyTask, legacyTask.ID).Error; err != nil {
		t.Fatalf("failed to reload legacy failover task: %v", err)
	}
	if reloadedLegacyTask.CooldownSeconds != 0 {
		t.Fatalf("expected legacy failover task cooldown to migrate to 0, got %d", reloadedLegacyTask.CooldownSeconds)
	}

	var reloadedCustomTask models.FailoverTask
	if err := db.First(&reloadedCustomTask, customTask.ID).Error; err != nil {
		t.Fatalf("failed to reload custom failover task: %v", err)
	}
	if reloadedCustomTask.CooldownSeconds != 900 {
		t.Fatalf("expected custom failover task cooldown to remain 900, got %d", reloadedCustomTask.CooldownSeconds)
	}

	var reloadedLegacyMember models.FailoverV2Member
	if err := db.First(&reloadedLegacyMember, legacyMember.ID).Error; err != nil {
		t.Fatalf("failed to reload legacy failover v2 member: %v", err)
	}
	if reloadedLegacyMember.CooldownSeconds != 0 {
		t.Fatalf("expected legacy failover v2 member cooldown to migrate to 0, got %d", reloadedLegacyMember.CooldownSeconds)
	}

	var reloadedCustomMember models.FailoverV2Member
	if err := db.First(&reloadedCustomMember, customMember.ID).Error; err != nil {
		t.Fatalf("failed to reload custom failover v2 member: %v", err)
	}
	if reloadedCustomMember.CooldownSeconds != 900 {
		t.Fatalf("expected custom failover v2 member cooldown to remain 900, got %d", reloadedCustomMember.CooldownSeconds)
	}

	var markerCount int64
	if err := db.Model(&models.DBMigrationMarker{}).
		Where("key = ?", migrationKeyFailoverCooldownDefaultZero).
		Count(&markerCount).Error; err != nil {
		t.Fatalf("failed to count migration markers: %v", err)
	}
	if markerCount != 1 {
		t.Fatalf("expected one migration marker after first run, got %d", markerCount)
	}

	if err := db.Model(&models.FailoverTask{}).Where("id = ?", customTask.ID).Update("cooldown_seconds", 1800).Error; err != nil {
		t.Fatalf("failed to seed second-run task cooldown: %v", err)
	}
	if err := db.Model(&models.FailoverV2Member{}).Where("id = ?", customMember.ID).Update("cooldown_seconds", 1800).Error; err != nil {
		t.Fatalf("failed to seed second-run member cooldown: %v", err)
	}

	applyFailoverCooldownDefaultZeroMigration(db)

	if err := db.First(&reloadedCustomTask, customTask.ID).Error; err != nil {
		t.Fatalf("failed to reload custom failover task after second migration run: %v", err)
	}
	if reloadedCustomTask.CooldownSeconds != 1800 {
		t.Fatalf("expected migration to be one-time and keep task cooldown at 1800 on second run, got %d", reloadedCustomTask.CooldownSeconds)
	}

	if err := db.First(&reloadedCustomMember, customMember.ID).Error; err != nil {
		t.Fatalf("failed to reload custom failover v2 member after second migration run: %v", err)
	}
	if reloadedCustomMember.CooldownSeconds != 1800 {
		t.Fatalf("expected migration to be one-time and keep member cooldown at 1800 on second run, got %d", reloadedCustomMember.CooldownSeconds)
	}
}
