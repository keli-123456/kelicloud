package failoverv2

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/komari-monitor/komari/database/models"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func openFailoverV2TestDB(t *testing.T) *gorm.DB {
	t.Helper()

	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}
	if err := db.AutoMigrate(&models.FailoverV2MemberLine{}); err != nil {
		t.Fatalf("failed to migrate failover v2 member line schema: %v", err)
	}
	return db
}

func TestRunLockClaimReleaseAndExpiry(t *testing.T) {
	db := openFailoverV2TestDB(t)
	if err := db.AutoMigrate(&models.FailoverV2RunLock{}); err != nil {
		t.Fatalf("failed to migrate run lock schema: %v", err)
	}

	now := time.Date(2026, time.April, 8, 10, 0, 0, 0, time.UTC)
	claimed, err := claimRunLockWithDB(db, "failover_v2:service:1", "owner-a", time.Minute, now)
	if err != nil {
		t.Fatalf("failed to claim run lock: %v", err)
	}
	if !claimed {
		t.Fatal("expected initial run lock claim to succeed")
	}

	claimed, err = claimRunLockWithDB(db, "failover_v2:service:1", "owner-b", time.Minute, now.Add(10*time.Second))
	if err != nil {
		t.Fatalf("failed to reject duplicate run lock: %v", err)
	}
	if claimed {
		t.Fatal("expected duplicate run lock claim to be rejected")
	}

	claimed, err = claimRunLockWithDB(db, "failover_v2:service:1", "owner-b", time.Minute, now.Add(2*time.Minute))
	if err != nil {
		t.Fatalf("failed to reclaim expired run lock: %v", err)
	}
	if !claimed {
		t.Fatal("expected expired run lock to be reclaimed")
	}

	if err := releaseRunLockWithDB(db, "failover_v2:service:1", "owner-a"); err != nil {
		t.Fatalf("failed to release wrong owner run lock: %v", err)
	}
	claimed, err = claimRunLockWithDB(db, "failover_v2:service:1", "owner-c", time.Minute, now.Add(2*time.Minute+10*time.Second))
	if err != nil {
		t.Fatalf("failed to reject lock after wrong-owner release: %v", err)
	}
	if claimed {
		t.Fatal("expected wrong-owner release to keep run lock held")
	}

	if err := releaseRunLockWithDB(db, "failover_v2:service:1", "owner-b"); err != nil {
		t.Fatalf("failed to release run lock: %v", err)
	}
	claimed, err = claimRunLockWithDB(db, "failover_v2:service:1", "owner-c", time.Minute, now.Add(2*time.Minute+20*time.Second))
	if err != nil {
		t.Fatalf("failed to claim released run lock: %v", err)
	}
	if !claimed {
		t.Fatal("expected released run lock to be claimable")
	}
}

func TestListServicesByUserWithDBPreloadsMembers(t *testing.T) {
	db := openFailoverV2TestDB(t)
	if err := db.AutoMigrate(&models.FailoverV2Service{}, &models.FailoverV2Member{}); err != nil {
		t.Fatalf("failed to migrate failover v2 schema: %v", err)
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
			Priority:        2,
			WatchClientUUID: "client-telecom",
			DNSLine:         "telecom",
			Provider:        "digitalocean",
			ProviderEntryID: "token-1",
		},
		{
			ServiceID:       service.ID,
			Name:            "unicom",
			Enabled:         true,
			Priority:        1,
			WatchClientUUID: "client-unicom",
			DNSLine:         "unicom",
			Provider:        "digitalocean",
			ProviderEntryID: "token-1",
		},
	}
	if err := db.Create(&members).Error; err != nil {
		t.Fatalf("failed to create members: %v", err)
	}

	services, err := listServicesByUserWithDB(db, "user-a")
	if err != nil {
		t.Fatalf("failed to list services: %v", err)
	}
	if len(services) != 1 {
		t.Fatalf("expected one service, got %d", len(services))
	}
	if len(services[0].Members) != 2 {
		t.Fatalf("expected two preloaded members, got %d", len(services[0].Members))
	}
	if services[0].Members[0].DNSLine != "unicom" {
		t.Fatalf("expected members to be ordered by priority, got first line %q", services[0].Members[0].DNSLine)
	}
}

func TestListEnabledServicesWithDBReturnsOnlyEnabledServices(t *testing.T) {
	db := openFailoverV2TestDB(t)
	if err := db.AutoMigrate(&models.FailoverV2Service{}, &models.FailoverV2Member{}); err != nil {
		t.Fatalf("failed to migrate failover v2 schema: %v", err)
	}

	enabledService := models.FailoverV2Service{
		UserID:      "user-a",
		Name:        "enabled-service",
		Enabled:     true,
		DNSProvider: models.FailoverDNSProviderAliyun,
		DNSEntryID:  "dns-entry-1",
		DNSPayload:  `{"domain_name":"example.com","rr":"@"}`,
	}
	disabledService := models.FailoverV2Service{
		UserID:      "user-b",
		Name:        "disabled-service",
		Enabled:     false,
		DNSProvider: models.FailoverDNSProviderAliyun,
		DNSEntryID:  "dns-entry-2",
		DNSPayload:  `{"domain_name":"example.net","rr":"api"}`,
	}
	if err := db.Create(&enabledService).Error; err != nil {
		t.Fatalf("failed to create enabled service: %v", err)
	}
	if err := db.Create(&disabledService).Error; err != nil {
		t.Fatalf("failed to create disabled service: %v", err)
	}
	if err := db.Model(&models.FailoverV2Service{}).
		Where("id = ?", disabledService.ID).
		Update("enabled", false).Error; err != nil {
		t.Fatalf("failed to disable service: %v", err)
	}

	if err := db.Create(&models.FailoverV2Member{
		ServiceID:       enabledService.ID,
		Name:            "telecom",
		Enabled:         true,
		Priority:        1,
		WatchClientUUID: "client-enabled",
		DNSLine:         "telecom",
		Provider:        "digitalocean",
		ProviderEntryID: "token-1",
	}).Error; err != nil {
		t.Fatalf("failed to create enabled service member: %v", err)
	}

	services, err := listEnabledServicesWithDB(db)
	if err != nil {
		t.Fatalf("failed to list enabled services: %v", err)
	}
	if len(services) != 1 {
		t.Fatalf("expected one enabled service, got %d", len(services))
	}
	if services[0].ID != enabledService.ID {
		t.Fatalf("expected enabled service id %d, got %d", enabledService.ID, services[0].ID)
	}
	if len(services[0].Members) != 1 {
		t.Fatalf("expected enabled service members to preload, got %d", len(services[0].Members))
	}
}

func TestListScheduledCheckCandidateServicesWithDBLoadsOnlyDueServices(t *testing.T) {
	db := openFailoverV2TestDB(t)
	if err := db.AutoMigrate(&models.FailoverV2Service{}, &models.FailoverV2Member{}); err != nil {
		t.Fatalf("failed to migrate failover v2 schema: %v", err)
	}

	now := time.Date(2026, time.May, 12, 8, 0, 0, 0, time.UTC)
	oldCheckedAt := models.FromTime(now.Add(-2 * time.Minute))
	recentCheckedAt := models.FromTime(now.Add(-30 * time.Second))

	dueService := models.FailoverV2Service{
		UserID:               "user-a",
		Name:                 "due-service",
		Enabled:              true,
		DNSProvider:          models.FailoverDNSProviderAliyun,
		DNSEntryID:           "dns-entry-1",
		DNSPayload:           `{"domain_name":"example.com","rr":"@"}`,
		CheckIntervalSeconds: 60,
		LastCheckedAt:        &oldCheckedAt,
	}
	recentService := models.FailoverV2Service{
		UserID:               "user-a",
		Name:                 "recent-service",
		Enabled:              true,
		DNSProvider:          models.FailoverDNSProviderAliyun,
		DNSEntryID:           "dns-entry-2",
		DNSPayload:           `{"domain_name":"example.net","rr":"@"}`,
		CheckIntervalSeconds: 60,
		LastCheckedAt:        &recentCheckedAt,
	}
	disabledService := models.FailoverV2Service{
		UserID:               "user-a",
		Name:                 "disabled-service",
		Enabled:              false,
		DNSProvider:          models.FailoverDNSProviderAliyun,
		DNSEntryID:           "dns-entry-3",
		DNSPayload:           `{"domain_name":"example.org","rr":"@"}`,
		CheckIntervalSeconds: 60,
		LastCheckedAt:        &oldCheckedAt,
	}
	if err := db.Create(&dueService).Error; err != nil {
		t.Fatalf("failed to create due service: %v", err)
	}
	if err := db.Create(&recentService).Error; err != nil {
		t.Fatalf("failed to create recent service: %v", err)
	}
	if err := db.Create(&disabledService).Error; err != nil {
		t.Fatalf("failed to create disabled service: %v", err)
	}
	if err := db.Model(&models.FailoverV2Service{}).
		Where("id = ?", disabledService.ID).
		Update("enabled", false).Error; err != nil {
		t.Fatalf("failed to persist disabled service state: %v", err)
	}

	dueMember := models.FailoverV2Member{
		ServiceID:       dueService.ID,
		Name:            "due-member",
		Enabled:         true,
		Priority:        1,
		WatchClientUUID: "client-due",
		DNSLine:         "default",
		Provider:        "digitalocean",
		ProviderEntryID: "token-1",
	}
	recentMember := models.FailoverV2Member{
		ServiceID:       recentService.ID,
		Name:            "recent-member",
		Enabled:         true,
		Priority:        1,
		WatchClientUUID: "client-recent",
		DNSLine:         "default",
		Provider:        "digitalocean",
		ProviderEntryID: "token-1",
	}
	disabledMember := models.FailoverV2Member{
		ServiceID:       disabledService.ID,
		Name:            "disabled-member",
		Enabled:         true,
		Priority:        1,
		WatchClientUUID: "client-disabled",
		DNSLine:         "default",
		Provider:        "digitalocean",
		ProviderEntryID: "token-1",
	}
	if err := db.Create(&[]models.FailoverV2Member{dueMember, recentMember, disabledMember}).Error; err != nil {
		t.Fatalf("failed to create members: %v", err)
	}

	var loadedDueMember models.FailoverV2Member
	if err := db.Where("service_id = ?", dueService.ID).First(&loadedDueMember).Error; err != nil {
		t.Fatalf("failed to reload due member: %v", err)
	}
	if err := db.Create(&models.FailoverV2MemberLine{
		ServiceID: dueService.ID,
		MemberID:  loadedDueMember.ID,
		LineCode:  "default",
	}).Error; err != nil {
		t.Fatalf("failed to create due member line: %v", err)
	}

	services, err := listScheduledCheckCandidateServicesWithDB(db, now)
	if err != nil {
		t.Fatalf("failed to list scheduled check candidates: %v", err)
	}
	if len(services) != 1 {
		t.Fatalf("expected one scheduled check candidate, got %d", len(services))
	}
	if services[0].ID != dueService.ID {
		t.Fatalf("expected due service id %d, got %d", dueService.ID, services[0].ID)
	}
	if len(services[0].Members) != 1 {
		t.Fatalf("expected due service members to preload, got %d", len(services[0].Members))
	}
	if len(services[0].Members[0].Lines) != 1 {
		t.Fatalf("expected due service member lines to preload, got %d", len(services[0].Members[0].Lines))
	}
}

func TestListScheduledCheckCandidateServicesWithDBIncludesExpiredCooldown(t *testing.T) {
	db := openFailoverV2TestDB(t)
	if err := db.AutoMigrate(&models.FailoverV2Service{}, &models.FailoverV2Member{}); err != nil {
		t.Fatalf("failed to migrate failover v2 schema: %v", err)
	}

	now := time.Date(2026, time.May, 12, 8, 0, 0, 0, time.UTC)
	recentCheckedAt := models.FromTime(now.Add(-30 * time.Second))
	triggeredAt := models.FromTime(now.Add(-2 * time.Minute))
	service := models.FailoverV2Service{
		UserID:               "user-a",
		Name:                 "cooldown-service",
		Enabled:              true,
		DNSProvider:          models.FailoverDNSProviderAliyun,
		DNSEntryID:           "dns-entry-1",
		DNSPayload:           `{"domain_name":"example.com","rr":"@"}`,
		CheckIntervalSeconds: 60,
		LastCheckedAt:        &recentCheckedAt,
	}
	if err := db.Create(&service).Error; err != nil {
		t.Fatalf("failed to create cooldown service: %v", err)
	}
	member := models.FailoverV2Member{
		ServiceID:       service.ID,
		Name:            "cooldown-member",
		Enabled:         true,
		Priority:        1,
		WatchClientUUID: "client-cooldown",
		DNSLine:         "default",
		Provider:        "digitalocean",
		ProviderEntryID: "token-1",
		CooldownSeconds: 60,
		LastStatus:      models.FailoverV2MemberStatusCooldown,
		LastMessage:     "cooldown until " + now.Add(-time.Minute).UTC().Format(time.RFC3339),
		LastTriggeredAt: &triggeredAt,
	}
	if err := db.Create(&member).Error; err != nil {
		t.Fatalf("failed to create cooldown member: %v", err)
	}

	services, err := listScheduledCheckCandidateServicesWithDB(db, now)
	if err != nil {
		t.Fatalf("failed to list scheduled check candidates: %v", err)
	}
	if len(services) != 1 {
		t.Fatalf("expected cooldown service candidate, got %d", len(services))
	}
	if services[0].ID != service.ID {
		t.Fatalf("expected service id %d, got %d", service.ID, services[0].ID)
	}
}

func TestListExecutionsByServiceForUserWithDBScopesServiceOwnership(t *testing.T) {
	db := openFailoverV2TestDB(t)
	if err := db.AutoMigrate(&models.FailoverV2Service{}, &models.FailoverV2Execution{}); err != nil {
		t.Fatalf("failed to migrate failover v2 schema: %v", err)
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

	firstStartedAt := models.FromTime(time.Now().Add(-1 * time.Minute))
	secondStartedAt := models.FromTime(time.Now())
	executions := []models.FailoverV2Execution{
		{
			ServiceID:     service.ID,
			MemberID:      1,
			Status:        models.FailoverExecutionStatusFailed,
			TriggerReason: "first",
			StartedAt:     firstStartedAt,
		},
		{
			ServiceID:     service.ID,
			MemberID:      1,
			Status:        models.FailoverExecutionStatusSuccess,
			TriggerReason: "second",
			StartedAt:     secondStartedAt,
		},
	}
	if err := db.Create(&executions).Error; err != nil {
		t.Fatalf("failed to create executions: %v", err)
	}

	items, err := listExecutionsByServiceForUserWithDB(db, "user-a", service.ID, 10)
	if err != nil {
		t.Fatalf("failed to list executions: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("expected two executions, got %d", len(items))
	}
	if items[0].TriggerReason != "second" {
		t.Fatalf("expected newest execution first, got %q", items[0].TriggerReason)
	}

	if _, err := listExecutionsByServiceForUserWithDB(db, "user-b", service.ID, 10); err == nil {
		t.Fatal("expected user scope mismatch to fail")
	}
}

func TestDeleteTerminalExecutionsStartedBeforeWithDBDeletesOnlyExpiredTerminalExecutions(t *testing.T) {
	db := openFailoverV2TestDB(t)
	if err := db.AutoMigrate(
		&models.FailoverV2Service{},
		&models.FailoverV2Member{},
		&models.FailoverV2Execution{},
		&models.FailoverV2ExecutionStep{},
	); err != nil {
		t.Fatalf("failed to migrate failover v2 schema: %v", err)
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
	member := models.FailoverV2Member{
		ServiceID:       service.ID,
		Name:            "telecom",
		Enabled:         true,
		Priority:        1,
		WatchClientUUID: "client-1",
		DNSLine:         "telecom",
		Provider:        "digitalocean",
		ProviderEntryID: "token-1",
	}
	if err := db.Create(&member).Error; err != nil {
		t.Fatalf("failed to create member: %v", err)
	}

	now := time.Now()
	seedExecution := func(status string, startedAt time.Time) uint {
		execution := models.FailoverV2Execution{
			ServiceID:     service.ID,
			MemberID:      member.ID,
			Status:        status,
			TriggerReason: status,
			StartedAt:     models.FromTime(startedAt),
		}
		if err := db.Create(&execution).Error; err != nil {
			t.Fatalf("failed to create execution: %v", err)
		}
		step := models.FailoverV2ExecutionStep{
			ExecutionID: execution.ID,
			Sort:        1,
			StepKey:     "seed",
			StepLabel:   "Seed",
			Status:      models.FailoverStepStatusSuccess,
		}
		if err := db.Create(&step).Error; err != nil {
			t.Fatalf("failed to create execution step: %v", err)
		}
		return execution.ID
	}

	oldSuccessID := seedExecution(models.FailoverV2ExecutionStatusSuccess, now.Add(-45*24*time.Hour))
	oldFailedID := seedExecution(models.FailoverV2ExecutionStatusFailed, now.Add(-35*24*time.Hour))
	recentSuccessID := seedExecution(models.FailoverV2ExecutionStatusSuccess, now.Add(-5*24*time.Hour))
	oldActiveID := seedExecution(models.FailoverV2ExecutionStatusWaitingAgent, now.Add(-60*24*time.Hour))

	cutoff := now.Add(-30 * 24 * time.Hour)

	deleted, err := deleteTerminalExecutionsStartedBeforeWithDB(db, cutoff, 1)
	if err != nil {
		t.Fatalf("failed to delete first execution batch: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("expected first batch to delete one execution, got %d", deleted)
	}

	deleted, err = deleteTerminalExecutionsStartedBeforeWithDB(db, cutoff, 10)
	if err != nil {
		t.Fatalf("failed to delete second execution batch: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("expected second batch to delete one execution, got %d", deleted)
	}

	deleted, err = deleteTerminalExecutionsStartedBeforeWithDB(db, cutoff, 10)
	if err != nil {
		t.Fatalf("failed to delete final execution batch: %v", err)
	}
	if deleted != 0 {
		t.Fatalf("expected no more expired terminal executions, got %d", deleted)
	}

	var remainingExecutionIDs []uint
	if err := db.Model(&models.FailoverV2Execution{}).
		Order("id ASC").
		Pluck("id", &remainingExecutionIDs).Error; err != nil {
		t.Fatalf("failed to list remaining executions: %v", err)
	}
	if len(remainingExecutionIDs) != 2 {
		t.Fatalf("expected two executions to remain, got %d", len(remainingExecutionIDs))
	}
	if remainingExecutionIDs[0] != recentSuccessID || remainingExecutionIDs[1] != oldActiveID {
		t.Fatalf("unexpected remaining execution ids: %#v", remainingExecutionIDs)
	}

	var remainingStepExecutionIDs []uint
	if err := db.Model(&models.FailoverV2ExecutionStep{}).
		Order("execution_id ASC").
		Pluck("execution_id", &remainingStepExecutionIDs).Error; err != nil {
		t.Fatalf("failed to list remaining execution steps: %v", err)
	}
	if len(remainingStepExecutionIDs) != 2 {
		t.Fatalf("expected two execution steps to remain, got %d", len(remainingStepExecutionIDs))
	}
	if remainingStepExecutionIDs[0] != recentSuccessID || remainingStepExecutionIDs[1] != oldActiveID {
		t.Fatalf("unexpected remaining step execution ids: %#v", remainingStepExecutionIDs)
	}

	if oldSuccessID == 0 || oldFailedID == 0 {
		t.Fatal("expected old terminal execution ids to be assigned")
	}
}

func TestGetExecutionByIDForUserWithDBPreloadsOrderedSteps(t *testing.T) {
	db := openFailoverV2TestDB(t)
	if err := db.AutoMigrate(
		&models.FailoverV2Service{},
		&models.FailoverV2Execution{},
		&models.FailoverV2ExecutionStep{},
	); err != nil {
		t.Fatalf("failed to migrate failover v2 schema: %v", err)
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

	execution := models.FailoverV2Execution{
		ServiceID:     service.ID,
		MemberID:      7,
		Status:        models.FailoverV2ExecutionStatusFailed,
		TriggerReason: "manual failover",
		StartedAt:     models.FromTime(time.Now()),
	}
	if err := db.Create(&execution).Error; err != nil {
		t.Fatalf("failed to create execution: %v", err)
	}

	steps := []models.FailoverV2ExecutionStep{
		{
			ExecutionID: execution.ID,
			Sort:        20,
			StepKey:     "attach_dns",
			StepLabel:   "Attach DNS",
			Status:      models.FailoverStepStatusSuccess,
		},
		{
			ExecutionID: execution.ID,
			Sort:        10,
			StepKey:     "detach_dns",
			StepLabel:   "Detach DNS",
			Status:      models.FailoverStepStatusSuccess,
		},
	}
	if err := db.Create(&steps).Error; err != nil {
		t.Fatalf("failed to create execution steps: %v", err)
	}

	item, err := getExecutionByIDForServiceForUserWithDB(db, "user-a", service.ID, execution.ID)
	if err != nil {
		t.Fatalf("failed to get execution: %v", err)
	}
	if len(item.Steps) != 2 {
		t.Fatalf("expected two preloaded steps, got %d", len(item.Steps))
	}
	if item.Steps[0].StepKey != "detach_dns" {
		t.Fatalf("expected steps ordered by sort, got first step %q", item.Steps[0].StepKey)
	}
}

func TestCreateMemberForUserAppliesDefaults(t *testing.T) {
	db := openFailoverV2TestDB(t)
	if err := db.AutoMigrate(&models.FailoverV2Service{}, &models.FailoverV2Member{}, &models.FailoverV2Execution{}); err != nil {
		t.Fatalf("failed to migrate failover v2 schema: %v", err)
	}

	service, err := createServiceForUserWithDB(db, "user-a", &models.FailoverV2Service{
		Name:        "edge-service",
		Enabled:     true,
		DNSProvider: models.FailoverDNSProviderAliyun,
		DNSEntryID:  "dns-entry-1",
		DNSPayload:  `{"domain_name":"example.com","rr":"@"}`,
	})
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	member, err := createMemberForUserWithDB(db, "user-a", service.ID, &models.FailoverV2Member{
		Name:            "telecom",
		Enabled:         true,
		DNSLine:         "telecom",
		WatchClientUUID: "client-1",
		Provider:        "digitalocean",
		ProviderEntryID: "token-1",
	})
	if err != nil {
		t.Fatalf("failed to create member: %v", err)
	}

	if member.ServiceID != service.ID {
		t.Fatalf("expected service id %d, got %d", service.ID, member.ServiceID)
	}
	if member.Priority != 1 {
		t.Fatalf("expected default priority 1, got %d", member.Priority)
	}
	if member.FailureThreshold != 2 {
		t.Fatalf("expected default failure threshold 2, got %d", member.FailureThreshold)
	}
	if member.StaleAfterSeconds != 300 {
		t.Fatalf("expected default stale_after_seconds 300, got %d", member.StaleAfterSeconds)
	}
	if member.CooldownSeconds != 0 {
		t.Fatalf("expected default cooldown_seconds 0, got %d", member.CooldownSeconds)
	}
	if member.PlanPayload != "{}" {
		t.Fatalf("expected default plan payload, got %q", member.PlanPayload)
	}
	if member.DNSRecordRefs != "{}" {
		t.Fatalf("expected default dns record refs, got %q", member.DNSRecordRefs)
	}
}

func TestCreateMemberForUserAllowsMultipleUninitializedMembers(t *testing.T) {
	db := openFailoverV2TestDB(t)
	if err := db.AutoMigrate(&models.FailoverV2Service{}, &models.FailoverV2Member{}, &models.FailoverV2Execution{}); err != nil {
		t.Fatalf("failed to migrate failover v2 schema: %v", err)
	}

	service, err := createServiceForUserWithDB(db, "user-a", &models.FailoverV2Service{
		Name:        "edge-service",
		Enabled:     true,
		DNSProvider: models.FailoverDNSProviderAliyun,
		DNSEntryID:  "dns-entry-1",
		DNSPayload:  `{"domain_name":"example.com","rr":"@"}`,
	})
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	first, err := createMemberForUserWithDB(db, "user-a", service.ID, &models.FailoverV2Member{
		Name:            "telecom",
		Enabled:         true,
		DNSLine:         "telecom",
		WatchClientUUID: "",
		Provider:        "digitalocean",
		ProviderEntryID: "token-1",
	})
	if err != nil {
		t.Fatalf("failed to create first uninitialized member: %v", err)
	}
	second, err := createMemberForUserWithDB(db, "user-a", service.ID, &models.FailoverV2Member{
		Name:            "telecom-backup",
		Enabled:         true,
		DNSLine:         "telecom",
		WatchClientUUID: "",
		Provider:        "digitalocean",
		ProviderEntryID: "token-1",
	})
	if err != nil {
		t.Fatalf("failed to create second member sharing dns line: %v", err)
	}

	if strings.TrimSpace(first.WatchClientUUID) != "" || strings.TrimSpace(second.WatchClientUUID) != "" {
		t.Fatalf("expected both members to remain uninitialized, got %q and %q", first.WatchClientUUID, second.WatchClientUUID)
	}
	if first.DNSLine != "telecom" || second.DNSLine != "telecom" {
		t.Fatalf("expected both members to keep shared telecom line, got %q and %q", first.DNSLine, second.DNSLine)
	}
}

func TestUpdateServiceForUserScopesOwnership(t *testing.T) {
	db := openFailoverV2TestDB(t)
	if err := db.AutoMigrate(&models.FailoverV2Service{}, &models.FailoverV2Member{}, &models.FailoverV2Execution{}, &models.FailoverV2ExecutionStep{}); err != nil {
		t.Fatalf("failed to migrate failover v2 schema: %v", err)
	}

	service, err := createServiceForUserWithDB(db, "user-a", &models.FailoverV2Service{
		Name:        "edge-service",
		Enabled:     true,
		DNSProvider: models.FailoverDNSProviderAliyun,
		DNSEntryID:  "dns-entry-1",
		DNSPayload:  `{"domain_name":"example.com","rr":"@"}`,
	})
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	if _, err := updateServiceForUserWithDB(db, "user-b", service.ID, &models.FailoverV2Service{
		Name:        "blocked",
		Enabled:     true,
		DNSProvider: models.FailoverDNSProviderAliyun,
		DNSEntryID:  "dns-entry-1",
		DNSPayload:  `{"domain_name":"example.com","rr":"@"}`,
	}); err == nil {
		t.Fatal("expected ownership mismatch to fail")
	}

	updated, err := updateServiceForUserWithDB(db, "user-a", service.ID, &models.FailoverV2Service{
		Name:                "edge-service-2",
		Enabled:             false,
		DNSProvider:         models.FailoverDNSProviderAliyun,
		DNSEntryID:          "dns-entry-2",
		DNSPayload:          `{"domain_name":"example.com","rr":"api"}`,
		ScriptClipboardIDs:  `[1,2]`,
		ScriptTimeoutSec:    900,
		WaitAgentTimeoutSec: 1200,
		DeleteStrategy:      models.FailoverDeleteStrategyDeleteAfterSuccessDelay,
		DeleteDelaySeconds:  30,
	})
	if err != nil {
		t.Fatalf("failed to update service: %v", err)
	}

	if updated.Name != "edge-service-2" {
		t.Fatalf("expected updated service name, got %q", updated.Name)
	}
	if updated.Enabled {
		t.Fatal("expected service to be disabled")
	}
	if updated.DNSEntryID != "dns-entry-2" {
		t.Fatalf("expected updated dns entry id, got %q", updated.DNSEntryID)
	}
	if updated.ScriptClipboardIDs != `[1,2]` {
		t.Fatalf("expected script clipboard ids to persist, got %q", updated.ScriptClipboardIDs)
	}
	if updated.DeleteDelaySeconds != 30 {
		t.Fatalf("expected delete delay 30, got %d", updated.DeleteDelaySeconds)
	}
}

func TestUpdateAndDeleteBlockActiveExecution(t *testing.T) {
	db := openFailoverV2TestDB(t)
	if err := db.AutoMigrate(&models.FailoverV2Service{}, &models.FailoverV2Member{}, &models.FailoverV2Execution{}, &models.FailoverV2ExecutionStep{}); err != nil {
		t.Fatalf("failed to migrate failover v2 schema: %v", err)
	}

	service, err := createServiceForUserWithDB(db, "user-a", &models.FailoverV2Service{
		Name:        "edge-service",
		Enabled:     true,
		DNSProvider: models.FailoverDNSProviderAliyun,
		DNSEntryID:  "dns-entry-1",
		DNSPayload:  `{"domain_name":"example.com","rr":"@"}`,
	})
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}
	member, err := createMemberForUserWithDB(db, "user-a", service.ID, &models.FailoverV2Member{
		Name:            "telecom",
		Enabled:         true,
		DNSLine:         "telecom",
		WatchClientUUID: "client-1",
		Provider:        "digitalocean",
		ProviderEntryID: "token-1",
	})
	if err != nil {
		t.Fatalf("failed to create member: %v", err)
	}
	if _, err := createExecutionForUserWithDB(db, "user-a", service.ID, member.ID, &models.FailoverV2Execution{
		Status: models.FailoverV2ExecutionStatusWaitingAgent,
	}); err != nil {
		t.Fatalf("failed to create active execution: %v", err)
	}

	if _, err := updateServiceForUserWithDB(db, "user-a", service.ID, &models.FailoverV2Service{
		Name:        "edge-service-updated",
		Enabled:     true,
		DNSProvider: models.FailoverDNSProviderAliyun,
		DNSEntryID:  "dns-entry-1",
		DNSPayload:  `{"domain_name":"example.com","rr":"api"}`,
	}); err == nil || !strings.Contains(err.Error(), "active") {
		t.Fatalf("expected active execution to block service update, got %v", err)
	}
	if _, err := updateMemberForUserWithDB(db, "user-a", service.ID, member.ID, &models.FailoverV2Member{
		Name:            "telecom-updated",
		Enabled:         true,
		DNSLine:         "telecom",
		WatchClientUUID: "client-1",
		Provider:        "digitalocean",
		ProviderEntryID: "token-1",
	}); err == nil || !strings.Contains(err.Error(), "active") {
		t.Fatalf("expected active execution to block member update, got %v", err)
	}
	if _, err := createMemberForUserWithDB(db, "user-a", service.ID, &models.FailoverV2Member{
		Name:            "mobile",
		Enabled:         true,
		DNSLine:         "mobile",
		WatchClientUUID: "client-2",
		Provider:        "digitalocean",
		ProviderEntryID: "token-1",
	}); err == nil || !strings.Contains(err.Error(), "active") {
		t.Fatalf("expected active execution to block member create, got %v", err)
	}
	if err := deleteMemberForUserWithDB(db, "user-a", service.ID, member.ID); err == nil || !strings.Contains(err.Error(), "active") {
		t.Fatalf("expected active execution to block member delete, got %v", err)
	}
	if err := deleteServiceForUserWithDB(db, "user-a", service.ID); err == nil || !strings.Contains(err.Error(), "active") {
		t.Fatalf("expected active execution to block service delete, got %v", err)
	}
}

func TestSetServiceEnabledForUserUpdatesStateAndBlocksActiveExecution(t *testing.T) {
	db := openFailoverV2TestDB(t)
	if err := db.AutoMigrate(&models.FailoverV2Service{}, &models.FailoverV2Member{}, &models.FailoverV2Execution{}, &models.FailoverV2ExecutionStep{}); err != nil {
		t.Fatalf("failed to migrate failover v2 schema: %v", err)
	}

	service, err := createServiceForUserWithDB(db, "user-a", &models.FailoverV2Service{
		Name:        "edge-service",
		Enabled:     true,
		DNSProvider: models.FailoverDNSProviderAliyun,
		DNSEntryID:  "dns-entry-1",
		DNSPayload:  `{"domain_name":"example.com","rr":"@"}`,
		LastStatus:  models.FailoverV2ServiceStatusHealthy,
	})
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	disabled, err := setServiceEnabledForUserWithDB(db, "user-a", service.ID, false)
	if err != nil {
		t.Fatalf("failed to disable service: %v", err)
	}
	if disabled.Enabled {
		t.Fatal("expected service to be disabled")
	}
	if disabled.LastStatus != models.FailoverV2ServiceStatusUnknown {
		t.Fatalf("expected disabled service status unknown, got %q", disabled.LastStatus)
	}
	if !strings.Contains(disabled.LastMessage, "paused by operator") {
		t.Fatalf("expected disabled service message to mention pause, got %q", disabled.LastMessage)
	}

	enabled, err := setServiceEnabledForUserWithDB(db, "user-a", service.ID, true)
	if err != nil {
		t.Fatalf("failed to enable service: %v", err)
	}
	if !enabled.Enabled {
		t.Fatal("expected service to be enabled")
	}
	if enabled.LastStatus != models.FailoverV2ServiceStatusUnknown {
		t.Fatalf("expected re-enabled service status unknown, got %q", enabled.LastStatus)
	}
	if !strings.Contains(enabled.LastMessage, "resumed by operator") {
		t.Fatalf("expected enabled service message to mention resume, got %q", enabled.LastMessage)
	}

	member, err := createMemberForUserWithDB(db, "user-a", service.ID, &models.FailoverV2Member{
		Name:            "telecom",
		Enabled:         true,
		DNSLine:         "telecom",
		WatchClientUUID: "client-1",
		Provider:        "digitalocean",
		ProviderEntryID: "token-1",
	})
	if err != nil {
		t.Fatalf("failed to create member: %v", err)
	}
	if _, err := createExecutionForUserWithDB(db, "user-a", service.ID, member.ID, &models.FailoverV2Execution{
		Status: models.FailoverV2ExecutionStatusRunningScripts,
	}); err != nil {
		t.Fatalf("failed to create active execution: %v", err)
	}

	if _, err := setServiceEnabledForUserWithDB(db, "user-a", service.ID, false); err == nil {
		t.Fatal("expected active execution to block service toggle")
	}
}

func TestSetMemberEnabledForUserUpdatesStateAndResetsFailures(t *testing.T) {
	db := openFailoverV2TestDB(t)
	if err := db.AutoMigrate(&models.FailoverV2Service{}, &models.FailoverV2Member{}, &models.FailoverV2Execution{}, &models.FailoverV2ExecutionStep{}); err != nil {
		t.Fatalf("failed to migrate failover v2 schema: %v", err)
	}

	service, err := createServiceForUserWithDB(db, "user-a", &models.FailoverV2Service{
		Name:        "edge-service",
		Enabled:     true,
		DNSProvider: models.FailoverDNSProviderAliyun,
		DNSEntryID:  "dns-entry-1",
		DNSPayload:  `{"domain_name":"example.com","rr":"@"}`,
	})
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}
	member, err := createMemberForUserWithDB(db, "user-a", service.ID, &models.FailoverV2Member{
		Name:                "telecom",
		Enabled:             true,
		DNSLine:             "telecom",
		WatchClientUUID:     "client-1",
		Provider:            "digitalocean",
		ProviderEntryID:     "token-1",
		TriggerFailureCount: 2,
		LastStatus:          models.FailoverV2MemberStatusTriggered,
	})
	if err != nil {
		t.Fatalf("failed to create member: %v", err)
	}

	disabled, err := setMemberEnabledForUserWithDB(db, "user-a", service.ID, member.ID, false)
	if err != nil {
		t.Fatalf("failed to disable member: %v", err)
	}
	if disabled.Enabled {
		t.Fatal("expected member to be disabled")
	}
	if disabled.LastStatus != models.FailoverV2MemberStatusDisabled {
		t.Fatalf("expected disabled member status disabled, got %q", disabled.LastStatus)
	}
	if disabled.TriggerFailureCount != 0 {
		t.Fatalf("expected member trigger failure count reset, got %d", disabled.TriggerFailureCount)
	}
	if !strings.Contains(disabled.LastMessage, "paused by operator") {
		t.Fatalf("expected disabled member message to mention pause, got %q", disabled.LastMessage)
	}

	enabled, err := setMemberEnabledForUserWithDB(db, "user-a", service.ID, member.ID, true)
	if err != nil {
		t.Fatalf("failed to enable member: %v", err)
	}
	if !enabled.Enabled {
		t.Fatal("expected member to be enabled")
	}
	if enabled.LastStatus != models.FailoverV2MemberStatusUnknown {
		t.Fatalf("expected re-enabled member status unknown, got %q", enabled.LastStatus)
	}
	if !strings.Contains(enabled.LastMessage, "resumed by operator") {
		t.Fatalf("expected enabled member message to mention resume, got %q", enabled.LastMessage)
	}

	if _, err := createExecutionForUserWithDB(db, "user-a", service.ID, member.ID, &models.FailoverV2Execution{
		Status: models.FailoverV2ExecutionStatusWaitingAgent,
	}); err != nil {
		t.Fatalf("failed to create active execution: %v", err)
	}
	if _, err := setMemberEnabledForUserWithDB(db, "user-a", service.ID, member.ID, false); err == nil {
		t.Fatal("expected active execution to block member toggle")
	}
}

func TestCreateExecutionForUserAppliesDefaultsAndScopesOwnership(t *testing.T) {
	db := openFailoverV2TestDB(t)
	if err := db.AutoMigrate(&models.FailoverV2Service{}, &models.FailoverV2Member{}, &models.FailoverV2Execution{}, &models.FailoverV2ExecutionStep{}); err != nil {
		t.Fatalf("failed to migrate failover v2 schema: %v", err)
	}

	service, err := createServiceForUserWithDB(db, "user-a", &models.FailoverV2Service{
		Name:        "edge-service",
		Enabled:     true,
		DNSProvider: models.FailoverDNSProviderAliyun,
		DNSEntryID:  "dns-entry-1",
		DNSPayload:  `{"domain_name":"example.com","rr":"@"}`,
	})
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}
	member, err := createMemberForUserWithDB(db, "user-a", service.ID, &models.FailoverV2Member{
		Name:            "telecom",
		Enabled:         true,
		DNSLine:         "telecom",
		WatchClientUUID: "client-1",
		Provider:        "digitalocean",
		ProviderEntryID: "token-1",
	})
	if err != nil {
		t.Fatalf("failed to create member: %v", err)
	}

	execution, err := createExecutionForUserWithDB(db, "user-a", service.ID, member.ID, &models.FailoverV2Execution{
		TriggerReason: "manual detach dns",
	})
	if err != nil {
		t.Fatalf("failed to create execution: %v", err)
	}
	if execution.ServiceID != service.ID || execution.MemberID != member.ID {
		t.Fatalf("unexpected service/member ids on execution: %#v", execution)
	}
	if execution.Status != models.FailoverV2ExecutionStatusQueued {
		t.Fatalf("expected queued status, got %q", execution.Status)
	}
	if execution.DetachDNSStatus != models.FailoverDNSStatusPending {
		t.Fatalf("expected pending detach dns status, got %q", execution.DetachDNSStatus)
	}
	if execution.AttachDNSStatus != models.FailoverDNSStatusPending {
		t.Fatalf("expected pending attach dns status, got %q", execution.AttachDNSStatus)
	}
	if execution.CleanupStatus != models.FailoverCleanupStatusPending {
		t.Fatalf("expected pending cleanup status, got %q", execution.CleanupStatus)
	}

	if _, err := createExecutionForUserWithDB(db, "user-b", service.ID, member.ID, &models.FailoverV2Execution{
		TriggerReason: "blocked",
	}); err == nil {
		t.Fatal("expected ownership mismatch to fail")
	}
}

func TestHasActiveExecutionForServiceAndRecovery(t *testing.T) {
	db := openFailoverV2TestDB(t)
	if err := db.AutoMigrate(&models.FailoverV2Service{}, &models.FailoverV2Member{}, &models.FailoverV2Execution{}, &models.FailoverV2ExecutionStep{}); err != nil {
		t.Fatalf("failed to migrate failover v2 schema: %v", err)
	}

	service, err := createServiceForUserWithDB(db, "user-a", &models.FailoverV2Service{
		Name:        "edge-service",
		Enabled:     true,
		DNSProvider: models.FailoverDNSProviderAliyun,
		DNSEntryID:  "dns-entry-1",
		DNSPayload:  `{"domain_name":"example.com","rr":"@"}`,
	})
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}
	member, err := createMemberForUserWithDB(db, "user-a", service.ID, &models.FailoverV2Member{
		Name:            "telecom",
		Enabled:         true,
		DNSLine:         "telecom",
		WatchClientUUID: "client-1",
		Provider:        "digitalocean",
		ProviderEntryID: "token-1",
	})
	if err != nil {
		t.Fatalf("failed to create member: %v", err)
	}
	execution, err := createExecutionForUserWithDB(db, "user-a", service.ID, member.ID, &models.FailoverV2Execution{
		Status: models.FailoverV2ExecutionStatusDetachingDNS,
	})
	if err != nil {
		t.Fatalf("failed to create active execution: %v", err)
	}
	if err := db.Model(&models.FailoverV2Service{}).
		Where("id = ?", service.ID).
		Updates(map[string]interface{}{
			"last_execution_id": execution.ID,
			"last_status":       models.FailoverV2ServiceStatusRunning,
			"last_message":      "running",
		}).Error; err != nil {
		t.Fatalf("failed to seed service state: %v", err)
	}
	if err := db.Model(&models.FailoverV2Member{}).
		Where("id = ?", member.ID).
		Updates(map[string]interface{}{
			"last_execution_id": execution.ID,
			"last_status":       models.FailoverV2MemberStatusRunning,
			"last_message":      "running",
		}).Error; err != nil {
		t.Fatalf("failed to seed member state: %v", err)
	}
	stepStartedAt := models.FromTime(time.Now())
	step := models.FailoverV2ExecutionStep{
		ExecutionID: execution.ID,
		Sort:        1,
		StepKey:     "detach_dns",
		StepLabel:   "Detach DNS",
		Status:      models.FailoverStepStatusRunning,
		StartedAt:   &stepStartedAt,
	}
	if err := db.Create(&step).Error; err != nil {
		t.Fatalf("failed to seed running step: %v", err)
	}

	active, err := hasActiveExecutionForServiceWithDB(db, "user-a", service.ID)
	if err != nil {
		t.Fatalf("failed to query active execution: %v", err)
	}
	if !active {
		t.Fatal("expected active execution to be detected")
	}
	activeExecution, err := getActiveExecutionForServiceForUserWithDB(db, "user-a", service.ID)
	if err != nil {
		t.Fatalf("failed to load active execution: %v", err)
	}
	if activeExecution.ID != execution.ID {
		t.Fatalf("expected active execution id %d, got %d", execution.ID, activeExecution.ID)
	}
	activeMemberExecution, err := getActiveExecutionForMemberForUserWithDB(db, "user-a", service.ID, member.ID)
	if err != nil {
		t.Fatalf("failed to load active member execution: %v", err)
	}
	if activeMemberExecution.ID != execution.ID {
		t.Fatalf("expected active member execution id %d, got %d", execution.ID, activeMemberExecution.ID)
	}

	recovered, err := recoverInterruptedExecutionsForServiceWithDB(db, "user-a", service.ID, "recovered")
	if err != nil {
		t.Fatalf("failed to recover interrupted execution: %v", err)
	}
	if recovered != 1 {
		t.Fatalf("expected one recovered execution, got %d", recovered)
	}

	active, err = hasActiveExecutionForServiceWithDB(db, "user-a", service.ID)
	if err != nil {
		t.Fatalf("failed to query active execution after recovery: %v", err)
	}
	if active {
		t.Fatal("expected no active execution after recovery")
	}
	if _, err := getActiveExecutionForServiceForUserWithDB(db, "user-a", service.ID); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("expected active execution lookup to return not found after recovery, got %v", err)
	}
	if _, err := getActiveExecutionForMemberForUserWithDB(db, "user-a", service.ID, member.ID); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("expected active member execution lookup to return not found after recovery, got %v", err)
	}
	reloadedExecution, err := getExecutionByIDForServiceForUserWithDB(db, "user-a", service.ID, execution.ID)
	if err != nil {
		t.Fatalf("failed to reload recovered execution: %v", err)
	}
	if reloadedExecution.Status != models.FailoverV2ExecutionStatusFailed {
		t.Fatalf("expected recovered execution failed, got %q", reloadedExecution.Status)
	}
	if reloadedExecution.ErrorMessage != "recovered" {
		t.Fatalf("expected recovery message, got %q", reloadedExecution.ErrorMessage)
	}
	if reloadedExecution.FinishedAt == nil {
		t.Fatal("expected recovered execution finished_at to be set")
	}
	if len(reloadedExecution.Steps) != 1 || reloadedExecution.Steps[0].Status != models.FailoverStepStatusFailed {
		t.Fatalf("expected running step to be failed by recovery, got %#v", reloadedExecution.Steps)
	}
	reloadedService, err := getServiceByIDForUserWithDB(db, "user-a", service.ID)
	if err != nil {
		t.Fatalf("failed to reload service: %v", err)
	}
	if reloadedService.LastStatus != models.FailoverV2ServiceStatusFailed || reloadedService.LastMessage != "recovered" {
		t.Fatalf("expected service recovery failure state, got %q %q", reloadedService.LastStatus, reloadedService.LastMessage)
	}
	reloadedMember, err := getMemberByIDForServiceForUserWithDB(db, "user-a", service.ID, member.ID)
	if err != nil {
		t.Fatalf("failed to reload member: %v", err)
	}
	if reloadedMember.LastStatus != models.FailoverV2MemberStatusFailed || reloadedMember.LastMessage != "recovered" {
		t.Fatalf("expected member recovery failure state, got %q %q", reloadedMember.LastStatus, reloadedMember.LastMessage)
	}
	if reloadedMember.LastFailedAt == nil {
		t.Fatal("expected member recovery last_failed_at to be set")
	}
}

func TestRecoverInterruptedExecutionsWithDBRecoversAllServices(t *testing.T) {
	db := openFailoverV2TestDB(t)
	if err := db.AutoMigrate(&models.FailoverV2Service{}, &models.FailoverV2Member{}, &models.FailoverV2Execution{}, &models.FailoverV2ExecutionStep{}); err != nil {
		t.Fatalf("failed to migrate failover v2 schema: %v", err)
	}

	service, err := createServiceForUserWithDB(db, "user-a", &models.FailoverV2Service{
		Name:        "edge-service",
		Enabled:     true,
		DNSProvider: models.FailoverDNSProviderAliyun,
		DNSEntryID:  "dns-entry-1",
		DNSPayload:  `{"domain_name":"example.com","rr":"@"}`,
	})
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}
	member, err := createMemberForUserWithDB(db, "user-a", service.ID, &models.FailoverV2Member{
		Name:            "telecom",
		Enabled:         true,
		DNSLine:         "telecom",
		WatchClientUUID: "client-1",
		Provider:        "digitalocean",
		ProviderEntryID: "token-1",
	})
	if err != nil {
		t.Fatalf("failed to create member: %v", err)
	}
	if _, err := createExecutionForUserWithDB(db, "user-a", service.ID, member.ID, &models.FailoverV2Execution{
		Status: models.FailoverV2ExecutionStatusWaitingAgent,
	}); err != nil {
		t.Fatalf("failed to create active execution: %v", err)
	}

	recovered, err := recoverInterruptedExecutionsWithDB(db, "", 0, "startup recovery", time.Now())
	if err != nil {
		t.Fatalf("failed to recover all interrupted executions: %v", err)
	}
	if recovered != 1 {
		t.Fatalf("expected one recovered execution, got %d", recovered)
	}
	active, err := hasActiveExecutionForServiceWithDB(db, "user-a", service.ID)
	if err != nil {
		t.Fatalf("failed to query active executions after global recovery: %v", err)
	}
	if active {
		t.Fatal("expected global recovery to clear active executions")
	}
}

func TestRecoverInterruptedExecutionsForServiceWithDBRecoversLegacyRunningStatus(t *testing.T) {
	db := openFailoverV2TestDB(t)
	if err := db.AutoMigrate(&models.FailoverV2Service{}, &models.FailoverV2Member{}, &models.FailoverV2Execution{}, &models.FailoverV2ExecutionStep{}); err != nil {
		t.Fatalf("failed to migrate failover v2 schema: %v", err)
	}

	service, err := createServiceForUserWithDB(db, "user-a", &models.FailoverV2Service{
		Name:        "legacy-running-service",
		Enabled:     true,
		DNSProvider: models.FailoverDNSProviderAliyun,
		DNSEntryID:  "dns-entry-legacy",
		DNSPayload:  `{"domain_name":"example.com","rr":"@"}`,
	})
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}
	member, err := createMemberForUserWithDB(db, "user-a", service.ID, &models.FailoverV2Member{
		Name:            "telecom",
		Enabled:         true,
		DNSLine:         "telecom",
		WatchClientUUID: "client-legacy",
		Provider:        "digitalocean",
		ProviderEntryID: "token-legacy",
	})
	if err != nil {
		t.Fatalf("failed to create member: %v", err)
	}
	execution, err := createExecutionForUserWithDB(db, "user-a", service.ID, member.ID, &models.FailoverV2Execution{
		Status:    "running",
		StartedAt: models.FromTime(time.Now()),
	})
	if err != nil {
		t.Fatalf("failed to create legacy running execution: %v", err)
	}
	if err := db.Model(&models.FailoverV2Service{}).
		Where("id = ?", service.ID).
		Updates(map[string]interface{}{
			"last_execution_id": execution.ID,
			"last_status":       models.FailoverV2ServiceStatusRunning,
			"last_message":      "running",
		}).Error; err != nil {
		t.Fatalf("failed to seed legacy service running state: %v", err)
	}
	if err := db.Model(&models.FailoverV2Member{}).
		Where("id = ?", member.ID).
		Updates(map[string]interface{}{
			"last_execution_id": execution.ID,
			"last_status":       models.FailoverV2MemberStatusRunning,
			"last_message":      "running",
		}).Error; err != nil {
		t.Fatalf("failed to seed legacy member running state: %v", err)
	}

	active, err := hasActiveExecutionForServiceWithDB(db, "user-a", service.ID)
	if err != nil {
		t.Fatalf("failed to query active execution: %v", err)
	}
	if !active {
		t.Fatal("expected legacy running execution to be treated as active")
	}

	recovered, err := recoverInterruptedExecutionsForServiceWithDB(db, "user-a", service.ID, "legacy recovered")
	if err != nil {
		t.Fatalf("failed to recover legacy running execution: %v", err)
	}
	if recovered != 1 {
		t.Fatalf("expected one recovered legacy execution, got %d", recovered)
	}

	reloadedExecution, err := getExecutionByIDForServiceForUserWithDB(db, "user-a", service.ID, execution.ID)
	if err != nil {
		t.Fatalf("failed to reload recovered execution: %v", err)
	}
	if reloadedExecution.Status != models.FailoverV2ExecutionStatusFailed {
		t.Fatalf("expected recovered execution failed, got %q", reloadedExecution.Status)
	}
	if reloadedExecution.FinishedAt == nil {
		t.Fatal("expected recovered execution finished_at to be set")
	}

	reloadedService, err := getServiceByIDForUserWithDB(db, "user-a", service.ID)
	if err != nil {
		t.Fatalf("failed to reload service: %v", err)
	}
	if reloadedService.LastStatus != models.FailoverV2ServiceStatusFailed {
		t.Fatalf("expected service status failed after recovery, got %q", reloadedService.LastStatus)
	}
	reloadedMember, err := getMemberByIDForServiceForUserWithDB(db, "user-a", service.ID, member.ID)
	if err != nil {
		t.Fatalf("failed to reload member: %v", err)
	}
	if reloadedMember.LastStatus != models.FailoverV2MemberStatusFailed {
		t.Fatalf("expected member status failed after recovery, got %q", reloadedMember.LastStatus)
	}
}

func TestCreateOrUpdatePendingCleanupAndListDue(t *testing.T) {
	db := openFailoverV2TestDB(t)
	if err := db.AutoMigrate(&models.FailoverV2PendingCleanup{}); err != nil {
		t.Fatalf("failed to migrate pending cleanup schema: %v", err)
	}

	now := time.Now()
	nextRetry := models.FromTime(now.Add(-1 * time.Minute))
	created, err := createOrUpdatePendingCleanupWithDB(db, &models.FailoverV2PendingCleanup{
		UserID:          "user-a",
		ServiceID:       1,
		MemberID:        2,
		ExecutionID:     3,
		Provider:        "digitalocean",
		ProviderEntryID: "token-1",
		ResourceType:    "droplet",
		ResourceID:      "100",
		InstanceRef:     `{"provider":"digitalocean","droplet_id":100}`,
		CleanupLabel:    "delete digitalocean droplet 100",
		Status:          models.FailoverV2PendingCleanupStatusPending,
		AttemptCount:    1,
		LastError:       "delete failed",
		NextRetryAt:     &nextRetry,
	})
	if err != nil {
		t.Fatalf("failed to create pending cleanup: %v", err)
	}
	if created.ID == 0 {
		t.Fatal("expected pending cleanup id to be assigned")
	}

	updated, err := createOrUpdatePendingCleanupWithDB(db, &models.FailoverV2PendingCleanup{
		UserID:          "user-a",
		ServiceID:       4,
		MemberID:        5,
		ExecutionID:     6,
		Provider:        "digitalocean",
		ProviderEntryID: "token-2",
		ResourceType:    "droplet",
		ResourceID:      "100",
		InstanceRef:     `{"provider":"digitalocean","droplet_id":100}`,
		CleanupLabel:    "delete digitalocean droplet 100",
		Status:          models.FailoverV2PendingCleanupStatusPending,
		AttemptCount:    2,
		LastError:       "still failed",
	})
	if err != nil {
		t.Fatalf("failed to update pending cleanup: %v", err)
	}
	if updated.ID != created.ID {
		t.Fatalf("expected upsert to reuse cleanup id %d, got %d", created.ID, updated.ID)
	}
	if updated.ProviderEntryID != "token-2" {
		t.Fatalf("expected provider entry to update, got %q", updated.ProviderEntryID)
	}

	items, err := listDuePendingCleanupsWithDB(db, 10, now)
	if err != nil {
		t.Fatalf("failed to list due pending cleanups: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected one due pending cleanup, got %d", len(items))
	}
	if items[0].ID != created.ID {
		t.Fatalf("expected due cleanup id %d, got %d", created.ID, items[0].ID)
	}
}

func TestListPendingCleanupsByServiceForUserScopesOwnership(t *testing.T) {
	db := openFailoverV2TestDB(t)
	if err := db.AutoMigrate(&models.FailoverV2Service{}, &models.FailoverV2Member{}, &models.FailoverV2PendingCleanup{}); err != nil {
		t.Fatalf("failed to migrate pending cleanup list schema: %v", err)
	}

	service, err := createServiceForUserWithDB(db, "user-a", &models.FailoverV2Service{
		Name:        "edge-service",
		Enabled:     true,
		DNSProvider: models.FailoverDNSProviderAliyun,
		DNSEntryID:  "dns-entry-1",
		DNSPayload:  `{"domain_name":"example.com","rr":"@"}`,
	})
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}
	otherService, err := createServiceForUserWithDB(db, "user-b", &models.FailoverV2Service{
		Name:        "other-service",
		Enabled:     true,
		DNSProvider: models.FailoverDNSProviderAliyun,
		DNSEntryID:  "dns-entry-2",
		DNSPayload:  `{"domain_name":"example.net","rr":"@"}`,
	})
	if err != nil {
		t.Fatalf("failed to create other service: %v", err)
	}

	first := models.FromTime(time.Now().Add(-2 * time.Minute))
	second := models.FromTime(time.Now().Add(-1 * time.Minute))
	third := models.FromTime(time.Now())
	items := []models.FailoverV2PendingCleanup{
		{
			UserID:          "user-a",
			ServiceID:       service.ID,
			Provider:        "digitalocean",
			ProviderEntryID: "token-1",
			ResourceType:    "droplet",
			ResourceID:      "101",
			Status:          models.FailoverV2PendingCleanupStatusPending,
			UpdatedAt:       first,
		},
		{
			UserID:          "user-a",
			ServiceID:       service.ID,
			Provider:        "digitalocean",
			ProviderEntryID: "token-1",
			ResourceType:    "droplet",
			ResourceID:      "102",
			Status:          models.FailoverV2PendingCleanupStatusManualReview,
			UpdatedAt:       second,
		},
		{
			UserID:          "user-b",
			ServiceID:       otherService.ID,
			Provider:        "digitalocean",
			ProviderEntryID: "token-2",
			ResourceType:    "droplet",
			ResourceID:      "103",
			Status:          models.FailoverV2PendingCleanupStatusPending,
			UpdatedAt:       third,
		},
	}
	if err := db.Create(&items).Error; err != nil {
		t.Fatalf("failed to seed pending cleanups: %v", err)
	}

	listed, err := listPendingCleanupsByServiceForUserWithDB(
		db,
		"user-a",
		service.ID,
		10,
		[]string{models.FailoverV2PendingCleanupStatusPending, models.FailoverV2PendingCleanupStatusManualReview},
	)
	if err != nil {
		t.Fatalf("failed to list pending cleanups: %v", err)
	}
	if len(listed) != 2 {
		t.Fatalf("expected two scoped pending cleanups, got %d", len(listed))
	}
	if listed[0].ResourceID != "102" {
		t.Fatalf("expected most recently updated cleanup first, got resource %q", listed[0].ResourceID)
	}

	item, err := getPendingCleanupByIDForServiceForUserWithDB(db, "user-a", service.ID, listed[0].ID)
	if err != nil {
		t.Fatalf("failed to get pending cleanup by id: %v", err)
	}
	if item.ServiceID != service.ID {
		t.Fatalf("expected cleanup service %d, got %d", service.ID, item.ServiceID)
	}

	if _, err := getPendingCleanupByIDForServiceForUserWithDB(db, "user-b", service.ID, listed[0].ID); err == nil {
		t.Fatal("expected cross-user pending cleanup access to fail")
	}
}
