package failoverv2

import (
	"testing"
	"time"

	"github.com/komari-monitor/komari/database/models"
)

func TestStopExecutionForUserWithDBMarksExecutionAndRunningStepsFailed(t *testing.T) {
	db := openFailoverV2TestDB(t)
	if err := db.AutoMigrate(
		&models.FailoverV2Service{},
		&models.FailoverV2Member{},
		&models.FailoverV2Execution{},
		&models.FailoverV2ExecutionStep{},
	); err != nil {
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
		Status:        models.FailoverV2ExecutionStatusWaitingAgent,
		TriggerReason: "manual failover",
		StartedAt:     models.FromTime(time.Now()),
	})
	if err != nil {
		t.Fatalf("failed to create execution: %v", err)
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
	startedAt := models.FromTime(time.Now())
	step := models.FailoverV2ExecutionStep{
		ExecutionID: execution.ID,
		Sort:        1,
		StepKey:     "wait_agent",
		StepLabel:   "Wait Agent",
		Status:      models.FailoverStepStatusRunning,
		StartedAt:   &startedAt,
	}
	if err := db.Create(&step).Error; err != nil {
		t.Fatalf("failed to create running step: %v", err)
	}

	stopped, err := stopExecutionForUserWithDB(db, "user-a", service.ID, execution.ID, "stopped by test")
	if err != nil {
		t.Fatalf("stopExecutionForUserWithDB returned error: %v", err)
	}
	if stopped.Status != models.FailoverV2ExecutionStatusFailed {
		t.Fatalf("expected execution failed, got %q", stopped.Status)
	}
	if stopped.ErrorMessage != "stopped by test" {
		t.Fatalf("expected stop reason to persist, got %q", stopped.ErrorMessage)
	}
	if stopped.FinishedAt == nil {
		t.Fatal("expected execution finished_at to be set")
	}
	if len(stopped.Steps) != 1 {
		t.Fatalf("expected one preloaded step, got %d", len(stopped.Steps))
	}
	if stopped.Steps[0].Status != models.FailoverStepStatusFailed {
		t.Fatalf("expected running step to be failed, got %q", stopped.Steps[0].Status)
	}
	if stopped.Steps[0].Message != "stopped by test" {
		t.Fatalf("expected running step message to update, got %q", stopped.Steps[0].Message)
	}

	reloadedService, err := getServiceByIDForUserWithDB(db, "user-a", service.ID)
	if err != nil {
		t.Fatalf("failed to reload service: %v", err)
	}
	if reloadedService.LastStatus != models.FailoverV2ServiceStatusFailed {
		t.Fatalf("expected service failed after stop, got %q", reloadedService.LastStatus)
	}
	if reloadedService.LastMessage != "stopped by test" {
		t.Fatalf("expected service stop message, got %q", reloadedService.LastMessage)
	}

	reloadedMember, err := getMemberByIDForServiceForUserWithDB(db, "user-a", service.ID, member.ID)
	if err != nil {
		t.Fatalf("failed to reload member: %v", err)
	}
	if reloadedMember.LastStatus != models.FailoverV2MemberStatusFailed {
		t.Fatalf("expected member failed after stop, got %q", reloadedMember.LastStatus)
	}
	if reloadedMember.LastMessage != "stopped by test" {
		t.Fatalf("expected member stop message, got %q", reloadedMember.LastMessage)
	}
	if reloadedMember.LastFailedAt == nil {
		t.Fatal("expected member last_failed_at to be set")
	}
}
