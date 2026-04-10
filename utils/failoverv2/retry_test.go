package failoverv2

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/komari-monitor/komari/database/dbcore"
	failoverv2db "github.com/komari-monitor/komari/database/failoverv2"
	"github.com/komari-monitor/komari/database/models"
)

func TestRetryAttachDNSForUserMarksExecutionAndMemberHealthy(t *testing.T) {
	configureFailoverV2RunnerTestDB(t)
	useMockFailoverV2RunnerDeps(t)
	events := captureFailoverV2Notifications(t)

	service, member, execution := createTestRunnerState(t)
	finishedAt := models.FromTime(time.Now())

	if err := failoverv2db.UpdateServiceFieldsForUser("user-a", service.ID, map[string]interface{}{
		"last_execution_id": execution.ID,
		"last_status":       models.FailoverV2ServiceStatusFailed,
		"last_message":      "attach failed",
	}); err != nil {
		t.Fatalf("failed to seed service state: %v", err)
	}
	if err := failoverv2db.UpdateMemberFieldsForUser("user-a", service.ID, member.ID, map[string]interface{}{
		"last_execution_id": execution.ID,
		"last_status":       models.FailoverV2MemberStatusFailed,
		"last_message":      "attach failed",
	}); err != nil {
		t.Fatalf("failed to seed member state: %v", err)
	}
	if err := failoverv2db.UpdateExecutionFields(execution.ID, map[string]interface{}{
		"status":            models.FailoverV2ExecutionStatusFailed,
		"detach_dns_status": models.FailoverDNSStatusSuccess,
		"attach_dns_status": models.FailoverDNSStatusFailed,
		"cleanup_status":    models.FailoverCleanupStatusSkipped,
		"new_client_uuid":   "client-new",
		"new_instance_ref":  `{"provider":"digitalocean","provider_entry_id":"token-1","droplet_id":321}`,
		"new_addresses":     `{"ipv4":[{"ip_address":"203.0.113.8"}],"ipv6":[]}`,
		"error_message":     "attach failed",
		"finished_at":       finishedAt,
	}); err != nil {
		t.Fatalf("failed to seed execution state: %v", err)
	}

	failoverV2AttachDNSFunc = func(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member, ipv4, ipv6 string) (interface{}, error) {
		if ipv4 != "203.0.113.8" {
			t.Fatalf("expected retry attach to use saved ipv4, got %q", ipv4)
		}
		return &AliyunMemberDNSResult{Line: member.DNSLine, RecordRefs: map[string]string{"A": "record-new"}}, nil
	}
	failoverV2VerifyAttachDNSFunc = func(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member, ipv4, ipv6 string) (interface{}, error) {
		return &AliyunMemberDNSVerification{Line: member.DNSLine, Success: true}, nil
	}

	updated, err := RetryAttachDNSForUser("user-a", service.ID, execution.ID)
	if err != nil {
		t.Fatalf("RetryAttachDNSForUser returned error: %v", err)
	}
	if updated.Status != models.FailoverV2ExecutionStatusSuccess {
		t.Fatalf("expected execution status %q, got %q", models.FailoverV2ExecutionStatusSuccess, updated.Status)
	}
	if updated.AttachDNSStatus != models.FailoverDNSStatusSuccess {
		t.Fatalf("expected attach status success, got %q", updated.AttachDNSStatus)
	}
	if updated.ErrorMessage != "" {
		t.Fatalf("expected execution error to clear, got %q", updated.ErrorMessage)
	}
	if len(updated.Steps) == 0 || updated.Steps[len(updated.Steps)-1].StepKey != "retry_attach_dns" {
		t.Fatalf("expected retry_attach_dns step to be appended, got %#v", updated.Steps)
	}

	reloadedService, err := failoverv2db.GetServiceByIDForUser("user-a", service.ID)
	if err != nil {
		t.Fatalf("failed to reload service: %v", err)
	}
	reloadedMember, err := findMemberOnService(reloadedService, member.ID)
	if err != nil {
		t.Fatalf("failed to reload member: %v", err)
	}
	if reloadedMember.WatchClientUUID != "client-new" {
		t.Fatalf("expected member watch client to update, got %q", reloadedMember.WatchClientUUID)
	}
	if reloadedMember.CurrentAddress != "203.0.113.8" {
		t.Fatalf("expected member current address to update, got %q", reloadedMember.CurrentAddress)
	}
	if !strings.Contains(reloadedMember.DNSRecordRefs, "record-new") {
		t.Fatalf("expected dns record refs to include new record, got %q", reloadedMember.DNSRecordRefs)
	}
	if len(reloadedMember.Lines) == 0 || !strings.Contains(reloadedMember.Lines[0].DNSRecordRefs, "record-new") {
		t.Fatalf("expected member line dns record refs to include new record, got %#v", reloadedMember.Lines)
	}
	if reloadedMember.LastStatus != models.FailoverV2MemberStatusHealthy {
		t.Fatalf("expected member status healthy, got %q", reloadedMember.LastStatus)
	}
	if reloadedService.LastStatus != models.FailoverV2ServiceStatusHealthy {
		t.Fatalf("expected service status healthy, got %q", reloadedService.LastStatus)
	}
	requireFailoverV2Notification(t, *events, failoverV2EventActionCompleted, "retry attach dns completed")
}

func TestRetryAttachDNSForUserUpdatesLineRecordRefsForAllLines(t *testing.T) {
	configureFailoverV2RunnerTestDB(t)
	useMockFailoverV2RunnerDeps(t)

	service, member, execution := createTestRunnerState(t)
	finishedAt := models.FromTime(time.Now())

	if err := dbcore.GetDBInstance().Create(&models.FailoverV2MemberLine{
		ServiceID:     service.ID,
		MemberID:      member.ID,
		LineCode:      "mobile",
		DNSRecordRefs: `{"A":"record-mobile-old"}`,
	}).Error; err != nil {
		t.Fatalf("failed to seed extra member line: %v", err)
	}

	if err := failoverv2db.UpdateServiceFieldsForUser("user-a", service.ID, map[string]interface{}{
		"last_execution_id": execution.ID,
		"last_status":       models.FailoverV2ServiceStatusFailed,
		"last_message":      "attach failed",
	}); err != nil {
		t.Fatalf("failed to seed service state: %v", err)
	}
	if err := failoverv2db.UpdateMemberFieldsForUser("user-a", service.ID, member.ID, map[string]interface{}{
		"last_execution_id": execution.ID,
		"last_status":       models.FailoverV2MemberStatusFailed,
		"last_message":      "attach failed",
	}); err != nil {
		t.Fatalf("failed to seed member state: %v", err)
	}
	if err := failoverv2db.UpdateExecutionFields(execution.ID, map[string]interface{}{
		"status":            models.FailoverV2ExecutionStatusFailed,
		"detach_dns_status": models.FailoverDNSStatusSuccess,
		"attach_dns_status": models.FailoverDNSStatusFailed,
		"cleanup_status":    models.FailoverCleanupStatusSkipped,
		"new_client_uuid":   "client-new",
		"new_instance_ref":  `{"provider":"digitalocean","provider_entry_id":"token-1","droplet_id":321}`,
		"new_addresses":     `{"ipv4":[{"ip_address":"203.0.113.8"}],"ipv6":[]}`,
		"error_message":     "attach failed",
		"finished_at":       finishedAt,
	}); err != nil {
		t.Fatalf("failed to seed execution state: %v", err)
	}

	failoverV2AttachDNSFunc = func(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member, ipv4, ipv6 string) (interface{}, error) {
		return &multiLineMemberDNSResult{
			Provider: "aliyun",
			Lines: []memberDNSResultLine{
				{Line: "telecom", RecordRefs: map[string]string{"A": "record-telecom-new"}},
				{Line: "mobile", RecordRefs: map[string]string{"A": "record-mobile-new"}},
			},
		}, nil
	}
	failoverV2VerifyAttachDNSFunc = func(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member, ipv4, ipv6 string) (interface{}, error) {
		return &multiLineMemberDNSVerification{Provider: "aliyun", Success: true}, nil
	}

	if _, err := RetryAttachDNSForUser("user-a", service.ID, execution.ID); err != nil {
		t.Fatalf("RetryAttachDNSForUser returned error: %v", err)
	}

	reloadedService, err := failoverv2db.GetServiceByIDForUser("user-a", service.ID)
	if err != nil {
		t.Fatalf("failed to reload service: %v", err)
	}
	reloadedMember, err := findMemberOnService(reloadedService, member.ID)
	if err != nil {
		t.Fatalf("failed to reload member: %v", err)
	}
	if len(reloadedMember.Lines) < 2 {
		t.Fatalf("expected multiple member lines after retry attach, got %#v", reloadedMember.Lines)
	}

	refsByLine := map[string]string{}
	for _, line := range reloadedMember.Lines {
		refsByLine[strings.TrimSpace(line.LineCode)] = strings.TrimSpace(line.DNSRecordRefs)
	}
	if !strings.Contains(refsByLine["telecom"], "record-telecom-new") {
		t.Fatalf("expected telecom line refs to be updated, got %#v", refsByLine)
	}
	if !strings.Contains(refsByLine["mobile"], "record-mobile-new") {
		t.Fatalf("expected mobile line refs to be updated, got %#v", refsByLine)
	}
	if strings.TrimSpace(reloadedMember.DNSRecordRefs) != strings.TrimSpace(reloadedMember.Lines[0].DNSRecordRefs) {
		t.Fatalf("expected legacy dns_record_refs to mirror first line, got member=%q line=%q", reloadedMember.DNSRecordRefs, reloadedMember.Lines[0].DNSRecordRefs)
	}
}

func TestRetryCleanupForUserMarksPendingCleanupSucceeded(t *testing.T) {
	configureFailoverV2RunnerTestDB(t)
	useMockFailoverV2RunnerDeps(t)
	events := captureFailoverV2Notifications(t)

	service, member, execution := createTestRunnerState(t)
	finishedAt := models.FromTime(time.Now())
	newInstanceRef := `{"provider":"digitalocean","provider_entry_id":"token-1","droplet_id":333}`

	if err := failoverv2db.UpdateServiceFieldsForUser("user-a", service.ID, map[string]interface{}{
		"delete_strategy":   models.FailoverDeleteStrategyDeleteAfterSuccess,
		"last_execution_id": execution.ID,
		"last_status":       models.FailoverV2ServiceStatusHealthy,
		"last_message":      "cleanup pending",
	}); err != nil {
		t.Fatalf("failed to seed service state: %v", err)
	}
	if err := failoverv2db.UpdateMemberFieldsForUser("user-a", service.ID, member.ID, map[string]interface{}{
		"watch_client_uuid":    "client-new",
		"current_address":      "203.0.113.9",
		"current_instance_ref": newInstanceRef,
		"last_execution_id":    execution.ID,
		"last_status":          models.FailoverV2MemberStatusHealthy,
		"last_message":         "cleanup pending",
	}); err != nil {
		t.Fatalf("failed to seed member state: %v", err)
	}
	if err := failoverv2db.UpdateExecutionFields(execution.ID, map[string]interface{}{
		"status":            models.FailoverV2ExecutionStatusSuccess,
		"detach_dns_status": models.FailoverDNSStatusSuccess,
		"attach_dns_status": models.FailoverDNSStatusSuccess,
		"cleanup_status":    models.FailoverCleanupStatusFailed,
		"cleanup_result":    `{"classification":"cleanup_delete_failed","pending_cleanup_id":1}`,
		"new_client_uuid":   "client-new",
		"new_instance_ref":  newInstanceRef,
		"new_addresses":     `{"ipv4":"203.0.113.9"}`,
		"finished_at":       finishedAt,
	}); err != nil {
		t.Fatalf("failed to seed execution state: %v", err)
	}

	if _, err := failoverv2db.CreateOrUpdatePendingCleanup(&models.FailoverV2PendingCleanup{
		UserID:          "user-a",
		ServiceID:       service.ID,
		MemberID:        member.ID,
		ExecutionID:     execution.ID,
		Provider:        "digitalocean",
		ProviderEntryID: "token-1",
		ResourceType:    "droplet",
		ResourceID:      "100",
		InstanceRef:     `{"provider":"digitalocean","provider_entry_id":"token-1","droplet_id":100}`,
		CleanupLabel:    "delete digitalocean droplet 100",
		Status:          models.FailoverV2PendingCleanupStatusPending,
		AttemptCount:    1,
		LastError:       "delete failed",
	}); err != nil {
		t.Fatalf("failed to seed pending cleanup: %v", err)
	}

	called := false
	failoverV2ResolveOldInstanceCleanupFromRefFunc = func(userUUID string, ref map[string]interface{}) (*oldInstanceCleanup, error) {
		return &oldInstanceCleanup{
			Ref:   cloneJSONMap(ref),
			Label: "delete digitalocean droplet 100",
			Cleanup: func(ctx context.Context) error {
				called = true
				return nil
			},
		}, nil
	}

	updated, err := RetryCleanupForUser("user-a", service.ID, execution.ID)
	if err != nil {
		t.Fatalf("RetryCleanupForUser returned error: %v", err)
	}
	if !called {
		t.Fatal("expected cleanup callback to run")
	}
	if updated.CleanupStatus != models.FailoverCleanupStatusSuccess {
		t.Fatalf("expected cleanup status success, got %q", updated.CleanupStatus)
	}
	if len(updated.Steps) == 0 || updated.Steps[len(updated.Steps)-1].StepKey != "retry_cleanup" {
		t.Fatalf("expected retry_cleanup step to be appended, got %#v", updated.Steps)
	}

	reloadedService, err := failoverv2db.GetServiceByIDForUser("user-a", service.ID)
	if err != nil {
		t.Fatalf("failed to reload service: %v", err)
	}
	reloadedMember, err := findMemberOnService(reloadedService, member.ID)
	if err != nil {
		t.Fatalf("failed to reload member: %v", err)
	}
	if !strings.Contains(reloadedMember.LastMessage, "cleanup completed after retry") {
		t.Fatalf("expected member message to mention cleanup retry success, got %q", reloadedMember.LastMessage)
	}

	var pending models.FailoverV2PendingCleanup
	if err := dbcore.GetDBInstance().
		Where("user_id = ? AND provider = ? AND resource_type = ? AND resource_id = ?", "user-a", "digitalocean", "droplet", "100").
		First(&pending).Error; err != nil {
		t.Fatalf("failed to reload pending cleanup: %v", err)
	}
	if pending.Status != models.FailoverV2PendingCleanupStatusSucceeded {
		t.Fatalf("expected pending cleanup status succeeded, got %q", pending.Status)
	}
	requireFailoverV2Notification(t, *events, failoverV2EventActionCompleted, "retry old instance cleanup completed")
}
