package failover

import (
	"context"
	"testing"
	"time"

	"github.com/komari-monitor/komari/database/dbcore"
	"github.com/komari-monitor/komari/database/models"
)

func prepareRetryTestDB(t *testing.T) {
	t.Helper()

	configureRunnerSQLiteDB(t)

	db := dbcore.GetDBInstance()
	if err := db.AutoMigrate(
		&models.FailoverTask{},
		&models.FailoverPlan{},
		&models.FailoverExecution{},
		&models.FailoverExecutionStep{},
	); err != nil {
		t.Fatalf("failed to migrate retry test schema: %v", err)
	}
}

func seedRetryExecution(t *testing.T, userUUID, taskName, executionStatus, dnsStatus, cleanupStatus string) (*models.FailoverTask, *models.FailoverExecution) {
	t.Helper()

	db := dbcore.GetDBInstance()
	now := models.FromTime(time.Now())
	oldRef := marshalJSON(map[string]interface{}{
		"provider":          "digitalocean",
		"provider_entry_id": "token-old",
		"droplet_id":        1001,
	})
	newRef := marshalJSON(map[string]interface{}{
		"provider":          "linode",
		"provider_entry_id": "token-new",
		"instance_id":       2002,
	})
	newAddresses := marshalJSON(map[string]interface{}{
		"public_ip":      "203.0.113.10",
		"ipv6_addresses": []string{"2001:db8::10"},
	})

	task := &models.FailoverTask{
		UserID:             userUUID,
		Name:               taskName,
		Enabled:            true,
		WatchClientUUID:    "old-client",
		CurrentAddress:     "198.51.100.10",
		CurrentInstanceRef: oldRef,
		DNSProvider:        aliyunProviderName,
		DNSEntryID:         "dns-entry-1",
		DNSPayload:         `{"domain_name":"example.com","rr":"ceshi","record_type":"A"}`,
		LastStatus:         models.FailoverTaskStatusFailed,
		LastMessage:        "dns step failed",
		CreatedAt:          now,
		UpdatedAt:          now,
	}
	if err := db.Create(task).Error; err != nil {
		t.Fatalf("failed to create retry test task: %v", err)
	}

	execution := &models.FailoverExecution{
		TaskID:          task.ID,
		Status:          executionStatus,
		TriggerReason:   "manual run",
		WatchClientUUID: "old-client",
		SelectedPlanID:  nil,
		OldInstanceRef:  oldRef,
		NewClientUUID:   "new-client",
		NewInstanceRef:  newRef,
		NewAddresses:    newAddresses,
		DNSProvider:     task.DNSProvider,
		DNSStatus:       dnsStatus,
		DNSResult:       `{"error":"old dns error"}`,
		CleanupStatus:   cleanupStatus,
		CleanupResult:   `{"classification":"cleanup_status_unknown","summary":"manual review required"}`,
		ErrorMessage:    "dns step failed",
		StartedAt:       now,
		FinishedAt:      &now,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	if err := db.Create(execution).Error; err != nil {
		t.Fatalf("failed to create retry test execution: %v", err)
	}

	if err := db.Model(&models.FailoverTask{}).
		Where("id = ?", task.ID).
		Updates(map[string]interface{}{
			"last_execution_id": execution.ID,
		}).Error; err != nil {
		t.Fatalf("failed to link retry execution: %v", err)
	}

	step := &models.FailoverExecutionStep{
		ExecutionID: execution.ID,
		Sort:        1,
		StepKey:     "switch_dns",
		StepLabel:   "Switch DNS",
		Status:      models.FailoverStepStatusFailed,
		Message:     "old dns error",
		StartedAt:   &now,
		FinishedAt:  &now,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	if err := db.Create(step).Error; err != nil {
		t.Fatalf("failed to create retry test step: %v", err)
	}

	return task, execution
}

func TestRetryDNSForUserMarksExecutionAndTaskSuccessful(t *testing.T) {
	prepareRetryTestDB(t)
	userUUID := "retry-dns-user"
	task, execution := seedRetryExecution(t, userUUID, "retry-dns-task", models.FailoverExecutionStatusFailed, models.FailoverDNSStatusFailed, models.FailoverCleanupStatusPending)

	originalApply := retryDNSApplyFunc
	originalVerify := retryDNSVerifyFunc
	retryDNSApplyFunc = func(ctx context.Context, userUUID, providerName, entryID, payloadJSON, ipv4, ipv6 string) (*dnsUpdateResult, error) {
		return &dnsUpdateResult{
			Provider: providerName,
			Type:     "A",
			Value:    ipv4,
		}, nil
	}
	retryDNSVerifyFunc = func(ctx context.Context, userUUID, providerName, entryID, payloadJSON, ipv4, ipv6 string) (*dnsVerificationResult, error) {
		return &dnsVerificationResult{
			Provider: providerName,
			Success:  true,
		}, nil
	}
	t.Cleanup(func() {
		retryDNSApplyFunc = originalApply
		retryDNSVerifyFunc = originalVerify
	})

	updated, err := RetryDNSForUser(userUUID, execution.ID)
	if err != nil {
		t.Fatalf("RetryDNSForUser returned error: %v", err)
	}
	if updated.Status != models.FailoverExecutionStatusSuccess {
		t.Fatalf("expected execution status %q, got %q", models.FailoverExecutionStatusSuccess, updated.Status)
	}
	if updated.DNSStatus != models.FailoverDNSStatusSuccess {
		t.Fatalf("expected dns status %q, got %q", models.FailoverDNSStatusSuccess, updated.DNSStatus)
	}
	if updated.ErrorMessage != "" {
		t.Fatalf("expected execution error to be cleared, got %q", updated.ErrorMessage)
	}
	if len(updated.Steps) < 2 || updated.Steps[len(updated.Steps)-1].StepKey != "retry_dns" {
		t.Fatalf("expected retry_dns step to be appended, got %#v", updated.Steps)
	}

	db := dbcore.GetDBInstance()
	var reloadedTask models.FailoverTask
	if err := db.First(&reloadedTask, task.ID).Error; err != nil {
		t.Fatalf("failed to reload retry task: %v", err)
	}
	if reloadedTask.LastStatus != models.FailoverTaskStatusCooldown {
		t.Fatalf("expected task last_status %q, got %q", models.FailoverTaskStatusCooldown, reloadedTask.LastStatus)
	}
	if reloadedTask.WatchClientUUID != "new-client" {
		t.Fatalf("expected task watch_client_uuid to switch to new client, got %q", reloadedTask.WatchClientUUID)
	}
	if reloadedTask.CurrentAddress != "203.0.113.10" {
		t.Fatalf("expected task current_address to use new public ip, got %q", reloadedTask.CurrentAddress)
	}
}

func TestRetryCleanupForUserDeletesOldInstance(t *testing.T) {
	prepareRetryTestDB(t)
	userUUID := "retry-cleanup-user"
	_, execution := seedRetryExecution(t, userUUID, "retry-cleanup-task", models.FailoverExecutionStatusSuccess, models.FailoverDNSStatusSuccess, models.FailoverCleanupStatusWarning)

	called := false
	originalResolver := retryCleanupResolveFunc
	retryCleanupResolveFunc = func(ctx context.Context, userUUID string, ref map[string]interface{}) (*currentInstanceCleanup, error) {
		return &currentInstanceCleanup{
			Ref:   cloneJSONMap(ref),
			Label: "delete old instance",
			Cleanup: func(ctx context.Context) error {
				called = true
				return nil
			},
		}, nil
	}
	t.Cleanup(func() {
		retryCleanupResolveFunc = originalResolver
	})

	updated, err := RetryCleanupForUser(userUUID, execution.ID)
	if err != nil {
		t.Fatalf("RetryCleanupForUser returned error: %v", err)
	}
	if !called {
		t.Fatal("expected cleanup callback to run")
	}
	if updated.Status != models.FailoverExecutionStatusSuccess {
		t.Fatalf("expected execution status to remain success, got %q", updated.Status)
	}
	if updated.CleanupStatus != models.FailoverCleanupStatusSuccess {
		t.Fatalf("expected cleanup status %q, got %q", models.FailoverCleanupStatusSuccess, updated.CleanupStatus)
	}
	if len(updated.Steps) < 2 || updated.Steps[len(updated.Steps)-1].StepKey != "retry_cleanup" {
		t.Fatalf("expected retry_cleanup step to be appended, got %#v", updated.Steps)
	}

	result := parseJSONMap(updated.CleanupResult)
	if stringMapValue(result, "classification") != "instance_deleted" {
		t.Fatalf("expected cleanup result classification instance_deleted, got %#v", result)
	}
}

func TestRetryCleanupForUserRequiresSuccessfulDNS(t *testing.T) {
	prepareRetryTestDB(t)
	userUUID := "retry-cleanup-guard-user"
	_, execution := seedRetryExecution(t, userUUID, "retry-cleanup-guard-task", models.FailoverExecutionStatusFailed, models.FailoverDNSStatusFailed, models.FailoverCleanupStatusPending)

	if _, err := RetryCleanupForUser(userUUID, execution.ID); err == nil {
		t.Fatal("expected cleanup retry to be rejected before dns succeeds")
	}
}

func TestDescribeExecutionAvailableActionsAllowsDNSRetryAfterFailure(t *testing.T) {
	prepareRetryTestDB(t)
	userUUID := "retry-actions-dns-user"
	task, execution := seedRetryExecution(t, userUUID, "retry-actions-dns-task", models.FailoverExecutionStatusFailed, models.FailoverDNSStatusFailed, models.FailoverCleanupStatusPending)

	actions := DescribeExecutionAvailableActions(task, execution)
	if !actions.RetryDNS.Available {
		t.Fatalf("expected dns retry to be available, got %#v", actions.RetryDNS)
	}
	if actions.RetryCleanup.Available {
		t.Fatalf("expected cleanup retry to stay unavailable before dns succeeds, got %#v", actions.RetryCleanup)
	}
}

func TestDescribeExecutionAvailableActionsExplainsCleanupManualReview(t *testing.T) {
	prepareRetryTestDB(t)
	userUUID := "retry-actions-cleanup-user"
	task, execution := seedRetryExecution(t, userUUID, "retry-actions-cleanup-task", models.FailoverExecutionStatusSuccess, models.FailoverDNSStatusSuccess, models.FailoverCleanupStatusWarning)
	execution.CleanupResult = `{"classification":"provider_entry_missing","summary":"manual review required"}`

	actions := DescribeExecutionAvailableActions(task, execution)
	if actions.RetryCleanup.Available {
		t.Fatalf("expected cleanup retry to be unavailable, got %#v", actions.RetryCleanup)
	}
	if actions.RetryCleanup.Reason == "" {
		t.Fatal("expected cleanup retry reason to be populated")
	}
}
