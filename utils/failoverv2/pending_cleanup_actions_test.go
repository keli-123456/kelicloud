package failoverv2

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	failoverv2db "github.com/komari-monitor/komari/database/failoverv2"
	"github.com/komari-monitor/komari/database/models"
)

func TestRetryPendingCleanupForUserMarksCleanupResolved(t *testing.T) {
	configureFailoverV2RunnerTestDB(t)
	events := captureFailoverV2Notifications(t)

	service, member := createTestRunnerServiceAndMember(t)
	execution, err := failoverv2db.CreateExecutionForUser("user-a", service.ID, member.ID, &models.FailoverV2Execution{
		Status:          models.FailoverV2ExecutionStatusSuccess,
		OldClientUUID:   member.WatchClientUUID,
		OldInstanceRef:  `{"provider":"digitalocean","provider_entry_id":"default","droplet_id":101}`,
		AttachDNSStatus: models.FailoverDNSStatusSuccess,
		CleanupStatus:   models.FailoverCleanupStatusFailed,
		CleanupResult:   `{"classification":"cleanup_delete_failed","pending_cleanup_id":1}`,
		FinishedAt:      ptrLocalTime(time.Now()),
	})
	if err != nil {
		t.Fatalf("failed to create execution: %v", err)
	}

	if err := failoverv2db.UpdateMemberFieldsForUser("user-a", service.ID, member.ID, map[string]interface{}{
		"current_address":   "203.0.113.10",
		"last_execution_id": execution.ID,
		"last_status":       models.FailoverV2MemberStatusHealthy,
		"last_message":      "failover completed to 203.0.113.10; old instance cleanup failed",
	}); err != nil {
		t.Fatalf("failed to seed member status: %v", err)
	}
	if err := failoverv2db.UpdateServiceFieldsForUser("user-a", service.ID, map[string]interface{}{
		"last_execution_id": execution.ID,
		"last_status":       models.FailoverV2ServiceStatusHealthy,
		"last_message":      "failover completed to 203.0.113.10; old instance cleanup failed",
	}); err != nil {
		t.Fatalf("failed to seed service status: %v", err)
	}

	cleanup, err := failoverv2db.CreateOrUpdatePendingCleanup(&models.FailoverV2PendingCleanup{
		UserID:          "user-a",
		ServiceID:       service.ID,
		MemberID:        member.ID,
		ExecutionID:     execution.ID,
		Provider:        "digitalocean",
		ProviderEntryID: "default",
		ResourceType:    "droplet",
		ResourceID:      "101",
		InstanceRef:     `{"provider":"digitalocean","provider_entry_id":"default","droplet_id":101}`,
		CleanupLabel:    "delete digitalocean droplet 101",
		Status:          models.FailoverV2PendingCleanupStatusPending,
		AttemptCount:    1,
		LastError:       "delete failed",
	})
	if err != nil {
		t.Fatalf("failed to seed pending cleanup: %v", err)
	}

	previousResolve := failoverV2ResolveOldInstanceCleanupFromRefFunc
	failoverV2ResolveOldInstanceCleanupFromRefFunc = func(userUUID string, ref map[string]interface{}) (*oldInstanceCleanup, error) {
		return &oldInstanceCleanup{
			Ref:   ref,
			Label: "delete digitalocean droplet 101",
			Cleanup: func(ctx context.Context) error {
				return nil
			},
		}, nil
	}
	t.Cleanup(func() {
		failoverV2ResolveOldInstanceCleanupFromRefFunc = previousResolve
	})

	updated, err := RetryPendingCleanupForUser("user-a", service.ID, cleanup.ID)
	if err != nil {
		t.Fatalf("expected pending cleanup retry to succeed: %v", err)
	}
	if updated.Status != models.FailoverV2PendingCleanupStatusSucceeded {
		t.Fatalf("expected pending cleanup status succeeded, got %q", updated.Status)
	}
	if updated.ResolvedAt == nil {
		t.Fatal("expected resolved_at to be set after retry success")
	}

	storedExecution, err := failoverv2db.GetExecutionByIDForUser("user-a", service.ID, execution.ID)
	if err != nil {
		t.Fatalf("failed to reload execution: %v", err)
	}
	if storedExecution.CleanupStatus != models.FailoverCleanupStatusSuccess {
		t.Fatalf("expected execution cleanup status success, got %q", storedExecution.CleanupStatus)
	}
	if !strings.Contains(storedExecution.CleanupResult, "pending cleanup retry") {
		t.Fatalf("expected cleanup result to mention pending cleanup retry, got %q", storedExecution.CleanupResult)
	}

	storedService, err := failoverv2db.GetServiceByIDForUser("user-a", service.ID)
	if err != nil {
		t.Fatalf("failed to reload service: %v", err)
	}
	if !strings.Contains(storedService.LastMessage, "old instance cleanup completed") {
		t.Fatalf("expected service message to mention cleanup completion, got %q", storedService.LastMessage)
	}
	requireFailoverV2Notification(t, *events, failoverV2EventActionCompleted, "pending cleanup retry completed")
}

func TestQueuePendingCleanupRetryForUserRunsInBackground(t *testing.T) {
	configureFailoverV2RunnerTestDB(t)

	service, member := createTestRunnerServiceAndMember(t)
	execution, err := failoverv2db.CreateExecutionForUser("user-a", service.ID, member.ID, &models.FailoverV2Execution{
		Status:          models.FailoverV2ExecutionStatusSuccess,
		OldClientUUID:   member.WatchClientUUID,
		OldInstanceRef:  `{"provider":"digitalocean","provider_entry_id":"default","droplet_id":105}`,
		AttachDNSStatus: models.FailoverDNSStatusSuccess,
		CleanupStatus:   models.FailoverCleanupStatusFailed,
		CleanupResult:   `{"classification":"cleanup_delete_failed","pending_cleanup_id":1}`,
		FinishedAt:      ptrLocalTime(time.Now()),
	})
	if err != nil {
		t.Fatalf("failed to create execution: %v", err)
	}
	cleanup, err := failoverv2db.CreateOrUpdatePendingCleanup(&models.FailoverV2PendingCleanup{
		UserID:          "user-a",
		ServiceID:       service.ID,
		MemberID:        member.ID,
		ExecutionID:     execution.ID,
		Provider:        "digitalocean",
		ProviderEntryID: "default",
		ResourceType:    "droplet",
		ResourceID:      "105",
		InstanceRef:     `{"provider":"digitalocean","provider_entry_id":"default","droplet_id":105}`,
		CleanupLabel:    "delete digitalocean droplet 105",
		Status:          models.FailoverV2PendingCleanupStatusPending,
		AttemptCount:    1,
		LastError:       "delete failed",
	})
	if err != nil {
		t.Fatalf("failed to seed pending cleanup: %v", err)
	}

	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	closeRelease := func() {
		releaseOnce.Do(func() {
			close(release)
		})
	}
	previousResolve := failoverV2ResolveOldInstanceCleanupFromRefFunc
	failoverV2ResolveOldInstanceCleanupFromRefFunc = func(userUUID string, ref map[string]interface{}) (*oldInstanceCleanup, error) {
		return &oldInstanceCleanup{
			Ref:   ref,
			Label: "delete digitalocean droplet 105",
			Cleanup: func(ctx context.Context) error {
				close(started)
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			},
		}, nil
	}
	t.Cleanup(func() {
		failoverV2ResolveOldInstanceCleanupFromRefFunc = previousResolve
		closeRelease()
	})

	queued, err := QueuePendingCleanupRetryForUser("user-a", service.ID, cleanup.ID)
	if err != nil {
		t.Fatalf("expected pending cleanup retry to queue: %v", err)
	}
	if queued.Status != models.FailoverV2PendingCleanupStatusRunning {
		t.Fatalf("expected queued pending cleanup status running, got %q", queued.Status)
	}

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("queued cleanup did not start")
	}
	closeRelease()

	var updated *models.FailoverV2PendingCleanup
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); {
		updated, err = failoverv2db.GetPendingCleanupByIDForUser("user-a", service.ID, cleanup.ID)
		if err != nil {
			t.Fatalf("failed to reload pending cleanup: %v", err)
		}
		if updated.Status == models.FailoverV2PendingCleanupStatusSucceeded {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("expected queued pending cleanup to succeed, got %+v", updated)
}

func TestMarkPendingCleanupResolvedForUserUpdatesExecution(t *testing.T) {
	configureFailoverV2RunnerTestDB(t)
	events := captureFailoverV2Notifications(t)

	service, member := createTestRunnerServiceAndMember(t)
	execution, err := failoverv2db.CreateExecutionForUser("user-a", service.ID, member.ID, &models.FailoverV2Execution{
		Status:          models.FailoverV2ExecutionStatusSuccess,
		OldClientUUID:   member.WatchClientUUID,
		OldInstanceRef:  `{"provider":"digitalocean","provider_entry_id":"default","droplet_id":102}`,
		AttachDNSStatus: models.FailoverDNSStatusSuccess,
		CleanupStatus:   models.FailoverCleanupStatusFailed,
		CleanupResult:   `{"classification":"cleanup_delete_failed","pending_cleanup_id":1}`,
		FinishedAt:      ptrLocalTime(time.Now()),
	})
	if err != nil {
		t.Fatalf("failed to create execution: %v", err)
	}

	if err := failoverv2db.UpdateMemberFieldsForUser("user-a", service.ID, member.ID, map[string]interface{}{
		"current_address":   "203.0.113.11",
		"last_execution_id": execution.ID,
		"last_status":       models.FailoverV2MemberStatusHealthy,
		"last_message":      "failover completed to 203.0.113.11; old instance cleanup failed",
	}); err != nil {
		t.Fatalf("failed to seed member status: %v", err)
	}
	if err := failoverv2db.UpdateServiceFieldsForUser("user-a", service.ID, map[string]interface{}{
		"last_execution_id": execution.ID,
		"last_status":       models.FailoverV2ServiceStatusHealthy,
		"last_message":      "failover completed to 203.0.113.11; old instance cleanup failed",
	}); err != nil {
		t.Fatalf("failed to seed service status: %v", err)
	}

	cleanup, err := failoverv2db.CreateOrUpdatePendingCleanup(&models.FailoverV2PendingCleanup{
		UserID:          "user-a",
		ServiceID:       service.ID,
		MemberID:        member.ID,
		ExecutionID:     execution.ID,
		Provider:        "digitalocean",
		ProviderEntryID: "default",
		ResourceType:    "droplet",
		ResourceID:      "102",
		InstanceRef:     `{"provider":"digitalocean","provider_entry_id":"default","droplet_id":102}`,
		CleanupLabel:    "delete digitalocean droplet 102",
		Status:          models.FailoverV2PendingCleanupStatusManualReview,
		AttemptCount:    3,
		LastError:       "operator deleted droplet manually",
	})
	if err != nil {
		t.Fatalf("failed to seed pending cleanup: %v", err)
	}

	updated, err := MarkPendingCleanupResolvedForUser("user-a", service.ID, cleanup.ID)
	if err != nil {
		t.Fatalf("expected pending cleanup resolve to succeed: %v", err)
	}
	if updated.Status != models.FailoverV2PendingCleanupStatusSucceeded {
		t.Fatalf("expected pending cleanup status succeeded, got %q", updated.Status)
	}

	storedExecution, err := failoverv2db.GetExecutionByIDForUser("user-a", service.ID, execution.ID)
	if err != nil {
		t.Fatalf("failed to reload execution: %v", err)
	}
	if storedExecution.CleanupStatus != models.FailoverCleanupStatusSuccess {
		t.Fatalf("expected execution cleanup status success, got %q", storedExecution.CleanupStatus)
	}
	if !strings.Contains(storedExecution.CleanupResult, "resolved manually") {
		t.Fatalf("expected cleanup result to mention manual resolution, got %q", storedExecution.CleanupResult)
	}
	requireFailoverV2Notification(t, *events, failoverV2EventActionCompleted, "pending cleanup marked resolved")
}

func TestMarkPendingCleanupManualReviewForUserSendsNotification(t *testing.T) {
	configureFailoverV2RunnerTestDB(t)
	events := captureFailoverV2Notifications(t)

	service, member := createTestRunnerServiceAndMember(t)
	execution, err := failoverv2db.CreateExecutionForUser("user-a", service.ID, member.ID, &models.FailoverV2Execution{
		Status:          models.FailoverV2ExecutionStatusFailed,
		OldClientUUID:   member.WatchClientUUID,
		OldInstanceRef:  `{"provider":"digitalocean","provider_entry_id":"default","droplet_id":103}`,
		AttachDNSStatus: models.FailoverDNSStatusSuccess,
		CleanupStatus:   models.FailoverCleanupStatusFailed,
		CleanupResult:   `{"classification":"cleanup_delete_failed","pending_cleanup_id":1}`,
		FinishedAt:      ptrLocalTime(time.Now()),
	})
	if err != nil {
		t.Fatalf("failed to create execution: %v", err)
	}

	cleanup, err := failoverv2db.CreateOrUpdatePendingCleanup(&models.FailoverV2PendingCleanup{
		UserID:          "user-a",
		ServiceID:       service.ID,
		MemberID:        member.ID,
		ExecutionID:     execution.ID,
		Provider:        "digitalocean",
		ProviderEntryID: "default",
		ResourceType:    "droplet",
		ResourceID:      "103",
		InstanceRef:     `{"provider":"digitalocean","provider_entry_id":"default","droplet_id":103}`,
		CleanupLabel:    "delete digitalocean droplet 103",
		Status:          models.FailoverV2PendingCleanupStatusPending,
		AttemptCount:    1,
		LastError:       "delete failed",
	})
	if err != nil {
		t.Fatalf("failed to seed pending cleanup: %v", err)
	}

	updated, err := MarkPendingCleanupManualReviewForUser("user-a", service.ID, cleanup.ID, "operator review required")
	if err != nil {
		t.Fatalf("expected pending cleanup manual review to succeed: %v", err)
	}
	if updated.Status != models.FailoverV2PendingCleanupStatusManualReview {
		t.Fatalf("expected pending cleanup status manual_review, got %q", updated.Status)
	}
	requireFailoverV2Notification(t, *events, failoverV2EventPendingManualReview, "operator review required")
}

func TestPendingCleanupManualActionsBlockActiveExecution(t *testing.T) {
	configureFailoverV2RunnerTestDB(t)

	service, member := createTestRunnerServiceAndMember(t)
	execution, err := failoverv2db.CreateExecutionForUser("user-a", service.ID, member.ID, &models.FailoverV2Execution{
		Status:         models.FailoverV2ExecutionStatusFailed,
		OldClientUUID:  member.WatchClientUUID,
		OldInstanceRef: `{"provider":"digitalocean","provider_entry_id":"default","droplet_id":104}`,
		CleanupStatus:  models.FailoverCleanupStatusFailed,
		FinishedAt:     ptrLocalTime(time.Now()),
	})
	if err != nil {
		t.Fatalf("failed to create terminal execution: %v", err)
	}
	cleanup, err := failoverv2db.CreateOrUpdatePendingCleanup(&models.FailoverV2PendingCleanup{
		UserID:          "user-a",
		ServiceID:       service.ID,
		MemberID:        member.ID,
		ExecutionID:     execution.ID,
		Provider:        "digitalocean",
		ProviderEntryID: "default",
		ResourceType:    "droplet",
		ResourceID:      "104",
		InstanceRef:     `{"provider":"digitalocean","provider_entry_id":"default","droplet_id":104}`,
		CleanupLabel:    "delete digitalocean droplet 104",
		Status:          models.FailoverV2PendingCleanupStatusPending,
		AttemptCount:    1,
		LastError:       "delete failed",
	})
	if err != nil {
		t.Fatalf("failed to seed pending cleanup: %v", err)
	}
	if _, err := failoverv2db.CreateExecutionForUser("user-a", service.ID, member.ID, &models.FailoverV2Execution{
		Status: models.FailoverV2ExecutionStatusWaitingAgent,
	}); err != nil {
		t.Fatalf("failed to create active execution: %v", err)
	}

	if _, err := MarkPendingCleanupResolvedForUser("user-a", service.ID, cleanup.ID); err == nil || !strings.Contains(err.Error(), "active") {
		t.Fatalf("expected active execution to block manual resolve, got %v", err)
	}
	if _, err := MarkPendingCleanupManualReviewForUser("user-a", service.ID, cleanup.ID, "manual check"); err == nil || !strings.Contains(err.Error(), "active") {
		t.Fatalf("expected active execution to block manual review, got %v", err)
	}

	reloaded, err := failoverv2db.GetPendingCleanupByIDForUser("user-a", service.ID, cleanup.ID)
	if err != nil {
		t.Fatalf("failed to reload pending cleanup: %v", err)
	}
	if reloaded.Status != models.FailoverV2PendingCleanupStatusPending {
		t.Fatalf("expected pending cleanup to stay pending, got %q", reloaded.Status)
	}
}

func TestRunStandalonePendingFailoverV2CleanupTimesOut(t *testing.T) {
	previousTimeout := pendingFailoverV2CleanupTimeout
	pendingFailoverV2CleanupTimeout = 10 * time.Millisecond
	t.Cleanup(func() {
		pendingFailoverV2CleanupTimeout = previousTimeout
	})

	err := runStandalonePendingFailoverV2Cleanup(&oldInstanceCleanup{
		Ref:   map[string]interface{}{"provider": "digitalocean"},
		Label: "slow cleanup",
		Cleanup: func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		},
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected cleanup retry to fail with deadline exceeded, got %v", err)
	}
}
