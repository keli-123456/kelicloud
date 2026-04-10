package failoverv2

import (
	"context"
	"testing"
	"time"

	failoverv2db "github.com/komari-monitor/komari/database/failoverv2"
	"github.com/komari-monitor/komari/database/models"
)

func TestStopExecutionForUserCancelsRunningFailoverAndRollsBack(t *testing.T) {
	configureFailoverV2RunnerTestDB(t)
	useMockFailoverV2RunnerDeps(t)
	events := captureFailoverV2Notifications(t)

	service, member, execution := createTestRunnerState(t)
	now := models.FromTime(time.Now())
	if err := failoverv2db.UpdateServiceFieldsForUser("user-a", service.ID, map[string]interface{}{
		"last_execution_id": execution.ID,
		"last_status":       models.FailoverV2ServiceStatusRunning,
		"last_message":      "manual failover started",
	}); err != nil {
		t.Fatalf("failed to seed service state: %v", err)
	}
	if err := failoverv2db.UpdateMemberFieldsForUser("user-a", service.ID, member.ID, map[string]interface{}{
		"last_execution_id": execution.ID,
		"last_status":       models.FailoverV2MemberStatusRunning,
		"last_message":      "manual failover started",
		"last_triggered_at": now,
	}); err != nil {
		t.Fatalf("failed to seed member state: %v", err)
	}

	rollbackCalled := make(chan struct{}, 1)
	failoverV2DetachDNSFunc = func(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member) (interface{}, error) {
		return &AliyunMemberDNSResult{Line: member.DNSLine, RecordRefs: map[string]string{}}, nil
	}
	failoverV2VerifyDetachDNSFunc = func(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member) (interface{}, error) {
		return &AliyunMemberDNSVerification{Line: member.DNSLine, Success: true}, nil
	}
	failoverV2ProvisionFunc = func(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member) (*memberProvisionOutcome, error) {
		return &memberProvisionOutcome{
			IPv4:             "203.0.113.88",
			AutoConnectGroup: "failover-v2/1/1",
			NewInstanceRef:   map[string]interface{}{"provider": "digitalocean", "provider_entry_id": "token-1", "droplet_id": 988},
			NewAddresses:     map[string]interface{}{"ipv4": "203.0.113.88"},
			RollbackLabel:    "delete failed digitalocean droplet 988",
			Rollback: func(ctx context.Context) error {
				rollbackCalled <- struct{}{}
				return nil
			},
		}, nil
	}
	failoverV2WaitClientFunc = func(ctx context.Context, userUUID, group, excludeUUID string, startedAt time.Time, timeoutSeconds int, expectedAddresses map[string]struct{}) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	}

	ctx, cancel := context.WithCancel(context.Background())
	registerExecutionCancel(execution.ID, cancel)
	defer unregisterExecutionCancel(execution.ID)

	done := make(chan struct{})
	go func() {
		defer close(done)
		runner := &memberExecutionRunner{
			userUUID:  "user-a",
			service:   service,
			member:    member,
			execution: execution,
			ctx:       ctx,
		}
		runner.runFailover()
	}()

	waitForFailoverV2ExecutionStatus(t, "user-a", service.ID, execution.ID, models.FailoverV2ExecutionStatusWaitingAgent)

	stopped, err := StopExecutionForUser("user-a", service.ID, execution.ID)
	if err != nil {
		t.Fatalf("StopExecutionForUser returned error: %v", err)
	}
	if stopped.Status != models.FailoverV2ExecutionStatusFailed {
		t.Fatalf("expected stopped execution failed, got %q", stopped.Status)
	}
	if stopped.ErrorMessage != errExecutionStopped.Error() {
		t.Fatalf("expected stop message %q, got %q", errExecutionStopped.Error(), stopped.ErrorMessage)
	}

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for runner to exit after stop")
	}

	select {
	case <-rollbackCalled:
	case <-time.After(3 * time.Second):
		t.Fatal("expected rollback to run after stopping a provisioned execution")
	}

	storedExecution, err := failoverv2db.GetExecutionByIDForUser("user-a", service.ID, execution.ID)
	if err != nil {
		t.Fatalf("failed to reload execution: %v", err)
	}
	if storedExecution.Status != models.FailoverV2ExecutionStatusFailed {
		t.Fatalf("expected final execution failed, got %q", storedExecution.Status)
	}
	if storedExecution.ErrorMessage != errExecutionStopped.Error() {
		t.Fatalf("expected final execution message %q, got %q", errExecutionStopped.Error(), storedExecution.ErrorMessage)
	}

	reloadedService, err := failoverv2db.GetServiceByIDForUser("user-a", service.ID)
	if err != nil {
		t.Fatalf("failed to reload service: %v", err)
	}
	reloadedMember, err := findMemberOnService(reloadedService, member.ID)
	if err != nil {
		t.Fatalf("failed to reload member: %v", err)
	}
	if reloadedService.LastStatus != models.FailoverV2ServiceStatusFailed {
		t.Fatalf("expected service failed after stop, got %q", reloadedService.LastStatus)
	}
	if reloadedMember.LastStatus != models.FailoverV2MemberStatusFailed {
		t.Fatalf("expected member failed after stop, got %q", reloadedMember.LastStatus)
	}
	requireFailoverV2Notification(t, *events, failoverV2EventActionCompleted, "stop execution request completed")
}

func TestDescribeExecutionAvailableActionsAllowsStoppingActiveExecution(t *testing.T) {
	actions := DescribeExecutionAvailableActions(
		&models.FailoverV2Service{},
		&models.FailoverV2Member{},
		&models.FailoverV2Execution{Status: models.FailoverV2ExecutionStatusWaitingAgent},
	)
	if !actions.StopExecution.Available {
		t.Fatalf("expected active execution to expose stop action, got reason %q", actions.StopExecution.Reason)
	}

	actions = DescribeExecutionAvailableActions(
		&models.FailoverV2Service{},
		&models.FailoverV2Member{},
		&models.FailoverV2Execution{Status: "running"},
	)
	if !actions.StopExecution.Available {
		t.Fatalf("expected legacy running execution to expose stop action, got reason %q", actions.StopExecution.Reason)
	}

	finishedAt := models.FromTime(time.Now())
	actions = DescribeExecutionAvailableActions(
		&models.FailoverV2Service{},
		&models.FailoverV2Member{},
		&models.FailoverV2Execution{Status: "running", FinishedAt: &finishedAt},
	)
	if actions.StopExecution.Available {
		t.Fatal("expected finished legacy execution stop action to be unavailable")
	}

	actions = DescribeExecutionAvailableActions(
		&models.FailoverV2Service{},
		&models.FailoverV2Member{},
		&models.FailoverV2Execution{Status: models.FailoverV2ExecutionStatusFailed},
	)
	if actions.StopExecution.Available {
		t.Fatal("expected completed execution stop action to be unavailable")
	}
}
