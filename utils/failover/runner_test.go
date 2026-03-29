package failover

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/komari-monitor/komari/common"
	"github.com/komari-monitor/komari/database/models"
)

func TestEvaluateTaskHealthRetriesMissingReportBeforeTriggering(t *testing.T) {
	now := time.Now()
	task := &models.FailoverTask{
		FailureThreshold:    2,
		WatchClientUUID:     "client-1",
		TriggerFailureCount: 0,
	}

	shouldTrigger, fields, reason := evaluateTaskHealth(task, nil, now)
	if shouldTrigger {
		t.Fatal("expected first missing report check to only increment retry count")
	}
	if got := intMapValue(fields, "trigger_failure_count"); got != 1 {
		t.Fatalf("expected trigger_failure_count=1, got %d", got)
	}
	if reason != "" {
		t.Fatalf("expected no trigger reason on first missing report check, got %q", reason)
	}
}

func TestEvaluateTaskHealthTriggersWhenMissingReportThresholdReached(t *testing.T) {
	now := time.Now()
	task := &models.FailoverTask{
		FailureThreshold:    2,
		WatchClientUUID:     "client-1",
		TriggerFailureCount: 1,
	}

	shouldTrigger, fields, reason := evaluateTaskHealth(task, nil, now)
	if !shouldTrigger {
		t.Fatal("expected missing report threshold to trigger failover")
	}
	if got := intMapValue(fields, "trigger_failure_count"); got != 2 {
		t.Fatalf("expected trigger_failure_count=2, got %d", got)
	}
	if reason == "" {
		t.Fatal("expected trigger reason for missing report threshold")
	}
}

func TestEvaluateTaskHealthTriggersOnStaleReportThreshold(t *testing.T) {
	now := time.Now()
	task := &models.FailoverTask{
		FailureThreshold:    2,
		StaleAfterSeconds:   30,
		WatchClientUUID:     "client-1",
		TriggerFailureCount: 1,
	}
	report := &common.Report{
		UpdatedAt: now.Add(-2 * time.Minute),
		CNConnectivity: &common.CNConnectivityReport{
			Status:    "ok",
			CheckedAt: now.Add(-2 * time.Minute),
		},
	}

	shouldTrigger, fields, reason := evaluateTaskHealth(task, report, now)
	if !shouldTrigger {
		t.Fatal("expected stale report threshold to trigger failover")
	}
	if got := intMapValue(fields, "trigger_failure_count"); got != 2 {
		t.Fatalf("expected trigger_failure_count=2, got %d", got)
	}
	if reason == "" {
		t.Fatal("expected trigger reason for stale report threshold")
	}
}

func TestEvaluateTaskHealthResetsRetryCounterOnHealthyReport(t *testing.T) {
	now := time.Now()
	task := &models.FailoverTask{
		FailureThreshold:    2,
		StaleAfterSeconds:   60,
		WatchClientUUID:     "client-1",
		TriggerFailureCount: 3,
	}
	report := &common.Report{
		UpdatedAt: now,
		CNConnectivity: &common.CNConnectivityReport{
			Status:              "ok",
			CheckedAt:           now,
			ConsecutiveFailures: 0,
		},
	}

	shouldTrigger, fields, reason := evaluateTaskHealth(task, report, now)
	if shouldTrigger {
		t.Fatal("expected healthy report to avoid triggering failover")
	}
	if got := intMapValue(fields, "trigger_failure_count"); got != 0 {
		t.Fatalf("expected trigger_failure_count reset to 0, got %d", got)
	}
	if reason != "" {
		t.Fatalf("expected no reason for healthy report, got %q", reason)
	}
}

func TestWaitForClientByGroupStopsWhenContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	clientUUID, err := waitForClientByGroup(ctx, "user-a", "test-group", "", time.Now(), 30, nil)
	if err == nil {
		t.Fatal("expected waitForClientByGroup to stop on cancelled context")
	}
	if !errors.Is(err, errExecutionStopped) {
		t.Fatalf("expected errExecutionStopped, got %v", err)
	}
	if clientUUID != "" {
		t.Fatalf("expected empty client UUID, got %q", clientUUID)
	}
}

func TestPickPreferredAutoConnectClientPrefersExpectedIPAddress(t *testing.T) {
	startedAt := time.Now()
	candidates := []models.Client{
		{
			UUID:      "first-task-client",
			IPv4:      "1.1.1.1",
			CreatedAt: models.FromTime(startedAt.Add(-10 * time.Minute)),
		},
		{
			UUID:      "new-task-client",
			IPv4:      "2.2.2.2",
			CreatedAt: models.FromTime(startedAt.Add(15 * time.Second)),
		},
	}

	expected := expectedClientAddresses(&actionOutcome{IPv4: "2.2.2.2"})
	if got := pickPreferredAutoConnectClient(candidates, startedAt, expected); got != "new-task-client" {
		t.Fatalf("expected new-task-client, got %q", got)
	}
}

func TestPickPreferredAutoConnectClientFallsBackToNewlyCreatedClient(t *testing.T) {
	startedAt := time.Now()
	candidates := []models.Client{
		{
			UUID:      "old-client",
			IPv4:      "1.1.1.1",
			CreatedAt: models.FromTime(startedAt.Add(-5 * time.Minute)),
		},
		{
			UUID:      "fresh-client",
			IPv4:      "2.2.2.2",
			CreatedAt: models.FromTime(startedAt.Add(10 * time.Second)),
		},
	}

	if got := pickPreferredAutoConnectClient(candidates, startedAt, nil); got != "fresh-client" {
		t.Fatalf("expected fresh-client, got %q", got)
	}
}

func TestPickPreferredAutoConnectClientRequiresIPMatchWhenExpectedAddressExists(t *testing.T) {
	startedAt := time.Now()
	candidates := []models.Client{
		{
			UUID:      "fresh-client",
			IPv4:      "2.2.2.2",
			CreatedAt: models.FromTime(startedAt.Add(10 * time.Second)),
		},
	}

	expected := expectedClientAddresses(&actionOutcome{IPv4: "3.3.3.3"})
	if got := pickPreferredAutoConnectClient(candidates, startedAt, expected); got != "" {
		t.Fatalf("expected no client without IP match, got %q", got)
	}
}

func TestPickPreferredAutoConnectClientIgnoresOlderSameGroupClients(t *testing.T) {
	startedAt := time.Now()
	candidates := []models.Client{
		{
			UUID:      "first-task-client",
			IPv4:      "1.1.1.1",
			CreatedAt: models.FromTime(startedAt.Add(-30 * time.Minute)),
		},
	}

	if got := pickPreferredAutoConnectClient(candidates, startedAt, nil); got != "" {
		t.Fatalf("expected no client to be selected, got %q", got)
	}
}

func TestCommandResultExecutionErrorTreatsNonZeroExitCodeAsFailure(t *testing.T) {
	exitCode := 2
	err := commandResultExecutionError(&commandResult{
		ExitCode: &exitCode,
		Output:   "sh: 41: Syntax error: \"(\" unexpected (expecting \"}\")\n",
	})

	if err == nil {
		t.Fatal("expected non-zero exit code to mark script execution as failed")
	}
	if got := err.Error(); got != "script exited with code 2: sh: 41: Syntax error: \"(\" unexpected (expecting \"}\")" {
		t.Fatalf("unexpected script execution error: %q", got)
	}
}

func TestCommandResultExecutionErrorAllowsZeroExitCode(t *testing.T) {
	exitCode := 0
	if err := commandResultExecutionError(&commandResult{ExitCode: &exitCode, Output: "ok"}); err != nil {
		t.Fatalf("expected zero exit code to be treated as success, got %v", err)
	}
}

func TestEnsureCommandResultReturnsEmptyResultForNil(t *testing.T) {
	result := ensureCommandResult(nil)
	if result == nil {
		t.Fatal("expected ensureCommandResult to return an empty result for nil input")
	}
	if result.TaskID != "" || result.Output != "" || result.ExitCode != nil {
		t.Fatalf("expected zero-value command result, got %+v", result)
	}
}

func TestEffectiveTaskDeleteStrategyForProvisionPlans(t *testing.T) {
	task := models.FailoverTask{
		DeleteStrategy: models.FailoverDeleteStrategyKeep,
		Plans: []models.FailoverPlan{
			{
				Enabled:    true,
				ActionType: models.FailoverActionProvisionInstance,
			},
		},
	}

	if got := effectiveTaskDeleteStrategy(task); got != models.FailoverDeleteStrategyDeleteAfterSuccess {
		t.Fatalf("expected provision plan to force delete_after_success, got %q", got)
	}
}

func TestEffectiveTaskDeleteStrategyKeepsRebindPlans(t *testing.T) {
	task := models.FailoverTask{
		DeleteStrategy: models.FailoverDeleteStrategyDeleteAfterSuccess,
		Plans: []models.FailoverPlan{
			{
				Enabled:    true,
				ActionType: models.FailoverActionRebindPublicIP,
			},
		},
	}

	if got := effectiveTaskDeleteStrategy(task); got != models.FailoverDeleteStrategyKeep {
		t.Fatalf("expected rebind-only plans to keep old instance, got %q", got)
	}
}

func TestInvalidateProvisionedEntrySnapshotClearsTrackedCapacity(t *testing.T) {
	originalScheduler := failoverProviderEntryScheduler
	failoverProviderEntryScheduler = &providerEntryScheduler{
		states: map[string]*providerEntryRuntimeState{},
	}
	defer func() {
		failoverProviderEntryScheduler = originalScheduler
	}()

	state := failoverProviderEntryScheduler.stateFor(providerEntryStateKey("user-1", "digitalocean", "entry-1"))
	state.snapshot = &providerEntryCapacitySnapshot{
		Mode:  providerEntryCapacityModeQuota,
		Limit: 5,
		Used:  4,
	}
	state.provisionedDelta = 2

	runner := &executionRunner{
		task: models.FailoverTask{
			UserID: "user-1",
		},
	}
	runner.invalidateProvisionedEntrySnapshot(&actionOutcome{
		NewInstanceRef: map[string]interface{}{
			"provider":          "digitalocean",
			"provider_entry_id": "entry-1",
		},
	})

	if state.snapshot != nil {
		t.Fatal("expected provider snapshot to be invalidated after rollback")
	}
	if state.provisionedDelta != 0 {
		t.Fatalf("expected provisioned delta reset to 0, got %d", state.provisionedDelta)
	}
}
