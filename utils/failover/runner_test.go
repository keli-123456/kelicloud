package failover

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/komari-monitor/komari/cmd/flags"
	"github.com/komari-monitor/komari/common"
	"github.com/komari-monitor/komari/database"
	"github.com/komari-monitor/komari/database/dbcore"
	"github.com/komari-monitor/komari/database/models"
	"github.com/komari-monitor/komari/utils/cloudprovider/digitalocean"
)

func configureRunnerSQLiteDB(t *testing.T) {
	t.Helper()

	flags.DatabaseType = "sqlite"
	flags.DatabaseFile = filepath.Join(os.TempDir(), "komari-failover-runner-tests.db")
	_ = os.Remove(flags.DatabaseFile)

	db := dbcore.GetDBInstance()
	if err := db.AutoMigrate(&models.CloudProvider{}); err != nil {
		t.Fatalf("failed to migrate cloud provider schema: %v", err)
	}
}

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

func TestProviderEntryMaxAttemptsUsesTaskProvisionRetryLimit(t *testing.T) {
	task := models.FailoverTask{
		ProvisionRetryLimit: 3,
	}
	provisionPlan := models.FailoverPlan{
		Provider:   "digitalocean",
		ActionType: models.FailoverActionProvisionInstance,
	}
	rebindPlan := models.FailoverPlan{
		Provider:   "aws",
		ActionType: models.FailoverActionRebindPublicIP,
	}

	if got := providerEntryMaxAttempts(task, provisionPlan); got != 3 {
		t.Fatalf("expected provision retries to use task limit 3, got %d", got)
	}
	if got := providerEntryMaxAttempts(task, rebindPlan); got != 1 {
		t.Fatalf("expected rebind retries to remain 1, got %d", got)
	}
}

func TestProviderEntryMaxAttemptsFallsBackToDefaultProvisionRetryLimit(t *testing.T) {
	task := models.FailoverTask{}
	provisionPlan := models.FailoverPlan{
		Provider:   "digitalocean",
		ActionType: models.FailoverActionProvisionInstance,
	}

	if got := providerEntryMaxAttempts(task, provisionPlan); got != models.FailoverProvisionRetryLimitDefault {
		t.Fatalf(
			"expected default provision retry limit %d, got %d",
			models.FailoverProvisionRetryLimitDefault,
			got,
		)
	}
}

func TestProvisionPlanFailureFallbackLimitUsesTaskSetting(t *testing.T) {
	task := models.FailoverTask{
		ProvisionFailureFallbackLimit: 4,
	}
	provisionPlan := models.FailoverPlan{
		Provider:   "digitalocean",
		ActionType: models.FailoverActionProvisionInstance,
	}
	rebindPlan := models.FailoverPlan{
		Provider:   "aws",
		ActionType: models.FailoverActionRebindPublicIP,
	}

	if got := provisionPlanFailureFallbackLimit(task, provisionPlan); got != 4 {
		t.Fatalf("expected provision fallback limit to use task value 4, got %d", got)
	}
	if got := provisionPlanFailureFallbackLimit(task, rebindPlan); got != 1 {
		t.Fatalf("expected rebind fallback limit to remain 1, got %d", got)
	}
}

func TestProvisionPlanFailureFallbackLimitFallsBackToDefault(t *testing.T) {
	task := models.FailoverTask{}
	provisionPlan := models.FailoverPlan{
		Provider:   "digitalocean",
		ActionType: models.FailoverActionProvisionInstance,
	}

	if got := provisionPlanFailureFallbackLimit(task, provisionPlan); got != models.FailoverProvisionFailureFallbackLimitDefault {
		t.Fatalf(
			"expected default provision fallback limit %d, got %d",
			models.FailoverProvisionFailureFallbackLimitDefault,
			got,
		)
	}
}

func TestNoPlanFallbackErrorWrapsUnderlyingError(t *testing.T) {
	baseErr := errors.New("blocked after successful provisioning")
	err := &noPlanFallbackError{err: baseErr}

	if !errors.Is(err, baseErr) {
		t.Fatal("expected noPlanFallbackError to unwrap the underlying error")
	}
	if got := err.Error(); got != baseErr.Error() {
		t.Fatalf("expected error message %q, got %q", baseErr.Error(), got)
	}
}

func TestBlockedOutletRetryBackoffUsesFixedDelay(t *testing.T) {
	cases := []struct {
		attempt  int
		expected time.Duration
	}{
		{attempt: 1, expected: 15 * time.Second},
		{attempt: 2, expected: 15 * time.Second},
		{attempt: 3, expected: 15 * time.Second},
		{attempt: 4, expected: 15 * time.Second},
		{attempt: 5, expected: 15 * time.Second},
	}

	for _, tc := range cases {
		if got := blockedOutletRetryBackoff(tc.attempt); got != tc.expected {
			t.Fatalf("attempt %d backoff = %s, want %s", tc.attempt, got, tc.expected)
		}
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

func TestResolveCurrentInstanceCleanupFromRefTreatsMissingDigitalOceanTokenAsManualReview(t *testing.T) {
	configureRunnerSQLiteDB(t)

	if err := database.SaveCloudProviderConfigForUser(&models.CloudProvider{
		UserID: "user-stale",
		Name:   "digitalocean",
		Addition: `{
			"active_token_id":"token-live",
			"tokens":[
				{"id":"token-live","name":"Token Live","token":"dop_v1_live"}
			]
		}`,
	}); err != nil {
		t.Fatalf("failed to seed digitalocean provider config: %v", err)
	}

	cleanup, err := resolveCurrentInstanceCleanupFromRef(context.Background(), "user-stale", map[string]interface{}{
		"provider":            "digitalocean",
		"provider_entry_id":   "token-deleted",
		"provider_entry_name": "Token 1",
		"droplet_id":          12345,
	})
	if err != nil {
		t.Fatalf("expected missing provider entry to be tolerated, got %v", err)
	}
	if cleanup == nil {
		t.Fatal("expected cleanup metadata to be returned")
	}
	if cleanup.Missing {
		t.Fatal("expected missing credential to require manual review instead of missing-instance classification")
	}
	if cleanup.Cleanup != nil {
		t.Fatal("expected no cleanup callback when provider entry is missing")
	}
	if cleanup.Assessment == nil {
		t.Fatal("expected cleanup assessment for missing provider entry")
	}
	if cleanup.Assessment.Status != models.FailoverCleanupStatusWarning {
		t.Fatalf("expected warning cleanup status, got %q", cleanup.Assessment.Status)
	}
	if got := stringMapValue(cleanup.Assessment.Result, "classification"); got != cleanupClassificationProviderEntryMissing {
		t.Fatalf("expected classification %q, got %q", cleanupClassificationProviderEntryMissing, got)
	}
	if got := providerEntryIDFromRef(cleanup.Ref); got != "token-deleted" {
		t.Fatalf("expected ref entry id token-deleted, got %q", got)
	}
	if got := providerEntryNameFromRef(cleanup.Ref); got != "Token 1" {
		t.Fatalf("expected ref entry name Token 1, got %q", got)
	}
	if cleanup.Label != "delete digitalocean droplet 12345" {
		t.Fatalf("unexpected cleanup label %q", cleanup.Label)
	}
}

func TestResolveCurrentInstanceCleanupByRefAddressTreatsMissingDigitalOceanTokenAsManualReview(t *testing.T) {
	configureRunnerSQLiteDB(t)

	if err := database.SaveCloudProviderConfigForUser(&models.CloudProvider{
		UserID: "user-stale-address",
		Name:   "digitalocean",
		Addition: `{
			"active_token_id":"token-live",
			"tokens":[
				{"id":"token-live","name":"Token Live","token":"dop_v1_live"}
			]
		}`,
	}); err != nil {
		t.Fatalf("failed to seed digitalocean provider config: %v", err)
	}

	cleanup, err := resolveCurrentInstanceCleanupByRefAddress(context.Background(), "user-stale-address", map[string]interface{}{
		"provider":            "digitalocean",
		"provider_entry_id":   "token-deleted",
		"provider_entry_name": "Token 1",
	}, "203.0.113.10")
	if err != nil {
		t.Fatalf("expected missing provider entry to be tolerated, got %v", err)
	}
	if cleanup == nil {
		t.Fatal("expected cleanup metadata to be returned")
	}
	if cleanup.Cleanup != nil {
		t.Fatal("expected no cleanup callback when provider entry is missing")
	}
	if cleanup.Assessment == nil {
		t.Fatal("expected cleanup assessment for missing provider entry")
	}
	if got := stringMapValue(cleanup.Assessment.Result, "classification"); got != cleanupClassificationProviderEntryMissing {
		t.Fatalf("expected classification %q, got %q", cleanupClassificationProviderEntryMissing, got)
	}
	if cleanup.Label != "delete digitalocean instance at 203.0.113.10" {
		t.Fatalf("unexpected cleanup label %q", cleanup.Label)
	}
}

func TestBuildProviderEntryQueryCleanupAssessmentMarksAuthInvalidAsWarning(t *testing.T) {
	assessment := buildProviderEntryQueryCleanupAssessment(
		"digitalocean",
		map[string]interface{}{
			"provider":          "digitalocean",
			"provider_entry_id": "token-old",
			"droplet_id":        42,
		},
		"delete digitalocean droplet 42",
		&digitalocean.APIError{StatusCode: 401, Message: "unauthorized"},
	)
	if assessment == nil {
		t.Fatal("expected cleanup assessment")
	}
	if assessment.Status != models.FailoverCleanupStatusWarning {
		t.Fatalf("expected warning cleanup status, got %q", assessment.Status)
	}
	if assessment.StepStatus != models.FailoverStepStatusSkipped {
		t.Fatalf("expected skipped step status, got %q", assessment.StepStatus)
	}
	if assessment.StepMessage != cleanupStepMessageProviderEntryUnhealthy {
		t.Fatalf("unexpected step message %q", assessment.StepMessage)
	}
	if got := stringMapValue(assessment.Result, "classification"); got != cleanupClassificationProviderEntryUnhealthy {
		t.Fatalf("expected classification %q, got %q", cleanupClassificationProviderEntryUnhealthy, got)
	}
	if got := stringMapValue(assessment.Result, "provider_failure_class"); got != "auth_invalid" {
		t.Fatalf("expected provider failure class auth_invalid, got %q", got)
	}
}

func TestBuildUnresolvedCurrentInstanceCleanupAssessmentMarksManualReview(t *testing.T) {
	assessment := buildUnresolvedCurrentInstanceCleanupAssessment(map[string]interface{}{
		"provider":            "digitalocean",
		"provider_entry_id":   "entry-1",
		"provider_entry_name": "Token 1",
		"droplet_id":          101,
	}, "203.0.113.10")
	if assessment == nil {
		t.Fatal("expected cleanup assessment")
	}
	if assessment.Status != models.FailoverCleanupStatusWarning {
		t.Fatalf("expected warning cleanup status, got %q", assessment.Status)
	}
	if assessment.StepStatus != models.FailoverStepStatusSkipped {
		t.Fatalf("expected skipped step status, got %q", assessment.StepStatus)
	}
	if assessment.StepMessage != cleanupStepMessageCleanupStatusUnknown {
		t.Fatalf("unexpected step message %q", assessment.StepMessage)
	}
	if got := stringMapValue(assessment.Result, "classification"); got != cleanupClassificationCleanupStatusUnknown {
		t.Fatalf("expected classification %q, got %q", cleanupClassificationCleanupStatusUnknown, got)
	}
	if got := stringMapValue(assessment.Result, "billing_risk"); got != "unknown" {
		t.Fatalf("expected billing risk unknown, got %q", got)
	}
	if got := stringMapValue(assessment.Result, "current_address"); got != "203.0.113.10" {
		t.Fatalf("expected current address 203.0.113.10, got %q", got)
	}
}
