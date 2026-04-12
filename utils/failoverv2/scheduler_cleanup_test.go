package failoverv2

import (
	"testing"
	"time"

	"github.com/komari-monitor/komari/config"
	failoverv2db "github.com/komari-monitor/komari/database/failoverv2"
	"github.com/komari-monitor/komari/database/models"
)

func TestShouldRunScheduledExecutionLogCleanup(t *testing.T) {
	now := time.Date(2026, time.April, 12, 8, 0, 0, 0, time.UTC)

	if !shouldRunScheduledExecutionLogCleanup(now, "") {
		t.Fatal("expected cleanup to run when last run timestamp is empty")
	}
	if !shouldRunScheduledExecutionLogCleanup(now, "invalid-time") {
		t.Fatal("expected cleanup to run when last run timestamp is invalid")
	}
	if shouldRunScheduledExecutionLogCleanup(now, now.Add(-1*time.Hour).Format(time.RFC3339)) {
		t.Fatal("expected cleanup to be throttled within 24h window")
	}
	if !shouldRunScheduledExecutionLogCleanup(now, now.Add(-25*time.Hour).Format(time.RFC3339)) {
		t.Fatal("expected cleanup to run after 24h window")
	}
}

func TestRunScheduledExecutionLogCleanupDeletesExpiredTerminalExecutions(t *testing.T) {
	configureFailoverV2RunnerTestDB(t)
	service, member := createTestRunnerServiceAndMember(t)

	seedExecution := func(status string, startedAt time.Time) uint {
		execution, err := failoverv2db.CreateExecutionForUser("user-a", service.ID, member.ID, &models.FailoverV2Execution{
			Status:        status,
			TriggerReason: status,
			StartedAt:     models.FromTime(startedAt),
		})
		if err != nil {
			t.Fatalf("failed to create execution: %v", err)
		}
		if _, err := failoverv2db.CreateExecutionStep(&models.FailoverV2ExecutionStep{
			ExecutionID: execution.ID,
			Sort:        1,
			StepKey:     "seed",
			StepLabel:   "Seed",
			Status:      models.FailoverStepStatusSuccess,
		}); err != nil {
			t.Fatalf("failed to create execution step: %v", err)
		}
		return execution.ID
	}

	now := time.Now()
	oldTerminalID := seedExecution(models.FailoverV2ExecutionStatusSuccess, now.Add(-45*24*time.Hour))
	oldActiveID := seedExecution(models.FailoverV2ExecutionStatusWaitingAgent, now.Add(-60*24*time.Hour))
	recentTerminalID := seedExecution(models.FailoverV2ExecutionStatusFailed, now.Add(-2*24*time.Hour))

	if err := config.Set(config.FailoverV2ExecutionLogRetentionDaysKey, 30); err != nil {
		t.Fatalf("failed to set retention days: %v", err)
	}
	if err := config.Set(config.FailoverV2ExecutionLogCleanupLastRunAtKey, ""); err != nil {
		t.Fatalf("failed to clear last run timestamp: %v", err)
	}

	if err := runScheduledExecutionLogCleanup(now); err != nil {
		t.Fatalf("runScheduledExecutionLogCleanup returned error: %v", err)
	}

	if _, err := failoverv2db.GetExecutionByIDForUser("user-a", service.ID, oldTerminalID); err == nil {
		t.Fatal("expected expired terminal execution to be deleted")
	}
	if _, err := failoverv2db.GetExecutionByIDForUser("user-a", service.ID, oldActiveID); err != nil {
		t.Fatalf("expected active execution to be preserved: %v", err)
	}
	if _, err := failoverv2db.GetExecutionByIDForUser("user-a", service.ID, recentTerminalID); err != nil {
		t.Fatalf("expected recent terminal execution to be preserved: %v", err)
	}

	lastRunRaw, err := config.GetAs[string](config.FailoverV2ExecutionLogCleanupLastRunAtKey, "")
	if err != nil {
		t.Fatalf("failed to load last run timestamp: %v", err)
	}
	if lastRunRaw == "" {
		t.Fatal("expected scheduled cleanup to persist last run timestamp")
	}

	lateOldTerminalID := seedExecution(models.FailoverV2ExecutionStatusSuccess, now.Add(-50*24*time.Hour))
	if err := runScheduledExecutionLogCleanup(now.Add(1 * time.Hour)); err != nil {
		t.Fatalf("runScheduledExecutionLogCleanup within throttle window failed: %v", err)
	}
	if _, err := failoverv2db.GetExecutionByIDForUser("user-a", service.ID, lateOldTerminalID); err != nil {
		t.Fatalf("expected throttled cleanup to skip deletion: %v", err)
	}

	if err := runScheduledExecutionLogCleanup(now.Add(25 * time.Hour)); err != nil {
		t.Fatalf("runScheduledExecutionLogCleanup after throttle window failed: %v", err)
	}
	if _, err := failoverv2db.GetExecutionByIDForUser("user-a", service.ID, lateOldTerminalID); err == nil {
		t.Fatal("expected cleanup after throttle window to delete old terminal execution")
	}
}
