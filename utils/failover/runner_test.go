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

	clientUUID, err := waitForClientByGroup(ctx, "user-a", "test-group", "", time.Now(), 30)
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
