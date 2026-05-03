package failover

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/komari-monitor/komari/cmd/flags"
	"github.com/komari-monitor/komari/common"
	"github.com/komari-monitor/komari/database"
	"github.com/komari-monitor/komari/database/dbcore"
	"github.com/komari-monitor/komari/database/models"
	awscloud "github.com/komari-monitor/komari/utils/cloudprovider/aws"
	"github.com/komari-monitor/komari/utils/cloudprovider/digitalocean"
	linodecloud "github.com/komari-monitor/komari/utils/cloudprovider/linode"
)

func configureRunnerSQLiteDB(t *testing.T) {
	t.Helper()

	flags.DatabaseType = "sqlite"
	flags.DatabaseFile = filepath.Join(os.TempDir(), "komari-failover-runner-tests.db")
	_ = os.Remove(flags.DatabaseFile)

	db := dbcore.GetDBInstance()
	if err := db.AutoMigrate(&models.CloudProvider{}, &models.FailoverPendingCleanup{}); err != nil {
		t.Fatalf("failed to migrate cloud provider schema: %v", err)
	}
}

func resetRunnerRuntimeStateForTest(t *testing.T) {
	t.Helper()

	runningTasksMu.Lock()
	runningTasks = map[uint]struct{}{}
	runningTasksMu.Unlock()

	runningTargetMu.Lock()
	runningTargets = map[string]uint{}
	runningTargetMu.Unlock()

	pendingRollbackCleanupRunMu.Lock()
	pendingRollbackCleanupRunActive = false
	pendingRollbackCleanupRunMu.Unlock()

	pendingRollbackCleanupResolveFunc = resolveCurrentInstanceCleanupFromRef
}

func resetRunnerPendingCleanupTable(t *testing.T) {
	t.Helper()

	configureRunnerSQLiteDB(t)
	resetRunnerRuntimeStateForTest(t)
	db := dbcore.GetDBInstance()
	if err := db.Exec("DELETE FROM failover_pending_cleanups").Error; err != nil {
		t.Fatalf("failed to reset pending cleanup table: %v", err)
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

func TestFailoverTargetRunKeyPrefersTrackedInstanceIdentity(t *testing.T) {
	key, err := failoverTargetRunKey(models.FailoverTask{
		UserID:             "user-a",
		WatchClientUUID:    "client-a",
		CurrentAddress:     "203.0.113.10",
		CurrentInstanceRef: `{"provider":"digitalocean","provider_entry_id":"token-1","droplet_id":123}`,
	})
	if err != nil {
		t.Fatalf("expected no error building target run key, got %v", err)
	}
	if key != "user-a|ref|digitalocean|token-1|droplet|123" {
		t.Fatalf("unexpected target run key %q", key)
	}
}

func TestClaimTargetRunRejectsConcurrentOutletHandling(t *testing.T) {
	resetRunnerRuntimeStateForTest(t)

	if activeTaskID, claimed := claimTargetRun("user-a|watch|client-a", 1); !claimed || activeTaskID != 0 {
		t.Fatalf("expected first target claim to succeed, got claimed=%v active=%d", claimed, activeTaskID)
	}
	if activeTaskID, claimed := claimTargetRun("user-a|watch|client-a", 2); claimed || activeTaskID != 1 {
		t.Fatalf("expected second target claim to fail with task 1, got claimed=%v active=%d", claimed, activeTaskID)
	}
	releaseTargetRun("user-a|watch|client-a", 1)
}

func TestQueueExecutionRejectsDisabledTaskBeforeClaimingLocks(t *testing.T) {
	resetRunnerRuntimeStateForTest(t)

	task := &models.FailoverTask{
		ID:              42,
		UserID:          "user-a",
		Enabled:         false,
		WatchClientUUID: "client-a",
	}
	if _, err := queueExecution(task, nil, "manual run"); err == nil || !strings.Contains(err.Error(), "disabled") {
		t.Fatalf("expected disabled task error, got %v", err)
	}

	runningTasksMu.Lock()
	_, taskLocked := runningTasks[task.ID]
	runningTasksMu.Unlock()
	if taskLocked {
		t.Fatal("expected disabled task to leave no task run lock behind")
	}
}

func TestQueueExecutionMarksCreatedExecutionFailedWhenTaskUpdateFails(t *testing.T) {
	configureRunnerSQLiteDB(t)
	resetRunnerRuntimeStateForTest(t)

	db := dbcore.GetDBInstance()
	if err := db.AutoMigrate(&models.FailoverTask{}, &models.FailoverExecution{}); err != nil {
		t.Fatalf("failed to migrate failover queue schema: %v", err)
	}

	task := &models.FailoverTask{
		ID:              999,
		UserID:          "user-queue-failure",
		Enabled:         true,
		WatchClientUUID: "client-queue-failure",
	}
	if _, err := queueExecution(task, nil, "manual run"); err == nil {
		t.Fatal("expected queueExecution to fail when the task row is missing")
	}

	var execution models.FailoverExecution
	if err := db.Where("task_id = ?", task.ID).First(&execution).Error; err != nil {
		t.Fatalf("expected queued execution to be persisted before update failure: %v", err)
	}
	if execution.Status != models.FailoverExecutionStatusFailed {
		t.Fatalf("expected dangling execution to be marked failed, got %q", execution.Status)
	}
	if !strings.Contains(execution.ErrorMessage, "failed to mark task running") {
		t.Fatalf("expected queue failure message, got %q", execution.ErrorMessage)
	}
	if execution.FinishedAt == nil {
		t.Fatal("expected failed queued execution to have finished_at set")
	}

	runningTargetMu.Lock()
	targetLockedBy := runningTargets["user-queue-failure|watch|client-queue-failure"]
	runningTargetMu.Unlock()
	if targetLockedBy != 0 {
		t.Fatalf("expected target run lock to be released, still held by task %d", targetLockedBy)
	}
}

func TestNormalizeIPAddressAcceptsCIDRNotation(t *testing.T) {
	if got := normalizeIPAddress("2001:db8::10/64"); got != "2001:db8::10" {
		t.Fatalf("expected normalized ipv6, got %q", got)
	}
	if got := normalizeIPAddress("203.0.113.8/32"); got != "203.0.113.8" {
		t.Fatalf("expected normalized ipv4, got %q", got)
	}
}

func TestFirstUsablePublicIPv6SkipsLinkLocalAndPrivateRanges(t *testing.T) {
	got := firstUsablePublicIPv6([]string{
		"fe80::82:26ff:fe5e:159/64",
		"fd00::1234",
		"2600:1f18:abcd:1200::10/128",
	})
	if got != "2600:1f18:abcd:1200::10" {
		t.Fatalf("expected public IPv6 to be selected, got %q", got)
	}
}

func TestFirstUsablePublicIPv6ReturnsEmptyWhenNoUsableAddressExists(t *testing.T) {
	got := firstUsablePublicIPv6([]string{
		"",
		"fe80::82:26ff:fe5e:159/64",
		"::1",
		"fd00::1234",
	})
	if got != "" {
		t.Fatalf("expected no usable public IPv6, got %q", got)
	}
}

func TestResolveAWSRebindPayloadUsesTrackedCurrentEC2Instance(t *testing.T) {
	payload, ok := resolveAWSRebindPayload(models.FailoverTask{
		CurrentInstanceRef: `{"provider":"aws","service":"ec2","provider_entry_id":"cred-1","region":"us-east-1","instance_id":"i-1234567890"}`,
	}, awsRebindPayload{
		Service: "ec2",
		Region:  "",
	}, "cred-1")
	if !ok {
		t.Fatal("expected tracked EC2 instance to resolve")
	}
	if payload.Region != "us-east-1" {
		t.Fatalf("expected tracked region, got %q", payload.Region)
	}
	if payload.InstanceID != "i-1234567890" {
		t.Fatalf("expected tracked instance id, got %q", payload.InstanceID)
	}
}

func TestResolveAWSRebindPayloadUsesTrackedCurrentLightsailInstance(t *testing.T) {
	payload, ok := resolveAWSRebindPayload(models.FailoverTask{
		CurrentInstanceRef: `{"provider":"aws","service":"lightsail","provider_entry_id":"cred-2","region":"us-west-2","instance_name":"edge-ls"}`,
	}, awsRebindPayload{
		Service: "lightsail",
	}, "cred-2")
	if !ok {
		t.Fatal("expected tracked Lightsail instance to resolve")
	}
	if payload.Region != "us-west-2" {
		t.Fatalf("expected tracked region, got %q", payload.Region)
	}
	if payload.InstanceName != "edge-ls" {
		t.Fatalf("expected tracked instance name, got %q", payload.InstanceName)
	}
}

func TestResolveAWSRebindPayloadSkipsTrackedInstanceFromDifferentProviderEntry(t *testing.T) {
	_, ok := resolveAWSRebindPayload(models.FailoverTask{
		CurrentInstanceRef: `{"provider":"aws","service":"ec2","provider_entry_id":"cred-1","region":"us-east-1","instance_id":"i-1234567890"}`,
	}, awsRebindPayload{
		Service: "ec2",
	}, "cred-2")
	if ok {
		t.Fatal("expected tracked instance from another credential to be ignored")
	}
}

func TestBuildAWSProvisionFallbackPlanRequiresProvisionConfig(t *testing.T) {
	plan := models.FailoverPlan{
		Provider:        "aws",
		ProviderEntryID: "cred-1",
		ActionType:      models.FailoverActionRebindPublicIP,
	}
	_, _, _, err := buildAWSProvisionFallbackPlan(plan, awsRebindPayload{
		Service: "ec2",
		Region:  "us-east-1",
		ImageID: "ami-123",
	}, "missing instance type")
	var noFallbackErr *noPlanFallbackError
	if !errors.As(err, &noFallbackErr) {
		t.Fatalf("expected noPlanFallbackError for incomplete fallback config, got %v", err)
	}
}

func TestPrepareAWSRebindExecutionPlanUsesTrackedInstanceWhenAlive(t *testing.T) {
	originalLoadCredential := awsFailoverLoadCredential
	originalResolveCurrent := awsFailoverResolveCurrentInstanceByAddress
	defer func() {
		awsFailoverLoadCredential = originalLoadCredential
		awsFailoverResolveCurrentInstanceByAddress = originalResolveCurrent
	}()

	awsFailoverLoadCredential = func(userUUID, entryID string) (*awscloud.Addition, *awscloud.CredentialRecord, error) {
		return &awscloud.Addition{ActiveRegion: "us-east-1"}, &awscloud.CredentialRecord{
			ID:            entryID,
			Name:          "cred-1",
			DefaultRegion: "us-east-1",
		}, nil
	}
	awsFailoverResolveCurrentInstanceByAddress = func(ctx context.Context, task models.FailoverTask, plan models.FailoverPlan, payload awsRebindPayload) (*currentInstanceCleanup, error) {
		return &currentInstanceCleanup{
			Ref: map[string]interface{}{
				"provider":          "aws",
				"service":           "ec2",
				"provider_entry_id": "cred-1",
				"region":            "us-east-1",
				"instance_id":       "i-1234567890",
			},
			Addresses: map[string]interface{}{
				"private_ip": "172.31.10.24",
			},
		}, nil
	}

	executionPlan, mode, reason, err := prepareAWSRebindExecutionPlan(context.Background(), models.FailoverTask{
		UserID:         "user-1",
		CurrentAddress: "203.0.113.10",
	}, models.FailoverPlan{
		Provider:        "aws",
		ProviderEntryID: "cred-1",
		ActionType:      models.FailoverActionRebindPublicIP,
		Payload:         `{"service":"ec2","region":"us-east-1","image_id":"ami-123","instance_type":"t3.micro"}`,
	})
	if err != nil {
		t.Fatalf("expected alive tracked instance to stay on rebind path, got %v", err)
	}
	if mode != "rebind_existing_instance" {
		t.Fatalf("expected rebind_existing_instance mode, got %q", mode)
	}
	if !strings.Contains(reason, "current IP") {
		t.Fatalf("expected rebind reason, got %q", reason)
	}

	var payload awsRebindPayload
	if err := json.Unmarshal([]byte(executionPlan.Payload), &payload); err != nil {
		t.Fatalf("expected normalized rebind payload, got %v", err)
	}
	if payload.InstanceID != "i-1234567890" {
		t.Fatalf("expected tracked instance id in rebind payload, got %q", payload.InstanceID)
	}
	if payload.PrivateIP != "172.31.10.24" {
		t.Fatalf("expected discovered private ip in rebind payload, got %q", payload.PrivateIP)
	}
}

func TestPrepareAWSRebindExecutionPlanFallsBackToProvisionWhenTrackedInstanceMissing(t *testing.T) {
	originalLoadCredential := awsFailoverLoadCredential
	originalResolveCurrent := awsFailoverResolveCurrentInstanceByAddress
	defer func() {
		awsFailoverLoadCredential = originalLoadCredential
		awsFailoverResolveCurrentInstanceByAddress = originalResolveCurrent
	}()

	awsFailoverLoadCredential = func(userUUID, entryID string) (*awscloud.Addition, *awscloud.CredentialRecord, error) {
		return &awscloud.Addition{ActiveRegion: "us-east-1"}, &awscloud.CredentialRecord{
			ID:            entryID,
			Name:          "cred-1",
			DefaultRegion: "us-east-1",
		}, nil
	}
	awsFailoverResolveCurrentInstanceByAddress = func(ctx context.Context, task models.FailoverTask, plan models.FailoverPlan, payload awsRebindPayload) (*currentInstanceCleanup, error) {
		return nil, nil
	}

	executionPlan, mode, reason, err := prepareAWSRebindExecutionPlan(context.Background(), models.FailoverTask{
		UserID:         "user-1",
		CurrentAddress: "203.0.113.10",
	}, models.FailoverPlan{
		Provider:        "aws",
		ProviderEntryID: "cred-1",
		ActionType:      models.FailoverActionRebindPublicIP,
		Payload:         `{"service":"ec2","region":"us-east-1","image_id":"ami-123","instance_type":"t3.micro","assign_public_ip":true}`,
	})
	if err != nil {
		t.Fatalf("expected missing tracked instance to fall back to provision, got %v", err)
	}
	if mode != "provision_new_instance" {
		t.Fatalf("expected provision_new_instance mode, got %q", mode)
	}
	if !strings.Contains(reason, "current IP") {
		t.Fatalf("expected provision fallback reason, got %q", reason)
	}
	if executionPlan.ActionType != models.FailoverActionProvisionInstance {
		t.Fatalf("expected execution plan to switch to provision_instance, got %q", executionPlan.ActionType)
	}

	var payload awsProvisionPayload
	if err := json.Unmarshal([]byte(executionPlan.Payload), &payload); err != nil {
		t.Fatalf("expected fallback provision payload, got %v", err)
	}
	if payload.ImageID != "ami-123" || payload.InstanceType != "t3.micro" {
		t.Fatalf("expected fallback create config to carry over, got %+v", payload)
	}
}

func TestPrepareAWSRebindExecutionPlanUsesTrackedInstanceRefWhenAddressLookupWouldMiss(t *testing.T) {
	originalLoadCredential := awsFailoverLoadCredential
	originalResolveCurrent := awsFailoverResolveCurrentInstanceByAddress
	originalGetDetail := awsFailoverGetEC2InstanceDetail
	defer func() {
		awsFailoverLoadCredential = originalLoadCredential
		awsFailoverResolveCurrentInstanceByAddress = originalResolveCurrent
		awsFailoverGetEC2InstanceDetail = originalGetDetail
	}()

	addressLookupCalled := false
	awsFailoverLoadCredential = func(userUUID, entryID string) (*awscloud.Addition, *awscloud.CredentialRecord, error) {
		return &awscloud.Addition{ActiveRegion: "us-east-1"}, &awscloud.CredentialRecord{
			ID:            entryID,
			Name:          "cred-1",
			DefaultRegion: "us-east-1",
		}, nil
	}
	awsFailoverResolveCurrentInstanceByAddress = func(ctx context.Context, task models.FailoverTask, plan models.FailoverPlan, payload awsRebindPayload) (*currentInstanceCleanup, error) {
		addressLookupCalled = true
		return nil, nil
	}
	awsFailoverGetEC2InstanceDetail = func(ctx context.Context, credential *awscloud.CredentialRecord, region, instanceID string) (*awscloud.InstanceDetail, error) {
		if region != "us-west-2" {
			t.Fatalf("expected tracked region us-west-2, got %q", region)
		}
		if instanceID != "i-tracked" {
			t.Fatalf("expected tracked instance id i-tracked, got %q", instanceID)
		}
		return &awscloud.InstanceDetail{
			Instance: awscloud.Instance{
				InstanceID: "i-tracked",
				State:      "running",
				PrivateIP:  "172.31.20.10",
			},
		}, nil
	}

	executionPlan, mode, reason, err := prepareAWSRebindExecutionPlan(context.Background(), models.FailoverTask{
		UserID:             "user-1",
		CurrentAddress:     "198.51.100.25",
		CurrentInstanceRef: `{"provider":"aws","service":"ec2","provider_entry_id":"cred-1","region":"us-west-2","instance_id":"i-tracked"}`,
	}, models.FailoverPlan{
		Provider:        "aws",
		ProviderEntryID: "cred-1",
		ActionType:      models.FailoverActionRebindPublicIP,
		Payload:         `{"service":"ec2","region":"us-east-1","image_id":"ami-123","instance_type":"t3.micro"}`,
	})
	if err != nil {
		t.Fatalf("expected tracked aws instance ref to stay on rebind path, got %v", err)
	}
	if mode != "rebind_existing_instance" {
		t.Fatalf("expected rebind_existing_instance mode, got %q", mode)
	}
	if !strings.Contains(reason, "tracks an AWS EC2 instance") {
		t.Fatalf("expected tracked ref reason, got %q", reason)
	}
	if addressLookupCalled {
		t.Fatal("expected tracked instance ref to avoid address lookup fallback")
	}

	var payload awsRebindPayload
	if err := json.Unmarshal([]byte(executionPlan.Payload), &payload); err != nil {
		t.Fatalf("expected normalized rebind payload, got %v", err)
	}
	if payload.Region != "us-west-2" {
		t.Fatalf("expected tracked region us-west-2, got %q", payload.Region)
	}
	if payload.InstanceID != "i-tracked" {
		t.Fatalf("expected tracked instance id i-tracked, got %q", payload.InstanceID)
	}
	if payload.PrivateIP != "172.31.20.10" {
		t.Fatalf("expected discovered private ip 172.31.20.10, got %q", payload.PrivateIP)
	}
}

func TestPrepareAWSRebindExecutionPlanLoadsCurrentAddressFromWatchClient(t *testing.T) {
	configureRunnerSQLiteDB(t)
	db := dbcore.GetDBInstance()
	if err := db.AutoMigrate(&models.Client{}); err != nil {
		t.Fatalf("failed to migrate client schema: %v", err)
	}
	if err := db.Create(&models.Client{
		UUID:   "client-1",
		Token:  "token-1",
		UserID: "user-1",
		Name:   "edge-1",
		IPv4:   "203.0.113.10",
	}).Error; err != nil {
		t.Fatalf("failed to seed current client: %v", err)
	}

	originalLoadCredential := awsFailoverLoadCredential
	originalResolveCurrent := awsFailoverResolveCurrentInstanceByAddress
	defer func() {
		awsFailoverLoadCredential = originalLoadCredential
		awsFailoverResolveCurrentInstanceByAddress = originalResolveCurrent
	}()

	addressLookupCalled := false
	awsFailoverLoadCredential = func(userUUID, entryID string) (*awscloud.Addition, *awscloud.CredentialRecord, error) {
		return &awscloud.Addition{ActiveRegion: "us-east-1"}, &awscloud.CredentialRecord{
			ID:            entryID,
			Name:          "cred-1",
			DefaultRegion: "us-east-1",
		}, nil
	}
	awsFailoverResolveCurrentInstanceByAddress = func(ctx context.Context, task models.FailoverTask, plan models.FailoverPlan, payload awsRebindPayload) (*currentInstanceCleanup, error) {
		addressLookupCalled = true
		if task.CurrentAddress != "203.0.113.10" {
			t.Fatalf("expected task current address to be refreshed from watch client, got %q", task.CurrentAddress)
		}
		return &currentInstanceCleanup{
			Ref: map[string]interface{}{
				"provider":          "aws",
				"service":           "ec2",
				"provider_entry_id": "cred-1",
				"region":            "us-east-1",
				"instance_id":       "i-1234567890",
			},
			Addresses: map[string]interface{}{
				"private_ip": "172.31.10.24",
			},
		}, nil
	}

	executionPlan, mode, reason, err := prepareAWSRebindExecutionPlan(context.Background(), models.FailoverTask{
		UserID:          "user-1",
		WatchClientUUID: "client-1",
	}, models.FailoverPlan{
		Provider:        "aws",
		ProviderEntryID: "cred-1",
		ActionType:      models.FailoverActionRebindPublicIP,
		Payload:         `{"service":"ec2","region":"us-east-1","image_id":"ami-123","instance_type":"t3.micro"}`,
	})
	if err != nil {
		t.Fatalf("expected watch client address refresh to keep rebind path, got %v", err)
	}
	if mode != "rebind_existing_instance" {
		t.Fatalf("expected rebind_existing_instance mode, got %q", mode)
	}
	if !strings.Contains(reason, "current IP") {
		t.Fatalf("expected current IP reason after address refresh, got %q", reason)
	}
	if !addressLookupCalled {
		t.Fatal("expected current IP lookup to run after refreshing address from watch client")
	}

	var payload awsRebindPayload
	if err := json.Unmarshal([]byte(executionPlan.Payload), &payload); err != nil {
		t.Fatalf("expected normalized rebind payload, got %v", err)
	}
	if payload.InstanceID != "i-1234567890" {
		t.Fatalf("expected discovered instance id i-1234567890, got %q", payload.InstanceID)
	}
	if payload.PrivateIP != "172.31.10.24" {
		t.Fatalf("expected discovered private ip 172.31.10.24, got %q", payload.PrivateIP)
	}
}

func TestPrepareAWSRebindExecutionPlanUsesTrackedLightsailRefWhenAddressLookupWouldMiss(t *testing.T) {
	originalLoadCredential := awsFailoverLoadCredential
	originalResolveCurrent := awsFailoverResolveCurrentInstanceByAddress
	originalGetLightsailDetail := awsFailoverGetLightsailDetail
	defer func() {
		awsFailoverLoadCredential = originalLoadCredential
		awsFailoverResolveCurrentInstanceByAddress = originalResolveCurrent
		awsFailoverGetLightsailDetail = originalGetLightsailDetail
	}()

	addressLookupCalled := false
	awsFailoverLoadCredential = func(userUUID, entryID string) (*awscloud.Addition, *awscloud.CredentialRecord, error) {
		return &awscloud.Addition{ActiveRegion: "us-east-1"}, &awscloud.CredentialRecord{
			ID:            entryID,
			Name:          "cred-1",
			DefaultRegion: "us-east-1",
		}, nil
	}
	awsFailoverResolveCurrentInstanceByAddress = func(ctx context.Context, task models.FailoverTask, plan models.FailoverPlan, payload awsRebindPayload) (*currentInstanceCleanup, error) {
		addressLookupCalled = true
		return nil, nil
	}
	awsFailoverGetLightsailDetail = func(ctx context.Context, credential *awscloud.CredentialRecord, region, instanceName string) (*awscloud.LightsailInstanceDetail, error) {
		if region != "us-west-2" {
			t.Fatalf("expected tracked region us-west-2, got %q", region)
		}
		if instanceName != "ls-tracked" {
			t.Fatalf("expected tracked instance name ls-tracked, got %q", instanceName)
		}
		return &awscloud.LightsailInstanceDetail{
			Instance: awscloud.LightsailInstance{
				Name:  "ls-tracked",
				State: "running",
			},
		}, nil
	}

	executionPlan, mode, reason, err := prepareAWSRebindExecutionPlan(context.Background(), models.FailoverTask{
		UserID:             "user-1",
		CurrentAddress:     "198.51.100.30",
		CurrentInstanceRef: `{"provider":"aws","service":"lightsail","provider_entry_id":"cred-1","region":"us-west-2","instance_name":"ls-tracked"}`,
	}, models.FailoverPlan{
		Provider:        "aws",
		ProviderEntryID: "cred-1",
		ActionType:      models.FailoverActionRebindPublicIP,
		Payload:         `{"service":"lightsail","region":"us-east-1","availability_zone":"us-east-1a","blueprint_id":"debian_12","bundle_id":"nano_3_0"}`,
	})
	if err != nil {
		t.Fatalf("expected tracked Lightsail ref to stay on rebind path, got %v", err)
	}
	if mode != "rebind_existing_instance" {
		t.Fatalf("expected rebind_existing_instance mode, got %q", mode)
	}
	if !strings.Contains(reason, "Lightsail instance") {
		t.Fatalf("expected tracked Lightsail reason, got %q", reason)
	}
	if addressLookupCalled {
		t.Fatal("expected tracked Lightsail ref to avoid address lookup fallback")
	}

	var payload awsRebindPayload
	if err := json.Unmarshal([]byte(executionPlan.Payload), &payload); err != nil {
		t.Fatalf("expected normalized Lightsail rebind payload, got %v", err)
	}
	if payload.Region != "us-west-2" {
		t.Fatalf("expected tracked region us-west-2, got %q", payload.Region)
	}
	if payload.InstanceName != "ls-tracked" {
		t.Fatalf("expected tracked instance name ls-tracked, got %q", payload.InstanceName)
	}
}

func TestPrepareAWSRebindExecutionPlanLoadsCurrentAddressFromWatchClientForLightsail(t *testing.T) {
	configureRunnerSQLiteDB(t)
	db := dbcore.GetDBInstance()
	if err := db.AutoMigrate(&models.Client{}); err != nil {
		t.Fatalf("failed to migrate client schema: %v", err)
	}
	if err := db.Create(&models.Client{
		UUID:   "client-ls-1",
		Token:  "token-ls-1",
		UserID: "user-1",
		Name:   "edge-ls-1",
		IPv4:   "203.0.113.20",
	}).Error; err != nil {
		t.Fatalf("failed to seed current Lightsail client: %v", err)
	}

	originalLoadCredential := awsFailoverLoadCredential
	originalResolveCurrent := awsFailoverResolveCurrentInstanceByAddress
	defer func() {
		awsFailoverLoadCredential = originalLoadCredential
		awsFailoverResolveCurrentInstanceByAddress = originalResolveCurrent
	}()

	addressLookupCalled := false
	awsFailoverLoadCredential = func(userUUID, entryID string) (*awscloud.Addition, *awscloud.CredentialRecord, error) {
		return &awscloud.Addition{ActiveRegion: "us-east-1"}, &awscloud.CredentialRecord{
			ID:            entryID,
			Name:          "cred-1",
			DefaultRegion: "us-east-1",
		}, nil
	}
	awsFailoverResolveCurrentInstanceByAddress = func(ctx context.Context, task models.FailoverTask, plan models.FailoverPlan, payload awsRebindPayload) (*currentInstanceCleanup, error) {
		addressLookupCalled = true
		if task.CurrentAddress != "203.0.113.20" {
			t.Fatalf("expected task current address to be refreshed from watch client, got %q", task.CurrentAddress)
		}
		return &currentInstanceCleanup{
			Ref: map[string]interface{}{
				"provider":          "aws",
				"service":           "lightsail",
				"provider_entry_id": "cred-1",
				"region":            "us-east-1",
				"instance_name":     "ls-current",
			},
		}, nil
	}

	executionPlan, mode, reason, err := prepareAWSRebindExecutionPlan(context.Background(), models.FailoverTask{
		UserID:          "user-1",
		WatchClientUUID: "client-ls-1",
	}, models.FailoverPlan{
		Provider:        "aws",
		ProviderEntryID: "cred-1",
		ActionType:      models.FailoverActionRebindPublicIP,
		Payload:         `{"service":"lightsail","region":"us-east-1","availability_zone":"us-east-1a","blueprint_id":"debian_12","bundle_id":"nano_3_0"}`,
	})
	if err != nil {
		t.Fatalf("expected watch client address refresh to keep Lightsail on rebind path, got %v", err)
	}
	if mode != "rebind_existing_instance" {
		t.Fatalf("expected rebind_existing_instance mode, got %q", mode)
	}
	if !strings.Contains(reason, "current IP") {
		t.Fatalf("expected current IP reason after Lightsail address refresh, got %q", reason)
	}
	if !addressLookupCalled {
		t.Fatal("expected current IP lookup to run after refreshing Lightsail address from watch client")
	}

	var payload awsRebindPayload
	if err := json.Unmarshal([]byte(executionPlan.Payload), &payload); err != nil {
		t.Fatalf("expected normalized Lightsail rebind payload, got %v", err)
	}
	if payload.InstanceName != "ls-current" {
		t.Fatalf("expected discovered instance name ls-current, got %q", payload.InstanceName)
	}
}

func TestSameAddressMatchesCIDRAndPlainIP(t *testing.T) {
	if !sameAddress("2001:db8::10", "2001:db8::10/64") {
		t.Fatal("expected plain ipv6 and cidr ipv6 to match")
	}
	if !sameAddress("203.0.113.8", "203.0.113.8/32") {
		t.Fatal("expected plain ipv4 and cidr ipv4 to match")
	}
}

func TestSamePublicIPv4AddressMatchesPublicIPv4(t *testing.T) {
	if !samePublicIPv4Address("203.0.113.8", "203.0.113.8") {
		t.Fatal("expected public ipv4 to match")
	}
}

func TestSamePublicIPv4AddressRejectsPrivateIPv4AndIPv6(t *testing.T) {
	if samePublicIPv4Address("172.31.10.24", "203.0.113.8") {
		t.Fatal("expected private ipv4 target not to match a different public ipv4")
	}
	if samePublicIPv4Address("2001:db8::10", "203.0.113.8") {
		t.Fatal("expected ipv6 target not to match public ipv4 only matcher")
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
	if got := providerEntryMaxAttempts(task, rebindPlan); got != 3 {
		t.Fatalf("expected aws smart plan retries to use task limit 3, got %d", got)
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
	if got := provisionPlanFailureFallbackLimit(task, rebindPlan); got != 4 {
		t.Fatalf("expected aws smart plan fallback limit to use task value 4, got %d", got)
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

func TestEffectiveTaskDeleteStrategyTreatsAWSPlansAsProvisionCapable(t *testing.T) {
	task := models.FailoverTask{
		DeleteStrategy: models.FailoverDeleteStrategyDeleteAfterSuccess,
		Plans: []models.FailoverPlan{
			{
				Enabled:    true,
				Provider:   "aws",
				ActionType: models.FailoverActionRebindPublicIP,
			},
		},
	}

	if got := effectiveTaskDeleteStrategy(task); got != models.FailoverDeleteStrategyDeleteAfterSuccess {
		t.Fatalf("expected aws smart plan to keep delete_after_success semantics, got %q", got)
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

func TestFinalizePlanPersistsOutletTrackingWhenDNSFailsAfterRebind(t *testing.T) {
	prepareRetryTestDB(t)

	db := dbcore.GetDBInstance()
	now := models.FromTime(time.Now())
	task := &models.FailoverTask{
		UserID:             "runner-dns-failure-user",
		Name:               "runner-dns-failure-task",
		Enabled:            true,
		WatchClientUUID:    "client-old",
		CurrentAddress:     "198.51.100.10",
		CurrentInstanceRef: `{"provider":"aws","service":"ec2","provider_entry_id":"cred-old","region":"us-east-1","instance_id":"i-old"}`,
		DNSProvider:        aliyunProviderName,
		DNSEntryID:         "dns-entry-1",
		DNSPayload:         `{"domain_name":"example.com","rr":"@"}`,
		CreatedAt:          now,
		UpdatedAt:          now,
	}
	if err := db.Create(task).Error; err != nil {
		t.Fatalf("failed to create finalize-plan task: %v", err)
	}

	execution := &models.FailoverExecution{
		TaskID:          task.ID,
		Status:          models.FailoverExecutionStatusRebindingIP,
		WatchClientUUID: task.WatchClientUUID,
		StartedAt:       now,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	if err := db.Create(execution).Error; err != nil {
		t.Fatalf("failed to create finalize-plan execution: %v", err)
	}

	originalApply := failoverDNSApplyFunc
	originalVerify := failoverDNSVerifyFunc
	failoverDNSApplyFunc = func(ctx context.Context, userUUID, providerName, entryID, payloadJSON, ipv4, ipv6 string) (*dnsUpdateResult, error) {
		return nil, errors.New("aliyun dns request failed: DomainRecordDuplicate")
	}
	failoverDNSVerifyFunc = func(ctx context.Context, userUUID, providerName, entryID, payloadJSON, ipv4, ipv6 string) (*dnsVerificationResult, error) {
		t.Fatal("did not expect dns verification after apply failure")
		return nil, nil
	}
	t.Cleanup(func() {
		failoverDNSApplyFunc = originalApply
		failoverDNSVerifyFunc = originalVerify
	})

	runner := &executionRunner{
		task:      *task,
		execution: execution,
		ctx:       context.Background(),
	}
	err := runner.finalizePlan(models.FailoverPlan{
		ActionType: models.FailoverActionRebindPublicIP,
	}, &actionOutcome{
		IPv4:             "203.0.113.20",
		TargetClientUUID: "client-new",
		NewClientUUID:    "client-new",
		OldInstanceRef: map[string]interface{}{
			"provider":          "aws",
			"service":           "ec2",
			"provider_entry_id": "cred-new",
			"region":            "us-west-2",
			"instance_id":       "i-rebound",
		},
		NewAddresses: map[string]interface{}{
			"public_ip": "203.0.113.20",
		},
	})
	if err == nil || !strings.Contains(err.Error(), "DomainRecordDuplicate") {
		t.Fatalf("expected dns apply failure, got %v", err)
	}

	var reloadedTask models.FailoverTask
	if err := db.First(&reloadedTask, task.ID).Error; err != nil {
		t.Fatalf("failed to reload finalize-plan task: %v", err)
	}
	if reloadedTask.CurrentAddress != "203.0.113.20" {
		t.Fatalf("expected task current_address to track rebound ip, got %q", reloadedTask.CurrentAddress)
	}
	if reloadedTask.WatchClientUUID != "client-new" {
		t.Fatalf("expected task watch_client_uuid to update, got %q", reloadedTask.WatchClientUUID)
	}
	currentRef := parseJSONMap(reloadedTask.CurrentInstanceRef)
	if stringMapValue(currentRef, "instance_id") != "i-rebound" {
		t.Fatalf("expected task current_instance_ref to keep rebound instance, got %#v", currentRef)
	}
	if runner.task.CurrentAddress != "203.0.113.20" {
		t.Fatalf("expected runner task current_address to update in-memory, got %q", runner.task.CurrentAddress)
	}
}

func TestFinalizePlanKeepsTaskTrackingWhenDNSFailsAndOutcomeRollsBack(t *testing.T) {
	prepareRetryTestDB(t)

	db := dbcore.GetDBInstance()
	now := models.FromTime(time.Now())
	task := &models.FailoverTask{
		UserID:             "runner-dns-rollback-user",
		Name:               "runner-dns-rollback-task",
		Enabled:            true,
		WatchClientUUID:    "client-old",
		CurrentAddress:     "198.51.100.11",
		CurrentInstanceRef: `{"provider":"aws","service":"ec2","provider_entry_id":"cred-old","region":"us-east-1","instance_id":"i-old"}`,
		DNSProvider:        aliyunProviderName,
		DNSEntryID:         "dns-entry-1",
		DNSPayload:         `{"domain_name":"example.com","rr":"@"}`,
		CreatedAt:          now,
		UpdatedAt:          now,
	}
	if err := db.Create(task).Error; err != nil {
		t.Fatalf("failed to create rollback finalize-plan task: %v", err)
	}

	execution := &models.FailoverExecution{
		TaskID:          task.ID,
		Status:          models.FailoverExecutionStatusProvisioning,
		WatchClientUUID: task.WatchClientUUID,
		StartedAt:       now,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	if err := db.Create(execution).Error; err != nil {
		t.Fatalf("failed to create rollback finalize-plan execution: %v", err)
	}

	originalApply := failoverDNSApplyFunc
	originalVerify := failoverDNSVerifyFunc
	failoverDNSApplyFunc = func(ctx context.Context, userUUID, providerName, entryID, payloadJSON, ipv4, ipv6 string) (*dnsUpdateResult, error) {
		return nil, errors.New("dns apply failed")
	}
	failoverDNSVerifyFunc = func(ctx context.Context, userUUID, providerName, entryID, payloadJSON, ipv4, ipv6 string) (*dnsVerificationResult, error) {
		t.Fatal("did not expect dns verification after apply failure")
		return nil, nil
	}
	t.Cleanup(func() {
		failoverDNSApplyFunc = originalApply
		failoverDNSVerifyFunc = originalVerify
	})

	rollbackCalled := false
	runner := &executionRunner{
		task:      *task,
		execution: execution,
		ctx:       context.Background(),
	}
	err := runner.finalizePlan(models.FailoverPlan{
		ActionType: models.FailoverActionProvisionInstance,
	}, &actionOutcome{
		IPv4: "203.0.113.21",
		NewInstanceRef: map[string]interface{}{
			"provider":          "aws",
			"service":           "ec2",
			"provider_entry_id": "cred-new",
			"region":            "us-west-2",
			"instance_id":       "i-new",
		},
		RollbackLabel: "terminate failed instance",
		Rollback: func(ctx context.Context) error {
			rollbackCalled = true
			return nil
		},
	})
	if err == nil || !strings.Contains(err.Error(), "dns apply failed") {
		t.Fatalf("expected dns apply failure, got %v", err)
	}
	if !rollbackCalled {
		t.Fatal("expected rollback to run for rollbackable outcome")
	}

	var reloadedTask models.FailoverTask
	if err := db.First(&reloadedTask, task.ID).Error; err != nil {
		t.Fatalf("failed to reload rollback finalize-plan task: %v", err)
	}
	if reloadedTask.CurrentAddress != "198.51.100.11" {
		t.Fatalf("expected task current_address to remain old after rollback, got %q", reloadedTask.CurrentAddress)
	}
	if currentRef := parseJSONMap(reloadedTask.CurrentInstanceRef); stringMapValue(currentRef, "instance_id") != "i-old" {
		t.Fatalf("expected task current_instance_ref to remain old after rollback, got %#v", currentRef)
	}
	if runner.task.CurrentAddress != "198.51.100.11" {
		t.Fatalf("expected runner task current_address to remain old in-memory, got %q", runner.task.CurrentAddress)
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

func TestPersistDigitalOceanRootPasswordReloadsLatestAdditionState(t *testing.T) {
	configureRunnerSQLiteDB(t)
	t.Setenv(digitalocean.DropletPasswordVaultKeyEnv, "failover-runner-test-secret")

	const userUUID = "user-do-password-persist"
	baseAddition := &digitalocean.Addition{
		ActiveTokenID: "token-1",
		Tokens: []digitalocean.TokenRecord{
			{
				ID:    "token-1",
				Name:  "Token 1",
				Token: "dop_v1_token_1",
			},
		},
	}
	if err := saveDigitalOceanAddition(userUUID, baseAddition); err != nil {
		t.Fatalf("failed to seed DigitalOcean provider config: %v", err)
	}

	staleAddition, staleToken, err := loadDigitalOceanToken(userUUID, "token-1")
	if err != nil {
		t.Fatalf("failed to load stale DigitalOcean token: %v", err)
	}

	latestAddition, latestToken, err := loadDigitalOceanToken(userUUID, "token-1")
	if err != nil {
		t.Fatalf("failed to reload DigitalOcean token: %v", err)
	}
	if err := latestToken.SaveDropletPassword(1001, "web-old", "custom", "Old!Password123", time.Now().UTC()); err != nil {
		t.Fatalf("failed to seed existing droplet password: %v", err)
	}
	if err := saveDigitalOceanAddition(userUUID, latestAddition); err != nil {
		t.Fatalf("failed to save existing droplet credential: %v", err)
	}

	if err := persistDigitalOceanRootPassword(
		userUUID,
		staleAddition,
		staleToken,
		2002,
		"web-new",
		"custom",
		"New!Password456",
	); err != nil {
		t.Fatalf("persistDigitalOceanRootPassword returned error: %v", err)
	}

	storedAddition, storedToken, err := loadDigitalOceanToken(userUUID, "token-1")
	if err != nil {
		t.Fatalf("failed to load updated DigitalOcean addition: %v", err)
	}
	if !storedAddition.HasSavedDropletPassword(1001) {
		t.Fatal("expected existing DigitalOcean droplet credential to be preserved")
	}
	if !storedAddition.HasSavedDropletPassword(2002) {
		t.Fatal("expected new DigitalOcean droplet credential to be saved")
	}

	passwordView, err := storedToken.RevealDropletPassword(2002)
	if err != nil {
		t.Fatalf("failed to reveal saved DigitalOcean droplet password: %v", err)
	}
	if passwordView.RootPassword != "New!Password456" {
		t.Fatalf("unexpected DigitalOcean root password %q", passwordView.RootPassword)
	}
}

func TestRemoveSavedDigitalOceanRootPasswordReloadsLatestAdditionState(t *testing.T) {
	configureRunnerSQLiteDB(t)
	t.Setenv(digitalocean.DropletPasswordVaultKeyEnv, "failover-runner-test-secret")

	const userUUID = "user-do-password-remove"
	baseAddition := &digitalocean.Addition{
		ActiveTokenID: "token-1",
		Tokens: []digitalocean.TokenRecord{
			{
				ID:    "token-1",
				Name:  "Token 1",
				Token: "dop_v1_token_1",
			},
		},
	}
	if err := saveDigitalOceanAddition(userUUID, baseAddition); err != nil {
		t.Fatalf("failed to seed DigitalOcean provider config: %v", err)
	}

	staleAddition, staleToken, err := loadDigitalOceanToken(userUUID, "token-1")
	if err != nil {
		t.Fatalf("failed to load stale DigitalOcean token: %v", err)
	}

	latestAddition, latestToken, err := loadDigitalOceanToken(userUUID, "token-1")
	if err != nil {
		t.Fatalf("failed to reload DigitalOcean token: %v", err)
	}
	if err := latestToken.SaveDropletPassword(1001, "web-old", "custom", "Old!Password123", time.Now().UTC()); err != nil {
		t.Fatalf("failed to seed DigitalOcean droplet password 1001: %v", err)
	}
	if err := latestToken.SaveDropletPassword(2002, "web-keep", "custom", "Keep!Password456", time.Now().UTC()); err != nil {
		t.Fatalf("failed to seed DigitalOcean droplet password 2002: %v", err)
	}
	if err := saveDigitalOceanAddition(userUUID, latestAddition); err != nil {
		t.Fatalf("failed to save seeded DigitalOcean credentials: %v", err)
	}

	removeSavedDigitalOceanRootPassword(userUUID, staleAddition, staleToken, 1001)

	storedAddition, _, err := loadDigitalOceanToken(userUUID, "token-1")
	if err != nil {
		t.Fatalf("failed to load updated DigitalOcean addition: %v", err)
	}
	if storedAddition.HasSavedDropletPassword(1001) {
		t.Fatal("expected targeted DigitalOcean droplet credential to be removed")
	}
	if !storedAddition.HasSavedDropletPassword(2002) {
		t.Fatal("expected non-target DigitalOcean droplet credential to be preserved")
	}
}

func TestPersistLinodeRootPasswordReloadsLatestAdditionState(t *testing.T) {
	configureRunnerSQLiteDB(t)
	t.Setenv(linodecloud.RootPasswordVaultKeyEnv, "failover-runner-test-secret")

	const userUUID = "user-linode-password-persist"
	baseAddition := &linodecloud.Addition{
		ActiveTokenID: "token-1",
		Tokens: []linodecloud.TokenRecord{
			{
				ID:    "token-1",
				Name:  "Token 1",
				Token: "linode_token_1",
			},
		},
	}
	if err := saveLinodeAddition(userUUID, baseAddition); err != nil {
		t.Fatalf("failed to seed Linode provider config: %v", err)
	}

	staleAddition, staleToken, err := loadLinodeToken(userUUID, "token-1")
	if err != nil {
		t.Fatalf("failed to load stale Linode token: %v", err)
	}

	latestAddition, latestToken, err := loadLinodeToken(userUUID, "token-1")
	if err != nil {
		t.Fatalf("failed to reload Linode token: %v", err)
	}
	if err := latestToken.SaveInstancePassword(1001, "web-old", "custom", "Old!Password123", time.Now().UTC()); err != nil {
		t.Fatalf("failed to seed existing Linode instance password: %v", err)
	}
	if err := saveLinodeAddition(userUUID, latestAddition); err != nil {
		t.Fatalf("failed to save existing Linode credential: %v", err)
	}

	if err := persistLinodeRootPassword(
		userUUID,
		staleAddition,
		staleToken,
		2002,
		"web-new",
		"custom",
		"New!Password456",
	); err != nil {
		t.Fatalf("persistLinodeRootPassword returned error: %v", err)
	}

	storedAddition, storedToken, err := loadLinodeToken(userUUID, "token-1")
	if err != nil {
		t.Fatalf("failed to load updated Linode addition: %v", err)
	}
	if !storedAddition.HasSavedInstancePassword(1001) {
		t.Fatal("expected existing Linode instance credential to be preserved")
	}
	if !storedAddition.HasSavedInstancePassword(2002) {
		t.Fatal("expected new Linode instance credential to be saved")
	}

	passwordView, err := storedToken.RevealInstancePassword(2002)
	if err != nil {
		t.Fatalf("failed to reveal saved Linode instance password: %v", err)
	}
	if passwordView.RootPassword != "New!Password456" {
		t.Fatalf("unexpected Linode root password %q", passwordView.RootPassword)
	}
}

func TestRemoveSavedLinodeRootPasswordReloadsLatestAdditionState(t *testing.T) {
	configureRunnerSQLiteDB(t)
	t.Setenv(linodecloud.RootPasswordVaultKeyEnv, "failover-runner-test-secret")

	const userUUID = "user-linode-password-remove"
	baseAddition := &linodecloud.Addition{
		ActiveTokenID: "token-1",
		Tokens: []linodecloud.TokenRecord{
			{
				ID:    "token-1",
				Name:  "Token 1",
				Token: "linode_token_1",
			},
		},
	}
	if err := saveLinodeAddition(userUUID, baseAddition); err != nil {
		t.Fatalf("failed to seed Linode provider config: %v", err)
	}

	staleAddition, staleToken, err := loadLinodeToken(userUUID, "token-1")
	if err != nil {
		t.Fatalf("failed to load stale Linode token: %v", err)
	}

	latestAddition, latestToken, err := loadLinodeToken(userUUID, "token-1")
	if err != nil {
		t.Fatalf("failed to reload Linode token: %v", err)
	}
	if err := latestToken.SaveInstancePassword(1001, "web-old", "custom", "Old!Password123", time.Now().UTC()); err != nil {
		t.Fatalf("failed to seed Linode instance password 1001: %v", err)
	}
	if err := latestToken.SaveInstancePassword(2002, "web-keep", "custom", "Keep!Password456", time.Now().UTC()); err != nil {
		t.Fatalf("failed to seed Linode instance password 2002: %v", err)
	}
	if err := saveLinodeAddition(userUUID, latestAddition); err != nil {
		t.Fatalf("failed to save seeded Linode credentials: %v", err)
	}

	removeSavedLinodeRootPassword(userUUID, staleAddition, staleToken, 1001)

	storedAddition, _, err := loadLinodeToken(userUUID, "token-1")
	if err != nil {
		t.Fatalf("failed to load updated Linode addition: %v", err)
	}
	if storedAddition.HasSavedInstancePassword(1001) {
		t.Fatal("expected targeted Linode instance credential to be removed")
	}
	if !storedAddition.HasSavedInstancePassword(2002) {
		t.Fatal("expected non-target Linode instance credential to be preserved")
	}
}

func TestPersistAWSRootPasswordReloadsLatestAdditionState(t *testing.T) {
	configureRunnerSQLiteDB(t)
	t.Setenv(awscloud.RootPasswordVaultKeyEnv, "failover-runner-test-secret")

	const (
		userUUID = "user-aws-password-persist"
		region   = "us-east-1"
	)
	baseAddition := &awscloud.Addition{
		ActiveCredentialID: "credential-1",
		Credentials: []awscloud.CredentialRecord{
			{
				ID:              "credential-1",
				Name:            "Credential 1",
				AccessKeyID:     "AKIAEXAMPLE1",
				SecretAccessKey: "secret-1",
				DefaultRegion:   region,
			},
		},
	}
	if err := saveAWSAddition(userUUID, baseAddition); err != nil {
		t.Fatalf("failed to seed AWS provider config: %v", err)
	}

	staleAddition, staleCredential, err := loadAWSCredential(userUUID, "credential-1")
	if err != nil {
		t.Fatalf("failed to load stale AWS credential: %v", err)
	}

	latestAddition, latestCredential, err := loadAWSCredential(userUUID, "credential-1")
	if err != nil {
		t.Fatalf("failed to reload AWS credential: %v", err)
	}
	if err := latestCredential.SaveResourcePassword(
		"ec2",
		buildAWSResourceCredentialID(region, "i-old"),
		"web-old",
		"custom",
		"Old!Password123",
		time.Now().UTC(),
	); err != nil {
		t.Fatalf("failed to seed existing AWS resource password: %v", err)
	}
	if err := saveAWSAddition(userUUID, latestAddition); err != nil {
		t.Fatalf("failed to save existing AWS credential: %v", err)
	}

	if err := persistAWSRootPassword(
		userUUID,
		staleAddition,
		staleCredential,
		"ec2",
		region,
		"i-new",
		"web-new",
		"custom",
		"New!Password456",
	); err != nil {
		t.Fatalf("persistAWSRootPassword returned error: %v", err)
	}

	storedAddition, storedCredential, err := loadAWSCredential(userUUID, "credential-1")
	if err != nil {
		t.Fatalf("failed to load updated AWS addition: %v", err)
	}
	if !storedAddition.HasSavedResourcePassword("ec2", buildAWSResourceCredentialID(region, "i-old")) {
		t.Fatal("expected existing AWS resource credential to be preserved")
	}
	if !storedAddition.HasSavedResourcePassword("ec2", buildAWSResourceCredentialID(region, "i-new")) {
		t.Fatal("expected new AWS resource credential to be saved")
	}

	passwordView, err := storedCredential.RevealResourcePassword("ec2", buildAWSResourceCredentialID(region, "i-new"))
	if err != nil {
		t.Fatalf("failed to reveal saved AWS resource password: %v", err)
	}
	if passwordView.RootPassword != "New!Password456" {
		t.Fatalf("unexpected AWS root password %q", passwordView.RootPassword)
	}
}

func TestRemoveSavedAWSRootPasswordReloadsLatestAdditionState(t *testing.T) {
	configureRunnerSQLiteDB(t)
	t.Setenv(awscloud.RootPasswordVaultKeyEnv, "failover-runner-test-secret")

	const (
		userUUID = "user-aws-password-remove"
		region   = "us-east-1"
	)
	baseAddition := &awscloud.Addition{
		ActiveCredentialID: "credential-1",
		Credentials: []awscloud.CredentialRecord{
			{
				ID:              "credential-1",
				Name:            "Credential 1",
				AccessKeyID:     "AKIAEXAMPLE1",
				SecretAccessKey: "secret-1",
				DefaultRegion:   region,
			},
		},
	}
	if err := saveAWSAddition(userUUID, baseAddition); err != nil {
		t.Fatalf("failed to seed AWS provider config: %v", err)
	}

	staleAddition, staleCredential, err := loadAWSCredential(userUUID, "credential-1")
	if err != nil {
		t.Fatalf("failed to load stale AWS credential: %v", err)
	}

	latestAddition, latestCredential, err := loadAWSCredential(userUUID, "credential-1")
	if err != nil {
		t.Fatalf("failed to reload AWS credential: %v", err)
	}
	if err := latestCredential.SaveResourcePassword(
		"ec2",
		buildAWSResourceCredentialID(region, "i-old"),
		"web-old",
		"custom",
		"Old!Password123",
		time.Now().UTC(),
	); err != nil {
		t.Fatalf("failed to seed AWS resource password i-old: %v", err)
	}
	if err := latestCredential.SaveResourcePassword(
		"ec2",
		buildAWSResourceCredentialID(region, "i-keep"),
		"web-keep",
		"custom",
		"Keep!Password456",
		time.Now().UTC(),
	); err != nil {
		t.Fatalf("failed to seed AWS resource password i-keep: %v", err)
	}
	if err := saveAWSAddition(userUUID, latestAddition); err != nil {
		t.Fatalf("failed to save seeded AWS credentials: %v", err)
	}

	removeSavedAWSRootPassword(userUUID, staleAddition, staleCredential, "ec2", region, "i-old")

	storedAddition, _, err := loadAWSCredential(userUUID, "credential-1")
	if err != nil {
		t.Fatalf("failed to load updated AWS addition: %v", err)
	}
	if storedAddition.HasSavedResourcePassword("ec2", buildAWSResourceCredentialID(region, "i-old")) {
		t.Fatal("expected targeted AWS resource credential to be removed")
	}
	if !storedAddition.HasSavedResourcePassword("ec2", buildAWSResourceCredentialID(region, "i-keep")) {
		t.Fatal("expected non-target AWS resource credential to be preserved")
	}
}

func TestAWSRebindProvisionPayloadCarriesRootPasswordFields(t *testing.T) {
	provisionPayload := awsRebindProvisionPayload(awsRebindPayload{
		Service:          "ec2",
		Region:           "us-east-1",
		Name:             "failover-ec2",
		ImageID:          "ami-123",
		InstanceType:     "t3.micro",
		RootPasswordMode: "custom",
		RootPassword:     " Passw0rd! ",
	})

	if provisionPayload.RootPasswordMode != "custom" {
		t.Fatalf("expected root_password_mode to be preserved, got %q", provisionPayload.RootPasswordMode)
	}
	if provisionPayload.RootPassword != "Passw0rd!" {
		t.Fatalf("expected root_password to be trimmed and preserved, got %q", provisionPayload.RootPassword)
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

func TestQueuePendingRollbackCleanupPersistsFailedNewInstance(t *testing.T) {
	resetRunnerPendingCleanupTable(t)
	db := dbcore.GetDBInstance()

	runner := &executionRunner{
		task: models.FailoverTask{
			ID:     11,
			UserID: "user-a",
		},
		execution: &models.FailoverExecution{ID: 22},
	}
	pendingCleanup, err := runner.queuePendingRollbackCleanup(&actionOutcome{
		NewInstanceRef: map[string]interface{}{
			"provider":          "digitalocean",
			"provider_entry_id": "token-1",
			"droplet_id":        123,
		},
		RollbackLabel: "delete digitalocean droplet 123",
	}, errors.New("delete failed"))
	if err != nil {
		t.Fatalf("expected pending cleanup to be persisted, got %v", err)
	}
	if pendingCleanup == nil {
		t.Fatal("expected pending cleanup row to be returned")
	}

	var stored models.FailoverPendingCleanup
	if err := db.First(&stored, pendingCleanup.ID).Error; err != nil {
		t.Fatalf("failed to reload pending cleanup: %v", err)
	}
	if stored.Provider != "digitalocean" || stored.ResourceType != "droplet" || stored.ResourceID != "123" {
		t.Fatalf("unexpected stored pending cleanup identity: %+v", stored)
	}
	if stored.AttemptCount != 1 {
		t.Fatalf("expected initial attempt count 1, got %d", stored.AttemptCount)
	}
	if stored.Status != models.FailoverPendingCleanupStatusPending {
		t.Fatalf("expected pending status, got %q", stored.Status)
	}
}

func TestRunPendingRollbackCleanupRetriesMarksCleanupSucceeded(t *testing.T) {
	resetRunnerPendingCleanupTable(t)
	db := dbcore.GetDBInstance()

	entry := models.FailoverPendingCleanup{
		UserID:          "user-a",
		TaskID:          11,
		ExecutionID:     22,
		Provider:        "digitalocean",
		ProviderEntryID: "token-1",
		ResourceType:    "droplet",
		ResourceID:      "123",
		InstanceRef:     `{"provider":"digitalocean","provider_entry_id":"token-1","droplet_id":123}`,
		CleanupLabel:    "delete digitalocean droplet 123",
		Status:          models.FailoverPendingCleanupStatusPending,
	}
	if err := db.Create(&entry).Error; err != nil {
		t.Fatalf("failed to seed pending cleanup: %v", err)
	}

	cleanupCalled := false
	originalResolve := pendingRollbackCleanupResolveFunc
	pendingRollbackCleanupResolveFunc = func(ctx context.Context, userUUID string, ref map[string]interface{}) (*currentInstanceCleanup, error) {
		return &currentInstanceCleanup{
			Ref: ref,
			Cleanup: func(ctx context.Context) error {
				cleanupCalled = true
				return nil
			},
		}, nil
	}
	defer func() {
		pendingRollbackCleanupResolveFunc = originalResolve
	}()

	if err := runPendingRollbackCleanupRetries(); err != nil {
		t.Fatalf("expected pending cleanup retries to succeed, got %v", err)
	}
	if !cleanupCalled {
		t.Fatal("expected cleanup callback to run")
	}

	var stored models.FailoverPendingCleanup
	if err := db.First(&stored, entry.ID).Error; err != nil {
		t.Fatalf("failed to reload pending cleanup row: %v", err)
	}
	if stored.Status != models.FailoverPendingCleanupStatusSucceeded {
		t.Fatalf("expected succeeded pending cleanup status, got %q", stored.Status)
	}
}
