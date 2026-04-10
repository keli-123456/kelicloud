package failoverv2

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/komari-monitor/komari/cmd/flags"
	"github.com/komari-monitor/komari/common"
	"github.com/komari-monitor/komari/config"
	"github.com/komari-monitor/komari/database/dbcore"
	failoverv2db "github.com/komari-monitor/komari/database/failoverv2"
	"github.com/komari-monitor/komari/database/models"
	"github.com/komari-monitor/komari/ws"
)

func configureFailoverV2RunnerTestDB(t *testing.T) {
	t.Helper()

	flags.DatabaseType = "sqlite"
	flags.DatabaseFile = filepath.Join(os.TempDir(), "komari-failoverv2-runner-tests.db")

	db := dbcore.GetDBInstance()
	if err := db.AutoMigrate(
		&models.FailoverTask{},
		&models.FailoverPlan{},
		&models.FailoverExecution{},
		&models.FailoverExecutionStep{},
		&models.FailoverPendingCleanup{},
		&models.FailoverV2Service{},
		&models.FailoverV2Member{},
		&models.FailoverV2MemberLine{},
		&models.FailoverV2Execution{},
		&models.FailoverV2ExecutionStep{},
		&models.FailoverV2PendingCleanup{},
		&models.FailoverV2RunLock{},
	); err != nil {
		t.Fatalf("failed to migrate V2 runner schema: %v", err)
	}
	for _, table := range []string{
		"failover_v2_run_locks",
		"failover_v2_pending_cleanups",
		"failover_v2_execution_steps",
		"failover_v2_executions",
		"failover_v2_member_lines",
		"failover_v2_members",
		"failover_v2_services",
		"failover_pending_cleanups",
		"failover_execution_steps",
		"failover_executions",
		"failover_plans",
		"failover_tasks",
	} {
		if err := db.Exec("DELETE FROM " + table).Error; err != nil {
			t.Fatalf("failed to reset table %s: %v", table, err)
		}
	}

	activeServiceRunsMu.Lock()
	activeServiceRuns = map[uint]struct{}{}
	activeServiceRunsMu.Unlock()

	executionStopMu.Lock()
	executionCancels = map[uint]context.CancelFunc{}
	executionStopMu.Unlock()

	activeDNSRunsMu.Lock()
	activeDNSRuns = map[string]uint{}
	activeDNSRunsMu.Unlock()

	pendingFailoverV2CleanupRunMu.Lock()
	pendingFailoverV2CleanupRunActive = false
	pendingFailoverV2CleanupRunMu.Unlock()

	activePendingCleanupRunsMu.Lock()
	activePendingCleanupRuns = map[uint]struct{}{}
	activePendingCleanupRunsMu.Unlock()

	if err := config.Set(config.FailoverV2SchedulerEnabledKey, false); err != nil {
		t.Fatalf("failed to reset failover v2 scheduler flag: %v", err)
	}

	ws.DeleteLatestReport("client-old")
	ws.DeleteLatestReport("client-new")
	ws.DeleteLatestReport("client-telecom")
}

func useMockFailoverV2RunnerDeps(t *testing.T) {
	t.Helper()

	previousDetach := failoverV2DetachDNSFunc
	previousVerifyDetach := failoverV2VerifyDetachDNSFunc
	previousProvision := failoverV2ProvisionFunc
	previousWait := failoverV2WaitClientFunc
	previousValidate := failoverV2ValidateOutletFunc
	previousScripts := failoverV2RunScriptsFunc
	previousAttach := failoverV2AttachDNSFunc
	previousVerifyAttach := failoverV2VerifyAttachDNSFunc
	previousResolveCleanup := failoverV2ResolveOldInstanceCleanupFunc
	previousResolveCleanupFromRef := failoverV2ResolveOldInstanceCleanupFromRefFunc
	previousLoadConfig := loadAliyunDNSConfigFunc

	loadAliyunDNSConfigFunc = func(userUUID, entryID string) (*aliyunDNSConfig, error) {
		return &aliyunDNSConfig{
			AccessKeyID:     "ak",
			AccessKeySecret: "sk",
			DomainName:      "example.com",
		}, nil
	}
	failoverV2ResolveOldInstanceCleanupFunc = func(userUUID string, member *models.FailoverV2Member) (*oldInstanceCleanup, error) {
		if member == nil {
			return nil, nil
		}
		provider := strings.ToLower(strings.TrimSpace(member.Provider))
		if provider != "digitalocean" && provider != "linode" {
			return nil, nil
		}
		return &oldInstanceCleanup{
			Ref:   resolvedMemberCurrentInstanceRef(member),
			Label: "mock old instance cleanup",
			Cleanup: func(ctx context.Context) error {
				return nil
			},
		}, nil
	}

	t.Cleanup(func() {
		failoverV2DetachDNSFunc = previousDetach
		failoverV2VerifyDetachDNSFunc = previousVerifyDetach
		failoverV2ProvisionFunc = previousProvision
		failoverV2WaitClientFunc = previousWait
		failoverV2ValidateOutletFunc = previousValidate
		failoverV2RunScriptsFunc = previousScripts
		failoverV2AttachDNSFunc = previousAttach
		failoverV2VerifyAttachDNSFunc = previousVerifyAttach
		failoverV2ResolveOldInstanceCleanupFunc = previousResolveCleanup
		failoverV2ResolveOldInstanceCleanupFromRefFunc = previousResolveCleanupFromRef
		loadAliyunDNSConfigFunc = previousLoadConfig
	})
}

func createTestRunnerServiceAndMember(t *testing.T) (*models.FailoverV2Service, *models.FailoverV2Member) {
	t.Helper()

	service, err := failoverv2db.CreateServiceForUser("user-a", &models.FailoverV2Service{
		Name:                "edge-service",
		Enabled:             true,
		DNSProvider:         models.FailoverDNSProviderAliyun,
		DNSEntryID:          "default",
		DNSPayload:          `{"domain_name":"example.com","rr":"@","record_type":"A","ttl":60}`,
		ScriptClipboardIDs:  "",
		ScriptTimeoutSec:    120,
		WaitAgentTimeoutSec: 120,
		DeleteStrategy:      models.FailoverDeleteStrategyKeep,
	})
	if err != nil {
		t.Fatalf("failed to create test service: %v", err)
	}

	member, err := failoverv2db.CreateMemberForUser("user-a", service.ID, &models.FailoverV2Member{
		Name:               "telecom",
		Enabled:            true,
		WatchClientUUID:    "client-old",
		DNSLine:            "telecom",
		DNSRecordRefs:      `{"A":"record-old"}`,
		CurrentAddress:     "198.51.100.10",
		CurrentInstanceRef: `{"provider":"digitalocean","droplet_id":100}`,
		Provider:           "digitalocean",
		ProviderEntryID:    "token-1",
		PlanPayload:        `{"region":"nyc1","size":"s-1vcpu-1gb","image":"ubuntu-24-04-x64"}`,
	})
	if err != nil {
		t.Fatalf("failed to create test member: %v", err)
	}

	return service, member
}

func createTestRunnerState(t *testing.T) (*models.FailoverV2Service, *models.FailoverV2Member, *models.FailoverV2Execution) {
	t.Helper()

	service, member := createTestRunnerServiceAndMember(t)

	execution, err := failoverv2db.CreateExecutionForUser("user-a", service.ID, member.ID, &models.FailoverV2Execution{
		Status:          models.FailoverV2ExecutionStatusQueued,
		TriggerReason:   "manual failover",
		OldClientUUID:   member.WatchClientUUID,
		OldInstanceRef:  member.CurrentInstanceRef,
		OldAddresses:    `{"current_address":"198.51.100.10"}`,
		DetachDNSStatus: models.FailoverDNSStatusPending,
		AttachDNSStatus: models.FailoverDNSStatusPending,
		CleanupStatus:   models.FailoverCleanupStatusSkipped,
	})
	if err != nil {
		t.Fatalf("failed to create test execution: %v", err)
	}

	return service, member, execution
}

func waitForFailoverV2ExecutionStatus(t *testing.T, userUUID string, serviceID, executionID uint, expected string) *models.FailoverV2Execution {
	t.Helper()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		execution, err := failoverv2db.GetExecutionByIDForUser(userUUID, serviceID, executionID)
		if err == nil && execution.Status == expected {
			return execution
		}
		time.Sleep(25 * time.Millisecond)
	}

	execution, err := failoverv2db.GetExecutionByIDForUser(userUUID, serviceID, executionID)
	if err != nil {
		t.Fatalf("failed to reload execution %d: %v", executionID, err)
	}
	t.Fatalf("expected execution %d status %q, got %q", executionID, expected, execution.Status)
	return nil
}

func TestEvaluateMemberHealthTriggersWhenMissingReportThresholdReached(t *testing.T) {
	now := time.Now()
	member := &models.FailoverV2Member{
		FailureThreshold:    2,
		WatchClientUUID:     "client-1",
		TriggerFailureCount: 1,
	}

	shouldTrigger, fields, reason := evaluateMemberHealth(member, nil, now)
	if !shouldTrigger {
		t.Fatal("expected missing report threshold to trigger failover v2")
	}
	if got := intMapValue(fields, "trigger_failure_count"); got != 2 {
		t.Fatalf("expected trigger_failure_count=2, got %d", got)
	}
	if stringMapValue(fields, "last_status") != models.FailoverV2MemberStatusTriggered {
		t.Fatalf("expected member status triggered, got %q", stringMapValue(fields, "last_status"))
	}
	if reason == "" {
		t.Fatal("expected trigger reason for missing report threshold")
	}
}

func TestRunScheduledWorkTriggersAutomaticFailover(t *testing.T) {
	configureFailoverV2RunnerTestDB(t)
	useMockFailoverV2RunnerDeps(t)
	if err := config.Set(config.FailoverV2SchedulerEnabledKey, true); err != nil {
		t.Fatalf("failed to enable failover v2 scheduler: %v", err)
	}

	service, member := createTestRunnerServiceAndMember(t)

	failoverV2DetachDNSFunc = func(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member) (interface{}, error) {
		return &AliyunMemberDNSResult{Line: member.DNSLine, RecordRefs: map[string]string{}}, nil
	}
	failoverV2VerifyDetachDNSFunc = func(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member) (interface{}, error) {
		return &AliyunMemberDNSVerification{Line: member.DNSLine, Success: true}, nil
	}
	failoverV2ProvisionFunc = func(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member) (*memberProvisionOutcome, error) {
		return &memberProvisionOutcome{
			IPv4:             "203.0.113.8",
			AutoConnectGroup: "failover-v2/1/1",
			NewInstanceRef:   map[string]interface{}{"provider": "digitalocean", "droplet_id": 321},
			NewAddresses:     map[string]interface{}{"ipv4": "203.0.113.8"},
		}, nil
	}
	failoverV2WaitClientFunc = func(ctx context.Context, userUUID, group, excludeUUID string, startedAt time.Time, timeoutSeconds int, expectedAddresses map[string]struct{}) (string, error) {
		return "client-new", nil
	}
	failoverV2ValidateOutletFunc = func(ctx context.Context, userUUID, clientUUID string, startedAt time.Time) (*common.Report, error) {
		return &common.Report{CNConnectivity: &common.CNConnectivityReport{Status: "ok"}}, nil
	}
	failoverV2RunScriptsFunc = func(ctx context.Context, userUUID, clientUUID string, service *models.FailoverV2Service) (interface{}, error) {
		return map[string]interface{}{"count": 0}, nil
	}
	failoverV2AttachDNSFunc = func(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member, ipv4, ipv6 string) (interface{}, error) {
		return &AliyunMemberDNSResult{Line: member.DNSLine, RecordRefs: map[string]string{"A": "record-new"}}, nil
	}
	failoverV2VerifyAttachDNSFunc = func(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member, ipv4, ipv6 string) (interface{}, error) {
		return &AliyunMemberDNSVerification{Line: member.DNSLine, Success: true}, nil
	}

	report := &common.Report{
		UpdatedAt: time.Now(),
		CNConnectivity: &common.CNConnectivityReport{
			Status:              "blocked_suspected",
			CheckedAt:           time.Now(),
			ConsecutiveFailures: member.FailureThreshold,
		},
	}
	ws.SetLatestReport(member.WatchClientUUID, report)
	t.Cleanup(func() {
		ws.DeleteLatestReport(member.WatchClientUUID)
		ws.DeleteLatestReport("client-new")
	})

	if err := RunScheduledWork(); err != nil {
		t.Fatalf("run scheduled work failed: %v", err)
	}

	var executionID uint
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		reloadedService, err := failoverv2db.GetServiceByIDForUser("user-a", service.ID)
		if err == nil && reloadedService.LastExecutionID != nil {
			executionID = *reloadedService.LastExecutionID
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if executionID == 0 {
		t.Fatal("expected scheduled work to create an execution")
	}

	storedExecution := waitForFailoverV2ExecutionStatus(t, "user-a", service.ID, executionID, models.FailoverV2ExecutionStatusSuccess)
	if storedExecution.TriggerReason != "cn_connectivity blocked_suspected (2 failures)" {
		t.Fatalf("unexpected trigger reason: %q", storedExecution.TriggerReason)
	}
	if !strings.Contains(storedExecution.TriggerSnapshot, "blocked_suspected") {
		t.Fatalf("expected trigger snapshot to persist health report, got %q", storedExecution.TriggerSnapshot)
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
	if reloadedMember.LastStatus != models.FailoverV2MemberStatusHealthy {
		t.Fatalf("expected member healthy after scheduled failover, got %q", reloadedMember.LastStatus)
	}
	if reloadedService.LastStatus != models.FailoverV2ServiceStatusHealthy {
		t.Fatalf("expected service healthy after scheduled failover, got %q", reloadedService.LastStatus)
	}
}

func TestRunScheduledWorkSkipsAutomaticFailoverWhenSchedulerDisabled(t *testing.T) {
	configureFailoverV2RunnerTestDB(t)
	useMockFailoverV2RunnerDeps(t)

	service, member := createTestRunnerServiceAndMember(t)
	failoverV2DetachDNSFunc = func(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member) (interface{}, error) {
		t.Fatal("did not expect scheduler to detach dns while disabled")
		return nil, nil
	}

	report := &common.Report{
		UpdatedAt: time.Now(),
		CNConnectivity: &common.CNConnectivityReport{
			Status:              "blocked_suspected",
			CheckedAt:           time.Now(),
			ConsecutiveFailures: member.FailureThreshold,
		},
	}
	ws.SetLatestReport(member.WatchClientUUID, report)
	t.Cleanup(func() {
		ws.DeleteLatestReport(member.WatchClientUUID)
	})

	if err := RunScheduledWork(); err != nil {
		t.Fatalf("run scheduled work failed: %v", err)
	}

	reloadedService, err := failoverv2db.GetServiceByIDForUser("user-a", service.ID)
	if err != nil {
		t.Fatalf("failed to reload service: %v", err)
	}
	if reloadedService.LastExecutionID != nil {
		t.Fatalf("expected no execution while scheduler disabled, got last_execution_id=%d", *reloadedService.LastExecutionID)
	}
}

func TestMemberExecutionRunnerRunFailoverSuccessUpdatesMemberAndExecution(t *testing.T) {
	configureFailoverV2RunnerTestDB(t)
	useMockFailoverV2RunnerDeps(t)

	service, member, execution := createTestRunnerState(t)
	rollbackCalled := false

	failoverV2DetachDNSFunc = func(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member) (interface{}, error) {
		return &AliyunMemberDNSResult{Line: "telecom", RecordRefs: map[string]string{}}, nil
	}
	failoverV2VerifyDetachDNSFunc = func(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member) (interface{}, error) {
		return &AliyunMemberDNSVerification{Line: "telecom", Success: true}, nil
	}
	failoverV2ProvisionFunc = func(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member) (*memberProvisionOutcome, error) {
		return &memberProvisionOutcome{
			IPv4:             "203.0.113.8",
			AutoConnectGroup: "failover-v2/1/1",
			NewInstanceRef:   map[string]interface{}{"provider": "digitalocean", "droplet_id": 321},
			NewAddresses:     map[string]interface{}{"ipv4": "203.0.113.8"},
			RollbackLabel:    "delete failed digitalocean droplet 321",
			Rollback: func(ctx context.Context) error {
				rollbackCalled = true
				return nil
			},
		}, nil
	}
	failoverV2WaitClientFunc = func(ctx context.Context, userUUID, group, excludeUUID string, startedAt time.Time, timeoutSeconds int, expectedAddresses map[string]struct{}) (string, error) {
		return "client-new", nil
	}
	failoverV2ValidateOutletFunc = func(ctx context.Context, userUUID, clientUUID string, startedAt time.Time) (*common.Report, error) {
		return &common.Report{
			CNConnectivity: &common.CNConnectivityReport{Status: "ok"},
		}, nil
	}
	failoverV2RunScriptsFunc = func(ctx context.Context, userUUID, clientUUID string, service *models.FailoverV2Service) (interface{}, error) {
		return map[string]interface{}{"count": 0}, nil
	}
	failoverV2AttachDNSFunc = func(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member, ipv4, ipv6 string) (interface{}, error) {
		return &AliyunMemberDNSResult{Line: member.DNSLine, RecordRefs: map[string]string{"A": "record-new"}}, nil
	}
	failoverV2VerifyAttachDNSFunc = func(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member, ipv4, ipv6 string) (interface{}, error) {
		return &AliyunMemberDNSVerification{Line: member.DNSLine, Success: true}, nil
	}

	runner := &memberExecutionRunner{
		userUUID:  "user-a",
		service:   service,
		member:    member,
		execution: execution,
		ctx:       context.Background(),
	}
	runner.runFailover()

	storedExecution, err := failoverv2db.GetExecutionByIDForUser("user-a", service.ID, execution.ID)
	if err != nil {
		t.Fatalf("failed to reload execution: %v", err)
	}
	if storedExecution.Status != models.FailoverV2ExecutionStatusSuccess {
		t.Fatalf("expected execution success, got %q", storedExecution.Status)
	}
	if storedExecution.NewClientUUID != "client-new" {
		t.Fatalf("expected new client uuid to be persisted, got %q", storedExecution.NewClientUUID)
	}
	if storedExecution.AttachDNSStatus != models.FailoverDNSStatusSuccess {
		t.Fatalf("expected attach dns success, got %q", storedExecution.AttachDNSStatus)
	}

	reloadedService, err := failoverv2db.GetServiceByIDForUser("user-a", service.ID)
	if err != nil {
		t.Fatalf("failed to reload service: %v", err)
	}
	reloadedMember, err := findMemberOnService(reloadedService, member.ID)
	if err != nil {
		t.Fatalf("failed to reload member from service: %v", err)
	}
	if reloadedMember.WatchClientUUID != "client-new" {
		t.Fatalf("expected member watch client to update, got %q", reloadedMember.WatchClientUUID)
	}
	if reloadedMember.CurrentAddress != "203.0.113.8" {
		t.Fatalf("expected current address to update, got %q", reloadedMember.CurrentAddress)
	}
	if len(reloadedMember.Lines) != 1 {
		t.Fatalf("expected one member line, got %d", len(reloadedMember.Lines))
	}
	if reloadedMember.Lines[0].LineCode != "telecom" {
		t.Fatalf("expected member line telecom, got %q", reloadedMember.Lines[0].LineCode)
	}
	if !strings.Contains(reloadedMember.Lines[0].DNSRecordRefs, "record-new") {
		t.Fatalf("expected member line dns record refs to include new record, got %q", reloadedMember.Lines[0].DNSRecordRefs)
	}
	if !strings.Contains(reloadedMember.DNSRecordRefs, "record-new") {
		t.Fatalf("expected legacy dns record refs to mirror first line, got %q", reloadedMember.DNSRecordRefs)
	}
	if reloadedMember.LastStatus != models.FailoverV2MemberStatusHealthy {
		t.Fatalf("expected member healthy, got %q", reloadedMember.LastStatus)
	}
	if reloadedService.LastStatus != models.FailoverV2ServiceStatusHealthy {
		t.Fatalf("expected service healthy, got %q", reloadedService.LastStatus)
	}
	if rollbackCalled {
		t.Fatal("did not expect rollback to run on success")
	}
}

func TestMemberExecutionRunnerRunFailoverValidationFailureRollsBackNewInstance(t *testing.T) {
	configureFailoverV2RunnerTestDB(t)
	useMockFailoverV2RunnerDeps(t)

	service, member, execution := createTestRunnerState(t)
	rollbackCalled := false

	failoverV2DetachDNSFunc = func(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member) (interface{}, error) {
		return &AliyunMemberDNSResult{Line: "telecom", RecordRefs: map[string]string{}}, nil
	}
	failoverV2VerifyDetachDNSFunc = func(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member) (interface{}, error) {
		return &AliyunMemberDNSVerification{Line: "telecom", Success: true}, nil
	}
	failoverV2ProvisionFunc = func(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member) (*memberProvisionOutcome, error) {
		return &memberProvisionOutcome{
			IPv4:             "203.0.113.9",
			AutoConnectGroup: "failover-v2/1/1",
			NewInstanceRef:   map[string]interface{}{"provider": "digitalocean", "droplet_id": 322},
			NewAddresses:     map[string]interface{}{"ipv4": "203.0.113.9"},
			RollbackLabel:    "delete failed digitalocean droplet 322",
			Rollback: func(ctx context.Context) error {
				rollbackCalled = true
				return nil
			},
		}, nil
	}
	failoverV2WaitClientFunc = func(ctx context.Context, userUUID, group, excludeUUID string, startedAt time.Time, timeoutSeconds int, expectedAddresses map[string]struct{}) (string, error) {
		return "client-new", nil
	}
	failoverV2ValidateOutletFunc = func(ctx context.Context, userUUID, clientUUID string, startedAt time.Time) (*common.Report, error) {
		return &common.Report{
			CNConnectivity: &common.CNConnectivityReport{Status: "blocked_suspected", Message: "blocked"},
		}, &blockedOutletError{ClientUUID: clientUUID, Status: "blocked_suspected", Message: "blocked"}
	}
	failoverV2RunScriptsFunc = func(ctx context.Context, userUUID, clientUUID string, service *models.FailoverV2Service) (interface{}, error) {
		return map[string]interface{}{"count": 0}, nil
	}
	failoverV2AttachDNSFunc = func(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member, ipv4, ipv6 string) (interface{}, error) {
		return nil, errors.New("attach should not run")
	}
	failoverV2VerifyAttachDNSFunc = func(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member, ipv4, ipv6 string) (interface{}, error) {
		return nil, errors.New("verify attach should not run")
	}

	runner := &memberExecutionRunner{
		userUUID:  "user-a",
		service:   service,
		member:    member,
		execution: execution,
		ctx:       context.Background(),
	}
	runner.runFailover()

	storedExecution, err := failoverv2db.GetExecutionByIDForUser("user-a", service.ID, execution.ID)
	if err != nil {
		t.Fatalf("failed to reload execution: %v", err)
	}
	if storedExecution.Status != models.FailoverV2ExecutionStatusFailed {
		t.Fatalf("expected execution failure, got %q", storedExecution.Status)
	}
	if !strings.Contains(storedExecution.ErrorMessage, "blocked") {
		t.Fatalf("expected execution error to mention validation failure, got %q", storedExecution.ErrorMessage)
	}
	if !rollbackCalled {
		t.Fatal("expected rollback to run after validation failure")
	}

	reloadedService, err := failoverv2db.GetServiceByIDForUser("user-a", service.ID)
	if err != nil {
		t.Fatalf("failed to reload service: %v", err)
	}
	reloadedMember, err := findMemberOnService(reloadedService, member.ID)
	if err != nil {
		t.Fatalf("failed to reload member: %v", err)
	}
	if reloadedMember.WatchClientUUID != "client-old" {
		t.Fatalf("expected member watch client to remain unchanged, got %q", reloadedMember.WatchClientUUID)
	}
	if reloadedMember.LastStatus != models.FailoverV2MemberStatusFailed {
		t.Fatalf("expected member failed after rollback path, got %q", reloadedMember.LastStatus)
	}
	if reloadedService.LastStatus != models.FailoverV2ServiceStatusFailed {
		t.Fatalf("expected service failed after rollback path, got %q", reloadedService.LastStatus)
	}
}

func TestMemberExecutionRunnerRunFailoverDeletesOldInstanceWhenConfigured(t *testing.T) {
	configureFailoverV2RunnerTestDB(t)
	useMockFailoverV2RunnerDeps(t)

	service, member, execution := createTestRunnerState(t)
	service.DeleteStrategy = models.FailoverDeleteStrategyDeleteAfterSuccess

	oldCleanupCalled := false
	failoverV2DetachDNSFunc = func(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member) (interface{}, error) {
		return &AliyunMemberDNSResult{Line: "telecom", RecordRefs: map[string]string{}}, nil
	}
	failoverV2VerifyDetachDNSFunc = func(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member) (interface{}, error) {
		return &AliyunMemberDNSVerification{Line: "telecom", Success: true}, nil
	}
	failoverV2ProvisionFunc = func(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member) (*memberProvisionOutcome, error) {
		return &memberProvisionOutcome{
			IPv4:             "203.0.113.10",
			AutoConnectGroup: "failover-v2/1/1",
			NewInstanceRef:   map[string]interface{}{"provider": "digitalocean", "provider_entry_id": "token-1", "droplet_id": 333},
			NewAddresses:     map[string]interface{}{"ipv4": "203.0.113.10"},
		}, nil
	}
	failoverV2WaitClientFunc = func(ctx context.Context, userUUID, group, excludeUUID string, startedAt time.Time, timeoutSeconds int, expectedAddresses map[string]struct{}) (string, error) {
		return "client-new", nil
	}
	failoverV2ValidateOutletFunc = func(ctx context.Context, userUUID, clientUUID string, startedAt time.Time) (*common.Report, error) {
		return &common.Report{CNConnectivity: &common.CNConnectivityReport{Status: "ok"}}, nil
	}
	failoverV2RunScriptsFunc = func(ctx context.Context, userUUID, clientUUID string, service *models.FailoverV2Service) (interface{}, error) {
		return map[string]interface{}{"count": 0}, nil
	}
	failoverV2AttachDNSFunc = func(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member, ipv4, ipv6 string) (interface{}, error) {
		return &AliyunMemberDNSResult{Line: member.DNSLine, RecordRefs: map[string]string{"A": "record-new"}}, nil
	}
	failoverV2VerifyAttachDNSFunc = func(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member, ipv4, ipv6 string) (interface{}, error) {
		return &AliyunMemberDNSVerification{Line: member.DNSLine, Success: true}, nil
	}
	failoverV2ResolveOldInstanceCleanupFunc = func(userUUID string, member *models.FailoverV2Member) (*oldInstanceCleanup, error) {
		return &oldInstanceCleanup{
			Ref:   map[string]interface{}{"provider": "digitalocean", "provider_entry_id": "token-1", "droplet_id": 100},
			Label: "delete digitalocean droplet 100",
			Cleanup: func(ctx context.Context) error {
				oldCleanupCalled = true
				return nil
			},
		}, nil
	}

	runner := &memberExecutionRunner{
		userUUID:  "user-a",
		service:   service,
		member:    member,
		execution: execution,
		ctx:       context.Background(),
	}
	runner.runFailover()

	storedExecution, err := failoverv2db.GetExecutionByIDForUser("user-a", service.ID, execution.ID)
	if err != nil {
		t.Fatalf("failed to reload execution: %v", err)
	}
	if storedExecution.CleanupStatus != models.FailoverCleanupStatusSuccess {
		t.Fatalf("expected cleanup success, got %q", storedExecution.CleanupStatus)
	}
	if !strings.Contains(storedExecution.CleanupResult, "instance_deleted") {
		t.Fatalf("expected cleanup result to note instance deletion, got %q", storedExecution.CleanupResult)
	}
	if !oldCleanupCalled {
		t.Fatal("expected old instance cleanup to run")
	}
}

func TestMemberExecutionRunnerRunFailoverQueuesPendingCleanupWhenOldCleanupFails(t *testing.T) {
	configureFailoverV2RunnerTestDB(t)
	useMockFailoverV2RunnerDeps(t)

	service, member, execution := createTestRunnerState(t)
	service.DeleteStrategy = models.FailoverDeleteStrategyDeleteAfterSuccess
	member.Provider = "aws"
	member.ProviderEntryID = "cred-1"
	member.CurrentInstanceRef = `{"provider":"aws","service":"ec2","provider_entry_id":"cred-1","region":"us-east-1","instance_id":"i-old"}`

	failoverV2DetachDNSFunc = func(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member) (interface{}, error) {
		return &AliyunMemberDNSResult{Line: "telecom", RecordRefs: map[string]string{}}, nil
	}
	failoverV2VerifyDetachDNSFunc = func(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member) (interface{}, error) {
		return &AliyunMemberDNSVerification{Line: "telecom", Success: true}, nil
	}
	failoverV2ProvisionFunc = func(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member) (*memberProvisionOutcome, error) {
		return &memberProvisionOutcome{
			IPv4:             "203.0.113.11",
			AutoConnectGroup: "failover-v2/1/1",
			NewInstanceRef:   map[string]interface{}{"provider": "aws", "service": "ec2", "provider_entry_id": "cred-1", "region": "us-east-1", "instance_id": "i-new"},
			NewAddresses:     map[string]interface{}{"ipv4": "203.0.113.11"},
		}, nil
	}
	failoverV2WaitClientFunc = func(ctx context.Context, userUUID, group, excludeUUID string, startedAt time.Time, timeoutSeconds int, expectedAddresses map[string]struct{}) (string, error) {
		return "client-new", nil
	}
	failoverV2ValidateOutletFunc = func(ctx context.Context, userUUID, clientUUID string, startedAt time.Time) (*common.Report, error) {
		return &common.Report{CNConnectivity: &common.CNConnectivityReport{Status: "ok"}}, nil
	}
	failoverV2RunScriptsFunc = func(ctx context.Context, userUUID, clientUUID string, service *models.FailoverV2Service) (interface{}, error) {
		return map[string]interface{}{"count": 0}, nil
	}
	failoverV2AttachDNSFunc = func(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member, ipv4, ipv6 string) (interface{}, error) {
		return &AliyunMemberDNSResult{Line: member.DNSLine, RecordRefs: map[string]string{"A": "record-new"}}, nil
	}
	failoverV2VerifyAttachDNSFunc = func(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member, ipv4, ipv6 string) (interface{}, error) {
		return &AliyunMemberDNSVerification{Line: member.DNSLine, Success: true}, nil
	}
	failoverV2ResolveOldInstanceCleanupFunc = func(userUUID string, member *models.FailoverV2Member) (*oldInstanceCleanup, error) {
		return &oldInstanceCleanup{
			Ref:   map[string]interface{}{"provider": "digitalocean", "provider_entry_id": "token-1", "droplet_id": 100},
			Label: "delete digitalocean droplet 100",
			Cleanup: func(ctx context.Context) error {
				return errors.New("delete failed")
			},
		}, nil
	}

	runner := &memberExecutionRunner{
		userUUID:  "user-a",
		service:   service,
		member:    member,
		execution: execution,
		ctx:       context.Background(),
	}
	runner.runFailover()

	storedExecution, err := failoverv2db.GetExecutionByIDForUser("user-a", service.ID, execution.ID)
	if err != nil {
		t.Fatalf("failed to reload execution: %v", err)
	}
	if storedExecution.Status != models.FailoverV2ExecutionStatusSuccess {
		t.Fatalf("expected overall execution success, got %q", storedExecution.Status)
	}
	if storedExecution.CleanupStatus != models.FailoverCleanupStatusFailed {
		t.Fatalf("expected cleanup failure to be recorded, got %q", storedExecution.CleanupStatus)
	}
	if !strings.Contains(storedExecution.CleanupResult, "pending_cleanup_id") {
		t.Fatalf("expected cleanup result to include pending cleanup metadata, got %q", storedExecution.CleanupResult)
	}

	db := dbcore.GetDBInstance()
	var count int64
	if err := db.Model(&models.FailoverV2PendingCleanup{}).Count(&count).Error; err != nil {
		t.Fatalf("failed to count pending cleanups: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected one pending cleanup row, got %d", count)
	}
}

func TestMemberExecutionRunnerRunFailoverDeletesOldInstanceBeforeProvisionForDigitalOcean(t *testing.T) {
	configureFailoverV2RunnerTestDB(t)
	useMockFailoverV2RunnerDeps(t)

	service, member, execution := createTestRunnerState(t)
	service.DeleteStrategy = models.FailoverDeleteStrategyKeep

	callOrder := make([]string, 0, 2)
	oldCleanupCalls := 0

	failoverV2DetachDNSFunc = func(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member) (interface{}, error) {
		return &AliyunMemberDNSResult{Line: "telecom", RecordRefs: map[string]string{}}, nil
	}
	failoverV2VerifyDetachDNSFunc = func(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member) (interface{}, error) {
		return &AliyunMemberDNSVerification{Line: "telecom", Success: true}, nil
	}
	failoverV2ResolveOldInstanceCleanupFunc = func(userUUID string, member *models.FailoverV2Member) (*oldInstanceCleanup, error) {
		return &oldInstanceCleanup{
			Ref:   map[string]interface{}{"provider": "digitalocean", "provider_entry_id": "token-1", "droplet_id": 100},
			Label: "delete digitalocean droplet 100",
			Cleanup: func(ctx context.Context) error {
				oldCleanupCalls++
				callOrder = append(callOrder, "cleanup_old")
				return nil
			},
		}, nil
	}
	failoverV2ProvisionFunc = func(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member) (*memberProvisionOutcome, error) {
		callOrder = append(callOrder, "provision")
		return &memberProvisionOutcome{
			IPv4:             "203.0.113.12",
			AutoConnectGroup: "failover-v2/1/1",
			NewInstanceRef:   map[string]interface{}{"provider": "digitalocean", "provider_entry_id": "token-1", "droplet_id": 335},
			NewAddresses:     map[string]interface{}{"ipv4": "203.0.113.12"},
		}, nil
	}
	failoverV2WaitClientFunc = func(ctx context.Context, userUUID, group, excludeUUID string, startedAt time.Time, timeoutSeconds int, expectedAddresses map[string]struct{}) (string, error) {
		return "client-new", nil
	}
	failoverV2ValidateOutletFunc = func(ctx context.Context, userUUID, clientUUID string, startedAt time.Time) (*common.Report, error) {
		return &common.Report{CNConnectivity: &common.CNConnectivityReport{Status: "ok"}}, nil
	}
	failoverV2RunScriptsFunc = func(ctx context.Context, userUUID, clientUUID string, service *models.FailoverV2Service) (interface{}, error) {
		return map[string]interface{}{"count": 0}, nil
	}
	failoverV2AttachDNSFunc = func(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member, ipv4, ipv6 string) (interface{}, error) {
		return &AliyunMemberDNSResult{Line: member.DNSLine, RecordRefs: map[string]string{"A": "record-new"}}, nil
	}
	failoverV2VerifyAttachDNSFunc = func(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member, ipv4, ipv6 string) (interface{}, error) {
		return &AliyunMemberDNSVerification{Line: member.DNSLine, Success: true}, nil
	}

	runner := &memberExecutionRunner{
		userUUID:  "user-a",
		service:   service,
		member:    member,
		execution: execution,
		ctx:       context.Background(),
	}
	runner.runFailover()

	if oldCleanupCalls != 1 {
		t.Fatalf("expected one pre-provision cleanup call, got %d", oldCleanupCalls)
	}
	if strings.Join(callOrder, ",") != "cleanup_old,provision" {
		t.Fatalf("expected cleanup then provision order, got %q", strings.Join(callOrder, ","))
	}

	storedExecution, err := failoverv2db.GetExecutionByIDForUser("user-a", service.ID, execution.ID)
	if err != nil {
		t.Fatalf("failed to reload execution: %v", err)
	}
	if storedExecution.Status != models.FailoverV2ExecutionStatusSuccess {
		t.Fatalf("expected execution success, got %q", storedExecution.Status)
	}
	if storedExecution.CleanupStatus != models.FailoverCleanupStatusSuccess {
		t.Fatalf("expected cleanup status success from pre-provision deletion, got %q", storedExecution.CleanupStatus)
	}
	if !strings.Contains(storedExecution.CleanupResult, "instance_deleted_before_provision") {
		t.Fatalf("expected cleanup result to note pre-provision deletion, got %q", storedExecution.CleanupResult)
	}
}

func TestMemberExecutionRunnerRunFailoverRebindAWSUsesExistingClientWithoutWaitOrScripts(t *testing.T) {
	configureFailoverV2RunnerTestDB(t)
	useMockFailoverV2RunnerDeps(t)

	service, member, execution := createTestRunnerState(t)
	member.Provider = "aws"
	member.ProviderEntryID = "cred-1"
	member.CurrentInstanceRef = `{"provider":"aws","service":"ec2","provider_entry_id":"cred-1","region":"us-east-1","instance_id":"i-old"}`
	member.PlanPayload = `{"service":"ec2","region":"us-east-1","image_id":"ami-123","instance_type":"t3.micro"}`

	waitCalled := false
	scriptsCalled := false

	failoverV2DetachDNSFunc = func(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member) (interface{}, error) {
		return &AliyunMemberDNSResult{Line: "telecom", RecordRefs: map[string]string{}}, nil
	}
	failoverV2VerifyDetachDNSFunc = func(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member) (interface{}, error) {
		return &AliyunMemberDNSVerification{Line: "telecom", Success: true}, nil
	}
	failoverV2ProvisionFunc = func(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member) (*memberProvisionOutcome, error) {
		return &memberProvisionOutcome{
			IPv4:             "203.0.113.22",
			TargetClientUUID: "client-old",
			NewInstanceRef: map[string]interface{}{
				"provider":          "aws",
				"service":           "ec2",
				"region":            "us-east-1",
				"instance_id":       "i-old",
				"rebind_public_ip":  true,
				"provider_entry_id": "cred-1",
			},
			NewAddresses: map[string]interface{}{
				"public_ip": "203.0.113.22",
			},
			SkipScripts:     true,
			SkipPostCleanup: true,
			CleanupStatus:   models.FailoverCleanupStatusSkipped,
			CleanupResult: map[string]interface{}{
				"classification": "cleanup_skipped_rebind_existing_instance",
			},
		}, nil
	}
	failoverV2WaitClientFunc = func(ctx context.Context, userUUID, group, excludeUUID string, startedAt time.Time, timeoutSeconds int, expectedAddresses map[string]struct{}) (string, error) {
		waitCalled = true
		return "", errors.New("wait client should not be called for aws rebind")
	}
	failoverV2ValidateOutletFunc = func(ctx context.Context, userUUID, clientUUID string, startedAt time.Time) (*common.Report, error) {
		if clientUUID != "client-old" {
			t.Fatalf("expected validate to target existing client-old, got %q", clientUUID)
		}
		return &common.Report{CNConnectivity: &common.CNConnectivityReport{Status: "ok"}}, nil
	}
	failoverV2RunScriptsFunc = func(ctx context.Context, userUUID, clientUUID string, service *models.FailoverV2Service) (interface{}, error) {
		scriptsCalled = true
		return map[string]interface{}{"count": 1}, nil
	}
	failoverV2AttachDNSFunc = func(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member, ipv4, ipv6 string) (interface{}, error) {
		return &AliyunMemberDNSResult{Line: member.DNSLine, RecordRefs: map[string]string{"A": "record-new"}}, nil
	}
	failoverV2VerifyAttachDNSFunc = func(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member, ipv4, ipv6 string) (interface{}, error) {
		return &AliyunMemberDNSVerification{Line: member.DNSLine, Success: true}, nil
	}

	runner := &memberExecutionRunner{
		userUUID:  "user-a",
		service:   service,
		member:    member,
		execution: execution,
		ctx:       context.Background(),
	}
	runner.runFailover()

	if waitCalled {
		t.Fatal("did not expect wait client function to run for aws rebind")
	}
	if scriptsCalled {
		t.Fatal("did not expect scripts dispatcher to run for aws rebind")
	}

	storedExecution, err := failoverv2db.GetExecutionByIDForUser("user-a", service.ID, execution.ID)
	if err != nil {
		t.Fatalf("failed to reload execution: %v", err)
	}
	if storedExecution.Status != models.FailoverV2ExecutionStatusSuccess {
		t.Fatalf("expected execution success, got %q", storedExecution.Status)
	}
	if storedExecution.NewClientUUID != "client-old" {
		t.Fatalf("expected execution to reuse existing client, got %q", storedExecution.NewClientUUID)
	}
	if storedExecution.CleanupStatus != models.FailoverCleanupStatusSkipped {
		t.Fatalf("expected cleanup skipped for aws rebind, got %q", storedExecution.CleanupStatus)
	}
	if !strings.Contains(storedExecution.CleanupResult, "cleanup_skipped_rebind_existing_instance") {
		t.Fatalf("expected cleanup result to mention rebind skip, got %q", storedExecution.CleanupResult)
	}

	waitSkipped := false
	scriptsSkipped := false
	for _, step := range storedExecution.Steps {
		if step.StepKey == "wait_agent" && step.Status == models.FailoverStepStatusSkipped {
			waitSkipped = true
		}
		if step.StepKey == "run_scripts" && step.Status == models.FailoverStepStatusSkipped {
			scriptsSkipped = true
		}
	}
	if !waitSkipped {
		t.Fatal("expected wait_agent step to be skipped for aws rebind")
	}
	if !scriptsSkipped {
		t.Fatal("expected run_scripts step to be skipped for aws rebind")
	}
}
