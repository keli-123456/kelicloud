package failoverv2

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	failoverv2db "github.com/komari-monitor/komari/database/failoverv2"
	"github.com/komari-monitor/komari/database/models"
	"gorm.io/gorm"
)

const interruptedExecutionMessage = "interrupted execution was marked failed during recovery"
const interruptedPendingCleanupMessage = "interrupted pending cleanup retry was marked pending during recovery"

const (
	failoverV2MinRunLockTTL            = 30 * time.Minute
	failoverV2MaxRunLockTTL            = 12 * time.Hour
	failoverV2PendingCleanupRunLockTTL = 5 * time.Minute
)

var (
	activeServiceRunsMu sync.Mutex
	activeServiceRuns   = map[uint]struct{}{}
)

type failoverV2RunLockHandle struct {
	lockKey      string
	owner        string
	releaseLocal func()
	releaseOnce  sync.Once
}

type memberExecutionRunner struct {
	userUUID  string
	service   *models.FailoverV2Service
	member    *models.FailoverV2Member
	execution *models.FailoverV2Execution
	ctx       context.Context
	stepSort  int
}

func RecoverInterruptedExecutions() error {
	recovered, err := failoverv2db.RecoverInterruptedExecutions(interruptedExecutionMessage)
	if err != nil {
		return err
	}
	if recovered > 0 {
		log.Printf("failoverv2: recovered %d interrupted execution(s)", recovered)
	}
	cleanupRecovered, err := failoverv2db.RecoverStaleRunningPendingCleanups(time.Now().Add(time.Second), interruptedPendingCleanupMessage)
	if err != nil {
		return err
	}
	if cleanupRecovered > 0 {
		log.Printf("failoverv2: recovered %d interrupted pending cleanup retry job(s)", cleanupRecovered)
	}
	return nil
}

func failoverV2ServiceRunLockTTL(service *models.FailoverV2Service) time.Duration {
	seconds := int64(3600)
	if service != nil {
		if service.ScriptTimeoutSec > 0 {
			seconds += int64(service.ScriptTimeoutSec)
		}
		if service.WaitAgentTimeoutSec > 0 {
			seconds += int64(service.WaitAgentTimeoutSec)
		}
		if service.DeleteDelaySeconds > 0 {
			seconds += int64(service.DeleteDelaySeconds)
		}
	}

	ttl := time.Duration(seconds) * time.Second
	if ttl < failoverV2MinRunLockTTL {
		return failoverV2MinRunLockTTL
	}
	if ttl > failoverV2MaxRunLockTTL {
		return failoverV2MaxRunLockTTL
	}
	return ttl
}

func failoverV2ServiceRunLockKey(serviceID uint) string {
	return fmt.Sprintf("failover_v2:service:%d", serviceID)
}

func failoverV2DNSRunLockKey(ownershipKey string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(ownershipKey)))
	return "failover_v2:dns:" + hex.EncodeToString(sum[:])
}

func failoverV2ProvisionRunLockKey(userUUID string, member *models.FailoverV2Member) string {
	if member == nil {
		return ""
	}
	provider := strings.ToLower(strings.TrimSpace(member.Provider))
	if provider == "" {
		return ""
	}
	entryID := strings.TrimSpace(member.ProviderEntryID)
	if entryID == "" {
		entryID = activeProviderEntryID
	}
	entryGroup := normalizeProviderEntryGroup(member.ProviderEntryGroup)
	payload := strings.Join([]string{
		strings.ToLower(strings.TrimSpace(userUUID)),
		provider,
		strings.ToLower(entryID),
		strings.ToLower(entryGroup),
	}, "|")
	sum := sha256.Sum256([]byte(payload))
	return "failover_v2:provision:" + hex.EncodeToString(sum[:])
}

func newFailoverV2RunLockOwner() string {
	var random [16]byte
	if _, err := rand.Read(random[:]); err == nil {
		return hex.EncodeToString(random[:])
	}
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

func claimFailoverV2RunLock(lockKey string, ttl time.Duration, releaseLocal func()) (*failoverV2RunLockHandle, error) {
	lockKey = strings.TrimSpace(lockKey)
	if lockKey == "" {
		if releaseLocal != nil {
			releaseLocal()
		}
		return nil, fmt.Errorf("failover v2 run lock key is required")
	}
	owner := newFailoverV2RunLockOwner()
	claimed, err := failoverv2db.ClaimRunLock(lockKey, owner, ttl)
	if err != nil {
		if releaseLocal != nil {
			releaseLocal()
		}
		return nil, err
	}
	if !claimed {
		if releaseLocal != nil {
			releaseLocal()
		}
		return nil, fmt.Errorf("failover v2 run lock %s is already held", lockKey)
	}
	return &failoverV2RunLockHandle{
		lockKey:      lockKey,
		owner:        owner,
		releaseLocal: releaseLocal,
	}, nil
}

func (h *failoverV2RunLockHandle) release() {
	if h == nil {
		return
	}
	h.releaseOnce.Do(func() {
		if h.lockKey != "" && h.owner != "" {
			if err := failoverv2db.ReleaseRunLock(h.lockKey, h.owner); err != nil {
				log.Printf("failoverv2: failed to release run lock %s: %v", h.lockKey, err)
			}
		}
		if h.releaseLocal != nil {
			h.releaseLocal()
		}
	})
}

func claimServiceRunLock(serviceID uint, ttl time.Duration) (*failoverV2RunLockHandle, error) {
	if !claimServiceRun(serviceID) {
		return nil, fmt.Errorf("failover v2 service %d is already running", serviceID)
	}
	return claimFailoverV2RunLock(
		failoverV2ServiceRunLockKey(serviceID),
		ttl,
		func() {
			releaseServiceRun(serviceID)
		},
	)
}

func claimMemberProvisionRunLock(userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member) (*failoverV2RunLockHandle, error) {
	if member == nil || memberUsesExistingClient(member) {
		return nil, nil
	}
	lockKey := failoverV2ProvisionRunLockKey(userUUID, member)
	if lockKey == "" {
		return nil, nil
	}

	owner := newFailoverV2RunLockOwner()
	claimed, err := failoverv2db.ClaimRunLock(lockKey, owner, failoverV2ServiceRunLockTTL(service))
	if err != nil {
		return nil, err
	}
	if !claimed {
		return nil, fmt.Errorf("provider credential is busy provisioning another member")
	}

	return &failoverV2RunLockHandle{
		lockKey: lockKey,
		owner:   owner,
	}, nil
}

func buildMemberOldAddressesSnapshot(member *models.FailoverV2Member) string {
	return string(marshalJSON(map[string]interface{}{
		"current_address": strings.TrimSpace(member.CurrentAddress),
		"dns_lines":       memberLineCodes(member),
	}))
}

func queueMemberExecution(
	userUUID string,
	service *models.FailoverV2Service,
	member *models.FailoverV2Member,
	executionTemplate *models.FailoverV2Execution,
	startMessage string,
	run func(*memberExecutionRunner),
) (*models.FailoverV2Execution, error) {
	if service == nil {
		return nil, errors.New("service is required")
	}
	if member == nil {
		return nil, errors.New("member is required")
	}
	if executionTemplate == nil {
		return nil, errors.New("execution template is required")
	}
	if run == nil {
		return nil, errors.New("member execution runner is required")
	}
	if err := ensureMemberTargetAvailableFromLegacyFailover(userUUID, member); err != nil {
		return nil, err
	}
	if _, err := EnsureServiceDNSOwnershipAvailable(userUUID, service.ID, service); err != nil {
		return nil, err
	}

	now := time.Now()
	executionTemplate.StartedAt = models.FromTime(now)
	startMessage = strings.TrimSpace(startMessage)
	if startMessage == "" {
		startMessage = fmt.Sprintf("execution started for member %s", memberDisplayLabel(member))
	}
	execution, err := failoverv2db.CreateExecutionForUser(userUUID, service.ID, member.ID, executionTemplate)
	if err != nil {
		return nil, err
	}

	if err := failoverv2db.UpdateServiceFieldsForUser(userUUID, service.ID, map[string]interface{}{
		"last_execution_id": execution.ID,
		"last_status":       models.FailoverV2ServiceStatusRunning,
		"last_message":      startMessage,
	}); err != nil {
		return nil, err
	}
	if err := failoverv2db.UpdateMemberFieldsForUser(userUUID, service.ID, member.ID, map[string]interface{}{
		"last_execution_id": execution.ID,
		"last_status":       models.FailoverV2MemberStatusRunning,
		"last_message":      startMessage,
		"last_triggered_at": models.FromTime(now),
	}); err != nil {
		return nil, err
	}

	go func(serviceCopy models.FailoverV2Service, memberCopy models.FailoverV2Member, executionCopy models.FailoverV2Execution) {
		ctx, cancel := context.WithCancel(context.Background())
		registerExecutionCancel(executionCopy.ID, cancel)
		defer unregisterExecutionCancel(executionCopy.ID)
		runner := &memberExecutionRunner{
			userUUID:  userUUID,
			service:   &serviceCopy,
			member:    &memberCopy,
			execution: &executionCopy,
			ctx:       ctx,
		}
		run(runner)
	}(*service, *member, *execution)

	return execution, nil
}

func queueMemberDetachExecution(userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member, triggerReason, triggerSnapshot, startMessage string) (*models.FailoverV2Execution, error) {
	triggerReason = strings.TrimSpace(triggerReason)
	if triggerReason == "" {
		triggerReason = "manual detach dns"
	}
	return queueMemberExecution(
		userUUID,
		service,
		member,
		&models.FailoverV2Execution{
			Status:          models.FailoverV2ExecutionStatusQueued,
			TriggerReason:   triggerReason,
			TriggerSnapshot: strings.TrimSpace(triggerSnapshot),
			OldClientUUID:   strings.TrimSpace(member.WatchClientUUID),
			OldInstanceRef:  strings.TrimSpace(member.CurrentInstanceRef),
			OldAddresses:    buildMemberOldAddressesSnapshot(member),
			DetachDNSStatus: models.FailoverDNSStatusPending,
			AttachDNSStatus: models.FailoverDNSStatusSkipped,
			CleanupStatus:   models.FailoverCleanupStatusSkipped,
		},
		startMessage,
		func(runner *memberExecutionRunner) {
			runner.runDetachOnly()
		},
	)
}

func RunMemberDetachDNSNowForUser(userUUID string, serviceID, memberID uint) (*models.FailoverV2Execution, error) {
	if err := runPendingFailoverV2CleanupRetries(); err != nil {
		log.Printf("failoverv2: pending cleanup retry failed before dns detach: %v", err)
	}

	service, err := failoverv2db.GetServiceByIDForUser(userUUID, serviceID)
	if err != nil {
		return nil, err
	}
	member, err := findMemberOnService(service, memberID)
	if err != nil {
		return nil, err
	}

	memberLabel := memberDisplayLabel(member)
	startMessage := fmt.Sprintf("manual dns detach started for member %s", memberLabel)
	return queueMemberDetachExecution(userUUID, service, member, "manual detach dns", "", startMessage)
}

func (r *memberExecutionRunner) runDetachOnly() {
	defer func() {
		if recovered := recover(); recovered != nil {
			message := fmt.Sprintf("failover v2 execution panicked: %v", recovered)
			log.Printf("failoverv2: execution %d panicked: %v\n%s", r.execution.ID, recovered, debug.Stack())
			r.failExecution(message)
		}
	}()

	if err := r.checkStopped(); err != nil {
		r.failExecution(err.Error())
		return
	}

	detail := map[string]interface{}{
		"service_id": r.service.ID,
		"member_id":  r.member.ID,
		"dns_lines":  memberLineCodes(r.member),
	}

	detachStep := r.startStep("detach_dns", "Detach Member DNS", detail)
	r.updateActiveExecutionFields("mark execution detaching", map[string]interface{}{
		"status":            models.FailoverV2ExecutionStatusDetachingDNS,
		"detach_dns_status": models.FailoverStepStatusRunning,
	})

	detachResult, err := failoverV2DetachDNSFunc(r.ctx, r.userUUID, r.service, r.member)
	if err != nil {
		err = normalizeExecutionStopError(err)
		r.updateActiveExecutionFields("persist detach failure", map[string]interface{}{
			"detach_dns_status": models.FailoverDNSStatusFailed,
			"detach_dns_result": string(marshalJSON(map[string]interface{}{"error": err.Error()})),
		})
		r.finishStep(detachStep, models.FailoverStepStatusFailed, err.Error(), map[string]interface{}{"error": err.Error()})
		r.failExecution(executionFailureMessage("failed to detach member dns", err))
		return
	}
	if err := r.checkStopped(); err != nil {
		r.failExecution(err.Error())
		return
	}

	r.updateActiveExecutionFields("persist detach result", map[string]interface{}{
		"status":            models.FailoverV2ExecutionStatusVerifyingDetachDNS,
		"detach_dns_status": models.FailoverStepStatusRunning,
		"detach_dns_result": string(marshalJSON(map[string]interface{}{"apply": detachResult})),
	})
	r.finishStep(detachStep, models.FailoverStepStatusSuccess, "member dns detached", detachResult)

	verifyStep := r.startStep("verify_detach_dns", "Verify DNS Detach", detail)
	verification, verifyErr := failoverV2VerifyDetachDNSFunc(r.ctx, r.userUUID, r.service, r.member)
	if verifyErr != nil {
		verifyErr = normalizeExecutionStopError(verifyErr)
		detail := map[string]interface{}{
			"apply":        detachResult,
			"verification": map[string]interface{}{"error": verifyErr.Error()},
		}
		r.updateActiveExecutionFields("persist detach verify error", map[string]interface{}{
			"detach_dns_status": models.FailoverDNSStatusFailed,
			"detach_dns_result": string(marshalJSON(detail)),
		})
		r.finishStep(verifyStep, models.FailoverStepStatusFailed, verifyErr.Error(), detail)
		r.failExecution(executionFailureMessage("failed to verify member dns detach", verifyErr))
		return
	}
	if !dnsVerificationSucceeded(verification) {
		detail := map[string]interface{}{
			"apply":        detachResult,
			"verification": verification,
		}
		message := "member dns detach verification failed"
		r.updateActiveExecutionFields("persist detach verification mismatch", map[string]interface{}{
			"detach_dns_status": models.FailoverDNSStatusFailed,
			"detach_dns_result": string(marshalJSON(detail)),
		})
		r.finishStep(verifyStep, models.FailoverStepStatusFailed, message, detail)
		r.failExecution(message)
		return
	}
	if err := r.checkStopped(); err != nil {
		r.failExecution(err.Error())
		return
	}

	successDetail := map[string]interface{}{
		"apply":        detachResult,
		"verification": verification,
	}
	if !r.updateActiveExecutionFields("persist execution success", map[string]interface{}{
		"status":            models.FailoverV2ExecutionStatusSuccess,
		"detach_dns_status": models.FailoverDNSStatusSuccess,
		"detach_dns_result": string(marshalJSON(successDetail)),
		"attach_dns_status": models.FailoverDNSStatusSkipped,
		"cleanup_status":    models.FailoverCleanupStatusSkipped,
		"finished_at":       models.FromTime(time.Now()),
	}) {
		return
	}
	r.finishStep(verifyStep, models.FailoverStepStatusSuccess, "member dns detach verified", verification)

	memberMessage := fmt.Sprintf("dns detached for lines %s", strings.Join(memberLineCodes(r.member), ", "))
	if err := failoverv2db.UpdateMemberFieldsAndLineRecordRefsForUser(r.userUUID, r.service.ID, r.member.ID, map[string]interface{}{
		"last_execution_id": r.execution.ID,
		"last_status":       models.FailoverV2MemberStatusFailed,
		"last_message":      memberMessage,
		"last_failed_at":    models.FromTime(time.Now()),
	}, map[string]map[string]string{}); err != nil {
		log.Printf("failoverv2: failed to update member %d after dns detach: %v", r.member.ID, err)
	}
	if err := failoverv2db.UpdateServiceFieldsForUser(r.userUUID, r.service.ID, map[string]interface{}{
		"last_execution_id": r.execution.ID,
		"last_status":       models.FailoverV2ServiceStatusRunning,
		"last_message":      fmt.Sprintf("member %s dns detached", memberDisplayLabel(r.member)),
	}); err != nil {
		log.Printf("failoverv2: failed to update service %d after dns detach: %v", r.service.ID, err)
	}
}

func (r *memberExecutionRunner) runDetachDNS() {
	r.runDetachOnly()
}

func (r *memberExecutionRunner) failExecution(message string) {
	now := models.FromTime(time.Now())
	message = strings.TrimSpace(message)
	if message == "" {
		message = "failover v2 execution failed"
	}

	if !r.updateActiveExecutionFields("mark execution failed", map[string]interface{}{
		"status":        models.FailoverV2ExecutionStatusFailed,
		"error_message": message,
		"finished_at":   now,
	}) {
		return
	}
	if err := failoverv2db.UpdateMemberFieldsForUser(r.userUUID, r.service.ID, r.member.ID, map[string]interface{}{
		"last_execution_id": r.execution.ID,
		"last_status":       models.FailoverV2MemberStatusFailed,
		"last_message":      message,
		"last_failed_at":    now,
	}); err != nil {
		log.Printf("failoverv2: failed to update member %d failure state: %v", r.member.ID, err)
	}
	if err := failoverv2db.UpdateServiceFieldsForUser(r.userUUID, r.service.ID, map[string]interface{}{
		"last_execution_id": r.execution.ID,
		"last_status":       models.FailoverV2ServiceStatusFailed,
		"last_message":      message,
	}); err != nil {
		log.Printf("failoverv2: failed to update service %d failure state: %v", r.service.ID, err)
	}
	notifyExecutionFailed(r.userUUID, r.service, r.member, r.execution, message)
}

func (r *memberExecutionRunner) startStep(key, label string, detail interface{}) *models.FailoverV2ExecutionStep {
	r.stepSort++
	step, err := failoverv2db.CreateExecutionStep(&models.FailoverV2ExecutionStep{
		ExecutionID: r.execution.ID,
		Sort:        r.stepSort,
		StepKey:     key,
		StepLabel:   label,
		Status:      models.FailoverStepStatusRunning,
		Detail:      string(marshalJSON(detail)),
		StartedAt:   ptrLocalTime(time.Now()),
	})
	if err != nil {
		log.Printf("failoverv2: failed to create step for execution %d: %v", r.execution.ID, err)
		return nil
	}
	return step
}

func (r *memberExecutionRunner) finishStep(step *models.FailoverV2ExecutionStep, status, message string, detail interface{}) {
	if step == nil {
		return
	}
	if _, err := failoverv2db.UpdateRunningExecutionStepFields(step.ID, map[string]interface{}{
		"status":      status,
		"message":     strings.TrimSpace(message),
		"detail":      string(marshalJSON(detail)),
		"finished_at": models.FromTime(time.Now()),
	}); err != nil {
		log.Printf("failoverv2: failed to update step %d: %v", step.ID, err)
	}
}

func (r *memberExecutionRunner) updateActiveExecutionFields(action string, fields map[string]interface{}) bool {
	if r == nil || r.execution == nil {
		return false
	}
	updated, err := failoverv2db.UpdateActiveExecutionFields(r.execution.ID, fields)
	if err != nil {
		log.Printf("failoverv2: failed to %s for execution %d: %v", strings.TrimSpace(action), r.execution.ID, err)
		return false
	}
	return updated
}

func findMemberOnService(service *models.FailoverV2Service, memberID uint) (*models.FailoverV2Member, error) {
	if service == nil {
		return nil, errors.New("service is required")
	}
	for index := range service.Members {
		if service.Members[index].ID == memberID {
			return &service.Members[index], nil
		}
	}
	return nil, gorm.ErrRecordNotFound
}

func memberDisplayLabel(member *models.FailoverV2Member) string {
	if member == nil {
		return "unknown"
	}
	return firstNonEmpty(strings.TrimSpace(member.Name), firstMemberLineCode(member), fmt.Sprintf("#%d", member.ID))
}

func claimServiceRun(serviceID uint) bool {
	activeServiceRunsMu.Lock()
	defer activeServiceRunsMu.Unlock()
	if _, exists := activeServiceRuns[serviceID]; exists {
		return false
	}
	activeServiceRuns[serviceID] = struct{}{}
	return true
}

func releaseServiceRun(serviceID uint) {
	activeServiceRunsMu.Lock()
	delete(activeServiceRuns, serviceID)
	activeServiceRunsMu.Unlock()
}

func ptrLocalTime(value time.Time) *models.LocalTime {
	local := models.FromTime(value)
	return &local
}
