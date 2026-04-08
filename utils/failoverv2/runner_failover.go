package failoverv2

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"runtime/debug"
	"sort"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/komari-monitor/komari/common"
	"github.com/komari-monitor/komari/config"
	clientdb "github.com/komari-monitor/komari/database/clients"
	clipboarddb "github.com/komari-monitor/komari/database/clipboard"
	failoverv2db "github.com/komari-monitor/komari/database/failoverv2"
	"github.com/komari-monitor/komari/database/models"
	"github.com/komari-monitor/komari/database/tasks"
	"github.com/komari-monitor/komari/utils"
	"github.com/komari-monitor/komari/ws"
	"gorm.io/gorm"
)

var (
	failoverV2DetachDNSFunc       = applyMemberDNSDetach
	failoverV2VerifyDetachDNSFunc = verifyMemberDNSDetached
	failoverV2ProvisionFunc       = provisionMember
	failoverV2AttachDNSFunc       = applyMemberDNSAttach
	failoverV2VerifyAttachDNSFunc = verifyMemberDNSAttached
	failoverV2WaitClientFunc      = waitForClientByGroup
	failoverV2ValidateOutletFunc  = waitForHealthyClientConnectivity
	failoverV2RunScriptsFunc      = runFailoverV2Scripts
)

type commandResult struct {
	TaskID     string
	Output     string
	ExitCode   *int
	FinishedAt *models.LocalTime
	Truncated  bool
}

type blockedOutletError struct {
	ClientUUID string
	Status     string
	Message    string
}

func (e *blockedOutletError) Error() string {
	if e == nil {
		return "new outlet connectivity validation failed"
	}
	parts := make([]string, 0, 3)
	if strings.TrimSpace(e.ClientUUID) != "" {
		parts = append(parts, "client "+strings.TrimSpace(e.ClientUUID))
	}
	if strings.TrimSpace(e.Status) != "" {
		parts = append(parts, "status "+strings.TrimSpace(e.Status))
	}
	if strings.TrimSpace(e.Message) != "" {
		parts = append(parts, strings.TrimSpace(e.Message))
	}
	if len(parts) == 0 {
		return "new outlet connectivity validation failed"
	}
	return "new outlet connectivity validation failed: " + strings.Join(parts, ", ")
}

func RunMemberFailoverNowForUser(userUUID string, serviceID, memberID uint) (*models.FailoverV2Execution, error) {
	if err := runPendingFailoverV2CleanupRetries(); err != nil {
		log.Printf("failoverv2: pending cleanup retry failed before manual failover: %v", err)
	}

	service, err := failoverv2db.GetServiceByIDForUser(userUUID, serviceID)
	if err != nil {
		return nil, err
	}
	member, err := findMemberOnService(service, memberID)
	if err != nil {
		return nil, err
	}

	startMessage := fmt.Sprintf("manual failover started for member %s", memberDisplayLabel(member))
	return queueMemberFailoverExecution(
		userUUID,
		service,
		member,
		"manual failover",
		"",
		startMessage,
	)
}

func queueMemberFailoverExecution(userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member, triggerReason, triggerSnapshot, startMessage string) (*models.FailoverV2Execution, error) {
	if service == nil {
		return nil, errors.New("service is required")
	}
	if member == nil {
		return nil, errors.New("member is required")
	}
	if err := ensureMemberTargetAvailableFromLegacyFailover(userUUID, member); err != nil {
		return nil, err
	}
	ownership, err := claimServiceExecutionLocks(userUUID, service)
	if err != nil {
		return nil, err
	}

	recovered, err := failoverv2db.RecoverInterruptedExecutionsForService(userUUID, service.ID, interruptedExecutionMessage)
	if err != nil {
		releaseServiceExecutionLocks(service.ID, ownership)
		return nil, err
	}
	if recovered > 0 {
		log.Printf("failoverv2: recovered %d interrupted execution(s) for service %d", recovered, service.ID)
	}

	active, err := failoverv2db.HasActiveExecutionForService(userUUID, service.ID)
	if err != nil {
		releaseServiceExecutionLocks(service.ID, ownership)
		return nil, err
	}
	if active {
		releaseServiceExecutionLocks(service.ID, ownership)
		return nil, fmt.Errorf("failover v2 service %d already has an active execution", service.ID)
	}

	now := time.Now()
	triggerReason = strings.TrimSpace(triggerReason)
	if triggerReason == "" {
		triggerReason = "failover"
	}
	startMessage = strings.TrimSpace(startMessage)
	if startMessage == "" {
		startMessage = fmt.Sprintf("failover started for member %s", memberDisplayLabel(member))
	}
	execution, err := failoverv2db.CreateExecutionForUser(userUUID, service.ID, member.ID, &models.FailoverV2Execution{
		Status:          models.FailoverV2ExecutionStatusQueued,
		TriggerReason:   triggerReason,
		TriggerSnapshot: strings.TrimSpace(triggerSnapshot),
		OldClientUUID:   strings.TrimSpace(member.WatchClientUUID),
		OldInstanceRef:  strings.TrimSpace(member.CurrentInstanceRef),
		OldAddresses:    string(marshalJSON(map[string]interface{}{"current_address": strings.TrimSpace(member.CurrentAddress)})),
		DetachDNSStatus: models.FailoverDNSStatusPending,
		AttachDNSStatus: models.FailoverDNSStatusPending,
		CleanupStatus:   models.FailoverCleanupStatusSkipped,
		StartedAt:       models.FromTime(now),
	})
	if err != nil {
		releaseServiceExecutionLocks(service.ID, ownership)
		return nil, err
	}

	if err := failoverv2db.UpdateServiceFieldsForUser(userUUID, service.ID, map[string]interface{}{
		"last_execution_id": execution.ID,
		"last_status":       models.FailoverV2ServiceStatusRunning,
		"last_message":      startMessage,
	}); err != nil {
		releaseServiceExecutionLocks(service.ID, ownership)
		return nil, err
	}
	if err := failoverv2db.UpdateMemberFieldsForUser(userUUID, service.ID, member.ID, map[string]interface{}{
		"last_execution_id": execution.ID,
		"last_status":       models.FailoverV2MemberStatusRunning,
		"last_message":      startMessage,
		"last_triggered_at": models.FromTime(now),
	}); err != nil {
		releaseServiceExecutionLocks(service.ID, ownership)
		return nil, err
	}

	go func(serviceCopy models.FailoverV2Service, memberCopy models.FailoverV2Member, executionCopy models.FailoverV2Execution, ownershipCopy ServiceDNSOwnership) {
		defer releaseServiceExecutionLocks(serviceCopy.ID, &ownershipCopy)
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
		runner.runFailover()
	}(*service, *member, *execution, *ownership)

	return execution, nil
}

func (r *memberExecutionRunner) runFailover() {
	defer func() {
		if recovered := recover(); recovered != nil {
			message := fmt.Sprintf("failover v2 execution panicked: %v", recovered)
			log.Printf("failoverv2: execution %d panicked: %v\n%s", r.execution.ID, recovered, debugStack())
			r.failExecution(message)
		}
	}()

	if err := r.checkStopped(); err != nil {
		r.failExecution(err.Error())
		return
	}

	baseDetail := map[string]interface{}{
		"service_id": r.service.ID,
		"member_id":  r.member.ID,
		"dns_line":   strings.TrimSpace(r.member.DNSLine),
	}

	detachResult, detachVerification, err := r.detachMemberDNSFlow(baseDetail)
	if err != nil {
		return
	}
	if err := r.checkStopped(); err != nil {
		r.failExecution(err.Error())
		return
	}
	if err := failoverv2db.UpdateMemberFieldsForUser(r.userUUID, r.service.ID, r.member.ID, map[string]interface{}{
		"dns_record_refs": "{}",
		"last_status":     models.FailoverV2MemberStatusRunning,
		"last_message":    "member dns detached and failover is continuing",
	}); err != nil {
		log.Printf("failoverv2: failed to clear member %d dns refs after detach: %v", r.member.ID, err)
	}

	outcome, err := r.provisionReplacementInstance()
	if err != nil {
		r.failExecution(executionFailureMessage("failed to provision replacement instance", err))
		return
	}
	if err := r.checkStopped(); err != nil {
		r.failWithRollback(outcome, executionFailureMessage("", err), err)
		return
	}

	targetClientUUID, err := r.waitForReplacementAgent(outcome)
	if err != nil {
		r.failWithRollback(outcome, executionFailureMessage("failed to wait for replacement agent", err), err)
		return
	}
	if err := r.checkStopped(); err != nil {
		r.failWithRollback(outcome, executionFailureMessage("", err), err)
		return
	}

	if err := r.validateReplacementOutlet(targetClientUUID); err != nil {
		r.failWithRollback(outcome, executionFailureMessage("", err), err)
		return
	}
	if err := r.checkStopped(); err != nil {
		r.failWithRollback(outcome, executionFailureMessage("", err), err)
		return
	}

	scriptDetail, err := r.runReplacementScripts(targetClientUUID)
	if err != nil {
		r.failWithRollback(outcome, executionFailureMessage("", err), err)
		return
	}
	if err := r.checkStopped(); err != nil {
		r.failWithRollback(outcome, executionFailureMessage("", err), err)
		return
	}

	attachResult, attachVerification, err := r.attachMemberDNSFlow(baseDetail, outcome)
	if err != nil {
		r.failExecution(executionFailureMessage("", err))
		return
	}

	finishedAt := models.FromTime(time.Now())
	currentAddress := outcome.primaryAddress()
	cleanupStatus, cleanupResult, cleanupMessage := r.cleanupOldInstanceOnSuccess(outcome)
	if !r.updateActiveExecutionFields("persist execution success", map[string]interface{}{
		"status":            models.FailoverV2ExecutionStatusSuccess,
		"detach_dns_status": models.FailoverDNSStatusSuccess,
		"detach_dns_result": string(marshalJSON(map[string]interface{}{
			"apply":        detachResult,
			"verification": detachVerification,
		})),
		"new_client_uuid":   targetClientUUID,
		"new_instance_ref":  string(marshalJSON(outcome.NewInstanceRef)),
		"new_addresses":     string(marshalJSON(outcome.NewAddresses)),
		"attach_dns_status": models.FailoverDNSStatusSuccess,
		"attach_dns_result": string(marshalJSON(map[string]interface{}{
			"apply":        attachResult,
			"verification": attachVerification,
			"scripts":      scriptDetail,
		})),
		"cleanup_status": cleanupStatus,
		"cleanup_result": string(marshalJSON(cleanupResult)),
		"finished_at":    finishedAt,
	}) {
		return
	}

	memberMessage := fmt.Sprintf("failover completed to %s", currentAddress)
	if cleanupMessage != "" {
		memberMessage = memberMessage + "; " + cleanupMessage
	}
	if err := failoverv2db.UpdateMemberFieldsForUser(r.userUUID, r.service.ID, r.member.ID, map[string]interface{}{
		"watch_client_uuid":    targetClientUUID,
		"current_address":      currentAddress,
		"current_instance_ref": string(marshalJSON(outcome.NewInstanceRef)),
		"dns_record_refs":      encodeMemberDNSRecordRefs(extractMemberDNSRecordRefs(attachResult)),
		"last_execution_id":    r.execution.ID,
		"last_status":          models.FailoverV2MemberStatusHealthy,
		"last_message":         memberMessage,
		"last_succeeded_at":    finishedAt,
	}); err != nil {
		log.Printf("failoverv2: failed to update member %d after success: %v", r.member.ID, err)
	}
	if err := failoverv2db.UpdateServiceFieldsForUser(r.userUUID, r.service.ID, map[string]interface{}{
		"last_execution_id": r.execution.ID,
		"last_status":       models.FailoverV2ServiceStatusHealthy,
		"last_message":      memberMessage,
	}); err != nil {
		log.Printf("failoverv2: failed to update service %d after success: %v", r.service.ID, err)
	}
}

func (r *memberExecutionRunner) detachMemberDNSFlow(baseDetail map[string]interface{}) (interface{}, interface{}, error) {
	detail := cloneDetailMap(baseDetail)

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
		r.failExecution("failed to detach member dns: " + err.Error())
		return nil, nil, err
	}
	if err := r.checkStopped(); err != nil {
		r.failExecution(err.Error())
		return nil, nil, err
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
		verifyDetail := map[string]interface{}{
			"apply":        detachResult,
			"verification": map[string]interface{}{"error": verifyErr.Error()},
		}
		r.updateActiveExecutionFields("persist detach verify error", map[string]interface{}{
			"detach_dns_status": models.FailoverDNSStatusFailed,
			"detach_dns_result": string(marshalJSON(verifyDetail)),
		})
		r.finishStep(verifyStep, models.FailoverStepStatusFailed, verifyErr.Error(), verifyDetail)
		r.failExecution("failed to verify member dns detach: " + verifyErr.Error())
		return nil, nil, verifyErr
	}
	if !dnsVerificationSucceeded(verification) {
		verifyDetail := map[string]interface{}{
			"apply":        detachResult,
			"verification": verification,
		}
		message := "member dns detach verification failed"
		r.updateActiveExecutionFields("persist detach verification mismatch", map[string]interface{}{
			"detach_dns_status": models.FailoverDNSStatusFailed,
			"detach_dns_result": string(marshalJSON(verifyDetail)),
		})
		r.finishStep(verifyStep, models.FailoverStepStatusFailed, message, verifyDetail)
		r.failExecution(message)
		return nil, nil, errors.New(message)
	}
	if err := r.checkStopped(); err != nil {
		r.failExecution(err.Error())
		return nil, nil, err
	}

	r.finishStep(verifyStep, models.FailoverStepStatusSuccess, "member dns detach verified", verification)
	return detachResult, verification, nil
}

func (r *memberExecutionRunner) provisionReplacementInstance() (*memberProvisionOutcome, error) {
	detail := map[string]interface{}{
		"provider":          strings.TrimSpace(r.member.Provider),
		"provider_entry_id": strings.TrimSpace(r.member.ProviderEntryID),
	}
	step := r.startStep("provision_instance", "Provision Replacement Instance", detail)
	r.updateActiveExecutionFields("mark execution provisioning", map[string]interface{}{
		"status": models.FailoverV2ExecutionStatusProvisioning,
	})

	outcome, err := failoverV2ProvisionFunc(r.ctx, r.userUUID, r.service, r.member)
	if err != nil {
		err = normalizeExecutionStopError(err)
		r.finishStep(step, models.FailoverStepStatusFailed, err.Error(), map[string]interface{}{"error": err.Error()})
		return nil, err
	}

	r.updateActiveExecutionFields("persist provision outcome", map[string]interface{}{
		"new_instance_ref": string(marshalJSON(outcome.NewInstanceRef)),
		"new_addresses":    string(marshalJSON(outcome.NewAddresses)),
	})
	r.finishStep(step, models.FailoverStepStatusSuccess, "replacement instance provisioned", map[string]interface{}{
		"instance_ref":       outcome.NewInstanceRef,
		"addresses":          outcome.NewAddresses,
		"auto_connect_group": outcome.AutoConnectGroup,
	})
	return outcome, nil
}

func (r *memberExecutionRunner) waitForReplacementAgent(outcome *memberProvisionOutcome) (string, error) {
	group := ""
	if outcome != nil {
		group = strings.TrimSpace(outcome.AutoConnectGroup)
	}
	if group == "" {
		return "", errors.New("replacement instance did not provide an auto-connect group")
	}

	step := r.startStep("wait_agent", "Wait For Replacement Agent", map[string]interface{}{
		"group": group,
	})
	r.updateActiveExecutionFields("mark execution waiting_agent", map[string]interface{}{
		"status": models.FailoverV2ExecutionStatusWaitingAgent,
	})

	clientUUID, err := failoverV2WaitClientFunc(
		r.ctx,
		r.userUUID,
		group,
		r.member.WatchClientUUID,
		r.execution.StartedAt.ToTime(),
		r.service.WaitAgentTimeoutSec,
		expectedClientAddresses(outcome),
	)
	if err != nil {
		err = normalizeExecutionStopError(err)
		r.finishStep(step, models.FailoverStepStatusFailed, err.Error(), map[string]interface{}{"group": group, "error": err.Error()})
		return "", err
	}

	r.updateActiveExecutionFields("persist new client uuid", map[string]interface{}{
		"new_client_uuid": clientUUID,
	})
	r.finishStep(step, models.FailoverStepStatusSuccess, "replacement agent connected", map[string]interface{}{
		"group":       group,
		"client_uuid": clientUUID,
	})
	return clientUUID, nil
}

func (r *memberExecutionRunner) validateReplacementOutlet(clientUUID string) error {
	step := r.startStep("validate_outlet", "Validate Replacement Outlet", map[string]interface{}{
		"client_uuid": clientUUID,
	})
	r.updateActiveExecutionFields("mark execution validating_outlet", map[string]interface{}{
		"status": models.FailoverV2ExecutionStatusValidatingOutlet,
	})

	report, err := failoverV2ValidateOutletFunc(r.ctx, r.userUUID, clientUUID, r.execution.StartedAt.ToTime())
	if err != nil {
		err = normalizeExecutionStopError(err)
		detail := map[string]interface{}{
			"client_uuid": clientUUID,
		}
		if report != nil && report.CNConnectivity != nil {
			detail["status"] = strings.TrimSpace(report.CNConnectivity.Status)
			detail["message"] = strings.TrimSpace(report.CNConnectivity.Message)
			detail["checked_at"] = report.CNConnectivity.CheckedAt
			detail["consecutive_failures"] = report.CNConnectivity.ConsecutiveFailures
		}
		r.finishStep(step, models.FailoverStepStatusFailed, err.Error(), detail)
		return err
	}

	detail := map[string]interface{}{
		"client_uuid": clientUUID,
	}
	if report != nil && report.CNConnectivity != nil {
		detail["status"] = strings.TrimSpace(report.CNConnectivity.Status)
		detail["target"] = strings.TrimSpace(report.CNConnectivity.Target)
		detail["latency"] = report.CNConnectivity.Latency
		detail["checked_at"] = report.CNConnectivity.CheckedAt
	}
	r.finishStep(step, models.FailoverStepStatusSuccess, "replacement outlet connectivity looks healthy", detail)
	return nil
}

func (r *memberExecutionRunner) runReplacementScripts(clientUUID string) (interface{}, error) {
	scriptIDs := models.NormalizeFailoverScriptClipboardIDs(nil, r.service.ScriptClipboardIDs)
	step := r.startStep("run_scripts", "Run Scripts", map[string]interface{}{
		"client_uuid":    clientUUID,
		"clipboard_ids":  scriptIDs,
		"script_timeout": r.service.ScriptTimeoutSec,
	})
	if len(scriptIDs) == 0 {
		r.finishStep(step, models.FailoverStepStatusSkipped, "service has no scripts configured", map[string]interface{}{
			"client_uuid": clientUUID,
		})
		return map[string]interface{}{"skipped": true}, nil
	}

	r.updateActiveExecutionFields("mark execution running_scripts", map[string]interface{}{
		"status": models.FailoverV2ExecutionStatusRunningScripts,
	})

	result, err := failoverV2RunScriptsFunc(r.ctx, r.userUUID, clientUUID, r.service)
	if err != nil {
		err = normalizeExecutionStopError(err)
		r.finishStep(step, models.FailoverStepStatusFailed, err.Error(), result)
		return result, err
	}
	r.finishStep(step, models.FailoverStepStatusSuccess, "replacement scripts finished successfully", result)
	return result, nil
}

func (r *memberExecutionRunner) attachMemberDNSFlow(baseDetail map[string]interface{}, outcome *memberProvisionOutcome) (interface{}, interface{}, error) {
	if outcome == nil {
		return nil, nil, errors.New("replacement instance outcome is required")
	}

	attachDetail := cloneDetailMap(baseDetail)
	attachDetail["ipv4"] = outcome.IPv4
	attachDetail["ipv6"] = outcome.IPv6

	attachStep := r.startStep("attach_dns", "Attach Replacement DNS", attachDetail)
	r.updateActiveExecutionFields("mark execution attaching_dns", map[string]interface{}{
		"status":            models.FailoverV2ExecutionStatusAttachingDNS,
		"attach_dns_status": models.FailoverStepStatusRunning,
	})

	attachResult, err := failoverV2AttachDNSFunc(r.ctx, r.userUUID, r.service, r.member, outcome.IPv4, outcome.IPv6)
	if err != nil {
		err = normalizeExecutionStopError(err)
		r.updateActiveExecutionFields("persist attach failure", map[string]interface{}{
			"attach_dns_status": models.FailoverDNSStatusFailed,
			"attach_dns_result": string(marshalJSON(map[string]interface{}{"error": err.Error()})),
		})
		r.finishStep(attachStep, models.FailoverStepStatusFailed, err.Error(), map[string]interface{}{"error": err.Error()})
		return nil, nil, fmt.Errorf("failed to attach replacement dns: %w", err)
	}

	r.updateActiveExecutionFields("persist attach result", map[string]interface{}{
		"status":            models.FailoverV2ExecutionStatusVerifyingAttachDNS,
		"attach_dns_status": models.FailoverStepStatusRunning,
		"attach_dns_result": string(marshalJSON(map[string]interface{}{"apply": attachResult})),
	})
	r.finishStep(attachStep, models.FailoverStepStatusSuccess, "replacement dns attached", attachResult)

	verifyStep := r.startStep("verify_attach_dns", "Verify DNS Attach", attachDetail)
	verification, verifyErr := failoverV2VerifyAttachDNSFunc(r.ctx, r.userUUID, r.service, r.member, outcome.IPv4, outcome.IPv6)
	if verifyErr != nil {
		verifyErr = normalizeExecutionStopError(verifyErr)
		detail := map[string]interface{}{
			"apply":        attachResult,
			"verification": map[string]interface{}{"error": verifyErr.Error()},
		}
		r.updateActiveExecutionFields("persist attach verify error", map[string]interface{}{
			"attach_dns_status": models.FailoverDNSStatusFailed,
			"attach_dns_result": string(marshalJSON(detail)),
		})
		r.finishStep(verifyStep, models.FailoverStepStatusFailed, verifyErr.Error(), detail)
		return nil, nil, fmt.Errorf("failed to verify replacement dns attach: %w", verifyErr)
	}
	if !dnsVerificationSucceeded(verification) {
		detail := map[string]interface{}{
			"apply":        attachResult,
			"verification": verification,
		}
		message := "replacement dns attach verification failed"
		r.updateActiveExecutionFields("persist attach verification mismatch", map[string]interface{}{
			"attach_dns_status": models.FailoverDNSStatusFailed,
			"attach_dns_result": string(marshalJSON(detail)),
		})
		r.finishStep(verifyStep, models.FailoverStepStatusFailed, message, detail)
		return nil, nil, errors.New(message)
	}

	r.finishStep(verifyStep, models.FailoverStepStatusSuccess, "replacement dns attach verified", verification)
	return attachResult, verification, nil
}

func (r *memberExecutionRunner) failWithRollback(outcome *memberProvisionOutcome, message string, cause error) {
	message = strings.TrimSpace(message)
	if outcome == nil || outcome.Rollback == nil {
		r.failExecution(message)
		return
	}

	step := r.startStep("rollback_new", "Cleanup Failed New Instance", map[string]interface{}{
		"label": outcome.RollbackLabel,
		"error": message,
	})
	rollbackCtx := r.ctx
	if errors.Is(normalizeExecutionStopError(cause), errExecutionStopped) {
		rollbackCtx = context.Background()
	}
	if err := normalizeExecutionStopError(outcome.Rollback(rollbackCtx)); err != nil {
		detail := map[string]interface{}{
			"label": outcome.RollbackLabel,
			"error": err.Error(),
		}
		r.finishStep(step, models.FailoverStepStatusFailed, err.Error(), detail)
		r.failExecution(message + "; rollback_new failed: " + err.Error())
		return
	}

	r.finishStep(step, models.FailoverStepStatusSuccess, "replacement instance rolled back", map[string]interface{}{
		"label": outcome.RollbackLabel,
	})
	r.failExecution(message)
}

func runFailoverV2Scripts(ctx context.Context, userUUID, clientUUID string, service *models.FailoverV2Service) (interface{}, error) {
	if service == nil {
		return map[string]interface{}{"scripts": []interface{}{}}, nil
	}

	scriptIDs := models.NormalizeFailoverScriptClipboardIDs(nil, service.ScriptClipboardIDs)
	results := make([]map[string]interface{}, 0, len(scriptIDs))
	if len(scriptIDs) == 0 {
		return map[string]interface{}{
			"scripts": []interface{}{},
			"count":   0,
		}, nil
	}

	timeout := time.Duration(service.ScriptTimeoutSec) * time.Second
	if service.ScriptTimeoutSec <= 0 {
		timeout = 600 * time.Second
	}

	for index, scriptClipboardID := range scriptIDs {
		clipboard, err := clipboarddb.GetClipboardByIDForUser(scriptClipboardID, userUUID)
		if err != nil {
			return map[string]interface{}{
				"scripts": results,
			}, err
		}

		result, err := dispatchScriptToClient(ctx, userUUID, clientUUID, clipboard.Text, timeout)
		err = normalizeExecutionStopError(err)
		result = ensureCommandResult(result)
		item := map[string]interface{}{
			"clipboard_id": clipboard.Id,
			"script_name":  clipboard.Name,
			"index":        index + 1,
			"task_id":      result.TaskID,
			"output":       result.Output,
			"truncated":    result.Truncated,
		}
		if result.ExitCode != nil {
			item["exit_code"] = *result.ExitCode
		}
		if result.FinishedAt != nil {
			item["finished_at"] = result.FinishedAt
		}
		if err != nil {
			item["error"] = err.Error()
			results = append(results, item)
			return map[string]interface{}{
				"scripts": results,
				"count":   len(results),
			}, err
		}
		results = append(results, item)
	}

	return map[string]interface{}{
		"scripts": results,
		"count":   len(results),
	}, nil
}

func dispatchScriptToClient(ctx context.Context, userUUID, clientUUID, command string, timeout time.Duration) (*commandResult, error) {
	command = strings.TrimSpace(command)
	if command == "" {
		return nil, errors.New("script content is empty")
	}

	client := ws.GetConnectedClients()[clientUUID]
	if client == nil {
		return nil, fmt.Errorf("client is offline: %s", clientUUID)
	}

	taskID := utils.GenerateRandomString(16)
	if err := tasks.CreateTaskForUser(userUUID, taskID, []string{clientUUID}); err != nil {
		return nil, err
	}

	payload, err := json.Marshal(map[string]string{
		"message": "exec",
		"command": command,
		"task_id": taskID,
	})
	if err != nil {
		return nil, err
	}

	writeTimeout := 10 * time.Second
	if timeout > 0 && timeout < writeTimeout {
		writeTimeout = timeout
	}
	if err := client.WriteMessageWithDeadline(websocket.TextMessage, payload, time.Now().Add(writeTimeout)); err != nil {
		return &commandResult{TaskID: taskID}, err
	}

	ctx = contextOrBackground(ctx)
	var cancel context.CancelFunc
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, timeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return &commandResult{TaskID: taskID}, normalizeExecutionStopError(ctx.Err())
		case <-ticker.C:
			result, err := tasks.GetSpecificTaskResultForUser(userUUID, taskID, clientUUID)
			if err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					continue
				}
				return &commandResult{TaskID: taskID}, err
			}
			if result.FinishedAt == nil {
				continue
			}
			output, truncated := truncateOutput(result.Result, 65535)
			return &commandResult{
				TaskID:     taskID,
				Output:     output,
				ExitCode:   result.ExitCode,
				FinishedAt: result.FinishedAt,
				Truncated:  truncated,
			}, nil
		}
	}
}

func ensureCommandResult(result *commandResult) *commandResult {
	if result != nil {
		return result
	}
	return &commandResult{}
}

func truncateOutput(output string, limit int) (string, bool) {
	if limit <= 0 || len(output) <= limit {
		return output, false
	}
	return output[:limit], true
}

func waitForClientByGroup(ctx context.Context, userUUID, group, excludeUUID string, startedAt time.Time, timeoutSeconds int, expectedAddresses map[string]struct{}) (string, error) {
	if timeoutSeconds <= 0 {
		timeoutSeconds = 600
	}
	deadline := time.Now().Add(time.Duration(timeoutSeconds) * time.Second)
	group = strings.TrimSpace(group)

	for time.Now().Before(deadline) {
		if err := waitContextOrDelay(ctx, 0); err != nil {
			return "", err
		}

		clientList, err := clientdb.GetAllClientBasicInfoByUser(userUUID)
		if err != nil {
			return "", err
		}
		online := ws.GetConnectedClients()

		candidates := make([]models.Client, 0)
		for _, client := range clientList {
			if strings.TrimSpace(client.Group) != group || client.UUID == excludeUUID {
				continue
			}
			if _, ok := online[client.UUID]; !ok {
				continue
			}
			candidates = append(candidates, client)
		}
		if clientUUID := pickPreferredAutoConnectClient(candidates, startedAt, expectedAddresses); clientUUID != "" {
			return clientUUID, nil
		}

		if err := waitContextOrDelay(ctx, 5*time.Second); err != nil {
			return "", err
		}
	}

	return "", fmt.Errorf("timed out waiting for agent group %q", group)
}

func expectedClientAddresses(outcome *memberProvisionOutcome) map[string]struct{} {
	addresses := make(map[string]struct{})
	if outcome == nil {
		return addresses
	}

	for _, value := range []string{outcome.IPv4, outcome.IPv6, outcome.primaryAddress()} {
		addExpectedClientAddress(addresses, value)
	}
	collectExpectedAddresses(addresses, outcome.NewAddresses)
	return addresses
}

func addExpectedClientAddress(addresses map[string]struct{}, value string) {
	normalized := normalizeIPAddress(value)
	if normalized == "" {
		return
	}
	addresses[normalized] = struct{}{}
}

func collectExpectedAddresses(addresses map[string]struct{}, value interface{}) {
	switch raw := value.(type) {
	case map[string]interface{}:
		for _, nested := range raw {
			collectExpectedAddresses(addresses, nested)
		}
	case []interface{}:
		for _, nested := range raw {
			collectExpectedAddresses(addresses, nested)
		}
	case []string:
		for _, nested := range raw {
			addExpectedClientAddress(addresses, nested)
		}
	case string:
		addExpectedClientAddress(addresses, raw)
	}
}

func clientMatchesExpectedAddress(client models.Client, expectedAddresses map[string]struct{}) bool {
	if len(expectedAddresses) == 0 {
		return false
	}
	for _, value := range []string{client.IPv4, client.IPv6} {
		normalized := normalizeIPAddress(value)
		if normalized == "" {
			continue
		}
		if _, ok := expectedAddresses[normalized]; ok {
			return true
		}
	}
	return false
}

func clientCreatedForExecution(client models.Client, startedAt time.Time) bool {
	createdAt := client.CreatedAt.ToTime()
	if createdAt.IsZero() {
		return false
	}
	return !createdAt.Before(startedAt)
}

func pickPreferredAutoConnectClient(candidates []models.Client, startedAt time.Time, expectedAddresses map[string]struct{}) string {
	if len(candidates) == 0 {
		return ""
	}

	ipMatches := make([]models.Client, 0, len(candidates))
	newClients := make([]models.Client, 0, len(candidates))
	for _, client := range candidates {
		if clientMatchesExpectedAddress(client, expectedAddresses) {
			ipMatches = append(ipMatches, client)
		}
		if clientCreatedForExecution(client, startedAt) {
			newClients = append(newClients, client)
		}
	}

	if len(ipMatches) > 0 {
		sortClientsNewestFirst(ipMatches)
		return ipMatches[0].UUID
	}
	if len(expectedAddresses) > 0 {
		return ""
	}
	if len(newClients) > 0 {
		sortClientsNewestFirst(newClients)
		return newClients[0].UUID
	}
	return ""
}

func sortClientsNewestFirst(clients []models.Client) {
	sort.Slice(clients, func(i, j int) bool {
		return clients[i].CreatedAt.ToTime().After(clients[j].CreatedAt.ToTime())
	})
}

func waitForHealthyClientConnectivity(ctx context.Context, userUUID, clientUUID string, startedAt time.Time) (*common.Report, error) {
	timeout := failoverConnectivityValidationTimeout(userUUID)
	deadline := time.Now().Add(timeout)
	clientUUID = strings.TrimSpace(clientUUID)
	var lastReport *common.Report

	for time.Now().Before(deadline) {
		if err := waitContextOrDelay(ctx, 0); err != nil {
			return nil, err
		}

		report := ws.GetLatestReport()[clientUUID]
		if report != nil && report.CNConnectivity != nil {
			lastReport = cloneReport(report)
			reportTime := report.UpdatedAt
			if report.CNConnectivity.CheckedAt.After(reportTime) {
				reportTime = report.CNConnectivity.CheckedAt
			}
			if reportTime.After(startedAt) || report.CNConnectivity.CheckedAt.After(startedAt) {
				status := strings.ToLower(strings.TrimSpace(report.CNConnectivity.Status))
				switch status {
				case "ok":
					return cloneReport(report), nil
				case "blocked_suspected":
					return cloneReport(report), &blockedOutletError{
						ClientUUID: clientUUID,
						Status:     report.CNConnectivity.Status,
						Message:    report.CNConnectivity.Message,
					}
				}
			}
		}

		if err := waitContextOrDelay(ctx, 5*time.Second); err != nil {
			return nil, err
		}
	}

	if lastReport != nil {
		return lastReport, fmt.Errorf("timed out waiting for healthy connectivity on client %s", clientUUID)
	}
	return nil, fmt.Errorf("timed out waiting for connectivity reports from client %s", clientUUID)
}

func failoverConnectivityValidationTimeout(userUUID string) time.Duration {
	interval, err := config.GetAsForUser[int](userUUID, config.CNConnectivityIntervalKey, 60)
	if err != nil || interval <= 0 {
		interval = 60
	}
	timeoutSeconds := interval*2 + 20
	if timeoutSeconds < 90 {
		timeoutSeconds = 90
	}
	return time.Duration(timeoutSeconds) * time.Second
}

func cloneReport(report *common.Report) *common.Report {
	if report == nil {
		return nil
	}
	payload, err := json.Marshal(report)
	if err != nil {
		return report
	}
	var cloned common.Report
	if err := json.Unmarshal(payload, &cloned); err != nil {
		return report
	}
	return &cloned
}

func cloneDetailMap(detail map[string]interface{}) map[string]interface{} {
	if len(detail) == 0 {
		return map[string]interface{}{}
	}
	cloned := make(map[string]interface{}, len(detail))
	for key, value := range detail {
		cloned[key] = value
	}
	return cloned
}

func debugStack() string {
	return string(debug.Stack())
}
