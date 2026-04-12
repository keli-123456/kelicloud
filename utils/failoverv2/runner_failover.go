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
	failoverV2GetClientByUUIDFunc = clientdb.GetClientByUUIDForUser
)

const failoverV2BlockedOutletRetryLimit = 3

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

	if memberUsesExistingClient(member) {
		return queueMemberDetachExecution(
			userUUID,
			service,
			member,
			"manual existing_client protection",
			"",
			fmt.Sprintf("manual protection started for member %s", memberDisplayLabel(member)),
		)
	}

	return queueMemberProvisioningFailoverExecution(
		userUUID,
		service,
		member,
		"manual failover",
		"",
		fmt.Sprintf("manual failover started for member %s", memberDisplayLabel(member)),
	)
}

func queueMemberFailoverExecution(userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member, triggerReason, triggerSnapshot, startMessage string) (*models.FailoverV2Execution, error) {
	if memberUsesExistingClient(member) {
		return queueMemberDetachExecution(userUUID, service, member, triggerReason, triggerSnapshot, startMessage)
	}
	return queueMemberProvisioningFailoverExecution(userUUID, service, member, triggerReason, triggerSnapshot, startMessage)
}

func queueMemberProvisioningFailoverExecution(userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member, triggerReason, triggerSnapshot, startMessage string) (*models.FailoverV2Execution, error) {
	if service == nil {
		return nil, errors.New("service is required")
	}
	if member == nil {
		return nil, errors.New("member is required")
	}
	triggerReason = strings.TrimSpace(triggerReason)
	if triggerReason == "" {
		triggerReason = "failover"
	}
	startMessage = strings.TrimSpace(startMessage)
	if startMessage == "" {
		startMessage = fmt.Sprintf("failover started for member %s", memberDisplayLabel(member))
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
			AttachDNSStatus: models.FailoverDNSStatusPending,
			CleanupStatus:   models.FailoverCleanupStatusSkipped,
		},
		startMessage,
		true,
		func(runner *memberExecutionRunner) {
			runner.runProvisioningFailover()
		},
	)
}

func (r *memberExecutionRunner) runProvisioningFailover() {
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
		"dns_lines":  memberLineCodes(r.member),
	}

	detachResult, detachVerification, err := r.detachMemberDNSFlow(baseDetail)
	if err != nil {
		return
	}
	if err := r.checkStopped(); err != nil {
		r.failExecution(err.Error())
		return
	}
	if err := failoverv2db.UpdateMemberFieldsAndLineRecordRefsForUser(r.userUUID, r.service.ID, r.member.ID, map[string]interface{}{
		"last_status":  models.FailoverV2MemberStatusRunning,
		"last_message": "member dns detached and failover is continuing",
	}, map[string]map[string]string{}); err != nil {
		log.Printf("failoverv2: failed to clear member %d dns refs after detach: %v", r.member.ID, err)
	}
	preCleanupStatus, preCleanupResult, preCleanupMessage, err := r.cleanupOldInstanceBeforeProvisionIfRequired()
	if err != nil {
		r.failExecution(executionFailureMessage("failed to delete old instance before provisioning replacement", err))
		return
	}

	var (
		outcome          *memberProvisionOutcome
		targetClientUUID string
		scriptDetail     interface{}
	)
	for attempt := 1; attempt <= failoverV2BlockedOutletRetryLimit; attempt++ {
		outcome, err = r.provisionReplacementInstance()
		if err != nil {
			r.failExecution(executionFailureMessage("failed to provision replacement instance", err))
			return
		}
		if err := r.checkStopped(); err != nil {
			r.failWithRollback(outcome, executionFailureMessage("", err), err)
			return
		}

		targetClientUUID, err = r.waitForReplacementAgent(outcome)
		if err != nil {
			r.failWithRollback(outcome, executionFailureMessage("failed to wait for replacement agent", err), err)
			return
		}
		if err := r.checkStopped(); err != nil {
			r.failWithRollback(outcome, executionFailureMessage("", err), err)
			return
		}

		outletErr := r.validateReplacementOutlet(targetClientUUID)
		if outletErr != nil {
			var blockedErr *blockedOutletError
			if errors.As(outletErr, &blockedErr) && attempt < failoverV2BlockedOutletRetryLimit {
				retryMessage := fmt.Sprintf(
					"replacement outlet blocked on attempt %d/%d; reprovisioning",
					attempt,
					failoverV2BlockedOutletRetryLimit,
				)
				if rollbackErr := r.rollbackProvisionOutcome(outcome, retryMessage, outletErr); rollbackErr != nil {
					r.failExecution(executionFailureMessage("", outletErr) + "; rollback_new failed: " + rollbackErr.Error())
					return
				}
				continue
			}
			r.failWithRollback(outcome, executionFailureMessage("", outletErr), outletErr)
			return
		}
		if err := r.checkStopped(); err != nil {
			r.failWithRollback(outcome, executionFailureMessage("", err), err)
			return
		}

		scriptDetail, err = r.runReplacementScripts(targetClientUUID, outcome)
		if err != nil {
			r.failWithRollback(outcome, executionFailureMessage("", err), err)
			return
		}
		if err := r.checkStopped(); err != nil {
			r.failWithRollback(outcome, executionFailureMessage("", err), err)
			return
		}
		break
	}

	r.mergeOutcomeAddressesFromClient(targetClientUUID, outcome)

	attachResult, attachVerification, err := r.attachMemberDNSFlow(baseDetail, outcome)
	if err != nil {
		r.failExecution(executionFailureMessage("", err))
		return
	}

	finishedAt := models.FromTime(time.Now())
	currentAddress := outcome.primaryAddress()
	cleanupStatus, cleanupResult, cleanupMessage := r.resolveFinalCleanupStatus(outcome, preCleanupStatus, preCleanupResult, preCleanupMessage)
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
	if err := failoverv2db.UpdateMemberFieldsAndLineRecordRefsForUser(r.userUUID, r.service.ID, r.member.ID, map[string]interface{}{
		"watch_client_uuid":    targetClientUUID,
		"current_address":      currentAddress,
		"current_instance_ref": string(marshalJSON(outcome.NewInstanceRef)),
		"last_execution_id":    r.execution.ID,
		"last_status":          models.FailoverV2MemberStatusHealthy,
		"last_message":         memberMessage,
		"last_succeeded_at":    finishedAt,
	}, extractMemberLineRecordRefs(attachResult)); err != nil {
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

func (r *memberExecutionRunner) runFailover() {
	r.runProvisioningFailover()
}

func (r *memberExecutionRunner) mergeOutcomeAddressesFromClient(clientUUID string, outcome *memberProvisionOutcome) {
	if outcome == nil {
		return
	}
	clientUUID = strings.TrimSpace(clientUUID)
	if clientUUID == "" {
		return
	}
	if strings.TrimSpace(outcome.IPv4) != "" && strings.TrimSpace(outcome.IPv6) != "" {
		return
	}

	client, err := failoverV2GetClientByUUIDFunc(clientUUID, r.userUUID)
	if err != nil {
		return
	}

	if strings.TrimSpace(outcome.IPv4) == "" {
		outcome.IPv4 = normalizeIPAddress(client.IPv4)
	}
	if strings.TrimSpace(outcome.IPv6) == "" {
		outcome.IPv6 = normalizeIPAddress(client.IPv6)
	}

	if outcome.NewAddresses == nil {
		outcome.NewAddresses = map[string]interface{}{}
	}
	if _, exists := outcome.NewAddresses["ipv4"]; !exists && strings.TrimSpace(outcome.IPv4) != "" {
		outcome.NewAddresses["ipv4"] = outcome.IPv4
	}
	if _, exists := outcome.NewAddresses["ipv6"]; !exists && strings.TrimSpace(outcome.IPv6) != "" {
		outcome.NewAddresses["ipv6"] = outcome.IPv6
	}
}

func (r *memberExecutionRunner) cleanupOldInstanceBeforeProvisionIfRequired() (string, map[string]interface{}, string, error) {
	provider := strings.ToLower(strings.TrimSpace(r.member.Provider))
	if provider != "digitalocean" && provider != "linode" {
		return "", nil, "", nil
	}

	cleanup, err := failoverV2ResolveOldInstanceCleanupFunc(r.userUUID, r.member)
	if err != nil {
		return "", nil, "", normalizeExecutionStopError(err)
	}
	if cleanup == nil {
		return "", nil, "", errors.New("old instance cleanup requires a resolvable current instance reference")
	}

	step := r.startStep("cleanup_old_before_provision", "Cleanup Old Instance Before Provision", map[string]interface{}{
		"provider": provider,
		"label":    cleanup.Label,
		"ref":      cleanup.Ref,
	})
	r.updateActiveExecutionFields("mark execution cleaning_old_before_provision", map[string]interface{}{
		"status": models.FailoverV2ExecutionStatusCleaningOld,
	})

	if err := normalizeExecutionStopError(cleanup.Cleanup(r.ctx)); err != nil {
		detail := map[string]interface{}{
			"classification": "cleanup_delete_failed_before_provision",
			"summary":        "old instance cleanup failed before replacement provisioning",
			"provider":       provider,
			"ref":            cleanup.Ref,
			"cleanup_label":  cleanup.Label,
			"error":          err.Error(),
		}
		r.finishStep(step, models.FailoverStepStatusFailed, err.Error(), detail)
		return "", nil, "", err
	}

	result := map[string]interface{}{
		"classification": "instance_deleted_before_provision",
		"summary":        "old instance was deleted before provisioning replacement",
		"provider":       provider,
		"ref":            cleanup.Ref,
		"cleanup_label":  cleanup.Label,
	}
	r.finishStep(step, models.FailoverStepStatusSuccess, "old instance deleted before provisioning replacement", result)
	return models.FailoverCleanupStatusSuccess, result, "old instance deleted before provisioning replacement", nil
}

func (r *memberExecutionRunner) resolveFinalCleanupStatus(outcome *memberProvisionOutcome, preStatus string, preResult map[string]interface{}, preMessage string) (string, map[string]interface{}, string) {
	if strings.TrimSpace(preStatus) != "" {
		if len(preResult) == 0 {
			preResult = map[string]interface{}{
				"classification": "instance_deleted_before_provision",
				"summary":        "old instance was deleted before provisioning replacement",
			}
		}
		return preStatus, preResult, strings.TrimSpace(preMessage)
	}
	if outcome != nil && outcome.SkipPostCleanup {
		status := strings.TrimSpace(outcome.CleanupStatus)
		if status == "" {
			status = models.FailoverCleanupStatusSkipped
		}
		result := cloneJSONMap(outcome.CleanupResult)
		if len(result) == 0 {
			result = map[string]interface{}{
				"classification": "cleanup_not_required",
				"summary":        "old instance cleanup was skipped for this execution",
			}
		}
		return status, result, strings.TrimSpace(outcome.CleanupMessage)
	}
	return r.cleanupOldInstanceOnSuccess(outcome)
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
		"provider":             strings.TrimSpace(r.member.Provider),
		"provider_entry_id":    strings.TrimSpace(r.member.ProviderEntryID),
		"provider_entry_group": normalizeProviderEntryGroup(r.member.ProviderEntryGroup),
	}
	step := r.startStep("provision_instance", "Provision Replacement Instance", detail)
	r.updateActiveExecutionFields("mark execution provisioning", map[string]interface{}{
		"status": models.FailoverV2ExecutionStatusProvisioning,
	})

	provisionLock, lockErr := claimMemberProvisionRunLock(r.ctx, r.userUUID, r.service, r.member)
	if lockErr != nil {
		lockErr = normalizeExecutionStopError(lockErr)
		r.finishStep(step, models.FailoverStepStatusFailed, lockErr.Error(), map[string]interface{}{"error": lockErr.Error()})
		return nil, lockErr
	}
	if provisionLock != nil {
		defer provisionLock.release()
	}

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
	targetClientUUID := ""
	if outcome != nil {
		group = strings.TrimSpace(outcome.AutoConnectGroup)
		targetClientUUID = strings.TrimSpace(outcome.TargetClientUUID)
	}
	if group == "" {
		if targetClientUUID != "" {
			step := r.startStep("wait_agent", "Wait For Replacement Agent", map[string]interface{}{
				"client_uuid": targetClientUUID,
				"mode":        "reuse_existing",
			})
			r.updateActiveExecutionFields("mark execution waiting_agent", map[string]interface{}{
				"status": models.FailoverV2ExecutionStatusWaitingAgent,
			})
			r.updateActiveExecutionFields("persist new client uuid", map[string]interface{}{
				"new_client_uuid": targetClientUUID,
			})
			r.finishStep(step, models.FailoverStepStatusSkipped, "reused existing connected agent", map[string]interface{}{
				"client_uuid": targetClientUUID,
				"mode":        "reuse_existing",
			})
			return targetClientUUID, nil
		}
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

func (r *memberExecutionRunner) runReplacementScripts(clientUUID string, outcome *memberProvisionOutcome) (interface{}, error) {
	if outcome != nil && outcome.SkipScripts {
		step := r.startStep("run_scripts", "Run Scripts", map[string]interface{}{
			"client_uuid": clientUUID,
		})
		result := map[string]interface{}{
			"skipped": true,
			"reason":  "replacement reused existing instance",
		}
		r.finishStep(step, models.FailoverStepStatusSkipped, "scripts skipped for reused existing instance", result)
		return result, nil
	}

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

	if err := r.rollbackProvisionOutcome(outcome, message, cause); err != nil {
		r.failExecution(message + "; rollback_new failed: " + err.Error())
		return
	}
	r.failExecution(message)
}

func (r *memberExecutionRunner) rollbackProvisionOutcome(outcome *memberProvisionOutcome, message string, cause error) error {
	if outcome == nil || outcome.Rollback == nil {
		return nil
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
		return err
	}

	r.finishStep(step, models.FailoverStepStatusSuccess, "replacement instance rolled back", map[string]interface{}{
		"label": outcome.RollbackLabel,
	})
	return nil
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
