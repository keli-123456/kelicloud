package failover

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	failoverdb "github.com/komari-monitor/komari/database/failover"
	"github.com/komari-monitor/komari/database/models"
)

var (
	retryDNSApplyFunc       = applyDNSRecord
	retryDNSVerifyFunc      = verifyDNSRecord
	retryCleanupResolveFunc = resolveCurrentInstanceCleanupFromRef
)

type ExecutionActionAvailability struct {
	Available bool
	Reason    string
}

type ExecutionAvailableActions struct {
	RetryDNS     ExecutionActionAvailability
	RetryCleanup ExecutionActionAvailability
}

func DescribeExecutionAvailableActions(task *models.FailoverTask, execution *models.FailoverExecution) ExecutionAvailableActions {
	return ExecutionAvailableActions{
		RetryDNS:     describeRetryDNSAvailability(task, execution),
		RetryCleanup: describeRetryCleanupAvailability(task, execution),
	}
}

func RetryDNSForUser(userUUID string, executionID uint) (*models.FailoverExecution, error) {
	task, execution, runner, err := loadRetryExecutionContext(userUUID, executionID)
	if err != nil {
		return nil, err
	}
	availableActions := DescribeExecutionAvailableActions(task, execution)
	if !availableActions.RetryDNS.Available {
		return nil, errors.New(firstNonEmpty(availableActions.RetryDNS.Reason, "dns retry is not available for this execution"))
	}

	ipv4, ipv6 := executionRetryDNSAddresses(execution)
	previousStatus := normalizeRetryExecutionStatus(execution.Status)
	retryStep := runner.startStep("retry_dns", "Retry DNS", map[string]interface{}{
		"provider":        task.DNSProvider,
		"entry_id":        task.DNSEntryID,
		"previous_status": previousStatus,
		"ipv4":            ipv4,
		"ipv6":            ipv6,
	})

	_ = failoverdb.UpdateExecutionFields(execution.ID, map[string]interface{}{
		"status": models.FailoverExecutionStatusSwitchingDNS,
	})

	dnsResult, err := retryDNSApplyFunc(context.Background(), task.UserID, task.DNSProvider, task.DNSEntryID, task.DNSPayload, ipv4, ipv6)
	if err != nil {
		failedResult := map[string]interface{}{
			"error": err.Error(),
			"retry": true,
		}
		_ = failoverdb.UpdateExecutionFields(execution.ID, map[string]interface{}{
			"status":        previousStatus,
			"dns_status":    models.FailoverDNSStatusFailed,
			"dns_result":    marshalJSON(failedResult),
			"error_message": strings.TrimSpace(err.Error()),
		})
		runner.finishStep(retryStep, models.FailoverStepStatusFailed, err.Error(), failedResult)
		return nil, err
	}

	dnsVerification, verifyErr := retryDNSVerifyFunc(context.Background(), task.UserID, task.DNSProvider, task.DNSEntryID, task.DNSPayload, ipv4, ipv6)
	if dnsResult == nil {
		dnsResult = &dnsUpdateResult{
			Provider: strings.TrimSpace(task.DNSProvider),
		}
	}
	dnsResult.Verification = dnsVerification
	if verifyErr != nil {
		failedResult := map[string]interface{}{
			"error": verifyErr.Error(),
			"retry": true,
		}
		if dnsResult != nil {
			failedResult["applied"] = dnsResult
		}
		if dnsVerification != nil {
			failedResult["verification"] = dnsVerification
		}
		_ = failoverdb.UpdateExecutionFields(execution.ID, map[string]interface{}{
			"status":        previousStatus,
			"dns_status":    models.FailoverDNSStatusFailed,
			"dns_result":    marshalJSON(failedResult),
			"error_message": strings.TrimSpace(verifyErr.Error()),
		})
		runner.finishStep(retryStep, models.FailoverStepStatusFailed, verifyErr.Error(), failedResult)
		return nil, verifyErr
	}

	fields := map[string]interface{}{
		"dns_status": models.FailoverDNSStatusSuccess,
		"dns_result": marshalJSON(dnsResult),
	}
	taskUpdates := map[string]interface{}{}
	now := time.Now()
	if previousStatus == models.FailoverExecutionStatusFailed {
		fields["status"] = models.FailoverExecutionStatusSuccess
		fields["finished_at"] = models.FromTime(now)
		fields["error_message"] = ""
		taskUpdates = buildRetryTaskSuccessFields(task, execution, "failover completed after dns retry", now)
	}
	if len(fields) > 0 {
		_ = failoverdb.UpdateExecutionFields(execution.ID, fields)
	}
	if len(taskUpdates) > 0 {
		_ = failoverdb.UpdateTaskFields(task.ID, taskUpdates)
	}

	runner.finishStep(retryStep, models.FailoverStepStatusSuccess, "dns updated and verified", dnsResult)
	return failoverdb.GetExecutionByIDForUser(userUUID, execution.ID)
}

func RetryCleanupForUser(userUUID string, executionID uint) (*models.FailoverExecution, error) {
	task, execution, runner, err := loadRetryExecutionContext(userUUID, executionID)
	if err != nil {
		return nil, err
	}
	availableActions := DescribeExecutionAvailableActions(task, execution)
	if !availableActions.RetryCleanup.Available {
		return nil, errors.New(firstNonEmpty(availableActions.RetryCleanup.Reason, "cleanup retry is not available for this execution"))
	}
	oldRef := parseJSONMap(execution.OldInstanceRef)
	previousStatus := normalizeRetryExecutionStatus(execution.Status)
	retryStep := runner.startStep("retry_cleanup", "Retry Old Instance Cleanup", map[string]interface{}{
		"previous_status": previousStatus,
		"cleanup_status":  execution.CleanupStatus,
		"instance_ref":    oldRef,
	})
	_ = failoverdb.UpdateExecutionFields(execution.ID, map[string]interface{}{
		"status": models.FailoverExecutionStatusCleaningOld,
	})

	cleanup, err := retryCleanupResolveFunc(context.Background(), task.UserID, oldRef)
	if err != nil {
		_ = failoverdb.UpdateExecutionFields(execution.ID, map[string]interface{}{
			"status": previousStatus,
		})
		runner.finishStep(retryStep, models.FailoverStepStatusFailed, err.Error(), map[string]interface{}{
			"error": err.Error(),
			"retry": true,
		})
		return nil, err
	}

	if cleanup == nil {
		address := firstNonEmpty(primaryExecutionAddress(parseJSONMap(execution.OldAddresses)), task.CurrentAddress)
		cleanup = &currentInstanceCleanup{
			Ref:        cloneJSONMap(oldRef),
			Label:      currentInstanceCleanupLabelFromRef(oldRef, address),
			Assessment: buildUnresolvedCurrentInstanceCleanupAssessment(oldRef, address),
		}
	}

	if cleanup.Assessment != nil && cleanup.Cleanup == nil {
		cleanupStatus := firstNonEmpty(strings.TrimSpace(cleanup.Assessment.Status), models.FailoverCleanupStatusWarning)
		cleanupResult := cloneJSONMap(cleanup.Assessment.Result)
		stepStatus := firstNonEmpty(strings.TrimSpace(cleanup.Assessment.StepStatus), models.FailoverStepStatusSkipped)
		stepMessage := firstNonEmpty(
			strings.TrimSpace(cleanup.Assessment.StepMessage),
			strings.TrimSpace(stringMapValue(cleanupResult, "summary")),
			"old instance cleanup requires manual review",
		)
		_ = failoverdb.UpdateExecutionFields(execution.ID, map[string]interface{}{
			"status":         previousStatus,
			"cleanup_status": cleanupStatus,
			"cleanup_result": marshalJSON(cleanupResult),
		})
		runner.finishStep(retryStep, stepStatus, stepMessage, cleanupResult)
		return failoverdb.GetExecutionByIDForUser(userUUID, execution.ID)
	}

	if cleanup.Cleanup == nil {
		_ = failoverdb.UpdateExecutionFields(execution.ID, map[string]interface{}{
			"status": previousStatus,
		})
		runner.finishStep(retryStep, models.FailoverStepStatusFailed, cleanupStepMessageCleanupStatusUnknown, map[string]interface{}{
			"retry": true,
			"error": "cleanup action is unavailable for the saved instance reference",
		})
		return nil, fmt.Errorf("cleanup action is unavailable for the saved instance reference")
	}

	if err := normalizeExecutionStopError(cleanup.Cleanup(context.Background())); err != nil {
		cleanupStatus := models.FailoverCleanupStatusFailed
		cleanupResult := buildCleanupDeleteFailedResult(cleanup.Ref, cleanup.Label, err)
		if errors.Is(err, errExecutionStopped) {
			cleanupStatus = models.FailoverCleanupStatusWarning
			cleanupResult = buildCleanupInterruptedResult(cleanup.Ref, cleanup.Label, err)
		}
		_ = failoverdb.UpdateExecutionFields(execution.ID, map[string]interface{}{
			"status":         previousStatus,
			"cleanup_status": cleanupStatus,
			"cleanup_result": marshalJSON(cleanupResult),
		})
		runner.finishStep(retryStep, models.FailoverStepStatusFailed, err.Error(), cleanupResult)
		return nil, err
	}

	oldProvider := strings.TrimSpace(stringMapValue(cleanup.Ref, "provider"))
	oldEntryID := strings.TrimSpace(providerEntryIDFromRef(cleanup.Ref))
	if oldProvider != "" && oldEntryID != "" {
		invalidateProviderEntrySnapshot(task.UserID, oldProvider, oldEntryID)
	}
	cleanupResult := buildCleanupDeletedResult(cleanup.Ref, cleanup.Label)
	_ = failoverdb.UpdateExecutionFields(execution.ID, map[string]interface{}{
		"status":         previousStatus,
		"cleanup_status": models.FailoverCleanupStatusSuccess,
		"cleanup_result": marshalJSON(cleanupResult),
	})
	runner.finishStep(retryStep, models.FailoverStepStatusSuccess, "old instance deleted", cleanupResult)
	return failoverdb.GetExecutionByIDForUser(userUUID, execution.ID)
}

func loadRetryExecutionContext(userUUID string, executionID uint) (*models.FailoverTask, *models.FailoverExecution, *executionRunner, error) {
	execution, err := failoverdb.GetExecutionByIDForUser(userUUID, executionID)
	if err != nil {
		return nil, nil, nil, err
	}
	if retryExecutionIsActive(execution.Status) {
		return nil, nil, nil, fmt.Errorf("failover execution %d is still active", executionID)
	}

	task, err := failoverdb.GetTaskByIDForUser(userUUID, execution.TaskID)
	if err != nil {
		return nil, nil, nil, err
	}
	if hasActive, activeErr := failoverdb.HasActiveExecution(task.ID); activeErr != nil {
		return nil, nil, nil, activeErr
	} else if hasActive {
		return nil, nil, nil, fmt.Errorf("another failover execution is still active for this task")
	}

	maxSort := 0
	for _, step := range execution.Steps {
		if step.Sort > maxSort {
			maxSort = step.Sort
		}
	}

	runner := &executionRunner{
		task:      *task,
		execution: execution,
		ctx:       context.Background(),
		stepSort:  maxSort,
	}
	return task, execution, runner, nil
}

func retryExecutionIsActive(status string) bool {
	switch strings.TrimSpace(status) {
	case models.FailoverExecutionStatusQueued,
		models.FailoverExecutionStatusDetecting,
		models.FailoverExecutionStatusProvisioning,
		models.FailoverExecutionStatusRebindingIP,
		models.FailoverExecutionStatusWaitingAgent,
		models.FailoverExecutionStatusRunningScript,
		models.FailoverExecutionStatusSwitchingDNS,
		models.FailoverExecutionStatusCleaningOld:
		return true
	default:
		return false
	}
}

func normalizeRetryExecutionStatus(status string) string {
	status = strings.TrimSpace(status)
	if status == "" {
		return models.FailoverExecutionStatusFailed
	}
	return status
}

func describeRetryDNSAvailability(task *models.FailoverTask, execution *models.FailoverExecution) ExecutionActionAvailability {
	if execution == nil {
		return ExecutionActionAvailability{Reason: "execution is unavailable"}
	}
	if retryExecutionIsActive(execution.Status) {
		return ExecutionActionAvailability{Reason: "execution is still running"}
	}
	if task == nil || strings.TrimSpace(task.DNSProvider) == "" || strings.TrimSpace(task.DNSEntryID) == "" {
		return ExecutionActionAvailability{Reason: "dns switching is not configured for this task"}
	}

	switch strings.TrimSpace(execution.DNSStatus) {
	case models.FailoverDNSStatusFailed:
		ipv4, ipv6 := executionRetryDNSAddresses(execution)
		if ipv4 == "" && ipv6 == "" {
			return ExecutionActionAvailability{Reason: "no saved execution addresses are available for dns retry"}
		}
		return ExecutionActionAvailability{Available: true}
	case models.FailoverDNSStatusSuccess:
		return ExecutionActionAvailability{Reason: "dns already succeeded for this execution"}
	case models.FailoverDNSStatusSkipped:
		return ExecutionActionAvailability{Reason: "dns switching was skipped for this execution"}
	case models.FailoverDNSStatusPending:
		return ExecutionActionAvailability{Reason: "dns retry is only available after a failed dns step"}
	default:
		return ExecutionActionAvailability{Reason: "dns retry is only available after a failed dns step"}
	}
}

func describeRetryCleanupAvailability(task *models.FailoverTask, execution *models.FailoverExecution) ExecutionActionAvailability {
	if execution == nil {
		return ExecutionActionAvailability{Reason: "execution is unavailable"}
	}
	if retryExecutionIsActive(execution.Status) {
		return ExecutionActionAvailability{Reason: "execution is still running"}
	}
	if strings.TrimSpace(execution.DNSStatus) != models.FailoverDNSStatusSuccess {
		return ExecutionActionAvailability{Reason: "dns must succeed before old instance cleanup can be retried"}
	}

	classification := strings.TrimSpace(stringMapValue(parseJSONMap(execution.CleanupResult), "classification"))
	switch classification {
	case cleanupClassificationNotRequested:
		return ExecutionActionAvailability{Reason: "this execution did not require old instance cleanup"}
	case cleanupClassificationInstanceDeleted:
		return ExecutionActionAvailability{Reason: "old instance cleanup already succeeded"}
	case cleanupClassificationInstanceMissing:
		return ExecutionActionAvailability{Reason: "the old instance was already missing"}
	case cleanupClassificationProviderEntryMissing:
		return ExecutionActionAvailability{Reason: "the original cloud credential entry was deleted; manual review is required"}
	case cleanupClassificationProviderEntryUnhealthy:
		return ExecutionActionAvailability{Reason: "the original cloud credential is unavailable; manual review is required"}
	}

	switch strings.TrimSpace(execution.CleanupStatus) {
	case models.FailoverCleanupStatusPending, models.FailoverCleanupStatusFailed, models.FailoverCleanupStatusWarning:
	default:
		if strings.TrimSpace(execution.CleanupStatus) == models.FailoverCleanupStatusSuccess {
			return ExecutionActionAvailability{Reason: "old instance cleanup already succeeded"}
		}
		return ExecutionActionAvailability{Reason: "cleanup retry is only available when cleanup is pending, failed, or needs review"}
	}

	if len(parseJSONMap(execution.OldInstanceRef)) == 0 {
		return ExecutionActionAvailability{Reason: "no saved old instance reference is available for cleanup retry"}
	}
	return ExecutionActionAvailability{Available: true}
}

func executionRetryDNSAddresses(execution *models.FailoverExecution) (string, string) {
	addresses := parseJSONMap(execution.NewAddresses)
	return firstNonEmpty(
			normalizeIPAddress(stringMapValue(addresses, "public_ip")),
			normalizeIPAddress(stringMapValue(addresses, "ipv4")),
		),
		firstNonEmpty(
			normalizeIPAddress(firstString(interfaceSliceToStrings(addresses["ipv6_addresses"]))),
			normalizeIPAddress(stringMapValue(addresses, "ipv6")),
		)
}

func primaryExecutionAddress(addresses map[string]interface{}) string {
	return firstNonEmpty(
		normalizeIPAddress(stringMapValue(addresses, "public_ip")),
		normalizeIPAddress(stringMapValue(addresses, "ipv4")),
		normalizeIPAddress(firstString(interfaceSliceToStrings(addresses["ipv6_addresses"]))),
		normalizeIPAddress(stringMapValue(addresses, "ipv6")),
	)
}

func buildRetryTaskSuccessFields(task *models.FailoverTask, execution *models.FailoverExecution, message string, now time.Time) map[string]interface{} {
	if task == nil || execution == nil || task.LastExecutionID == nil || *task.LastExecutionID != execution.ID {
		return nil
	}

	fields := map[string]interface{}{
		"last_status":           models.FailoverTaskStatusCooldown,
		"last_message":          strings.TrimSpace(message),
		"last_succeeded_at":     models.FromTime(now),
		"trigger_failure_count": 0,
	}
	if nextClientUUID := strings.TrimSpace(firstNonEmpty(execution.NewClientUUID, execution.WatchClientUUID)); nextClientUUID != "" {
		fields["watch_client_uuid"] = nextClientUUID
	}
	if nextAddress := primaryExecutionAddress(parseJSONMap(execution.NewAddresses)); nextAddress != "" {
		fields["current_address"] = nextAddress
	}
	if nextRef := parseJSONMap(firstNonEmpty(execution.NewInstanceRef, execution.OldInstanceRef)); len(nextRef) > 0 {
		fields["current_instance_ref"] = marshalJSON(nextRef)
	}
	return fields
}

func interfaceSliceToStrings(value interface{}) []string {
	switch typed := value.(type) {
	case []string:
		return typed
	case []interface{}:
		result := make([]string, 0, len(typed))
		for _, item := range typed {
			if text := strings.TrimSpace(fmt.Sprintf("%v", item)); text != "" {
				result = append(result, text)
			}
		}
		return result
	default:
		return nil
	}
}
