package failoverv2

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	failoverv2db "github.com/komari-monitor/komari/database/failoverv2"
	"github.com/komari-monitor/komari/database/models"
)

type ExecutionActionAvailability struct {
	Available bool
	Reason    string
}

type ExecutionAvailableActions struct {
	StopExecution  ExecutionActionAvailability
	RetryAttachDNS ExecutionActionAvailability
	RetryCleanup   ExecutionActionAvailability
}

func DescribeExecutionAvailableActions(service *models.FailoverV2Service, member *models.FailoverV2Member, execution *models.FailoverV2Execution) ExecutionAvailableActions {
	return ExecutionAvailableActions{
		StopExecution:  describeStopExecutionAvailability(execution),
		RetryAttachDNS: describeRetryAttachDNSAvailability(service, member, execution),
		RetryCleanup:   describeRetryCleanupAvailability(service, member, execution),
	}
}

func RetryAttachDNSForUser(userUUID string, serviceID, executionID uint) (*models.FailoverV2Execution, error) {
	service, err := failoverv2db.GetServiceByIDForUser(userUUID, serviceID)
	if err != nil {
		return nil, err
	}

	ownership, err := claimServiceExecutionLocks(userUUID, service)
	if err != nil {
		return nil, err
	}
	defer releaseServiceExecutionLocks(service.ID, ownership)

	ctx, err := loadRetryExecutionContext(userUUID, serviceID, executionID)
	if err != nil {
		return nil, err
	}
	availableActions := DescribeExecutionAvailableActions(ctx.service, ctx.member, ctx.execution)
	if !availableActions.RetryAttachDNS.Available {
		return nil, errors.New(firstNonEmpty(availableActions.RetryAttachDNS.Reason, "attach dns retry is not available for this execution"))
	}

	ipv4, ipv6 := retryExecutionAttachAddresses(ctx.execution)
	previousStatus := normalizeRetryExecutionStatus(ctx.execution.Status)
	retryStep := ctx.runner.startStep("retry_attach_dns", "Retry DNS Attach", map[string]interface{}{
		"previous_status": previousStatus,
		"attach_status":   strings.TrimSpace(ctx.execution.AttachDNSStatus),
		"ipv4":            ipv4,
		"ipv6":            ipv6,
	})

	_ = failoverv2db.UpdateExecutionFields(ctx.execution.ID, map[string]interface{}{
		"status":            models.FailoverV2ExecutionStatusAttachingDNS,
		"attach_dns_status": models.FailoverStepStatusRunning,
	})

	attachResult, err := failoverV2AttachDNSFunc(ctx.runner.ctx, userUUID, ctx.service, ctx.member, ipv4, ipv6)
	if err != nil {
		detail := map[string]interface{}{
			"error": err.Error(),
			"retry": true,
		}
		_ = failoverv2db.UpdateExecutionFields(ctx.execution.ID, map[string]interface{}{
			"status":            previousStatus,
			"attach_dns_status": models.FailoverDNSStatusFailed,
			"attach_dns_result": string(marshalJSON(detail)),
			"error_message":     strings.TrimSpace(err.Error()),
		})
		ctx.runner.finishStep(retryStep, models.FailoverStepStatusFailed, err.Error(), detail)
		return nil, err
	}

	verification, verifyErr := failoverV2VerifyAttachDNSFunc(ctx.runner.ctx, userUUID, ctx.service, ctx.member, ipv4, ipv6)
	if verifyErr != nil {
		detail := map[string]interface{}{
			"apply":        attachResult,
			"verification": map[string]interface{}{"error": verifyErr.Error()},
			"retry":        true,
		}
		_ = failoverv2db.UpdateExecutionFields(ctx.execution.ID, map[string]interface{}{
			"status":            previousStatus,
			"attach_dns_status": models.FailoverDNSStatusFailed,
			"attach_dns_result": string(marshalJSON(detail)),
			"error_message":     strings.TrimSpace(verifyErr.Error()),
		})
		ctx.runner.finishStep(retryStep, models.FailoverStepStatusFailed, verifyErr.Error(), detail)
		return nil, verifyErr
	}
	if !dnsVerificationSucceeded(verification) {
		detail := map[string]interface{}{
			"apply":        attachResult,
			"verification": verification,
			"retry":        true,
		}
		message := "replacement dns attach verification failed"
		_ = failoverv2db.UpdateExecutionFields(ctx.execution.ID, map[string]interface{}{
			"status":            previousStatus,
			"attach_dns_status": models.FailoverDNSStatusFailed,
			"attach_dns_result": string(marshalJSON(detail)),
			"error_message":     message,
		})
		ctx.runner.finishStep(retryStep, models.FailoverStepStatusFailed, message, detail)
		return nil, errors.New(message)
	}

	now := time.Now()
	cleanupStatus, cleanupResult := buildRetryAttachCleanupState(ctx.service, ctx.execution)
	attachDetail := map[string]interface{}{
		"apply":        attachResult,
		"verification": verification,
		"retry":        true,
	}

	fields := map[string]interface{}{
		"attach_dns_status": models.FailoverDNSStatusSuccess,
		"attach_dns_result": string(marshalJSON(attachDetail)),
		"error_message":     "",
	}
	if previousStatus == models.FailoverV2ExecutionStatusFailed {
		fields["status"] = models.FailoverV2ExecutionStatusSuccess
		fields["finished_at"] = models.FromTime(now)
	}
	if cleanupStatus != "" {
		fields["cleanup_status"] = cleanupStatus
	}
	if len(cleanupResult) > 0 {
		fields["cleanup_result"] = string(marshalJSON(cleanupResult))
	}
	_ = failoverv2db.UpdateExecutionFields(ctx.execution.ID, fields)

	memberMessage := buildRetryAttachMemberMessage(ctx.execution, cleanupStatus)
	if memberUpdates := buildRetryAttachMemberFields(ctx.member, ctx.execution, attachResult, memberMessage, now); len(memberUpdates) > 0 {
		_ = failoverv2db.UpdateMemberFieldsForUser(userUUID, ctx.service.ID, ctx.member.ID, memberUpdates)
	}
	if serviceUpdates := buildRetryAttachServiceFields(ctx.service, ctx.execution, memberMessage, now); len(serviceUpdates) > 0 {
		_ = failoverv2db.UpdateServiceFieldsForUser(userUUID, ctx.service.ID, serviceUpdates)
	}

	ctx.runner.finishStep(retryStep, models.FailoverStepStatusSuccess, "replacement dns attached and verified after retry", attachDetail)
	updated, err := failoverv2db.GetExecutionByIDForUser(userUUID, ctx.service.ID, ctx.execution.ID)
	if err == nil {
		notifyExecutionActionCompleted(
			"retry attach dns completed",
			ctx.service,
			ctx.member,
			updated,
			"replacement dns attached and verified after retry",
		)
	}
	return updated, err
}

func RetryCleanupForUser(userUUID string, serviceID, executionID uint) (*models.FailoverV2Execution, error) {
	service, err := failoverv2db.GetServiceByIDForUser(userUUID, serviceID)
	if err != nil {
		return nil, err
	}
	serviceRunLock, err := claimServiceRunLock(service.ID, failoverV2PendingCleanupRunLockTTL)
	if err != nil {
		return nil, err
	}
	defer serviceRunLock.release()

	ctx, err := loadRetryExecutionContext(userUUID, serviceID, executionID)
	if err != nil {
		return nil, err
	}
	availableActions := DescribeExecutionAvailableActions(ctx.service, ctx.member, ctx.execution)
	if !availableActions.RetryCleanup.Available {
		return nil, errors.New(firstNonEmpty(availableActions.RetryCleanup.Reason, "cleanup retry is not available for this execution"))
	}

	oldRef := parseJSONMap(ctx.execution.OldInstanceRef)
	previousStatus := normalizeRetryExecutionStatus(ctx.execution.Status)
	retryStep := ctx.runner.startStep("retry_cleanup", "Retry Old Instance Cleanup", map[string]interface{}{
		"previous_status": previousStatus,
		"cleanup_status":  strings.TrimSpace(ctx.execution.CleanupStatus),
		"instance_ref":    oldRef,
	})
	_ = failoverv2db.UpdateExecutionFields(ctx.execution.ID, map[string]interface{}{
		"status": models.FailoverV2ExecutionStatusCleaningOld,
	})

	cleanup, err := failoverV2ResolveOldInstanceCleanupFunc(userUUID, ctx.member)
	if len(oldRef) > 0 {
		cleanup, err = failoverV2ResolveOldInstanceCleanupFromRefFunc(userUUID, oldRef)
	}
	if err != nil {
		_ = failoverv2db.UpdateExecutionFields(ctx.execution.ID, map[string]interface{}{
			"status": previousStatus,
		})
		ctx.runner.finishStep(retryStep, models.FailoverStepStatusFailed, err.Error(), map[string]interface{}{
			"error": err.Error(),
			"retry": true,
		})
		return nil, err
	}
	if cleanup == nil || cleanup.Cleanup == nil {
		message := "cleanup action is unavailable for the saved instance reference"
		result := map[string]interface{}{
			"retry":          true,
			"classification": "cleanup_unavailable",
			"summary":        message,
			"ref":            oldRef,
		}
		_ = failoverv2db.UpdateExecutionFields(ctx.execution.ID, map[string]interface{}{
			"status":         previousStatus,
			"cleanup_status": models.FailoverCleanupStatusWarning,
			"cleanup_result": string(marshalJSON(result)),
		})
		ctx.runner.finishStep(retryStep, models.FailoverStepStatusSkipped, message, result)
		return failoverv2db.GetExecutionByIDForUser(userUUID, ctx.service.ID, ctx.execution.ID)
	}

	if err := runStandalonePendingFailoverV2Cleanup(cleanup); err != nil {
		result := map[string]interface{}{
			"retry":          true,
			"classification": "cleanup_delete_failed",
			"summary":        "old instance cleanup failed during retry",
			"ref":            cleanup.Ref,
			"cleanup_label":  cleanup.Label,
			"error":          err.Error(),
		}
		_ = failoverv2db.UpdateExecutionFields(ctx.execution.ID, map[string]interface{}{
			"status":         previousStatus,
			"cleanup_status": models.FailoverCleanupStatusFailed,
			"cleanup_result": string(marshalJSON(result)),
		})
		ctx.runner.finishStep(retryStep, models.FailoverStepStatusFailed, err.Error(), result)
		return nil, err
	}

	result := map[string]interface{}{
		"retry":          true,
		"classification": "instance_deleted",
		"summary":        "old instance cleanup completed after retry",
		"ref":            cleanup.Ref,
		"cleanup_label":  cleanup.Label,
	}
	_ = failoverv2db.UpdateExecutionFields(ctx.execution.ID, map[string]interface{}{
		"status":         previousStatus,
		"cleanup_status": models.FailoverCleanupStatusSuccess,
		"cleanup_result": string(marshalJSON(result)),
		"error_message":  "",
	})

	if provider, resourceType, resourceID, _ := pendingCleanupIdentityFromRef(cleanup.Ref); provider != "" && resourceType != "" && resourceID != "" {
		_ = failoverv2db.MarkPendingCleanupSucceededByResource(userUUID, provider, resourceType, resourceID)
	}

	message := buildRetryCleanupMessage(ctx.member)
	if memberUpdates := buildRetryCleanupMemberFields(ctx.member, ctx.execution, message, nowLocalTime(nowTime())); len(memberUpdates) > 0 {
		_ = failoverv2db.UpdateMemberFieldsForUser(userUUID, ctx.service.ID, ctx.member.ID, memberUpdates)
	}
	if serviceUpdates := buildRetryCleanupServiceFields(ctx.service, ctx.execution, message); len(serviceUpdates) > 0 {
		_ = failoverv2db.UpdateServiceFieldsForUser(userUUID, ctx.service.ID, serviceUpdates)
	}

	ctx.runner.finishStep(retryStep, models.FailoverStepStatusSuccess, "old instance deleted after retry", result)
	updated, err := failoverv2db.GetExecutionByIDForUser(userUUID, ctx.service.ID, ctx.execution.ID)
	if err == nil {
		notifyExecutionActionCompleted(
			"retry old instance cleanup completed",
			ctx.service,
			ctx.member,
			updated,
			"old instance deleted after retry",
		)
	}
	return updated, err
}

type retryExecutionContext struct {
	service   *models.FailoverV2Service
	member    *models.FailoverV2Member
	execution *models.FailoverV2Execution
	runner    *memberExecutionRunner
}

func loadRetryExecutionContext(userUUID string, serviceID, executionID uint) (*retryExecutionContext, error) {
	service, err := failoverv2db.GetServiceByIDForUser(userUUID, serviceID)
	if err != nil {
		return nil, err
	}
	execution, err := failoverv2db.GetExecutionByIDForUser(userUUID, serviceID, executionID)
	if err != nil {
		return nil, err
	}
	if retryExecutionIsActive(execution.Status) {
		return nil, fmt.Errorf("failover v2 execution %d is still active", executionID)
	}
	member, err := findMemberOnService(service, execution.MemberID)
	if err != nil {
		return nil, err
	}
	if hasActive, activeErr := failoverv2db.HasActiveExecutionForService(userUUID, serviceID); activeErr != nil {
		return nil, activeErr
	} else if hasActive {
		return nil, fmt.Errorf("another failover v2 execution is still active for this service")
	}

	maxSort := 0
	for _, step := range execution.Steps {
		if step.Sort > maxSort {
			maxSort = step.Sort
		}
	}

	return &retryExecutionContext{
		service:   service,
		member:    member,
		execution: execution,
		runner: &memberExecutionRunner{
			userUUID:  userUUID,
			service:   service,
			member:    member,
			execution: execution,
			ctx:       context.Background(),
			stepSort:  maxSort,
		},
	}, nil
}

func retryExecutionIsActive(status string) bool {
	switch strings.TrimSpace(status) {
	case models.FailoverV2ExecutionStatusQueued,
		models.FailoverV2ExecutionStatusDetachingDNS,
		models.FailoverV2ExecutionStatusVerifyingDetachDNS,
		models.FailoverV2ExecutionStatusProvisioning,
		models.FailoverV2ExecutionStatusWaitingAgent,
		models.FailoverV2ExecutionStatusValidatingOutlet,
		models.FailoverV2ExecutionStatusRunningScripts,
		models.FailoverV2ExecutionStatusAttachingDNS,
		models.FailoverV2ExecutionStatusVerifyingAttachDNS,
		models.FailoverV2ExecutionStatusCleaningOld:
		return true
	default:
		return false
	}
}

func normalizeRetryExecutionStatus(status string) string {
	status = strings.TrimSpace(status)
	if status == "" {
		return models.FailoverV2ExecutionStatusFailed
	}
	return status
}

func describeStopExecutionAvailability(execution *models.FailoverV2Execution) ExecutionActionAvailability {
	if execution == nil {
		return ExecutionActionAvailability{Reason: "execution is unavailable"}
	}
	if !retryExecutionIsActive(execution.Status) {
		return ExecutionActionAvailability{Reason: "execution is not running"}
	}
	return ExecutionActionAvailability{Available: true}
}

func describeRetryAttachDNSAvailability(service *models.FailoverV2Service, member *models.FailoverV2Member, execution *models.FailoverV2Execution) ExecutionActionAvailability {
	if execution == nil {
		return ExecutionActionAvailability{Reason: "execution is unavailable"}
	}
	if retryExecutionIsActive(execution.Status) {
		return ExecutionActionAvailability{Reason: "execution is still running"}
	}
	if service == nil || member == nil {
		return ExecutionActionAvailability{Reason: "service or member context is unavailable"}
	}
	if service.LastExecutionID == nil || *service.LastExecutionID != execution.ID {
		return ExecutionActionAvailability{Reason: "attach retry is only available on the latest service execution"}
	}
	if member.LastExecutionID == nil || *member.LastExecutionID != execution.ID {
		return ExecutionActionAvailability{Reason: "attach retry is only available on the latest member execution"}
	}
	if strings.TrimSpace(execution.DetachDNSStatus) != models.FailoverDNSStatusSuccess {
		return ExecutionActionAvailability{Reason: "member dns detach must succeed before attach retry is available"}
	}

	switch strings.TrimSpace(execution.AttachDNSStatus) {
	case models.FailoverDNSStatusFailed:
	case models.FailoverDNSStatusSuccess:
		return ExecutionActionAvailability{Reason: "replacement dns already succeeded for this execution"}
	case models.FailoverDNSStatusSkipped:
		return ExecutionActionAvailability{Reason: "replacement dns was skipped for this execution"}
	default:
		return ExecutionActionAvailability{Reason: "attach retry is only available after a failed dns attach"}
	}

	ipv4, ipv6 := retryExecutionAttachAddresses(execution)
	if ipv4 == "" && ipv6 == "" {
		return ExecutionActionAvailability{Reason: "no saved replacement addresses are available for attach retry"}
	}
	if strings.TrimSpace(execution.NewClientUUID) == "" && len(parseJSONMap(execution.NewInstanceRef)) == 0 {
		return ExecutionActionAvailability{Reason: "replacement instance context is incomplete for attach retry"}
	}
	return ExecutionActionAvailability{Available: true}
}

func describeRetryCleanupAvailability(service *models.FailoverV2Service, member *models.FailoverV2Member, execution *models.FailoverV2Execution) ExecutionActionAvailability {
	if execution == nil {
		return ExecutionActionAvailability{Reason: "execution is unavailable"}
	}
	if retryExecutionIsActive(execution.Status) {
		return ExecutionActionAvailability{Reason: "execution is still running"}
	}
	if service == nil || member == nil {
		return ExecutionActionAvailability{Reason: "service or member context is unavailable"}
	}
	if strings.TrimSpace(execution.AttachDNSStatus) != models.FailoverDNSStatusSuccess {
		return ExecutionActionAvailability{Reason: "replacement dns must succeed before old instance cleanup can be retried"}
	}

	oldRef := parseJSONMap(execution.OldInstanceRef)
	if len(oldRef) == 0 {
		return ExecutionActionAvailability{Reason: "no saved old instance reference is available for cleanup retry"}
	}
	if sameManagedResource(oldRef, resolvedMemberCurrentInstanceRef(member)) {
		return ExecutionActionAvailability{Reason: "the saved old instance now matches the member's current instance"}
	}

	classification := strings.TrimSpace(stringMapValue(parseJSONMap(execution.CleanupResult), "classification"))
	switch classification {
	case "instance_deleted":
		return ExecutionActionAvailability{Reason: "old instance cleanup already succeeded"}
	case "not_requested":
		return ExecutionActionAvailability{Reason: "this execution did not require old instance cleanup"}
	}

	switch strings.TrimSpace(execution.CleanupStatus) {
	case models.FailoverCleanupStatusPending, models.FailoverCleanupStatusFailed, models.FailoverCleanupStatusWarning:
		return ExecutionActionAvailability{Available: true}
	case models.FailoverCleanupStatusSkipped:
		if serviceCleanupRequested(service) {
			return ExecutionActionAvailability{Available: true}
		}
		return ExecutionActionAvailability{Reason: "old instance cleanup was not requested for this service"}
	case models.FailoverCleanupStatusSuccess:
		return ExecutionActionAvailability{Reason: "old instance cleanup already succeeded"}
	default:
		return ExecutionActionAvailability{Reason: "cleanup retry is only available when cleanup is pending, failed, or needs review"}
	}
}

func retryExecutionAttachAddresses(execution *models.FailoverV2Execution) (string, string) {
	addresses := parseJSONMap(execution.NewAddresses)
	return firstNonEmpty(
			normalizeIPAddress(stringMapValue(addresses, "public_ip")),
			normalizeIPAddress(stringMapValue(addresses, "ipv4")),
			extractAddressFromValue(addresses["ipv4"]),
		),
		firstNonEmpty(
			normalizeIPAddress(firstString(interfaceSliceToStrings(addresses["ipv6_addresses"]))),
			normalizeIPAddress(stringMapValue(addresses, "ipv6")),
			extractAddressFromValue(addresses["ipv6"]),
		)
}

func serviceCleanupRequested(service *models.FailoverV2Service) bool {
	if service == nil {
		return false
	}
	switch strings.TrimSpace(service.DeleteStrategy) {
	case models.FailoverDeleteStrategyDeleteAfterSuccess, models.FailoverDeleteStrategyDeleteAfterSuccessDelay:
		return true
	default:
		return false
	}
}

func buildRetryAttachCleanupState(service *models.FailoverV2Service, execution *models.FailoverV2Execution) (string, map[string]interface{}) {
	if execution == nil {
		return "", nil
	}
	if !serviceCleanupRequested(service) || len(parseJSONMap(execution.OldInstanceRef)) == 0 {
		return "", nil
	}
	if strings.TrimSpace(execution.CleanupStatus) != models.FailoverCleanupStatusSkipped {
		return "", nil
	}
	return models.FailoverCleanupStatusPending, map[string]interface{}{
		"classification": "cleanup_pending_after_attach_retry",
		"summary":        "replacement dns succeeded after retry; old instance cleanup is still pending",
		"retry":          true,
		"ref":            parseJSONMap(execution.OldInstanceRef),
	}
}

func buildRetryAttachMemberMessage(execution *models.FailoverV2Execution, cleanupStatus string) string {
	address := retryExecutionPrimaryAddress(execution)
	message := "failover completed after dns attach retry"
	if address != "" {
		message = fmt.Sprintf("failover completed after dns attach retry to %s", address)
	}
	if cleanupStatus == models.FailoverCleanupStatusPending {
		message += "; old instance cleanup is pending"
	}
	return message
}

func buildRetryAttachMemberFields(member *models.FailoverV2Member, execution *models.FailoverV2Execution, attachResult interface{}, message string, now time.Time) map[string]interface{} {
	if member == nil || execution == nil || member.LastExecutionID == nil || *member.LastExecutionID != execution.ID {
		return nil
	}

	fields := map[string]interface{}{
		"last_execution_id": execution.ID,
		"last_status":       models.FailoverV2MemberStatusHealthy,
		"last_message":      strings.TrimSpace(message),
		"last_succeeded_at": models.FromTime(now),
	}
	if nextClientUUID := strings.TrimSpace(execution.NewClientUUID); nextClientUUID != "" {
		fields["watch_client_uuid"] = nextClientUUID
	}
	if nextAddress := retryExecutionPrimaryAddress(execution); nextAddress != "" {
		fields["current_address"] = nextAddress
	}
	if nextRef := parseJSONMap(execution.NewInstanceRef); len(nextRef) > 0 {
		fields["current_instance_ref"] = string(marshalJSON(nextRef))
	}
	if recordRefs := extractMemberDNSRecordRefs(attachResult); len(recordRefs) > 0 {
		fields["dns_record_refs"] = encodeMemberDNSRecordRefs(recordRefs)
	}
	return fields
}

func buildRetryAttachServiceFields(service *models.FailoverV2Service, execution *models.FailoverV2Execution, message string, now time.Time) map[string]interface{} {
	if service == nil || execution == nil || service.LastExecutionID == nil || *service.LastExecutionID != execution.ID {
		return nil
	}
	return map[string]interface{}{
		"last_execution_id": execution.ID,
		"last_status":       models.FailoverV2ServiceStatusHealthy,
		"last_message":      strings.TrimSpace(message),
	}
}

func buildRetryCleanupMessage(member *models.FailoverV2Member) string {
	address := ""
	if member != nil {
		address = strings.TrimSpace(member.CurrentAddress)
	}
	if address == "" {
		return "old instance cleanup completed after retry"
	}
	return fmt.Sprintf("failover completed to %s; old instance cleanup completed after retry", address)
}

func buildRetryCleanupMemberFields(member *models.FailoverV2Member, execution *models.FailoverV2Execution, message string, now models.LocalTime) map[string]interface{} {
	if member == nil || execution == nil || member.LastExecutionID == nil || *member.LastExecutionID != execution.ID {
		return nil
	}
	return map[string]interface{}{
		"last_status":       models.FailoverV2MemberStatusHealthy,
		"last_message":      strings.TrimSpace(message),
		"last_succeeded_at": now,
	}
}

func buildRetryCleanupServiceFields(service *models.FailoverV2Service, execution *models.FailoverV2Execution, message string) map[string]interface{} {
	if service == nil || execution == nil || service.LastExecutionID == nil || *service.LastExecutionID != execution.ID {
		return nil
	}
	return map[string]interface{}{
		"last_status":  models.FailoverV2ServiceStatusHealthy,
		"last_message": strings.TrimSpace(message),
	}
}

func retryExecutionPrimaryAddress(execution *models.FailoverV2Execution) string {
	ipv4, ipv6 := retryExecutionAttachAddresses(execution)
	return firstNonEmpty(ipv4, ipv6)
}

func extractAddressFromValue(value interface{}) string {
	switch typed := value.(type) {
	case string:
		return normalizeIPAddress(typed)
	case []string:
		return normalizeIPAddress(firstString(typed))
	case []interface{}:
		for _, item := range typed {
			if address := extractAddressFromValue(item); address != "" {
				return address
			}
		}
	case map[string]interface{}:
		for _, key := range []string{"ip_address", "address", "ip", "gateway"} {
			if address := normalizeIPAddress(stringMapValue(typed, key)); address != "" {
				return address
			}
		}
	case json.Number:
		return normalizeIPAddress(typed.String())
	}
	return ""
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

func firstString(values []string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func nowTime() time.Time {
	return time.Now()
}

func nowLocalTime(value time.Time) models.LocalTime {
	return models.FromTime(value)
}
