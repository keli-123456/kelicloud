package failoverv2

import (
	"fmt"
	"log"
	"strings"

	failoverv2db "github.com/komari-monitor/komari/database/failoverv2"
	"github.com/komari-monitor/komari/database/models"
)

type pendingCleanupSyncMode string

const (
	pendingCleanupSyncModeRetry        pendingCleanupSyncMode = "retry"
	pendingCleanupSyncModeResolved     pendingCleanupSyncMode = "resolved"
	pendingCleanupSyncModeManualReview pendingCleanupSyncMode = "manual_review"
)

func loadPendingCleanupActionTargetForUser(userUUID string, serviceID, cleanupID uint) (*models.FailoverV2Service, *models.FailoverV2PendingCleanup, func(), error) {
	service, err := failoverv2db.GetServiceByIDForUser(userUUID, serviceID)
	if err != nil {
		return nil, nil, nil, err
	}
	serviceRunLock, err := claimServiceRunLock(service.ID, failoverV2PendingCleanupRunLockTTL)
	if err != nil {
		return nil, nil, nil, err
	}

	release := func() {
		serviceRunLock.release()
	}
	if hasActive, err := failoverv2db.HasActiveExecutionForService(userUUID, service.ID); err != nil {
		release()
		return nil, nil, nil, err
	} else if hasActive {
		release()
		return nil, nil, nil, fmt.Errorf("another failover v2 execution is still active for this service")
	}

	item, err := failoverv2db.GetPendingCleanupByIDForUser(userUUID, service.ID, cleanupID)
	if err != nil {
		release()
		return nil, nil, nil, err
	}
	return service, item, release, nil
}

func RetryPendingCleanupForUser(userUUID string, serviceID, cleanupID uint) (*models.FailoverV2PendingCleanup, error) {
	service, item, release, err := loadPendingCleanupActionTargetForUser(userUUID, serviceID, cleanupID)
	if err != nil {
		return nil, err
	}
	defer release()
	if strings.TrimSpace(item.Status) == models.FailoverV2PendingCleanupStatusSucceeded {
		return item, nil
	}
	if strings.TrimSpace(item.Status) == models.FailoverV2PendingCleanupStatusRunning {
		return nil, fmt.Errorf("pending cleanup %d is already running", item.ID)
	}

	if claimed, err := claimPendingFailoverV2CleanupItemForRetry(item.ID, "pending cleanup retry started by operator"); err != nil {
		return nil, err
	} else if !claimed {
		return nil, fmt.Errorf("pending cleanup %d is already running", item.ID)
	}
	defer releasePendingFailoverV2CleanupItem(item.ID)

	if err := retryPendingFailoverV2Cleanup(*item); err != nil {
		return nil, err
	}

	updated, err := failoverv2db.GetPendingCleanupByIDForUser(userUUID, service.ID, cleanupID)
	if err != nil {
		return nil, err
	}
	syncPendingCleanupActionResult(updated, pendingCleanupSyncModeRetry)
	var member *models.FailoverV2Member
	if updated.MemberID > 0 {
		member, _ = findMemberOnService(service, updated.MemberID)
	}
	notifyPendingCleanupActionCompleted(
		"pending cleanup retry completed",
		updated,
		service,
		member,
		fmt.Sprintf("pending cleanup retry finished with status %s", strings.TrimSpace(updated.Status)),
	)
	if strings.TrimSpace(updated.Status) == models.FailoverV2PendingCleanupStatusManualReview {
		notifyPendingCleanupManualReview(updated, service, member, updated.LastError)
	}
	return updated, nil
}

func QueuePendingCleanupRetryForUser(userUUID string, serviceID, cleanupID uint) (*models.FailoverV2PendingCleanup, error) {
	service, item, release, err := loadPendingCleanupActionTargetForUser(userUUID, serviceID, cleanupID)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(item.Status) == models.FailoverV2PendingCleanupStatusSucceeded ||
		strings.TrimSpace(item.Status) == models.FailoverV2PendingCleanupStatusRunning {
		release()
		return item, nil
	}

	if claimed, err := claimPendingFailoverV2CleanupItemForRetry(item.ID, "pending cleanup retry queued"); err != nil {
		release()
		return nil, err
	} else if !claimed {
		release()
		updated, loadErr := failoverv2db.GetPendingCleanupByIDForUser(userUUID, service.ID, cleanupID)
		if loadErr != nil {
			return nil, loadErr
		}
		return updated, nil
	}

	queued, err := failoverv2db.GetPendingCleanupByIDForUser(userUUID, service.ID, cleanupID)
	if err != nil {
		releasePendingFailoverV2CleanupItem(item.ID)
		release()
		return nil, err
	}

	go runQueuedPendingCleanupRetry(userUUID, service, cleanupID, item.ID, release)
	return queued, nil
}

func runQueuedPendingCleanupRetry(userUUID string, service *models.FailoverV2Service, cleanupID, itemID uint, releaseService func()) {
	defer releaseService()
	defer releasePendingFailoverV2CleanupItem(itemID)

	item, err := failoverv2db.GetPendingCleanupByIDForUser(userUUID, service.ID, cleanupID)
	if err != nil {
		log.Printf("failoverv2: failed to load queued pending cleanup %d: %v", cleanupID, err)
		return
	}
	if err := retryPendingFailoverV2Cleanup(*item); err != nil {
		log.Printf("failoverv2: queued pending cleanup %d failed: %v", item.ID, err)
	}

	updated, err := failoverv2db.GetPendingCleanupByIDForUser(userUUID, service.ID, cleanupID)
	if err != nil {
		log.Printf("failoverv2: failed to reload queued pending cleanup %d: %v", cleanupID, err)
		return
	}
	syncPendingCleanupActionResult(updated, pendingCleanupSyncModeRetry)
	var member *models.FailoverV2Member
	if updated.MemberID > 0 {
		member, _ = findMemberOnService(service, updated.MemberID)
	}
	notifyPendingCleanupActionCompleted(
		"pending cleanup retry completed",
		updated,
		service,
		member,
		fmt.Sprintf("pending cleanup retry finished with status %s", strings.TrimSpace(updated.Status)),
	)
	if strings.TrimSpace(updated.Status) == models.FailoverV2PendingCleanupStatusManualReview {
		notifyPendingCleanupManualReview(updated, service, member, updated.LastError)
	}
}

func MarkPendingCleanupResolvedForUser(userUUID string, serviceID, cleanupID uint) (*models.FailoverV2PendingCleanup, error) {
	service, item, release, err := loadPendingCleanupActionTargetForUser(userUUID, serviceID, cleanupID)
	if err != nil {
		return nil, err
	}
	defer release()
	if strings.TrimSpace(item.Status) == models.FailoverV2PendingCleanupStatusSucceeded {
		return item, nil
	}
	if strings.TrimSpace(item.Status) == models.FailoverV2PendingCleanupStatusRunning {
		return nil, fmt.Errorf("pending cleanup %d is already running", item.ID)
	}

	if !claimPendingFailoverV2CleanupItem(item.ID) {
		return nil, fmt.Errorf("pending cleanup %d is already running", item.ID)
	}
	defer releasePendingFailoverV2CleanupItem(item.ID)

	if err := failoverv2db.MarkPendingCleanupSucceededIfNotRunning(item.ID); err != nil {
		return nil, err
	}

	updated, err := failoverv2db.GetPendingCleanupByIDForUser(userUUID, serviceID, cleanupID)
	if err != nil {
		return nil, err
	}
	syncPendingCleanupActionResult(updated, pendingCleanupSyncModeResolved)
	var member *models.FailoverV2Member
	if updated.MemberID > 0 {
		member, _ = findMemberOnService(service, updated.MemberID)
	}
	notifyPendingCleanupActionCompleted(
		"pending cleanup marked resolved",
		updated,
		service,
		member,
		"pending cleanup was marked resolved manually",
	)
	return updated, nil
}

func MarkPendingCleanupManualReviewForUser(userUUID string, serviceID, cleanupID uint, reason string) (*models.FailoverV2PendingCleanup, error) {
	service, item, release, err := loadPendingCleanupActionTargetForUser(userUUID, serviceID, cleanupID)
	if err != nil {
		return nil, err
	}
	defer release()
	if strings.TrimSpace(item.Status) == models.FailoverV2PendingCleanupStatusSucceeded {
		return item, nil
	}
	if strings.TrimSpace(item.Status) == models.FailoverV2PendingCleanupStatusRunning {
		return nil, fmt.Errorf("pending cleanup %d is already running", item.ID)
	}

	if !claimPendingFailoverV2CleanupItem(item.ID) {
		return nil, fmt.Errorf("pending cleanup %d is already running", item.ID)
	}
	defer releasePendingFailoverV2CleanupItem(item.ID)

	message := strings.TrimSpace(reason)
	if message == "" {
		message = firstNonEmpty(
			strings.TrimSpace(item.LastError),
			"pending cleanup was marked for manual review by operator",
		)
	}
	if err := failoverv2db.MarkPendingCleanupManualReviewIfNotRunning(item.ID, item.AttemptCount, message); err != nil {
		return nil, err
	}

	updated, err := failoverv2db.GetPendingCleanupByIDForUser(userUUID, serviceID, cleanupID)
	if err != nil {
		return nil, err
	}
	syncPendingCleanupActionResult(updated, pendingCleanupSyncModeManualReview)
	var member *models.FailoverV2Member
	if updated.MemberID > 0 {
		member, _ = findMemberOnService(service, updated.MemberID)
	}
	notifyPendingCleanupManualReview(updated, service, member, message)
	return updated, nil
}

func syncPendingCleanupActionResult(item *models.FailoverV2PendingCleanup, mode pendingCleanupSyncMode) {
	if item == nil {
		return
	}

	cleanupStatus, detail, messageSuffix := buildPendingCleanupActionState(item, mode)
	if item.ExecutionID > 0 {
		fields := map[string]interface{}{
			"cleanup_status": cleanupStatus,
			"cleanup_result": string(marshalJSON(detail)),
		}
		if cleanupStatus == models.FailoverCleanupStatusSuccess {
			fields["error_message"] = ""
		}
		_ = failoverv2db.UpdateExecutionFields(item.ExecutionID, fields)
	}

	if strings.TrimSpace(messageSuffix) != "" {
		syncPendingCleanupLatestMessages(item, messageSuffix)
	}
}

func buildPendingCleanupActionState(item *models.FailoverV2PendingCleanup, mode pendingCleanupSyncMode) (string, map[string]interface{}, string) {
	result := map[string]interface{}{
		"pending_cleanup_id": item.ID,
		"provider":           strings.TrimSpace(item.Provider),
		"resource_type":      strings.TrimSpace(item.ResourceType),
		"resource_id":        strings.TrimSpace(item.ResourceID),
		"cleanup_label":      strings.TrimSpace(item.CleanupLabel),
		"ref":                parseJSONMap(item.InstanceRef),
		"attempt_count":      item.AttemptCount,
	}
	if message := strings.TrimSpace(item.LastError); message != "" {
		result["last_error"] = message
	}
	if item.NextRetryAt != nil {
		result["next_retry_at"] = *item.NextRetryAt
	}
	if item.ResolvedAt != nil {
		result["resolved_at"] = *item.ResolvedAt
	}

	switch mode {
	case pendingCleanupSyncModeResolved:
		result["classification"] = "cleanup_resolved_manually"
		result["summary"] = "pending cleanup was marked resolved manually"
		return models.FailoverCleanupStatusSuccess, result, "old instance cleanup was resolved manually"
	case pendingCleanupSyncModeManualReview:
		result["classification"] = "cleanup_manual_review"
		result["summary"] = "pending cleanup was marked for manual review"
		return models.FailoverCleanupStatusWarning, result, "old instance cleanup requires manual review"
	default:
		switch strings.TrimSpace(item.Status) {
		case models.FailoverV2PendingCleanupStatusSucceeded:
			result["classification"] = "instance_deleted"
			result["summary"] = "old instance cleanup completed via pending cleanup retry"
			return models.FailoverCleanupStatusSuccess, result, "old instance cleanup completed after pending cleanup retry"
		case models.FailoverV2PendingCleanupStatusManualReview:
			result["classification"] = "cleanup_manual_review"
			result["summary"] = "old instance cleanup still requires manual review after retry"
			return models.FailoverCleanupStatusWarning, result, "old instance cleanup still requires manual review"
		default:
			result["classification"] = "cleanup_delete_failed"
			result["summary"] = "old instance cleanup retry failed and remains pending"
			return models.FailoverCleanupStatusFailed, result, "old instance cleanup retry failed and remains pending"
		}
	}
}

func syncPendingCleanupLatestMessages(item *models.FailoverV2PendingCleanup, messageSuffix string) {
	if item == nil || item.ServiceID == 0 || item.ExecutionID == 0 {
		return
	}

	service, err := failoverv2db.GetServiceByIDForUser(item.UserID, item.ServiceID)
	if err != nil {
		return
	}

	var member *models.FailoverV2Member
	if item.MemberID > 0 {
		member, _ = findMemberOnService(service, item.MemberID)
	}

	message := strings.TrimSpace(messageSuffix)
	if member != nil {
		if currentAddress := strings.TrimSpace(member.CurrentAddress); currentAddress != "" {
			message = fmt.Sprintf("failover completed to %s; %s", currentAddress, message)
		}
	}
	if message == "" {
		return
	}

	if service.LastExecutionID != nil && *service.LastExecutionID == item.ExecutionID {
		_ = failoverv2db.UpdateServiceFieldsForUser(item.UserID, service.ID, map[string]interface{}{
			"last_message": message,
		})
	}
	if member != nil && member.LastExecutionID != nil && *member.LastExecutionID == item.ExecutionID {
		_ = failoverv2db.UpdateMemberFieldsForUser(item.UserID, service.ID, member.ID, map[string]interface{}{
			"last_message": message,
		})
	}
}
