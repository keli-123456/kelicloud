package failover

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/komari-monitor/komari/database/dbcore"
	"github.com/komari-monitor/komari/database/models"
	"gorm.io/gorm"
)

const (
	defaultFailureThreshold              = 2
	defaultStaleAfterSeconds             = 300
	defaultCooldownSeconds               = 0
	defaultProvisionRetryLimit           = models.FailoverProvisionRetryLimitDefault
	defaultProvisionFailureFallbackLimit = models.FailoverProvisionFailureFallbackLimitDefault
	defaultScriptTimeoutSec              = 600
	defaultWaitAgentTimeoutSec           = 600
)

func normalizeFailoverUserID(userUUID string) (string, error) {
	userUUID = strings.TrimSpace(userUUID)
	if userUUID == "" {
		return "", fmt.Errorf("user id is required")
	}
	return userUUID, nil
}

func taskScopeWithDB(db *gorm.DB, userUUID string) *gorm.DB {
	userUUID = strings.TrimSpace(userUUID)
	if userUUID == "" {
		return db.Where("1 = 0")
	}
	return db.Where("user_id = ?", userUUID)
}

func taskStatusForEnabled(enabled bool, current string) string {
	if !enabled {
		return models.FailoverTaskStatusDisabled
	}
	current = strings.TrimSpace(current)
	if current == "" || current == models.FailoverTaskStatusDisabled {
		return models.FailoverTaskStatusUnknown
	}
	return current
}

func applyTaskDefaults(task *models.FailoverTask) {
	task.Name = strings.TrimSpace(task.Name)
	task.WatchClientUUID = strings.TrimSpace(task.WatchClientUUID)
	task.CurrentAddress = strings.TrimSpace(task.CurrentAddress)
	task.CurrentInstanceRef = strings.TrimSpace(task.CurrentInstanceRef)
	if task.TriggerFailureCount < 0 {
		task.TriggerFailureCount = 0
	}
	task.TriggerSource = strings.TrimSpace(task.TriggerSource)
	task.DNSProvider = strings.TrimSpace(task.DNSProvider)
	task.DNSEntryID = strings.TrimSpace(task.DNSEntryID)
	task.DeleteStrategy = strings.TrimSpace(task.DeleteStrategy)

	if task.TriggerSource == "" {
		task.TriggerSource = models.FailoverTriggerSourceCNConnectivity
	}
	if task.FailureThreshold <= 0 {
		task.FailureThreshold = defaultFailureThreshold
	}
	if task.StaleAfterSeconds <= 0 {
		task.StaleAfterSeconds = defaultStaleAfterSeconds
	}
	if task.CooldownSeconds < 0 {
		task.CooldownSeconds = defaultCooldownSeconds
	}
	if task.ProvisionRetryLimit <= 0 {
		task.ProvisionRetryLimit = defaultProvisionRetryLimit
	}
	if task.ProvisionFailureFallbackLimit <= 0 {
		task.ProvisionFailureFallbackLimit = defaultProvisionFailureFallbackLimit
	}
	if strings.TrimSpace(task.DNSPayload) == "" {
		task.DNSPayload = "{}"
	}
	if task.DeleteStrategy == "" {
		task.DeleteStrategy = models.FailoverDeleteStrategyKeep
	}
	task.LastStatus = taskStatusForEnabled(task.Enabled, task.LastStatus)
}

func applyPlanDefaults(plan *models.FailoverPlan) {
	plan.Name = strings.TrimSpace(plan.Name)
	plan.Provider = strings.TrimSpace(plan.Provider)
	plan.ProviderEntryID = strings.TrimSpace(plan.ProviderEntryID)
	plan.ProviderEntryGroup = strings.TrimSpace(plan.ProviderEntryGroup)
	plan.ActionType = strings.TrimSpace(plan.ActionType)
	plan.AutoConnectGroup = strings.TrimSpace(plan.AutoConnectGroup)
	scriptClipboardIDs := models.NormalizeFailoverScriptClipboardIDs(plan.ScriptClipboardID, plan.ScriptClipboardIDs)
	plan.ScriptClipboardID = models.FirstFailoverScriptClipboardID(scriptClipboardIDs)
	plan.ScriptClipboardIDs = models.EncodeFailoverScriptClipboardIDs(scriptClipboardIDs)

	if plan.Priority <= 0 {
		plan.Priority = 1
	}
	if strings.TrimSpace(plan.Payload) == "" {
		plan.Payload = "{}"
	}
	if plan.ScriptTimeoutSec <= 0 {
		plan.ScriptTimeoutSec = defaultScriptTimeoutSec
	}
	if plan.WaitAgentTimeoutSec <= 0 {
		plan.WaitAgentTimeoutSec = defaultWaitAgentTimeoutSec
	}
}

func preloadFailoverTask(db *gorm.DB) *gorm.DB {
	return db.Preload("Plans", func(tx *gorm.DB) *gorm.DB {
		return tx.Order("priority ASC").Order("id ASC")
	})
}

func getTaskByIDForUserWithDB(db *gorm.DB, userUUID string, taskID uint) (*models.FailoverTask, error) {
	userUUID, err := normalizeFailoverUserID(userUUID)
	if err != nil {
		return nil, err
	}

	var task models.FailoverTask
	if err := preloadFailoverTask(taskScopeWithDB(db, userUUID)).
		Where("id = ?", taskID).
		First(&task).Error; err != nil {
		return nil, err
	}
	return &task, nil
}

func GetTaskByIDForUser(userUUID string, taskID uint) (*models.FailoverTask, error) {
	return getTaskByIDForUserWithDB(dbcore.GetDBInstance(), userUUID, taskID)
}

func listTasksByUserWithDB(db *gorm.DB, userUUID string) ([]models.FailoverTask, error) {
	userUUID, err := normalizeFailoverUserID(userUUID)
	if err != nil {
		return nil, err
	}

	var taskList []models.FailoverTask
	if err := preloadFailoverTask(taskScopeWithDB(db, userUUID)).
		Order("id ASC").
		Find(&taskList).Error; err != nil {
		return nil, err
	}
	return taskList, nil
}

func ListTasksByUser(userUUID string) ([]models.FailoverTask, error) {
	return listTasksByUserWithDB(dbcore.GetDBInstance(), userUUID)
}

func CreateTaskForUser(userUUID string, task *models.FailoverTask, plans []models.FailoverPlan) (*models.FailoverTask, error) {
	userUUID, err := normalizeFailoverUserID(userUUID)
	if err != nil {
		return nil, err
	}
	if task == nil {
		return nil, fmt.Errorf("task is required")
	}

	db := dbcore.GetDBInstance()
	var created *models.FailoverTask
	err = db.Transaction(func(tx *gorm.DB) error {
		task.UserID = userUUID
		applyTaskDefaults(task)
		requestedCooldownSeconds := task.CooldownSeconds
		if err := tx.Create(task).Error; err != nil {
			return err
		}
		// GORM may omit zero-valued fields with DB defaults on create.
		// Force cooldown_seconds to the validated value (including 0).
		if err := tx.Model(&models.FailoverTask{}).
			Where("id = ?", task.ID).
			Update("cooldown_seconds", requestedCooldownSeconds).Error; err != nil {
			return err
		}

		if len(plans) > 0 {
			normalizedPlans := make([]models.FailoverPlan, 0, len(plans))
			for _, plan := range plans {
				plan.TaskID = task.ID
				applyPlanDefaults(&plan)
				normalizedPlans = append(normalizedPlans, plan)
			}
			if err := tx.Create(&normalizedPlans).Error; err != nil {
				return err
			}
		}

		loaded, err := getTaskByIDForUserWithDB(tx, userUUID, task.ID)
		if err != nil {
			return err
		}
		created = loaded
		return nil
	})
	if err != nil {
		return nil, err
	}
	return created, nil
}

func UpdateTaskForUser(userUUID string, taskID uint, task *models.FailoverTask, plans []models.FailoverPlan) (*models.FailoverTask, error) {
	userUUID, err := normalizeFailoverUserID(userUUID)
	if err != nil {
		return nil, err
	}
	if task == nil {
		return nil, fmt.Errorf("task is required")
	}

	db := dbcore.GetDBInstance()
	var updated *models.FailoverTask
	err = db.Transaction(func(tx *gorm.DB) error {
		existing, err := getTaskByIDForUserWithDB(tx, userUUID, taskID)
		if err != nil {
			return err
		}

		applyTaskDefaults(task)
		updates := map[string]interface{}{
			"name":                             task.Name,
			"enabled":                          task.Enabled,
			"watch_client_uuid":                strings.TrimSpace(existing.WatchClientUUID),
			"current_address":                  strings.TrimSpace(existing.CurrentAddress),
			"current_instance_ref":             strings.TrimSpace(existing.CurrentInstanceRef),
			"trigger_failure_count":            existing.TriggerFailureCount,
			"trigger_source":                   task.TriggerSource,
			"failure_threshold":                task.FailureThreshold,
			"stale_after_seconds":              task.StaleAfterSeconds,
			"cooldown_seconds":                 task.CooldownSeconds,
			"provision_retry_limit":            task.ProvisionRetryLimit,
			"provision_failure_fallback_limit": task.ProvisionFailureFallbackLimit,
			"dns_provider":                     task.DNSProvider,
			"dns_entry_id":                     task.DNSEntryID,
			"dns_payload":                      task.DNSPayload,
			"delete_strategy":                  task.DeleteStrategy,
			"delete_delay_seconds":             task.DeleteDelaySeconds,
			"last_status":                      taskStatusForEnabled(task.Enabled, existing.LastStatus),
		}
		if task.WatchClientUUID != "" {
			updates["watch_client_uuid"] = task.WatchClientUUID
		}
		if task.CurrentAddress != "" {
			updates["current_address"] = task.CurrentAddress
		}
		if task.CurrentInstanceRef != "" {
			updates["current_instance_ref"] = task.CurrentInstanceRef
		}
		if err := taskScopeWithDB(tx.Model(&models.FailoverTask{}), userUUID).
			Where("id = ?", taskID).
			Updates(updates).Error; err != nil {
			return err
		}

		if err := tx.Where("task_id = ?", taskID).Delete(&models.FailoverPlan{}).Error; err != nil {
			return err
		}
		if len(plans) > 0 {
			normalizedPlans := make([]models.FailoverPlan, 0, len(plans))
			for _, plan := range plans {
				plan.TaskID = taskID
				applyPlanDefaults(&plan)
				normalizedPlans = append(normalizedPlans, plan)
			}
			if err := tx.Create(&normalizedPlans).Error; err != nil {
				return err
			}
		}

		loaded, err := getTaskByIDForUserWithDB(tx, userUUID, taskID)
		if err != nil {
			return err
		}
		updated = loaded
		return nil
	})
	if err != nil {
		return nil, err
	}
	return updated, nil
}

func ToggleTaskForUser(userUUID string, taskID uint, enabled bool) (*models.FailoverTask, error) {
	userUUID, err := normalizeFailoverUserID(userUUID)
	if err != nil {
		return nil, err
	}

	db := dbcore.GetDBInstance()
	var updated *models.FailoverTask
	err = db.Transaction(func(tx *gorm.DB) error {
		existing, err := getTaskByIDForUserWithDB(tx, userUUID, taskID)
		if err != nil {
			return err
		}

		if err := taskScopeWithDB(tx.Model(&models.FailoverTask{}), userUUID).
			Where("id = ?", taskID).
			Updates(map[string]interface{}{
				"enabled":     enabled,
				"last_status": taskStatusForEnabled(enabled, existing.LastStatus),
			}).Error; err != nil {
			return err
		}

		loaded, err := getTaskByIDForUserWithDB(tx, userUUID, taskID)
		if err != nil {
			return err
		}
		updated = loaded
		return nil
	})
	if err != nil {
		return nil, err
	}
	return updated, nil
}

func DeleteTaskForUser(userUUID string, taskID uint) error {
	userUUID, err := normalizeFailoverUserID(userUUID)
	if err != nil {
		return err
	}

	db := dbcore.GetDBInstance()
	result := taskScopeWithDB(db, userUUID).Where("id = ?", taskID).Delete(&models.FailoverTask{})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}
	return nil
}

func ListExecutionsByTaskForUser(userUUID string, taskID uint, limit int) ([]models.FailoverExecution, error) {
	userUUID, err := normalizeFailoverUserID(userUUID)
	if err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}

	db := dbcore.GetDBInstance()
	if _, err := getTaskByIDForUserWithDB(db, userUUID, taskID); err != nil {
		return nil, err
	}

	var executions []models.FailoverExecution
	if err := db.Where("task_id = ?", taskID).
		Order("started_at DESC").
		Order("id DESC").
		Limit(limit).
		Find(&executions).Error; err != nil {
		return nil, err
	}
	return executions, nil
}

func GetExecutionByIDForUser(userUUID string, executionID uint) (*models.FailoverExecution, error) {
	userUUID, err := normalizeFailoverUserID(userUUID)
	if err != nil {
		return nil, err
	}

	db := dbcore.GetDBInstance()
	var execution models.FailoverExecution
	if err := db.Model(&models.FailoverExecution{}).
		Joins("JOIN failover_tasks ON failover_tasks.id = failover_executions.task_id").
		Where("failover_tasks.user_id = ? AND failover_executions.id = ?", userUUID, executionID).
		Preload("Steps", func(tx *gorm.DB) *gorm.DB {
			return tx.Order("sort ASC").Order("id ASC")
		}).
		First(&execution).Error; err != nil {
		return nil, err
	}
	return &execution, nil
}

func stopExecutionForUserWithDB(db *gorm.DB, userUUID string, executionID uint, reason string) (*models.FailoverExecution, error) {
	userUUID, err := normalizeFailoverUserID(userUUID)
	if err != nil {
		return nil, err
	}
	if executionID == 0 {
		return nil, fmt.Errorf("execution id is required")
	}

	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "failover execution stopped by user"
	}

	var updated *models.FailoverExecution
	err = db.Transaction(func(tx *gorm.DB) error {
		var execution models.FailoverExecution
		if err := tx.Model(&models.FailoverExecution{}).
			Joins("JOIN failover_tasks ON failover_tasks.id = failover_executions.task_id").
			Where("failover_tasks.user_id = ? AND failover_executions.id = ?", userUUID, executionID).
			First(&execution).Error; err != nil {
			return err
		}
		if !containsString(activeExecutionStatuses, execution.Status) {
			return fmt.Errorf("failover execution %d is not active", executionID)
		}

		now := time.Now()
		finishedAt := models.FromTime(now)
		if err := tx.Model(&models.FailoverExecution{}).
			Where("id = ?", execution.ID).
			Updates(map[string]interface{}{
				"status":        models.FailoverExecutionStatusFailed,
				"error_message": reason,
				"finished_at":   finishedAt,
			}).Error; err != nil {
			return err
		}

		if err := failRunningStepsWithDB(tx, []uint{execution.ID}, reason, now); err != nil {
			return err
		}

		if err := tx.Model(&models.FailoverTask{}).
			Where("id = ? AND last_execution_id = ?", execution.TaskID, execution.ID).
			Updates(map[string]interface{}{
				"last_status":    models.FailoverTaskStatusFailed,
				"last_message":   reason,
				"last_failed_at": finishedAt,
			}).Error; err != nil {
			return err
		}

		if err := tx.Model(&models.FailoverExecution{}).
			Where("id = ?", execution.ID).
			Preload("Steps", func(stepTx *gorm.DB) *gorm.DB {
				return stepTx.Order("sort ASC").Order("id ASC")
			}).
			First(&execution).Error; err != nil {
			return err
		}

		updated = &execution
		return nil
	})
	if err != nil {
		return nil, err
	}
	return updated, nil
}

func StopExecutionForUser(userUUID string, executionID uint, reason string) (*models.FailoverExecution, error) {
	return stopExecutionForUserWithDB(dbcore.GetDBInstance(), userUUID, executionID, reason)
}

func ListExecutionsByIDs(executionIDs []uint) (map[uint]*models.FailoverExecution, error) {
	normalized := make([]uint, 0, len(executionIDs))
	seen := make(map[uint]struct{}, len(executionIDs))
	for _, executionID := range executionIDs {
		if executionID == 0 {
			continue
		}
		if _, exists := seen[executionID]; exists {
			continue
		}
		seen[executionID] = struct{}{}
		normalized = append(normalized, executionID)
	}

	result := make(map[uint]*models.FailoverExecution, len(normalized))
	if len(normalized) == 0 {
		return result, nil
	}

	var executions []models.FailoverExecution
	db := dbcore.GetDBInstance()
	if err := db.Where("id IN ?", normalized).
		Preload("Steps", func(tx *gorm.DB) *gorm.DB {
			return tx.Order("sort ASC").Order("id ASC")
		}).
		Find(&executions).Error; err != nil {
		return nil, err
	}
	for i := range executions {
		execution := &executions[i]
		result[execution.ID] = execution
	}
	return result, nil
}

var activeExecutionStatuses = []string{
	models.FailoverExecutionStatusQueued,
	models.FailoverExecutionStatusDetecting,
	models.FailoverExecutionStatusProvisioning,
	models.FailoverExecutionStatusRebindingIP,
	models.FailoverExecutionStatusWaitingAgent,
	models.FailoverExecutionStatusRunningScript,
	models.FailoverExecutionStatusSwitchingDNS,
	models.FailoverExecutionStatusCleaningOld,
}

func recoverInterruptedExecutionsWithDB(db *gorm.DB, taskID uint, reason string, now time.Time) (int64, error) {
	if db == nil {
		return 0, fmt.Errorf("db is required")
	}

	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "failover execution was interrupted before completion"
	}

	query := db.Model(&models.FailoverExecution{}).
		Where("status IN ?", activeExecutionStatuses)
	if taskID > 0 {
		query = query.Where("task_id = ?", taskID)
	}

	var executions []models.FailoverExecution
	if err := query.Find(&executions).Error; err != nil {
		return 0, err
	}
	if len(executions) == 0 {
		return 0, nil
	}

	executionIDs := make([]uint, 0, len(executions))
	for _, execution := range executions {
		executionIDs = append(executionIDs, execution.ID)
	}

	finishedAt := models.FromTime(now)
	if err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&models.FailoverExecution{}).
			Where("id IN ?", executionIDs).
			Updates(map[string]interface{}{
				"status":        models.FailoverExecutionStatusFailed,
				"error_message": reason,
				"finished_at":   finishedAt,
			}).Error; err != nil {
			return err
		}

		if err := failRunningStepsWithDB(tx, executionIDs, reason, now); err != nil {
			return err
		}

		for _, execution := range executions {
			if err := tx.Model(&models.FailoverTask{}).
				Where("id = ? AND last_execution_id = ?", execution.TaskID, execution.ID).
				Updates(map[string]interface{}{
					"last_status":    models.FailoverTaskStatusFailed,
					"last_message":   reason,
					"last_failed_at": finishedAt,
				}).Error; err != nil {
				return err
			}
		}

		return nil
	}); err != nil {
		return 0, err
	}

	return int64(len(executions)), nil
}

func RecoverInterruptedExecutions(reason string) (int64, error) {
	return recoverInterruptedExecutionsWithDB(dbcore.GetDBInstance(), 0, reason, time.Now())
}

func RecoverInterruptedExecutionsForTask(taskID uint, reason string) (int64, error) {
	if taskID == 0 {
		return 0, nil
	}
	return recoverInterruptedExecutionsWithDB(dbcore.GetDBInstance(), taskID, reason, time.Now())
}

func ListEnabledTasks() ([]models.FailoverTask, error) {
	db := dbcore.GetDBInstance()
	var taskList []models.FailoverTask
	if err := preloadFailoverTask(db).
		Where("enabled = ?", true).
		Order("id ASC").
		Find(&taskList).Error; err != nil {
		return nil, err
	}
	return taskList, nil
}

func HasActiveExecution(taskID uint) (bool, error) {
	db := dbcore.GetDBInstance()
	var total int64
	if err := db.Model(&models.FailoverExecution{}).
		Where("task_id = ? AND status IN ?", taskID, activeExecutionStatuses).
		Count(&total).Error; err != nil {
		return false, err
	}
	return total > 0, nil
}

func failRunningStepsWithDB(db *gorm.DB, executionIDs []uint, message string, now time.Time) error {
	if db == nil {
		return fmt.Errorf("db is required")
	}
	if len(executionIDs) == 0 {
		return nil
	}

	message = strings.TrimSpace(message)
	if message == "" {
		message = "failover execution failed"
	}

	return db.Model(&models.FailoverExecutionStep{}).
		Where("execution_id IN ? AND status = ?", executionIDs, models.FailoverStepStatusRunning).
		Updates(map[string]interface{}{
			"status":      models.FailoverStepStatusFailed,
			"message":     message,
			"finished_at": models.FromTime(now),
		}).Error
}

func FailRunningStepsForExecution(executionID uint, message string) error {
	if executionID == 0 {
		return nil
	}
	return failRunningStepsWithDB(dbcore.GetDBInstance(), []uint{executionID}, message, time.Now())
}

func containsString(values []string, target string) bool {
	target = strings.TrimSpace(target)
	for _, value := range values {
		if strings.TrimSpace(value) == target {
			return true
		}
	}
	return false
}

func applyPendingCleanupDefaults(cleanup *models.FailoverPendingCleanup) {
	if cleanup == nil {
		return
	}

	cleanup.UserID = strings.TrimSpace(cleanup.UserID)
	cleanup.Provider = strings.TrimSpace(cleanup.Provider)
	cleanup.ProviderEntryID = strings.TrimSpace(cleanup.ProviderEntryID)
	cleanup.ResourceType = strings.TrimSpace(cleanup.ResourceType)
	cleanup.ResourceID = strings.TrimSpace(cleanup.ResourceID)
	cleanup.InstanceRef = strings.TrimSpace(cleanup.InstanceRef)
	cleanup.CleanupLabel = strings.TrimSpace(cleanup.CleanupLabel)
	cleanup.LastError = strings.TrimSpace(cleanup.LastError)
	cleanup.Status = strings.TrimSpace(cleanup.Status)
	if cleanup.Status == "" {
		cleanup.Status = models.FailoverPendingCleanupStatusPending
	}
	if cleanup.AttemptCount < 0 {
		cleanup.AttemptCount = 0
	}
}

func CreateOrUpdatePendingCleanup(cleanup *models.FailoverPendingCleanup) (*models.FailoverPendingCleanup, error) {
	if cleanup == nil {
		return nil, fmt.Errorf("pending cleanup is required")
	}
	if _, err := normalizeFailoverUserID(cleanup.UserID); err != nil {
		return nil, err
	}

	applyPendingCleanupDefaults(cleanup)
	if cleanup.Provider == "" {
		return nil, fmt.Errorf("provider is required")
	}
	if cleanup.ResourceType == "" {
		return nil, fmt.Errorf("resource type is required")
	}
	if cleanup.ResourceID == "" {
		return nil, fmt.Errorf("resource id is required")
	}

	db := dbcore.GetDBInstance()
	var saved models.FailoverPendingCleanup
	err := db.Transaction(func(tx *gorm.DB) error {
		var existing models.FailoverPendingCleanup
		err := tx.Where(
			"user_id = ? AND provider = ? AND resource_type = ? AND resource_id = ?",
			cleanup.UserID,
			cleanup.Provider,
			cleanup.ResourceType,
			cleanup.ResourceID,
		).First(&existing).Error
		switch {
		case errors.Is(err, gorm.ErrRecordNotFound):
			if err := tx.Create(cleanup).Error; err != nil {
				return err
			}
			saved = *cleanup
			return nil
		case err != nil:
			return err
		}

		updates := map[string]interface{}{
			"task_id":           cleanup.TaskID,
			"execution_id":      cleanup.ExecutionID,
			"provider_entry_id": cleanup.ProviderEntryID,
			"instance_ref":      cleanup.InstanceRef,
			"cleanup_label":     cleanup.CleanupLabel,
			"status":            cleanup.Status,
			"attempt_count":     cleanup.AttemptCount,
			"last_error":        cleanup.LastError,
			"last_attempted_at": cleanup.LastAttemptedAt,
			"next_retry_at":     cleanup.NextRetryAt,
			"resolved_at":       cleanup.ResolvedAt,
		}
		if err := tx.Model(&models.FailoverPendingCleanup{}).
			Where("id = ?", existing.ID).
			Updates(updates).Error; err != nil {
			return err
		}
		return tx.First(&saved, existing.ID).Error
	})
	if err != nil {
		return nil, err
	}
	return &saved, nil
}

func ListDuePendingCleanups(limit int, before time.Time) ([]models.FailoverPendingCleanup, error) {
	if limit <= 0 {
		limit = 20
	}
	if limit > 200 {
		limit = 200
	}
	if before.IsZero() {
		before = time.Now()
	}

	var cleanups []models.FailoverPendingCleanup
	db := dbcore.GetDBInstance()
	if err := db.Where("status = ?", models.FailoverPendingCleanupStatusPending).
		Where("next_retry_at IS NULL OR next_retry_at <= ?", before).
		Order("next_retry_at ASC").
		Order("id ASC").
		Limit(limit).
		Find(&cleanups).Error; err != nil {
		return nil, err
	}
	return cleanups, nil
}

func MarkPendingCleanupSucceeded(cleanupID uint) error {
	if cleanupID == 0 {
		return nil
	}
	now := models.FromTime(time.Now())
	db := dbcore.GetDBInstance()
	result := db.Model(&models.FailoverPendingCleanup{}).
		Where("id = ?", cleanupID).
		Updates(map[string]interface{}{
			"status":            models.FailoverPendingCleanupStatusSucceeded,
			"last_error":        "",
			"next_retry_at":     nil,
			"last_attempted_at": now,
			"resolved_at":       now,
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}
	return nil
}

func SchedulePendingCleanupRetry(cleanupID uint, attemptCount int, lastError string, nextRetryAt time.Time) error {
	if cleanupID == 0 {
		return nil
	}
	if attemptCount < 0 {
		attemptCount = 0
	}
	now := models.FromTime(time.Now())
	next := models.FromTime(nextRetryAt)
	db := dbcore.GetDBInstance()
	result := db.Model(&models.FailoverPendingCleanup{}).
		Where("id = ?", cleanupID).
		Updates(map[string]interface{}{
			"status":            models.FailoverPendingCleanupStatusPending,
			"attempt_count":     attemptCount,
			"last_error":        strings.TrimSpace(lastError),
			"last_attempted_at": now,
			"next_retry_at":     next,
			"resolved_at":       nil,
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}
	return nil
}

func MarkPendingCleanupManualReview(cleanupID uint, attemptCount int, lastError string) error {
	if cleanupID == 0 {
		return nil
	}
	if attemptCount < 0 {
		attemptCount = 0
	}
	now := models.FromTime(time.Now())
	db := dbcore.GetDBInstance()
	result := db.Model(&models.FailoverPendingCleanup{}).
		Where("id = ?", cleanupID).
		Updates(map[string]interface{}{
			"status":            models.FailoverPendingCleanupStatusManualReview,
			"attempt_count":     attemptCount,
			"last_error":        strings.TrimSpace(lastError),
			"last_attempted_at": now,
			"next_retry_at":     nil,
			"resolved_at":       nil,
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}
	return nil
}

func CreateExecution(execution *models.FailoverExecution) (*models.FailoverExecution, error) {
	if execution == nil {
		return nil, fmt.Errorf("execution is required")
	}

	if execution.Status == "" {
		execution.Status = models.FailoverExecutionStatusQueued
	}
	if execution.ScriptStatus == "" {
		execution.ScriptStatus = models.FailoverScriptStatusPending
	}
	if execution.DNSStatus == "" {
		execution.DNSStatus = models.FailoverDNSStatusPending
	}
	if execution.CleanupStatus == "" {
		execution.CleanupStatus = models.FailoverCleanupStatusPending
	}
	if execution.StartedAt.ToTime().IsZero() {
		execution.StartedAt = models.FromTime(time.Now())
	}

	db := dbcore.GetDBInstance()
	if err := db.Create(execution).Error; err != nil {
		return nil, err
	}
	return execution, nil
}

func UpdateExecutionFields(executionID uint, fields map[string]interface{}) error {
	if len(fields) == 0 {
		return nil
	}
	db := dbcore.GetDBInstance()
	result := db.Model(&models.FailoverExecution{}).
		Where("id = ?", executionID).
		Updates(fields)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}
	return nil
}

func UpdateTaskFields(taskID uint, fields map[string]interface{}) error {
	if len(fields) == 0 {
		return nil
	}
	db := dbcore.GetDBInstance()
	result := db.Model(&models.FailoverTask{}).
		Where("id = ?", taskID).
		Updates(fields)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}
	return nil
}

func CreateExecutionStep(step *models.FailoverExecutionStep) (*models.FailoverExecutionStep, error) {
	if step == nil {
		return nil, fmt.Errorf("step is required")
	}
	if step.Status == "" {
		step.Status = models.FailoverStepStatusPending
	}
	db := dbcore.GetDBInstance()
	if err := db.Create(step).Error; err != nil {
		return nil, err
	}
	return step, nil
}

func UpdateExecutionStepFields(stepID uint, fields map[string]interface{}) error {
	if len(fields) == 0 {
		return nil
	}
	db := dbcore.GetDBInstance()
	result := db.Model(&models.FailoverExecutionStep{}).
		Where("id = ?", stepID).
		Updates(fields)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}
	return nil
}
