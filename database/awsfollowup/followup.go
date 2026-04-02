package awsfollowup

import (
	"errors"
	"strings"
	"time"

	"github.com/komari-monitor/komari/database/dbcore"
	"github.com/komari-monitor/komari/database/models"
	"gorm.io/gorm"
)

var retryableTaskStatuses = []string{
	models.AWSFollowUpTaskStatusFailed,
	models.AWSFollowUpTaskStatusCancelled,
	models.AWSFollowUpTaskStatusSkipped,
}

func normalizeTask(task *models.AWSFollowUpTask) error {
	if task == nil {
		return errors.New("aws follow-up task is required")
	}

	task.UserID = strings.TrimSpace(task.UserID)
	task.CredentialID = strings.TrimSpace(task.CredentialID)
	task.Region = strings.TrimSpace(task.Region)
	task.TaskType = strings.TrimSpace(task.TaskType)
	task.ResourceID = strings.TrimSpace(task.ResourceID)
	task.Status = strings.TrimSpace(task.Status)
	task.LastError = strings.TrimSpace(task.LastError)

	if task.UserID == "" {
		return errors.New("user id is required")
	}
	if task.CredentialID == "" {
		return errors.New("credential id is required")
	}
	if task.Region == "" {
		return errors.New("region is required")
	}
	if task.TaskType == "" {
		return errors.New("task type is required")
	}
	if task.ResourceID == "" {
		return errors.New("resource id is required")
	}
	if task.Status == "" {
		task.Status = models.AWSFollowUpTaskStatusPending
	}
	if task.MaxAttempts <= 0 {
		task.MaxAttempts = models.AWSFollowUpTaskMaxAttemptsDefault
	}
	if task.NextRunAt.ToTime().IsZero() {
		task.NextRunAt = models.FromTime(time.Now())
	}
	return nil
}

func EnqueueTask(task *models.AWSFollowUpTask) error {
	return enqueueTaskWithDB(dbcore.GetDBInstance(), task)
}

func enqueueTaskWithDB(db *gorm.DB, task *models.AWSFollowUpTask) error {
	if err := normalizeTask(task); err != nil {
		return err
	}

	now := time.Now()
	if task.CreatedAt.ToTime().IsZero() {
		task.CreatedAt = models.FromTime(now)
	}
	if task.UpdatedAt.ToTime().IsZero() {
		task.UpdatedAt = models.FromTime(now)
	}

	return db.Transaction(func(tx *gorm.DB) error {
		var existing models.AWSFollowUpTask
		err := tx.Where(
			"user_id = ? AND credential_id = ? AND region = ? AND task_type = ? AND resource_id = ? AND status = ?",
			task.UserID,
			task.CredentialID,
			task.Region,
			task.TaskType,
			task.ResourceID,
			models.AWSFollowUpTaskStatusPending,
		).First(&existing).Error
		if err == nil {
			updates := map[string]interface{}{
				"next_run_at":  task.NextRunAt,
				"max_attempts": task.MaxAttempts,
				"lease_until":  nil,
				"last_error":   "",
			}
			return tx.Model(&models.AWSFollowUpTask{}).Where("id = ?", existing.ID).Updates(updates).Error
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		return tx.Create(task).Error
	})
}

func ClaimDueTasks(limit int, now time.Time, leaseDuration time.Duration) ([]models.AWSFollowUpTask, error) {
	return claimDueTasksWithDB(dbcore.GetDBInstance(), limit, now, leaseDuration)
}

func claimDueTasksWithDB(db *gorm.DB, limit int, now time.Time, leaseDuration time.Duration) ([]models.AWSFollowUpTask, error) {
	if limit <= 0 {
		limit = 10
	}
	if leaseDuration <= 0 {
		leaseDuration = time.Minute
	}

	var candidates []models.AWSFollowUpTask
	if err := db.
		Where("status = ? AND attempts < max_attempts AND next_run_at <= ?", models.AWSFollowUpTaskStatusPending, models.FromTime(now)).
		Where("lease_until IS NULL OR lease_until <= ?", models.FromTime(now)).
		Order("next_run_at ASC").
		Limit(limit).
		Find(&candidates).Error; err != nil {
		return nil, err
	}

	claimed := make([]models.AWSFollowUpTask, 0, len(candidates))
	leaseUntil := models.FromTime(now.Add(leaseDuration))
	for _, candidate := range candidates {
		result := db.Model(&models.AWSFollowUpTask{}).
			Where("id = ? AND status = ? AND next_run_at <= ?", candidate.ID, models.AWSFollowUpTaskStatusPending, models.FromTime(now)).
			Where("lease_until IS NULL OR lease_until <= ?", models.FromTime(now)).
			Updates(map[string]interface{}{
				"lease_until": leaseUntil,
			})
		if result.Error != nil {
			return nil, result.Error
		}
		if result.RowsAffected == 0 {
			continue
		}
		candidate.LeaseUntil = &leaseUntil
		claimed = append(claimed, candidate)
	}

	return claimed, nil
}

func MarkTaskSucceeded(taskID uint, now time.Time) error {
	return markTaskSucceededWithDB(dbcore.GetDBInstance(), taskID, now)
}

func markTaskSucceededWithDB(db *gorm.DB, taskID uint, now time.Time) error {
	completedAt := models.FromTime(now)
	return db.Model(&models.AWSFollowUpTask{}).
		Where("id = ? AND status = ?", taskID, models.AWSFollowUpTaskStatusPending).
		Updates(map[string]interface{}{
			"status":          models.AWSFollowUpTaskStatusSuccess,
			"lease_until":     nil,
			"last_error":      "",
			"completed_at":    &completedAt,
			"last_attempt_at": &completedAt,
		}).Error
}

func MarkTaskAttempt(task models.AWSFollowUpTask, now time.Time, nextRunAt time.Time, err error) error {
	return markTaskAttemptWithDB(dbcore.GetDBInstance(), task, now, nextRunAt, err)
}

func markTaskAttemptWithDB(db *gorm.DB, task models.AWSFollowUpTask, now time.Time, nextRunAt time.Time, err error) error {
	attempts := task.Attempts + 1
	nowValue := models.FromTime(now)
	updates := map[string]interface{}{
		"attempts":        attempts,
		"lease_until":     nil,
		"last_error":      "",
		"last_attempt_at": &nowValue,
	}

	if err != nil {
		updates["last_error"] = strings.TrimSpace(err.Error())
	}

	if attempts >= effectiveMaxAttempts(task.MaxAttempts) {
		updates["status"] = models.AWSFollowUpTaskStatusFailed
		updates["completed_at"] = &nowValue
	} else {
		updates["status"] = models.AWSFollowUpTaskStatusPending
		updates["next_run_at"] = models.FromTime(nextRunAt)
		updates["completed_at"] = nil
	}

	return db.Model(&models.AWSFollowUpTask{}).
		Where("id = ? AND status = ?", task.ID, models.AWSFollowUpTaskStatusPending).
		Updates(updates).Error
}

func MarkTaskFailed(taskID uint, now time.Time, err error) error {
	return markTaskTerminalWithDB(dbcore.GetDBInstance(), taskID, models.AWSFollowUpTaskStatusFailed, now, err)
}

func markTaskFailedWithDB(db *gorm.DB, taskID uint, now time.Time, err error) error {
	return markTaskTerminalWithDB(db, taskID, models.AWSFollowUpTaskStatusFailed, now, err)
}

func MarkTaskCancelled(taskID uint, now time.Time, err error) error {
	return markTaskTerminalWithDB(dbcore.GetDBInstance(), taskID, models.AWSFollowUpTaskStatusCancelled, now, err)
}

func MarkTaskSkipped(taskID uint, now time.Time, err error) error {
	return markTaskTerminalWithDB(dbcore.GetDBInstance(), taskID, models.AWSFollowUpTaskStatusSkipped, now, err)
}

func markTaskTerminalWithDB(db *gorm.DB, taskID uint, status string, now time.Time, err error) error {
	nowValue := models.FromTime(now)
	status = strings.TrimSpace(status)
	if status == "" {
		status = models.AWSFollowUpTaskStatusFailed
	}
	return db.Model(&models.AWSFollowUpTask{}).
		Where("id = ? AND status = ?", taskID, models.AWSFollowUpTaskStatusPending).
		Updates(map[string]interface{}{
			"status":          status,
			"lease_until":     nil,
			"last_error":      strings.TrimSpace(errorMessage(err)),
			"completed_at":    &nowValue,
			"last_attempt_at": &nowValue,
		}).Error
}

func CancelPendingTasksByCredential(userID, credentialID string, now time.Time, reason string) (int64, error) {
	return cancelPendingTasksByCredentialWithDB(dbcore.GetDBInstance(), userID, credentialID, now, reason)
}

func cancelPendingTasksByCredentialWithDB(db *gorm.DB, userID, credentialID string, now time.Time, reason string) (int64, error) {
	userID = strings.TrimSpace(userID)
	credentialID = strings.TrimSpace(credentialID)
	if userID == "" {
		return 0, errors.New("user id is required")
	}
	if credentialID == "" {
		return 0, errors.New("credential id is required")
	}

	nowValue := models.FromTime(now)
	result := db.Model(&models.AWSFollowUpTask{}).
		Where("user_id = ? AND credential_id = ? AND status = ?", userID, credentialID, models.AWSFollowUpTaskStatusPending).
		Updates(map[string]interface{}{
			"status":          models.AWSFollowUpTaskStatusCancelled,
			"lease_until":     nil,
			"last_error":      strings.TrimSpace(reason),
			"completed_at":    &nowValue,
			"last_attempt_at": &nowValue,
		})
	return result.RowsAffected, result.Error
}

func ListTasksByUser(userID string, limit int, includeSuccess bool) ([]models.AWSFollowUpTask, error) {
	return listTasksByUserWithDB(dbcore.GetDBInstance(), userID, limit, includeSuccess)
}

func listTasksByUserWithDB(db *gorm.DB, userID string, limit int, includeSuccess bool) ([]models.AWSFollowUpTask, error) {
	userID = strings.TrimSpace(userID)
	if userID == "" {
		return nil, errors.New("user id is required")
	}
	if limit <= 0 {
		limit = 100
	}

	query := db.Where("user_id = ?", userID)
	if !includeSuccess {
		query = query.Where("status <> ?", models.AWSFollowUpTaskStatusSuccess)
	}

	var tasks []models.AWSFollowUpTask
	if err := query.Order("updated_at DESC").Order("id DESC").Limit(limit).Find(&tasks).Error; err != nil {
		return nil, err
	}
	return tasks, nil
}

func RetryTaskByID(userID string, taskID uint, now time.Time) error {
	return retryTaskByIDWithDB(dbcore.GetDBInstance(), userID, taskID, now)
}

func retryTaskByIDWithDB(db *gorm.DB, userID string, taskID uint, now time.Time) error {
	userID = strings.TrimSpace(userID)
	if userID == "" {
		return errors.New("user id is required")
	}
	if taskID == 0 {
		return errors.New("task id is required")
	}

	result := db.Model(&models.AWSFollowUpTask{}).
		Where("id = ? AND user_id = ? AND status IN ?", taskID, userID, retryableTaskStatuses).
		Updates(map[string]interface{}{
			"status":          models.AWSFollowUpTaskStatusPending,
			"attempts":        0,
			"lease_until":     nil,
			"last_error":      "",
			"last_attempt_at": nil,
			"next_run_at":     models.FromTime(now),
			"completed_at":    nil,
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}
	return nil
}

func DeleteTerminalTasksByUser(userID string) (int64, error) {
	return deleteTerminalTasksByUserWithDB(dbcore.GetDBInstance(), userID)
}

func deleteTerminalTasksByUserWithDB(db *gorm.DB, userID string) (int64, error) {
	userID = strings.TrimSpace(userID)
	if userID == "" {
		return 0, errors.New("user id is required")
	}

	result := db.Where("user_id = ? AND status IN ?", userID, retryableTaskStatuses).
		Delete(&models.AWSFollowUpTask{})
	return result.RowsAffected, result.Error
}

func errorMessage(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func effectiveMaxAttempts(maxAttempts int) int {
	if maxAttempts <= 0 {
		return models.AWSFollowUpTaskMaxAttemptsDefault
	}
	return maxAttempts
}
