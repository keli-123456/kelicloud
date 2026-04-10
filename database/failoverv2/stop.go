package failoverv2

import (
	"fmt"
	"strings"
	"time"

	"github.com/komari-monitor/komari/database/dbcore"
	"github.com/komari-monitor/komari/database/models"
	"gorm.io/gorm"
)

func StopExecutionForUser(userUUID string, serviceID, executionID uint, reason string) (*models.FailoverV2Execution, error) {
	return stopExecutionForUserWithDB(dbcore.GetDBInstance(), userUUID, serviceID, executionID, reason)
}

func stopExecutionForUserWithDB(db *gorm.DB, userUUID string, serviceID, executionID uint, reason string) (*models.FailoverV2Execution, error) {
	userUUID, err := normalizeFailoverV2UserID(userUUID)
	if err != nil {
		return nil, err
	}
	if serviceID == 0 {
		return nil, fmt.Errorf("service id is required")
	}
	if executionID == 0 {
		return nil, fmt.Errorf("execution id is required")
	}

	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "failover v2 execution stopped by user"
	}

	var updated *models.FailoverV2Execution
	err = db.Transaction(func(tx *gorm.DB) error {
		service, err := getServiceByIDForUserWithDB(tx, userUUID, serviceID)
		if err != nil {
			return err
		}

		execution, err := getExecutionByIDForServiceForUserWithDB(tx, userUUID, service.ID, executionID)
		if err != nil {
			return err
		}
		if !IsFailoverV2ExecutionActive(execution.Status, execution.FinishedAt) {
			return fmt.Errorf("failover v2 execution %d is not active", executionID)
		}

		now := time.Now()
		finishedAt := models.FromTime(now)
		if err := tx.Model(&models.FailoverV2Execution{}).
			Where("id = ?", execution.ID).
			Updates(map[string]interface{}{
				"status":        models.FailoverV2ExecutionStatusFailed,
				"error_message": reason,
				"finished_at":   finishedAt,
			}).Error; err != nil {
			return err
		}

		if err := failRunningExecutionStepsWithDB(tx, []uint{execution.ID}, reason, now); err != nil {
			return err
		}

		if err := tx.Model(&models.FailoverV2Service{}).
			Where("id = ? AND last_execution_id = ?", service.ID, execution.ID).
			Updates(map[string]interface{}{
				"last_status":  models.FailoverV2ServiceStatusFailed,
				"last_message": reason,
			}).Error; err != nil {
			return err
		}

		if err := tx.Model(&models.FailoverV2Member{}).
			Where("service_id = ? AND id = ? AND last_execution_id = ?", service.ID, execution.MemberID, execution.ID).
			Updates(map[string]interface{}{
				"last_status":    models.FailoverV2MemberStatusFailed,
				"last_message":   reason,
				"last_failed_at": finishedAt,
			}).Error; err != nil {
			return err
		}

		reloaded, err := getExecutionByIDForServiceForUserWithDB(tx, userUUID, service.ID, execution.ID)
		if err != nil {
			return err
		}
		updated = reloaded
		return nil
	})
	if err != nil {
		return nil, err
	}
	return updated, nil
}

func failRunningExecutionStepsWithDB(db *gorm.DB, executionIDs []uint, message string, now time.Time) error {
	if db == nil {
		return fmt.Errorf("db is required")
	}
	if len(executionIDs) == 0 {
		return nil
	}

	message = strings.TrimSpace(message)
	if message == "" {
		message = "failover v2 execution failed"
	}

	return db.Model(&models.FailoverV2ExecutionStep{}).
		Where("execution_id IN ? AND status = ?", executionIDs, models.FailoverStepStatusRunning).
		Updates(map[string]interface{}{
			"status":      models.FailoverStepStatusFailed,
			"message":     message,
			"finished_at": models.FromTime(now),
		}).Error
}
