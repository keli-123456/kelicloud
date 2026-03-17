package tasks

import (
	"fmt"
	"strings"
	"time"

	"github.com/komari-monitor/komari/database/dbcore"
	"github.com/komari-monitor/komari/database/models"
	"gorm.io/gorm"
)

func normalizeTaskOwnerScope(userUUID, tenantID string) (string, string, error) {
	userUUID = strings.TrimSpace(userUUID)
	if userUUID != "" {
		return userUUID, "", nil
	}
	return "", strings.TrimSpace(tenantID), nil
}

func applyTaskUserScopeWithDB(db *gorm.DB, userUUID string) *gorm.DB {
	userUUID = strings.TrimSpace(userUUID)
	if userUUID == "" {
		return db.Where("1 = 0")
	}
	return db.Where("user_id = ?", userUUID)
}

func CreateTask(taskId string, clients []string) error {
	return createTaskWithDB(dbcore.GetDBInstance(), "", "", taskId, clients)
}

func CreateTaskForUser(userUUID, taskId string, clientIDs []string) error {
	return createTaskWithDB(dbcore.GetDBInstance(), userUUID, "", taskId, clientIDs)
}

func createTaskWithDB(db *gorm.DB, userUUID, tenantID, taskId string, clientIDs []string) error {
	userUUID, tenantID, err := normalizeTaskOwnerScope(userUUID, tenantID)
	if err != nil {
		return err
	}
	if taskId == "" {
		return fmt.Errorf("task id is required")
	}

	// Persist task metadata without storing the raw command payload.
	task := models.Task{
		UserID:  userUUID,
		TaskId:  taskId,
		Clients: models.StringArray(clientIDs),
		Command: "",
	}
	if err := db.Create(&task).Error; err != nil {
		return err
	}
	var taskResults []models.TaskResult
	for _, client := range clientIDs {
		taskResults = append(taskResults, models.TaskResult{
			UserID:     userUUID,
			TaskId:     taskId,
			Client:     client,
			Result:     "",
			ExitCode:   nil,
			FinishedAt: nil,
			CreatedAt:  models.FromTime(time.Now()),
		})
	}
	if len(taskResults) > 0 {
		return db.Create(&taskResults).Error
	}
	return nil
}

func GetTaskByTaskIdForUser(userUUID, taskId string) (*models.Task, error) {
	return getTaskByTaskIdForUserWithDB(dbcore.GetDBInstance(), userUUID, taskId)
}

func getTaskByTaskIdForUserWithDB(db *gorm.DB, userUUID, taskId string) (*models.Task, error) {
	userUUID, _, err := normalizeTaskOwnerScope(userUUID, "")
	if err != nil {
		return nil, err
	}

	var task models.Task
	if err := applyTaskUserScopeWithDB(db, userUUID).Where("task_id = ?", taskId).First(&task).Error; err != nil {
		return nil, err
	}
	return &task, nil
}

func GetTasksByClientIdForUser(userUUID, clientID string) ([]models.Task, error) {
	return getTasksByClientIdForUserWithDB(dbcore.GetDBInstance(), userUUID, clientID)
}

func getTasksByClientIdForUserWithDB(db *gorm.DB, userUUID, clientID string) ([]models.Task, error) {
	userUUID, _, err := normalizeTaskOwnerScope(userUUID, "")
	if err != nil {
		return nil, err
	}

	var taskIDs []string
	if err := applyTaskUserScopeWithDB(db.Model(&models.TaskResult{}), userUUID).
		Where("client = ?", clientID).
		Distinct().
		Pluck("task_id", &taskIDs).Error; err != nil {
		return nil, err
	}
	if len(taskIDs) == 0 {
		return []models.Task{}, nil
	}
	var tasks []models.Task
	if err := applyTaskUserScopeWithDB(db.Model(&models.Task{}), userUUID).
		Where("task_id IN ?", taskIDs).
		Find(&tasks).Error; err != nil {
		return nil, err
	}
	return tasks, nil
}

func GetSpecificTaskResultForUser(userUUID, taskID, clientID string) (*models.TaskResult, error) {
	return getSpecificTaskResultForUserWithDB(dbcore.GetDBInstance(), userUUID, taskID, clientID)
}

func getSpecificTaskResultForUserWithDB(db *gorm.DB, userUUID, taskID, clientID string) (*models.TaskResult, error) {
	userUUID, _, err := normalizeTaskOwnerScope(userUUID, "")
	if err != nil {
		return nil, err
	}

	var result models.TaskResult
	if err := applyTaskUserScopeWithDB(db, userUUID).
		Where("task_id = ? AND client = ?", taskID, clientID).
		First(&result).Error; err != nil {
		return nil, err
	}
	return &result, nil
}

func GetAllTasksResultByUUIDForUser(userUUID, clientID string) ([]models.TaskResult, error) {
	return getAllTasksResultByUUIDForUserWithDB(dbcore.GetDBInstance(), userUUID, clientID)
}

func getAllTasksResultByUUIDForUserWithDB(db *gorm.DB, userUUID, clientID string) ([]models.TaskResult, error) {
	userUUID, _, err := normalizeTaskOwnerScope(userUUID, "")
	if err != nil {
		return nil, err
	}

	var results []models.TaskResult
	if err := applyTaskUserScopeWithDB(db, userUUID).Where("client = ?", clientID).Find(&results).Error; err != nil {
		return nil, err
	}
	return results, nil
}

func GetAllTasksByUser(userUUID string) ([]models.Task, error) {
	return getAllTasksByUserWithDB(dbcore.GetDBInstance(), userUUID)
}

func getAllTasksByUserWithDB(db *gorm.DB, userUUID string) ([]models.Task, error) {
	userUUID, _, err := normalizeTaskOwnerScope(userUUID, "")
	if err != nil {
		return nil, err
	}

	var tasks []models.Task
	if err := applyTaskUserScopeWithDB(db, userUUID).Find(&tasks).Error; err != nil {
		return nil, err
	}
	return tasks, nil
}

func GetTaskResultsByTaskIdForUser(userUUID, taskID string) ([]models.TaskResult, error) {
	return getTaskResultsByTaskIDForUserWithDB(dbcore.GetDBInstance(), userUUID, taskID)
}

func getTaskResultsByTaskIDForUserWithDB(db *gorm.DB, userUUID, taskID string) ([]models.TaskResult, error) {
	userUUID, _, err := normalizeTaskOwnerScope(userUUID, "")
	if err != nil {
		return nil, err
	}

	var results []models.TaskResult
	if err := applyTaskUserScopeWithDB(db, userUUID).Where("task_id = ?", taskID).Find(&results).Error; err != nil {
		return nil, err
	}
	return results, nil
}

func DeleteTaskByTaskIdForUser(userUUID, taskID string) error {
	userUUID, _, err := normalizeTaskOwnerScope(userUUID, "")
	if err != nil {
		return err
	}
	return applyTaskUserScopeWithDB(dbcore.GetDBInstance(), userUUID).
		Where("task_id = ?", taskID).
		Delete(&models.Task{}).Error
}

func SaveTaskResultForUser(userUUID, taskID, clientID, result string, exitCode int, timestamp models.LocalTime) error {
	return saveTaskResultWithDB(dbcore.GetDBInstance(), userUUID, "", taskID, clientID, result, exitCode, timestamp)
}

func saveTaskResultWithDB(db *gorm.DB, userUUID, tenantID, taskID, clientID, result string, exitCode int, timestamp models.LocalTime) error {
	userUUID, tenantID, err := normalizeTaskOwnerScope(userUUID, tenantID)
	if err != nil {
		return err
	}

	updateQuery := db.Model(&models.TaskResult{})
	if userUUID != "" {
		updateQuery = applyTaskUserScopeWithDB(updateQuery, userUUID)
	} else {
		updateQuery = updateQuery.Where("COALESCE(user_id, '') = ''")
	}

	update := updateQuery.Where("task_id = ? AND client = ?", taskID, clientID).Updates(map[string]interface{}{
		"result":      result,
		"exit_code":   exitCode,
		"finished_at": timestamp,
	})
	if update.Error != nil {
		return update.Error
	}
	if update.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}
	return nil
}

func ClearTaskResultsByTimeBefore(before time.Time) error {
	return dbcore.GetDBInstance().Where("created_at < ?", before.Format(time.RFC3339)).Delete(&models.TaskResult{}).Error
}
