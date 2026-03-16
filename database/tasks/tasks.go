package tasks

import (
	"fmt"
	"time"

	"github.com/komari-monitor/komari/database"
	"github.com/komari-monitor/komari/database/dbcore"
	"github.com/komari-monitor/komari/database/models"
	"gorm.io/gorm"
)

func CreateTask(taskId string, clients []string) error {
	tenantID, err := database.GetDefaultTenantID()
	if err != nil {
		return err
	}
	return CreateTaskForTenant(tenantID, taskId, clients)
}

func CreateTaskForTenant(tenantID, taskId string, clientIDs []string) error {
	return createTaskWithDB(dbcore.GetDBInstance(), tenantID, taskId, clientIDs)
}

func createTaskWithDB(db *gorm.DB, tenantID, taskId string, clientIDs []string) error {
	if tenantID == "" {
		return fmt.Errorf("tenant id is required")
	}
	if taskId == "" {
		return fmt.Errorf("task id is required")
	}

	// Persist task metadata without storing the raw command payload.
	task := models.Task{
		TenantID: tenantID,
		TaskId:   taskId,
		Clients:  models.StringArray(clientIDs),
		Command:  "",
	}
	if err := db.Create(&task).Error; err != nil {
		return err
	}
	var taskResults []models.TaskResult
	for _, client := range clientIDs {
		taskResults = append(taskResults, models.TaskResult{
			TenantID:   tenantID,
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

func GetTaskByTaskId(taskId string) (*models.Task, error) {
	tenantID, err := database.GetDefaultTenantID()
	if err != nil {
		return nil, err
	}
	return GetTaskByTaskIdForTenant(tenantID, taskId)
}

func GetTaskByTaskIdForTenant(tenantID, taskId string) (*models.Task, error) {
	return getTaskByTaskIdWithDB(dbcore.GetDBInstance(), tenantID, taskId)
}

func getTaskByTaskIdWithDB(db *gorm.DB, tenantID, taskId string) (*models.Task, error) {
	var task models.Task
	if err := db.Where("tenant_id = ? AND task_id = ?", tenantID, taskId).First(&task).Error; err != nil {
		return nil, err
	}
	return &task, nil
}

func GetTasksByClientId(clientId string) ([]models.Task, error) {
	tenantID, err := database.GetDefaultTenantID()
	if err != nil {
		return nil, err
	}
	return GetTasksByClientIdForTenant(tenantID, clientId)
}

func GetTasksByClientIdForTenant(tenantID, clientID string) ([]models.Task, error) {
	return getTasksByClientIdWithDB(dbcore.GetDBInstance(), tenantID, clientID)
}

func getTasksByClientIdWithDB(db *gorm.DB, tenantID, clientID string) ([]models.Task, error) {
	var taskIDs []string
	if err := db.Model(&models.TaskResult{}).
		Where("tenant_id = ? AND client = ?", tenantID, clientID).
		Distinct().
		Pluck("task_id", &taskIDs).Error; err != nil {
		return nil, err
	}
	if len(taskIDs) == 0 {
		return []models.Task{}, nil
	}
	var tasks []models.Task
	if err := db.Where("tenant_id = ? AND task_id IN ?", tenantID, taskIDs).Find(&tasks).Error; err != nil {
		return nil, err
	}
	return tasks, nil
}

func GetSpecificTaskResult(taskId, clientId string) (*models.TaskResult, error) {
	tenantID, err := database.GetDefaultTenantID()
	if err != nil {
		return nil, err
	}
	return GetSpecificTaskResultForTenant(tenantID, taskId, clientId)
}

func GetSpecificTaskResultForTenant(tenantID, taskID, clientID string) (*models.TaskResult, error) {
	return getSpecificTaskResultWithDB(dbcore.GetDBInstance(), tenantID, taskID, clientID)
}

func getSpecificTaskResultWithDB(db *gorm.DB, tenantID, taskID, clientID string) (*models.TaskResult, error) {
	var result models.TaskResult
	if err := db.Where("tenant_id = ? AND task_id = ? AND client = ?", tenantID, taskID, clientID).First(&result).Error; err != nil {
		return nil, err
	}
	return &result, nil
}

func GetAllTasksResultByUUID(uuid string) ([]models.TaskResult, error) {
	tenantID, err := database.GetDefaultTenantID()
	if err != nil {
		return nil, err
	}
	return GetAllTasksResultByUUIDForTenant(tenantID, uuid)
}

func GetAllTasksResultByUUIDForTenant(tenantID, clientID string) ([]models.TaskResult, error) {
	return getAllTasksResultByUUIDWithDB(dbcore.GetDBInstance(), tenantID, clientID)
}

func getAllTasksResultByUUIDWithDB(db *gorm.DB, tenantID, clientID string) ([]models.TaskResult, error) {
	var results []models.TaskResult
	if err := db.Where("tenant_id = ? AND client = ?", tenantID, clientID).Find(&results).Error; err != nil {
		return nil, err
	}
	return results, nil
}

func GetAllTasks() ([]models.Task, error) {
	tenantID, err := database.GetDefaultTenantID()
	if err != nil {
		return nil, err
	}
	return GetAllTasksByTenant(tenantID)
}

func GetAllTasksByTenant(tenantID string) ([]models.Task, error) {
	return getAllTasksWithDB(dbcore.GetDBInstance(), tenantID)
}

func getAllTasksWithDB(db *gorm.DB, tenantID string) ([]models.Task, error) {
	var tasks []models.Task
	if err := db.Where("tenant_id = ?", tenantID).Find(&tasks).Error; err != nil {
		return nil, err
	}
	return tasks, nil
}

func GetTaskResultsByTaskId(taskId string) ([]models.TaskResult, error) {
	tenantID, err := database.GetDefaultTenantID()
	if err != nil {
		return nil, err
	}
	return GetTaskResultsByTaskIdForTenant(tenantID, taskId)
}

func GetTaskResultsByTaskIdForTenant(tenantID, taskID string) ([]models.TaskResult, error) {
	return getTaskResultsByTaskIDWithDB(dbcore.GetDBInstance(), tenantID, taskID)
}

func getTaskResultsByTaskIDWithDB(db *gorm.DB, tenantID, taskID string) ([]models.TaskResult, error) {
	var results []models.TaskResult
	if err := db.Where("tenant_id = ? AND task_id = ?", tenantID, taskID).Find(&results).Error; err != nil {
		return nil, err
	}
	return results, nil
}

func DeleteTaskByTaskId(taskId string) error {
	tenantID, err := database.GetDefaultTenantID()
	if err != nil {
		return err
	}
	return DeleteTaskByTaskIdForTenant(tenantID, taskId)
}

func DeleteTaskByTaskIdForTenant(tenantID, taskID string) error {
	return dbcore.GetDBInstance().Where("tenant_id = ? AND task_id = ?", tenantID, taskID).Delete(&models.Task{}).Error
}

func SaveTaskResult(taskId, clientId, result string, exitCode int, timestamp models.LocalTime) error {
	tenantID, err := database.GetDefaultTenantID()
	if err != nil {
		return err
	}
	return SaveTaskResultForTenant(tenantID, taskId, clientId, result, exitCode, timestamp)
}

func SaveTaskResultForTenant(tenantID, taskID, clientID, result string, exitCode int, timestamp models.LocalTime) error {
	return saveTaskResultWithDB(dbcore.GetDBInstance(), tenantID, taskID, clientID, result, exitCode, timestamp)
}

func saveTaskResultWithDB(db *gorm.DB, tenantID, taskID, clientID, result string, exitCode int, timestamp models.LocalTime) error {
	update := db.
		Model(&models.TaskResult{}).
		Where("tenant_id = ? AND task_id = ? AND client = ?", tenantID, taskID, clientID).
		Updates(map[string]interface{}{
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
