package tasks

import (
	"time"

	"github.com/komari-monitor/komari/database"
	clientdb "github.com/komari-monitor/komari/database/clients"
	"github.com/komari-monitor/komari/database/dbcore"
	"github.com/komari-monitor/komari/database/models"
	"github.com/komari-monitor/komari/utils"
	"gorm.io/gorm"
)

func AddPingTask(clients []string, name string, target, task_type string, interval int) (uint, error) {
	tenantID, err := database.GetDefaultTenantID()
	if err != nil {
		return 0, err
	}
	return AddPingTaskForTenant(tenantID, clients, name, target, task_type, interval)
}

func AddPingTaskForTenant(tenantID string, clientUUIDs []string, name string, target, taskType string, interval int) (uint, error) {
	normalizedClients, err := clientdb.NormalizeClientUUIDsForTenant(tenantID, clientUUIDs)
	if err != nil {
		return 0, err
	}
	db := dbcore.GetDBInstance()
	task := models.PingTask{
		TenantID: tenantID,
		Clients:  normalizedClients,
		Name:     name,
		Type:     taskType,
		Target:   target,
		Interval: interval,
	}
	if err := db.Create(&task).Error; err != nil {
		return 0, err
	}
	ReloadPingSchedule()
	return task.Id, nil
}

func DeletePingTask(id []uint) error {
	db := dbcore.GetDBInstance()
	result := db.Where("id IN ?", id).Delete(&models.PingTask{})
	if result.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}
	ReloadPingSchedule()
	return result.Error
}

func DeletePingTaskForTenant(tenantID string, id []uint) error {
	db := dbcore.GetDBInstance()
	result := db.Where("tenant_id = ? AND id IN ?", tenantID, id).Delete(&models.PingTask{})
	if result.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}
	ReloadPingSchedule()
	return result.Error
}

func EditPingTask(tasks []*models.PingTask) error {
	db := dbcore.GetDBInstance()
	for _, task := range tasks {
		result := db.Model(&models.PingTask{}).Where("id = ?", task.Id).Updates(task)
		if result.RowsAffected == 0 {
			return gorm.ErrRecordNotFound
		}
	}
	ReloadPingSchedule()
	return nil
}

func EditPingTaskForTenant(tenantID string, pingTasks []*models.PingTask) error {
	db := dbcore.GetDBInstance()
	for _, task := range pingTasks {
		normalizedClients, err := clientdb.NormalizeClientUUIDsForTenant(tenantID, task.Clients)
		if err != nil {
			return err
		}
		task.TenantID = tenantID
		task.Clients = normalizedClients
		result := db.Model(&models.PingTask{}).Where("id = ? AND tenant_id = ?", task.Id, tenantID).Updates(task)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return gorm.ErrRecordNotFound
		}
	}
	ReloadPingSchedule()
	return nil
}

func GetAllPingTasks() ([]models.PingTask, error) {
	db := dbcore.GetDBInstance()
	var tasks []models.PingTask
	if err := db.Find(&tasks).Error; err != nil {
		return nil, err
	}
	return tasks, nil
}

func GetAllPingTasksByTenant(tenantID string) ([]models.PingTask, error) {
	db := dbcore.GetDBInstance()
	var tasks []models.PingTask
	if err := db.Where("tenant_id = ?", tenantID).Find(&tasks).Error; err != nil {
		return nil, err
	}
	return tasks, nil
}

func GetPingTasksByClient(uuid string) []models.PingTask {
	db := dbcore.GetDBInstance()
	var tasks []models.PingTask
	if err := db.Where("clients LIKE ?", `%"`+uuid+`"%`).Find(&tasks).Error; err != nil {
		return nil
	}
	return tasks
}

func SavePingRecord(record models.PingRecord) error {
	db := dbcore.GetDBInstance()
	return db.Create(&record).Error
}

func DeletePingRecordsBefore(time time.Time) error {
	db := dbcore.GetDBInstance()
	err := db.Where("time < ?", time).Delete(&models.PingRecord{}).Error
	return err
}

func DeletePingRecords(id []uint) error {
	db := dbcore.GetDBInstance()
	result := db.Where("task_id IN ?", id).Delete(&models.PingRecord{})
	if result.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}
	return result.Error
}

func DeleteAllPingRecords() error {
	db := dbcore.GetDBInstance()
	result := db.Exec("DELETE FROM ping_records")
	if result.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}
	return result.Error
}

func DeletePingRecordsByTenant(tenantID string) error {
	db := dbcore.GetDBInstance()
	clientScope := db.Model(&models.Client{}).Select("uuid").Where("tenant_id = ?", tenantID)
	result := db.Where("client IN (?)", clientScope).Delete(&models.PingRecord{})
	if result.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}
	return result.Error
}

func ReloadPingSchedule() error {
	db := dbcore.GetDBInstance()
	var pingTasks []models.PingTask
	if err := db.Find(&pingTasks).Error; err != nil {
		return err
	}
	return utils.ReloadPingSchedule(pingTasks)
}

func GetPingRecords(uuid string, taskId int, start, end time.Time) ([]models.PingRecord, error) {
	db := dbcore.GetDBInstance()
	var records []models.PingRecord
	dbQuery := db.Model(&models.PingRecord{})
	if uuid != "" {
		dbQuery = dbQuery.Where("client = ?", uuid)
	}
	if taskId >= 0 {
		dbQuery = dbQuery.Where("task_id = ?", uint(taskId))
	}
	if err := dbQuery.Where("time >= ? AND time <= ?", start, end).Order("time DESC").Find(&records).Error; err != nil {
		return nil, err
	}
	return records, nil
}

func GetPingRecordsByTenant(tenantID, uuid string, taskId int, start, end time.Time) ([]models.PingRecord, error) {
	db := dbcore.GetDBInstance()
	var records []models.PingRecord
	clientScope := db.Model(&models.Client{}).Select("uuid").Where("tenant_id = ?", tenantID)
	if uuid != "" {
		clientScope = clientScope.Where("uuid = ?", uuid)
	}

	dbQuery := db.Model(&models.PingRecord{}).Where("client IN (?)", clientScope)
	if taskId >= 0 {
		dbQuery = dbQuery.Where("task_id = ?", uint(taskId))
	}
	if err := dbQuery.Where("time >= ? AND time <= ?", start, end).Order("time DESC").Find(&records).Error; err != nil {
		return nil, err
	}
	return records, nil
}
