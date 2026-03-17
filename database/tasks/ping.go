package tasks

import (
	"strings"
	"time"

	clientdb "github.com/komari-monitor/komari/database/clients"
	"github.com/komari-monitor/komari/database/dbcore"
	"github.com/komari-monitor/komari/database/models"
	"github.com/komari-monitor/komari/utils"
	"gorm.io/gorm"
)

func normalizePingTaskOwnerScope(userUUID, tenantID string) (string, string, error) {
	userUUID = strings.TrimSpace(userUUID)
	if userUUID != "" {
		return userUUID, "", nil
	}
	return "", strings.TrimSpace(tenantID), nil
}

func pingTaskClientScopeByUserWithDB(db *gorm.DB, userUUID, clientUUID string) *gorm.DB {
	scope := clientdb.ClientUUIDScopeByUserWithDB(db, userUUID)
	if strings.TrimSpace(clientUUID) != "" {
		scope = scope.Where("uuid = ?", strings.TrimSpace(clientUUID))
	}
	return scope
}

func AddPingTaskForUser(userUUID string, clientUUIDs []string, name string, target, taskType string, interval int) (uint, error) {
	userUUID, _, err := normalizePingTaskOwnerScope(userUUID, "")
	if err != nil {
		return 0, err
	}

	normalizedClients, err := clientdb.NormalizeClientUUIDsForUser(userUUID, clientUUIDs)
	if err != nil {
		return 0, err
	}
	db := dbcore.GetDBInstance()
	task := models.PingTask{
		UserID:   userUUID,
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

func DeletePingTaskForUser(userUUID string, id []uint) error {
	userUUID, _, err := normalizePingTaskOwnerScope(userUUID, "")
	if err != nil {
		return err
	}

	db := dbcore.GetDBInstance()
	result := db.Where(
		"user_id = ? AND id IN ?",
		userUUID,
		id,
	).Delete(&models.PingTask{})
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

func EditPingTaskForUser(userUUID string, pingTasks []*models.PingTask) error {
	userUUID, _, err := normalizePingTaskOwnerScope(userUUID, "")
	if err != nil {
		return err
	}

	db := dbcore.GetDBInstance()
	for _, task := range pingTasks {
		normalizedClients, err := clientdb.NormalizeClientUUIDsForUser(userUUID, task.Clients)
		if err != nil {
			return err
		}
		task.UserID = userUUID
		task.Clients = normalizedClients
		result := db.Model(&models.PingTask{}).
			Where("id = ? AND user_id = ?", task.Id, userUUID).
			Updates(task)
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
	return getAllPingTasksWithDB(dbcore.GetDBInstance())
}

func GetAllPingTasksByUser(userUUID string) ([]models.PingTask, error) {
	return getAllPingTasksByUserWithDB(dbcore.GetDBInstance(), userUUID)
}

func getAllPingTasksWithDB(db *gorm.DB) ([]models.PingTask, error) {
	var tasks []models.PingTask
	if err := db.Find(&tasks).Error; err != nil {
		return nil, err
	}
	return tasks, nil
}

func getAllPingTasksByUserWithDB(db *gorm.DB, userUUID string) ([]models.PingTask, error) {
	userUUID, _, err := normalizePingTaskOwnerScope(userUUID, "")
	if err != nil {
		return nil, err
	}

	var tasks []models.PingTask
	if err := db.Where("user_id = ?", userUUID).Find(&tasks).Error; err != nil {
		return nil, err
	}
	return tasks, nil
}

func getLegacyPingTasksByClientScopeWithDB(db *gorm.DB, clientScope *gorm.DB) ([]models.PingTask, error) {
	var clientUUIDs []string
	if err := clientScope.Pluck("uuid", &clientUUIDs).Error; err != nil {
		return nil, err
	}
	if len(clientUUIDs) == 0 {
		return []models.PingTask{}, nil
	}

	allowed := make(map[string]struct{}, len(clientUUIDs))
	for _, uuid := range clientUUIDs {
		allowed[uuid] = struct{}{}
	}

	var tasks []models.PingTask
	if err := db.Where("COALESCE(user_id, '') = ''").Find(&tasks).Error; err != nil {
		return nil, err
	}

	filtered := make([]models.PingTask, 0, len(tasks))
	for _, task := range tasks {
		for _, clientUUID := range task.Clients {
			if _, ok := allowed[clientUUID]; ok {
				filtered = append(filtered, task)
				break
			}
		}
	}
	return filtered, nil
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

func DeletePingRecordsByUser(userUUID string) error {
	db := dbcore.GetDBInstance()
	clientScope := clientdb.ClientUUIDScopeByUserWithDB(db, userUUID)
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
	return getPingRecordsWithDB(dbcore.GetDBInstance(), uuid, taskId, start, end)
}

func GetPingRecordsByUser(userUUID, uuid string, taskId int, start, end time.Time) ([]models.PingRecord, error) {
	return getPingRecordsByUserWithDB(dbcore.GetDBInstance(), userUUID, uuid, taskId, start, end)
}

func getPingRecordsWithDB(db *gorm.DB, uuid string, taskId int, start, end time.Time) ([]models.PingRecord, error) {
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

func getPingRecordsByUserWithDB(db *gorm.DB, userUUID, uuid string, taskId int, start, end time.Time) ([]models.PingRecord, error) {
	var records []models.PingRecord
	clientScope := pingTaskClientScopeByUserWithDB(db, userUUID, uuid)

	dbQuery := db.Model(&models.PingRecord{}).Where("client IN (?)", clientScope)
	if taskId >= 0 {
		dbQuery = dbQuery.Where("task_id = ?", uint(taskId))
	}
	if err := dbQuery.Where("time >= ? AND time <= ?", start, end).Order("time DESC").Find(&records).Error; err != nil {
		return nil, err
	}
	return records, nil
}
