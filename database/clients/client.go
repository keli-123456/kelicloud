package clients

import (
	"errors"
	"math"
	"strings"
	"time"

	"github.com/komari-monitor/komari/common"
	"github.com/komari-monitor/komari/config"
	"github.com/komari-monitor/komari/database/accounts"
	"github.com/komari-monitor/komari/database/dbcore"
	"github.com/komari-monitor/komari/database/models"
	"github.com/komari-monitor/komari/utils"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"fmt"

	"github.com/google/uuid"
)

var ErrClientQuotaExceeded = errors.New("client quota exceeded")

// Deprecated: DeleteClientConfig is deprecated and will be removed in a future release. Use DeleteClient instead.
func DeleteClientConfig(clientUuid string) error {
	db := dbcore.GetDBInstance()
	err := db.Delete(&common.ClientConfig{ClientUUID: clientUuid}).Error
	if err != nil {
		return err
	}
	return nil
}
func DeleteClient(clientUuid string) error {
	return deleteClientWithDB(dbcore.GetDBInstance(), clientUuid)
}

func deleteClientWithDB(db *gorm.DB, clientUuid string) error {
	return db.Transaction(func(tx *gorm.DB) error {
		return deleteClientTx(tx, clientUuid)
	})
}

func deleteClientTx(tx *gorm.DB, clientUuid string) error {
	cleanupModels := []struct {
		model interface{}
		query string
	}{
		{model: &common.ClientConfig{}, query: "client_uuid = ?"},
		{model: &models.OfflineNotification{}, query: "client = ?"},
		{model: &models.TaskResult{}, query: "client = ?"},
		{model: &models.PingRecord{}, query: "client = ?"},
		{model: &models.Record{}, query: "client = ?"},
		{model: &models.GPURecord{}, query: "client = ?"},
	}

	for _, cleanup := range cleanupModels {
		if !tx.Migrator().HasTable(cleanup.model) {
			continue
		}
		if err := tx.Where(cleanup.query, clientUuid).Delete(cleanup.model).Error; err != nil {
			return err
		}
	}

	cleanupTables := []struct {
		table string
		model interface{}
	}{
		{table: "records_long_term", model: &models.Record{}},
		{table: "gpu_records_long_term", model: &models.GPURecord{}},
	}

	for _, cleanup := range cleanupTables {
		if !tx.Migrator().HasTable(cleanup.table) {
			continue
		}
		if err := tx.Table(cleanup.table).Where("client = ?", clientUuid).Delete(cleanup.model).Error; err != nil {
			return err
		}
	}

	return tx.Where("uuid = ?", clientUuid).Delete(&models.Client{}).Error
}

func DeleteClientForUser(userUUID, clientUUID string) error {
	db := dbcore.GetDBInstance()
	return db.Transaction(func(tx *gorm.DB) error {
		if _, err := getClientByUUIDForUserWithDB(tx, clientUUID, userUUID); err != nil {
			return err
		}
		return deleteClientTx(tx, clientUUID)
	})
}

func applyClientUserScopeWithDB(db *gorm.DB, userUUID string) *gorm.DB {
	userUUID = strings.TrimSpace(userUUID)
	if userUUID == "" {
		return db.Where("1 = 0")
	}

	return db.Where("user_id = ?", userUUID)
}

func ClientUUIDScopeByUserWithDB(db *gorm.DB, userUUID string) *gorm.DB {
	return applyClientUserScopeWithDB(db.Model(&models.Client{}).Select("uuid"), userUUID)
}

func ClientUUIDScopeByUser(userUUID string) *gorm.DB {
	return ClientUUIDScopeByUserWithDB(dbcore.GetDBInstance(), userUUID)
}

// Deprecated: UpdateOrInsertBasicInfo is deprecated and will be removed in a future release. Use SaveClientInfo instead.
func UpdateOrInsertBasicInfo(cbi common.ClientInfo) error {
	db := dbcore.GetDBInstance()
	updates := make(map[string]interface{})

	if cbi.Name != "" {
		updates["name"] = cbi.Name
	}
	if cbi.CpuName != "" {
		updates["cpu_name"] = cbi.CpuName
	}
	if cbi.Arch != "" {
		updates["arch"] = cbi.Arch
	}
	if cbi.CpuCores > 0 || cbi.CpuCores < math.MaxInt-1 {
		updates["cpu_cores"] = cbi.CpuCores
	}
	if cbi.OS != "" {
		updates["os"] = cbi.OS
	}
	if cbi.GpuName != "" {
		updates["gpu_name"] = cbi.GpuName
	}
	if cbi.IPv4 != "" {
		updates["ipv4"] = cbi.IPv4
	}
	if cbi.IPv6 != "" {
		updates["ipv6"] = cbi.IPv6
	}
	if cbi.Region != "" {
		updates["region"] = cbi.Region
	}
	if cbi.Remark != "" {
		updates["remark"] = cbi.Remark
	}
	updates["mem_total"] = cbi.MemTotal
	updates["swap_total"] = cbi.SwapTotal
	updates["disk_total"] = cbi.DiskTotal
	updates["version"] = cbi.Version
	updates["updated_at"] = time.Now()

	// 转换为更新Client表
	client := models.Client{
		UUID: cbi.UUID,
	}

	err := db.Model(&client).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "uuid"}},
		DoUpdates: clause.Assignments(updates),
	}).Create(map[string]interface{}{
		"uuid":       cbi.UUID,
		"name":       cbi.Name,
		"cpu_name":   cbi.CpuName,
		"arch":       cbi.Arch,
		"cpu_cores":  cbi.CpuCores,
		"os":         cbi.OS,
		"gpu_name":   cbi.GpuName,
		"ipv4":       cbi.IPv4,
		"ipv6":       cbi.IPv6,
		"region":     cbi.Region,
		"remark":     cbi.Remark,
		"mem_total":  cbi.MemTotal,
		"swap_total": cbi.SwapTotal,
		"disk_total": cbi.DiskTotal,
		"version":    cbi.Version,
		"updated_at": time.Now(),
	}).Error

	if err != nil {
		return err
	}
	return nil
}

func getClientByUUIDWithDB(db *gorm.DB, clientUUID string) (client models.Client, err error) {
	err = db.Where("uuid = ?", clientUUID).First(&client).Error
	if err != nil {
		return models.Client{}, err
	}
	return client, nil
}

func getClientByUUIDForUserWithDB(db *gorm.DB, clientUUID, userUUID string) (client models.Client, err error) {
	err = applyClientUserScopeWithDB(db, userUUID).
		Where("uuid = ?", clientUUID).
		First(&client).Error
	if err != nil {
		return models.Client{}, err
	}
	return client, nil
}

func getAllClientBasicInfoByUserWithDB(db *gorm.DB, userUUID string) (clients []models.Client, err error) {
	err = applyClientUserScopeWithDB(db, userUUID).Find(&clients).Error
	if err != nil {
		return nil, err
	}
	return clients, nil
}

func countClientsByUserWithDB(db *gorm.DB, userUUID string) (int64, error) {
	var total int64
	err := applyClientUserScopeWithDB(db.Model(&models.Client{}), userUUID).Count(&total).Error
	return total, err
}

func saveClientWithDB(db *gorm.DB, updates map[string]interface{}) error {
	clientUUID, ok := updates["uuid"].(string)
	if !ok || clientUUID == "" {
		return fmt.Errorf("invalid client UUID")
	}

	if len(updates) == 0 {
		return fmt.Errorf("no fields to update")
	}

	updates["updated_at"] = time.Now()

	err := db.Model(&models.Client{}).Where("uuid = ?", clientUUID).Updates(updates).Error
	if err != nil {
		return err
	}
	return nil
}

func saveClientForUserWithDB(db *gorm.DB, userUUID string, updates map[string]interface{}) error {
	clientUUID, ok := updates["uuid"].(string)
	if !ok || clientUUID == "" {
		return fmt.Errorf("invalid client UUID")
	}

	if len(updates) == 0 {
		return fmt.Errorf("no fields to update")
	}

	updates["updated_at"] = time.Now()
	delete(updates, "user_id")

	result := applyClientUserScopeWithDB(db.Model(&models.Client{}), userUUID).
		Where("uuid = ?", clientUUID).
		Updates(updates)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}
	return nil
}
func SaveClientInfo(update map[string]interface{}) error {
	db := dbcore.GetDBInstance()
	clientUUID, ok := update["uuid"].(string)
	if !ok || clientUUID == "" {
		return fmt.Errorf("invalid client UUID")
	}

	// 确保更新的字段不为空
	if len(update) == 0 {
		return fmt.Errorf("no fields to update")
	}

	update["updated_at"] = time.Now()

	checkInt64 := func(name string, val float64) error {
		if val < 0 {
			return fmt.Errorf("%s must be non-negative, got %d", name, int64(val))
		}
		if val > math.MaxInt64-1 {
			return fmt.Errorf("%s exceeds int64 max limit: %d", name, int64(val))
		}
		return nil
	}

	verify := func(update map[string]interface{}) error {
		if update["cpu_cores"].(float64) < 0 || update["cpu_cores"].(float64) > math.MaxInt-1 {
			return fmt.Errorf("Cpu.Cores be not a valid int64 number: %d", update["cpu_cores"])
		}
		if err := checkInt64("Ram.Total", update["mem_total"].(float64)); err != nil {
			return err
		}
		if err := checkInt64("Swap.Total", update["swap_total"].(float64)); err != nil {
			return err
		}
		if err := checkInt64("Disk.Total", update["disk_total"].(float64)); err != nil {
			return err
		}
		return nil
	}

	if err := verify(update); err != nil {
		return err
	}

	err := db.Model(&models.Client{}).Where("uuid = ?", clientUUID).Updates(update).Error
	if err != nil {
		return err
	}
	return nil
}

// 更新客户端设置
func UpdateClientConfig(config common.ClientConfig) error {
	db := dbcore.GetDBInstance()
	err := db.Save(&config).Error
	if err != nil {
		return err
	}
	return nil
}

func EditClientName(clientUUID, clientName string) error {
	db := dbcore.GetDBInstance()
	err := db.Model(&models.Client{}).Where("uuid = ?", clientUUID).Update("name", clientName).Error
	if err != nil {
		return err
	}
	return nil
}

/*
// UpdateClientByUUID 更新指定 UUID 的客户端配置

	func UpdateClientByUUID(config common.ClientConfig) error {
		db := dbcore.GetDBInstance()
		result := db.Model(&common.ClientConfig{}).Where("client_uuid = ?", config.ClientUUID).Updates(config)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return gorm.ErrRecordNotFound
		}
		return nil
	}
*/
func EditClientToken(clientUUID, token string) error {
	db := dbcore.GetDBInstance()
	err := db.Model(&models.Client{}).Where("uuid = ?", clientUUID).Update("token", token).Error
	if err != nil {
		return err
	}
	return nil
}

// CreateClient 创建新客户端
func CreateClient() (clientUUID, token string, err error) {
	userUUID, err := getDefaultClientOwnerUserUUID()
	if err != nil {
		return "", "", err
	}
	return CreateClientForUser(userUUID)
}

func CreateClientWithName(name string) (clientUUID, token string, err error) {
	userUUID, err := getDefaultClientOwnerUserUUID()
	if err != nil {
		return "", "", err
	}
	return CreateClientWithNameForUser(userUUID, name)
}

func CreateClientWithNameAndGroup(name, group string) (clientUUID, token string, err error) {
	userUUID, err := getDefaultClientOwnerUserUUID()
	if err != nil {
		return "", "", err
	}
	return CreateClientWithNameAndGroupForUser(userUUID, name, group)
}

func CreateClientForUser(userUUID string) (clientUUID, token string, err error) {
	return CreateClientWithNameAndGroupForUser(userUUID, "", "")
}

func CreateClientWithNameForUser(userUUID, name string) (clientUUID, token string, err error) {
	return CreateClientWithNameAndGroupForUser(userUUID, name, "")
}

func CreateClientWithNameAndGroupForUser(userUUID, name, group string) (clientUUID, token string, err error) {
	return createClientWithUserWithDB(dbcore.GetDBInstance(), userUUID, name, group)
}

func getDefaultClientOwnerUserUUID() (string, error) {
	return accounts.GetPreferredAdminUserUUID()
}

func createClientWithUserWithDB(db *gorm.DB, userUUID, name, group string) (clientUUID, token string, err error) {
	userUUID = strings.TrimSpace(userUUID)
	if userUUID == "" {
		return "", "", fmt.Errorf("user id is required")
	}
	if err := ensureClientQuotaWithDB(db, userUUID); err != nil {
		return "", "", err
	}

	token = utils.GenerateToken()
	clientUUID = uuid.New().String()
	if name == "" {
		name = "client_" + clientUUID[0:8]
	}
	client := models.Client{
		UUID:      clientUUID,
		Token:     token,
		UserID:    userUUID,
		Name:      name,
		Group:     group,
		CreatedAt: models.FromTime(time.Now()),
		UpdatedAt: models.FromTime(time.Now()),
	}

	err = db.Create(&client).Error
	if err != nil {
		return "", "", err
	}
	return clientUUID, token, nil
}

func ensureClientQuotaWithDB(db *gorm.DB, userUUID string) error {
	quota, err := config.GetUserServerQuota(userUUID)
	if err != nil {
		return err
	}
	if quota <= 0 {
		return nil
	}

	total, err := countClientsByUserWithDB(db, userUUID)
	if err != nil {
		return err
	}
	if total >= int64(quota) {
		return fmt.Errorf("%w: limit=%d", ErrClientQuotaExceeded, quota)
	}
	return nil
}

/*
// GetAllClients 获取所有客户端配置

	func getAllClients() (clients []models.Client, err error) {
		db := dbcore.GetDBInstance()
		err = db.Find(&clients).Error
		if err != nil {
			return nil, err
		}
		return clients, nil
	}
*/
func GetClientByUUID(uuid string) (client models.Client, err error) {
	db := dbcore.GetDBInstance()
	return getClientByUUIDWithDB(db, uuid)
}

func GetClientByUUIDForUser(uuid, userUUID string) (client models.Client, err error) {
	db := dbcore.GetDBInstance()
	return getClientByUUIDForUserWithDB(db, uuid, userUUID)
}

// GetClientBasicInfo 获取指定 UUID 的客户端基本信息
func GetClientBasicInfo(uuid string) (client models.Client, err error) {
	db := dbcore.GetDBInstance()
	err = db.Where("uuid = ?", uuid).First(&client).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return models.Client{}, fmt.Errorf("客户端不存在: %s", uuid)
		}
		return models.Client{}, err
	}
	return client, nil
}

func GetClientTokenByUUID(uuid string) (token string, err error) {
	client, err := GetClientByUUID(uuid)
	if err != nil {
		return "", err
	}
	return client.Token, nil
}

func GetClientTokenByUUIDForUser(uuid, userUUID string) (token string, err error) {
	client, err := GetClientByUUIDForUser(uuid, userUUID)
	if err != nil {
		return "", err
	}
	return client.Token, nil
}

func GetAllClientBasicInfo() (clients []models.Client, err error) {
	db := dbcore.GetDBInstance()
	err = db.Find(&clients).Error
	if err != nil {
		return nil, err
	}
	return clients, nil
}

func GetAllClientBasicInfoByUser(userUUID string) (clients []models.Client, err error) {
	db := dbcore.GetDBInstance()
	return getAllClientBasicInfoByUserWithDB(db, userUUID)
}

func CountClientsByUsers(userUUIDs []string) (map[string]int64, error) {
	db := dbcore.GetDBInstance()
	normalized := make([]string, 0, len(userUUIDs))
	seen := make(map[string]struct{}, len(userUUIDs))
	for _, userUUID := range userUUIDs {
		value := strings.TrimSpace(userUUID)
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		normalized = append(normalized, value)
	}

	result := make(map[string]int64, len(normalized))
	if len(normalized) == 0 {
		return result, nil
	}

	type row struct {
		UserID string
		Total  int64
	}
	var rows []row
	if err := db.Model(&models.Client{}).
		Select("user_id, COUNT(*) AS total").
		Where("user_id IN ?", normalized).
		Group("user_id").
		Scan(&rows).Error; err != nil {
		return nil, err
	}
	for _, row := range rows {
		result[row.UserID] = row.Total
	}
	for _, userUUID := range normalized {
		if _, ok := result[userUUID]; !ok {
			result[userUUID] = 0
		}
	}
	return result, nil
}

func NormalizeClientUUIDsForUser(userUUID string, clientUUIDs []string) ([]string, error) {
	userUUID = strings.TrimSpace(userUUID)
	if userUUID == "" {
		return nil, fmt.Errorf("user id is required")
	}

	normalized := make([]string, 0, len(clientUUIDs))
	seen := make(map[string]struct{}, len(clientUUIDs))
	for _, clientUUID := range clientUUIDs {
		value := strings.TrimSpace(clientUUID)
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		normalized = append(normalized, value)
	}

	if len(normalized) == 0 {
		return nil, fmt.Errorf("at least one client is required")
	}

	db := dbcore.GetDBInstance()
	var matched []string
	if err := applyClientUserScopeWithDB(db.Model(&models.Client{}), userUUID).
		Where("uuid IN ?", normalized).
		Pluck("uuid", &matched).Error; err != nil {
		return nil, err
	}

	if len(matched) != len(normalized) {
		valid := make(map[string]struct{}, len(matched))
		for _, item := range matched {
			valid[item] = struct{}{}
		}
		missing := make([]string, 0)
		for _, item := range normalized {
			if _, ok := valid[item]; !ok {
				missing = append(missing, item)
			}
		}
		return nil, fmt.Errorf("clients not found for current user: %s", strings.Join(missing, ", "))
	}

	return normalized, nil
}

func SaveClient(updates map[string]interface{}) error {
	db := dbcore.GetDBInstance()
	return saveClientWithDB(db, updates)
}

func SaveClientForUser(userUUID string, updates map[string]interface{}) error {
	db := dbcore.GetDBInstance()
	return saveClientForUserWithDB(db, userUUID, updates)
}
