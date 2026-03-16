package clients

import (
	"math"
	"strings"
	"time"

	"github.com/komari-monitor/komari/common"
	"github.com/komari-monitor/komari/database"
	"github.com/komari-monitor/komari/database/dbcore"
	"github.com/komari-monitor/komari/database/models"
	"github.com/komari-monitor/komari/utils"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"fmt"

	"github.com/google/uuid"
)

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

func DeleteClientForTenant(tenantID, clientUUID string) error {
	db := dbcore.GetDBInstance()
	return db.Transaction(func(tx *gorm.DB) error {
		if _, err := getClientByUUIDForTenantWithDB(tx, clientUUID, tenantID); err != nil {
			return err
		}
		return deleteClientTx(tx, clientUUID)
	})
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

func getClientByUUIDForTenantWithDB(db *gorm.DB, clientUUID, tenantID string) (client models.Client, err error) {
	err = db.Where("uuid = ? AND tenant_id = ?", clientUUID, tenantID).First(&client).Error
	if err != nil {
		return models.Client{}, err
	}
	return client, nil
}

func getAllClientBasicInfoWithDB(db *gorm.DB, tenantID string) (clients []models.Client, err error) {
	err = db.Where("tenant_id = ?", tenantID).Find(&clients).Error
	if err != nil {
		return nil, err
	}
	return clients, nil
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

func saveClientForTenantWithDB(db *gorm.DB, tenantID string, updates map[string]interface{}) error {
	clientUUID, ok := updates["uuid"].(string)
	if !ok || clientUUID == "" {
		return fmt.Errorf("invalid client UUID")
	}

	if len(updates) == 0 {
		return fmt.Errorf("no fields to update")
	}

	updates["updated_at"] = time.Now()

	result := db.Model(&models.Client{}).
		Where("uuid = ? AND tenant_id = ?", clientUUID, tenantID).
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
	tenantID, err := database.GetDefaultTenantID()
	if err != nil {
		return "", "", err
	}
	return CreateClientForTenant(tenantID)
}

func CreateClientWithName(name string) (clientUUID, token string, err error) {
	tenantID, err := database.GetDefaultTenantID()
	if err != nil {
		return "", "", err
	}
	return CreateClientWithNameForTenant(tenantID, name)
}

func CreateClientWithNameAndGroup(name, group string) (clientUUID, token string, err error) {
	tenantID, err := database.GetDefaultTenantID()
	if err != nil {
		return "", "", err
	}
	return CreateClientWithNameAndGroupForTenant(tenantID, name, group)
}

func CreateClientForTenant(tenantID string) (clientUUID, token string, err error) {
	return CreateClientWithNameAndGroupForTenant(tenantID, "", "")
}

func CreateClientWithNameForTenant(tenantID, name string) (clientUUID, token string, err error) {
	return CreateClientWithNameAndGroupForTenant(tenantID, name, "")
}

func CreateClientWithNameAndGroupForTenant(tenantID, name, group string) (clientUUID, token string, err error) {
	if tenantID == "" {
		return "", "", fmt.Errorf("tenant id is required")
	}

	db := dbcore.GetDBInstance()
	token = utils.GenerateToken()
	clientUUID = uuid.New().String()
	if name == "" {
		name = "client_" + clientUUID[0:8]
	}
	client := models.Client{
		UUID:      clientUUID,
		Token:     token,
		TenantID:  tenantID,
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

func GetClientByUUIDForTenant(uuid, tenantID string) (client models.Client, err error) {
	db := dbcore.GetDBInstance()
	return getClientByUUIDForTenantWithDB(db, uuid, tenantID)
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

func GetClientTokenByUUIDForTenant(uuid, tenantID string) (token string, err error) {
	client, err := GetClientByUUIDForTenant(uuid, tenantID)
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

func GetAllClientBasicInfoByTenant(tenantID string) (clients []models.Client, err error) {
	db := dbcore.GetDBInstance()
	return getAllClientBasicInfoWithDB(db, tenantID)
}

func NormalizeClientUUIDsForTenant(tenantID string, clientUUIDs []string) ([]string, error) {
	if tenantID == "" {
		return nil, fmt.Errorf("tenant id is required")
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
	if err := db.Model(&models.Client{}).
		Where("tenant_id = ? AND uuid IN ?", tenantID, normalized).
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
		return nil, fmt.Errorf("clients not found in current tenant: %s", strings.Join(missing, ", "))
	}

	return normalized, nil
}

func SaveClient(updates map[string]interface{}) error {
	db := dbcore.GetDBInstance()
	return saveClientWithDB(db, updates)
}

func SaveClientForTenant(tenantID string, updates map[string]interface{}) error {
	db := dbcore.GetDBInstance()
	return saveClientForTenantWithDB(db, tenantID, updates)
}
