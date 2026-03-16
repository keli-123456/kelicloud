package database

import (
	"errors"
	"sort"
	"strings"

	"github.com/google/uuid"
	"github.com/komari-monitor/komari/config"
	"github.com/komari-monitor/komari/database/dbcore"
	"github.com/komari-monitor/komari/database/models"
	"gorm.io/gorm"
)

const (
	DefaultTenantSlug = "default"
	DefaultTenantName = "Default Workspace"

	RoleOwner    = "owner"
	RoleAdmin    = "admin"
	RoleOperator = "operator"
	RoleViewer   = "viewer"
)

type AccessibleTenant struct {
	ID          string `json:"id"`
	Slug        string `json:"slug"`
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	IsDefault   bool   `json:"is_default"`
	Role        string `json:"role"`
}

type TenantMemberInfo struct {
	TenantID  string `json:"tenant_id"`
	UserUUID  string `json:"user_uuid"`
	Username  string `json:"username"`
	Role      string `json:"role"`
	CreatedAt string `json:"created_at,omitempty"`
	UpdatedAt string `json:"updated_at,omitempty"`
}

func EnsureTenantBootstrap() error {
	db := dbcore.GetDBInstance()
	defaultTenantID := ""
	err := db.Transaction(func(tx *gorm.DB) error {
		tenant, err := getOrCreateDefaultTenantTx(tx)
		if err != nil {
			return err
		}
		defaultTenantID = tenant.ID

		var users []models.User
		if err := tx.Find(&users).Error; err != nil {
			return err
		}

		for _, user := range users {
			if err := ensureTenantMemberTx(tx, tenant.ID, user.UUID, RoleOwner); err != nil {
				return err
			}
		}

		if tx.Migrator().HasTable(&models.Client{}) && tx.Migrator().HasColumn(&models.Client{}, "tenant_id") {
			if err := tx.Model(&models.Client{}).
				Where("tenant_id = '' OR tenant_id IS NULL").
				Update("tenant_id", tenant.ID).Error; err != nil {
				return err
			}
		}

		if tx.Migrator().HasTable(&models.Session{}) && tx.Migrator().HasColumn(&models.Session{}, "current_tenant_id") {
			if err := tx.Model(&models.Session{}).
				Where("current_tenant_id = '' OR current_tenant_id IS NULL").
				Update("current_tenant_id", tenant.ID).Error; err != nil {
				return err
			}
		}
		if tx.Migrator().HasTable(&models.Clipboard{}) && tx.Migrator().HasColumn(&models.Clipboard{}, "tenant_id") {
			if err := tx.Model(&models.Clipboard{}).
				Where("tenant_id = '' OR tenant_id IS NULL").
				Update("tenant_id", tenant.ID).Error; err != nil {
				return err
			}
		}
		if tx.Migrator().HasTable(&models.PingTask{}) && tx.Migrator().HasColumn(&models.PingTask{}, "tenant_id") {
			if err := tx.Model(&models.PingTask{}).
				Where("tenant_id = '' OR tenant_id IS NULL").
				Update("tenant_id", tenant.ID).Error; err != nil {
				return err
			}
		}
		if tx.Migrator().HasTable(&models.LoadNotification{}) && tx.Migrator().HasColumn(&models.LoadNotification{}, "tenant_id") {
			if err := tx.Model(&models.LoadNotification{}).
				Where("tenant_id = '' OR tenant_id IS NULL").
				Update("tenant_id", tenant.ID).Error; err != nil {
				return err
			}
		}
		if tx.Migrator().HasTable(&models.Task{}) && tx.Migrator().HasColumn(&models.Task{}, "tenant_id") {
			if err := tx.Model(&models.Task{}).
				Where("tenant_id = '' OR tenant_id IS NULL").
				Update("tenant_id", tenant.ID).Error; err != nil {
				return err
			}
		}
		if tx.Migrator().HasTable(&models.TaskResult{}) && tx.Migrator().HasColumn(&models.TaskResult{}, "tenant_id") {
			if err := tx.Model(&models.TaskResult{}).
				Where("tenant_id = '' OR tenant_id IS NULL").
				Update("tenant_id", tenant.ID).Error; err != nil {
				return err
			}
		}
		if tx.Migrator().HasTable(&models.Log{}) && tx.Migrator().HasColumn(&models.Log{}, "tenant_id") {
			if err := tx.Model(&models.Log{}).
				Where("tenant_id = '' OR tenant_id IS NULL").
				Update("tenant_id", tenant.ID).Error; err != nil {
				return err
			}
		}
		if tx.Migrator().HasTable(&models.CloudProvider{}) && tx.Migrator().HasColumn(&models.CloudProvider{}, "tenant_id") {
			if err := tx.Model(&models.CloudProvider{}).
				Where("tenant_id = '' OR tenant_id IS NULL").
				Update("tenant_id", tenant.ID).Error; err != nil {
				return err
			}
		}
		if tx.Migrator().HasTable(&models.CloudInstanceShare{}) && tx.Migrator().HasColumn(&models.CloudInstanceShare{}, "tenant_id") {
			if err := tx.Model(&models.CloudInstanceShare{}).
				Where("tenant_id = '' OR tenant_id IS NULL").
				Update("tenant_id", tenant.ID).Error; err != nil {
				return err
			}
		}
		if tx.Migrator().HasTable(&models.ThemeConfiguration{}) && tx.Migrator().HasColumn(&models.ThemeConfiguration{}, "tenant_id") {
			if err := tx.Model(&models.ThemeConfiguration{}).
				Where("tenant_id = '' OR tenant_id IS NULL").
				Update("tenant_id", tenant.ID).Error; err != nil {
				return err
			}
		}
		if err := ensureTenantScopedCloudSchemaTx(tx); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	return config.BackfillTenantScopedConfigs(defaultTenantID)
}

func EnsureDefaultTenantMembership(userUUID, role string) error {
	db := dbcore.GetDBInstance()
	return db.Transaction(func(tx *gorm.DB) error {
		tenant, err := getOrCreateDefaultTenantTx(tx)
		if err != nil {
			return err
		}
		return ensureTenantMemberTx(tx, tenant.ID, userUUID, NormalizeTenantRole(role))
	})
}

func ListAccessibleTenantsByUser(userUUID string) ([]AccessibleTenant, error) {
	type tenantRow struct {
		ID          string
		Slug        string
		Name        string
		Description string
		IsDefault   bool
		Role        string
	}

	var rows []tenantRow
	db := dbcore.GetDBInstance()
	if err := db.Table("tenant_members").
		Select("tenants.id, tenants.slug, tenants.name, tenants.description, tenants.is_default, tenant_members.role").
		Joins("JOIN tenants ON tenants.id = tenant_members.tenant_id").
		Where("tenant_members.user_uuid = ?", userUUID).
		Scan(&rows).Error; err != nil {
		return nil, err
	}

	result := make([]AccessibleTenant, 0, len(rows))
	for _, row := range rows {
		result = append(result, AccessibleTenant{
			ID:          row.ID,
			Slug:        row.Slug,
			Name:        row.Name,
			Description: row.Description,
			IsDefault:   row.IsDefault,
			Role:        NormalizeTenantRole(row.Role),
		})
	}

	sort.Slice(result, func(i, j int) bool {
		if result[i].IsDefault != result[j].IsDefault {
			return result[i].IsDefault
		}
		if result[i].Name != result[j].Name {
			return strings.ToLower(result[i].Name) < strings.ToLower(result[j].Name)
		}
		return result[i].ID < result[j].ID
	})

	return result, nil
}

func GetAccessibleTenantByUser(userUUID, tenantID string) (*AccessibleTenant, error) {
	tenants, err := ListAccessibleTenantsByUser(userUUID)
	if err != nil {
		return nil, err
	}
	for _, item := range tenants {
		if item.ID == tenantID {
			tenant := item
			return &tenant, nil
		}
	}
	return nil, gorm.ErrRecordNotFound
}

func GetTenantByIdentifier(identifier string) (*models.Tenant, error) {
	db := dbcore.GetDBInstance()
	var tenant models.Tenant

	identifier = strings.TrimSpace(identifier)
	if identifier == "" {
		return nil, gorm.ErrRecordNotFound
	}

	err := db.Where("id = ? OR slug = ?", identifier, identifier).First(&tenant).Error
	if err != nil {
		return nil, err
	}
	return &tenant, nil
}

func ResolveAccessibleTenant(userUUID, preferredTenantID string) (*AccessibleTenant, []AccessibleTenant, error) {
	tenants, err := ListAccessibleTenantsByUser(userUUID)
	if err != nil {
		return nil, nil, err
	}
	if len(tenants) == 0 {
		return nil, nil, gorm.ErrRecordNotFound
	}

	if preferredTenantID != "" {
		for _, item := range tenants {
			if item.ID == preferredTenantID {
				tenant := item
				return &tenant, tenants, nil
			}
		}
	}

	for _, item := range tenants {
		if item.IsDefault {
			tenant := item
			return &tenant, tenants, nil
		}
	}

	tenant := tenants[0]
	return &tenant, tenants, nil
}

func GetDefaultTenant() (*models.Tenant, error) {
	db := dbcore.GetDBInstance()
	var tenant *models.Tenant
	err := db.Transaction(func(tx *gorm.DB) error {
		item, err := getOrCreateDefaultTenantTx(tx)
		if err != nil {
			return err
		}
		tenant = item
		return nil
	})
	if err != nil {
		return nil, err
	}
	return tenant, nil
}

func GetDefaultTenantID() (string, error) {
	tenant, err := GetDefaultTenant()
	if err != nil {
		return "", err
	}
	return tenant.ID, nil
}

func CreateTenant(name, slug, description, ownerUUID string) (*models.Tenant, error) {
	db := dbcore.GetDBInstance()
	return createTenantWithDB(db, name, slug, description, ownerUUID)
}

func createTenantWithDB(db *gorm.DB, name, slug, description, ownerUUID string) (*models.Tenant, error) {
	var tenant *models.Tenant

	err := db.Transaction(func(tx *gorm.DB) error {
		item := &models.Tenant{
			ID:          uuid.NewString(),
			Slug:        normalizeTenantSlug(slug, name),
			Name:        strings.TrimSpace(name),
			Description: strings.TrimSpace(description),
			IsDefault:   false,
		}
		if item.Name == "" {
			return errors.New("tenant name is required")
		}
		if item.Slug == "" {
			return errors.New("tenant slug is required")
		}

		if err := tx.Create(item).Error; err != nil {
			return err
		}
		if err := ensureTenantMemberTx(tx, item.ID, ownerUUID, RoleOwner); err != nil {
			return err
		}
		tenant = item
		return nil
	})
	if err != nil {
		return nil, err
	}
	return tenant, nil
}

func ListTenantMembers(tenantID string) ([]TenantMemberInfo, error) {
	db := dbcore.GetDBInstance()
	return listTenantMembersWithDB(db, tenantID)
}

func listTenantMembersWithDB(db *gorm.DB, tenantID string) ([]TenantMemberInfo, error) {
	type memberRow struct {
		TenantID  string
		UserUUID  string
		Username  string
		Role      string
		CreatedAt models.LocalTime
		UpdatedAt models.LocalTime
	}

	var rows []memberRow
	if err := db.Table("tenant_members").
		Select("tenant_members.tenant_id, tenant_members.user_uuid, users.username, tenant_members.role, tenant_members.created_at, tenant_members.updated_at").
		Joins("JOIN users ON users.uuid = tenant_members.user_uuid").
		Where("tenant_members.tenant_id = ?", tenantID).
		Order("users.username ASC").
		Scan(&rows).Error; err != nil {
		return nil, err
	}

	result := make([]TenantMemberInfo, 0, len(rows))
	for _, row := range rows {
		result = append(result, TenantMemberInfo{
			TenantID:  row.TenantID,
			UserUUID:  row.UserUUID,
			Username:  row.Username,
			Role:      NormalizeTenantRole(row.Role),
			CreatedAt: row.CreatedAt.ToTime().Format("2006-01-02T15:04:05Z07:00"),
			UpdatedAt: row.UpdatedAt.ToTime().Format("2006-01-02T15:04:05Z07:00"),
		})
	}
	return result, nil
}

func GetTenantMember(tenantID, userUUID string) (*models.TenantMember, error) {
	db := dbcore.GetDBInstance()
	var member models.TenantMember
	if err := db.Where("tenant_id = ? AND user_uuid = ?", tenantID, userUUID).First(&member).Error; err != nil {
		return nil, err
	}
	member.Role = NormalizeTenantRole(member.Role)
	return &member, nil
}

func AddTenantMember(tenantID, userUUID, role string) error {
	db := dbcore.GetDBInstance()
	return db.Transaction(func(tx *gorm.DB) error {
		return ensureTenantMemberTx(tx, tenantID, userUUID, role)
	})
}

func UpdateTenantMemberRole(tenantID, userUUID, role string) error {
	db := dbcore.GetDBInstance()
	role = NormalizeTenantRole(role)
	return db.Model(&models.TenantMember{}).
		Where("tenant_id = ? AND user_uuid = ?", tenantID, userUUID).
		Update("role", role).Error
}

func DeleteTenantMember(tenantID, userUUID string) error {
	db := dbcore.GetDBInstance()
	return db.Where("tenant_id = ? AND user_uuid = ?", tenantID, userUUID).Delete(&models.TenantMember{}).Error
}

func CountTenantMembersByRole(tenantID, role string) (int64, error) {
	db := dbcore.GetDBInstance()
	var count int64
	if err := db.Model(&models.TenantMember{}).
		Where("tenant_id = ? AND role = ?", tenantID, NormalizeTenantRole(role)).
		Count(&count).Error; err != nil {
		return 0, err
	}
	return count, nil
}

func getOrCreateDefaultTenantTx(tx *gorm.DB) (*models.Tenant, error) {
	var tenant models.Tenant
	err := tx.Where("is_default = ?", true).First(&tenant).Error
	if err == nil {
		return &tenant, nil
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, err
	}

	tenant = models.Tenant{
		ID:        uuid.NewString(),
		Slug:      DefaultTenantSlug,
		Name:      DefaultTenantName,
		IsDefault: true,
	}
	if err := tx.Create(&tenant).Error; err != nil {
		return nil, err
	}
	return &tenant, nil
}

func ensureTenantMemberTx(tx *gorm.DB, tenantID, userUUID, role string) error {
	member := models.TenantMember{
		TenantID: tenantID,
		UserUUID: userUUID,
		Role:     NormalizeTenantRole(role),
	}
	return tx.Where("tenant_id = ? AND user_uuid = ?", tenantID, userUUID).
		Assign(map[string]any{"role": member.Role}).
		FirstOrCreate(&member).Error
}

func NormalizeTenantRole(role string) string {
	switch strings.ToLower(strings.TrimSpace(role)) {
	case RoleOwner:
		return RoleOwner
	case RoleAdmin:
		return RoleAdmin
	case RoleOperator:
		return RoleOperator
	default:
		return RoleViewer
	}
}

func normalizeTenantSlug(slug, fallbackName string) string {
	value := strings.TrimSpace(strings.ToLower(slug))
	if value == "" {
		value = strings.TrimSpace(strings.ToLower(fallbackName))
	}
	value = strings.ReplaceAll(value, "_", "-")

	var builder strings.Builder
	lastDash := false
	for _, ch := range value {
		switch {
		case ch >= 'a' && ch <= 'z':
			builder.WriteRune(ch)
			lastDash = false
		case ch >= '0' && ch <= '9':
			builder.WriteRune(ch)
			lastDash = false
		default:
			if !lastDash && builder.Len() > 0 {
				builder.WriteRune('-')
				lastDash = true
			}
		}
	}

	result := strings.Trim(builder.String(), "-")
	if len(result) > 64 {
		result = strings.Trim(result[:64], "-")
	}
	return result
}

func IsTenantRoleAtLeast(role, expected string) bool {
	return tenantRoleRank(role) >= tenantRoleRank(expected)
}

func tenantRoleRank(role string) int {
	switch NormalizeTenantRole(role) {
	case RoleOwner:
		return 4
	case RoleAdmin:
		return 3
	case RoleOperator:
		return 2
	default:
		return 1
	}
}

func ensureTenantScopedCloudSchemaTx(tx *gorm.DB) error {
	if tx == nil || tx.Dialector.Name() != "mysql" {
		return nil
	}
	if err := ensureCloudProviderSchemaTx(tx); err != nil {
		return err
	}
	if err := ensureCloudInstanceShareSchemaTx(tx); err != nil {
		return err
	}
	return ensureThemeConfigurationSchemaTx(tx)
}

func ensureCloudProviderSchemaTx(tx *gorm.DB) error {
	if !tx.Migrator().HasTable(&models.CloudProvider{}) || !tx.Migrator().HasColumn(&models.CloudProvider{}, "tenant_id") {
		return nil
	}

	if err := dropMySQLUniqueIndexesByColumnsTx(tx, "cloud_providers", []string{"name"}); err != nil {
		return err
	}

	primaryColumns, err := mysqlPrimaryKeyColumnsTx(tx, "cloud_providers")
	if err != nil {
		return err
	}
	expected := []string{"tenant_id", "name"}
	if stringSlicesEqual(primaryColumns, expected) {
		return nil
	}

	if len(primaryColumns) > 0 {
		if err := tx.Exec("ALTER TABLE `cloud_providers` DROP PRIMARY KEY").Error; err != nil {
			return err
		}
	}
	return tx.Exec("ALTER TABLE `cloud_providers` ADD PRIMARY KEY (`tenant_id`, `name`)").Error
}

func ensureCloudInstanceShareSchemaTx(tx *gorm.DB) error {
	if !tx.Migrator().HasTable(&models.CloudInstanceShare{}) || !tx.Migrator().HasColumn(&models.CloudInstanceShare{}, "tenant_id") {
		return nil
	}

	if err := dropMySQLIndexIfExistsTx(tx, "cloud_instance_shares", "idx_cloud_instance_shares_resource"); err != nil {
		return err
	}
	if err := dropMySQLUniqueIndexesByColumnsTx(tx, "cloud_instance_shares", []string{"provider", "resource_type", "resource_id"}); err != nil {
		return err
	}
	if tx.Migrator().HasIndex(&models.CloudInstanceShare{}, "idx_cloud_instance_shares_tenant_resource") {
		return nil
	}
	return tx.Exec(
		"CREATE UNIQUE INDEX `idx_cloud_instance_shares_tenant_resource` ON `cloud_instance_shares` (`tenant_id`, `provider`, `resource_type`, `resource_id`)",
	).Error
}

func ensureThemeConfigurationSchemaTx(tx *gorm.DB) error {
	if !tx.Migrator().HasTable(&models.ThemeConfiguration{}) || !tx.Migrator().HasColumn(&models.ThemeConfiguration{}, "tenant_id") {
		return nil
	}

	if err := dropMySQLUniqueIndexesByColumnsTx(tx, "theme_configurations", []string{"short"}); err != nil {
		return err
	}

	primaryColumns, err := mysqlPrimaryKeyColumnsTx(tx, "theme_configurations")
	if err != nil {
		return err
	}
	expected := []string{"tenant_id", "short"}
	if stringSlicesEqual(primaryColumns, expected) {
		return nil
	}

	if len(primaryColumns) > 0 {
		if err := tx.Exec("ALTER TABLE `theme_configurations` DROP PRIMARY KEY").Error; err != nil {
			return err
		}
	}
	return tx.Exec("ALTER TABLE `theme_configurations` ADD PRIMARY KEY (`tenant_id`, `short`)").Error
}

func mysqlPrimaryKeyColumnsTx(tx *gorm.DB, tableName string) ([]string, error) {
	type constraintRow struct {
		ColumnName      string
		OrdinalPosition int
	}

	var rows []constraintRow
	if err := tx.Raw(
		`SELECT column_name, ordinal_position
		FROM information_schema.key_column_usage
		WHERE table_schema = DATABASE() AND table_name = ? AND constraint_name = 'PRIMARY'
		ORDER BY ordinal_position`,
		tableName,
	).Scan(&rows).Error; err != nil {
		return nil, err
	}

	columns := make([]string, 0, len(rows))
	for _, row := range rows {
		columns = append(columns, strings.ToLower(strings.TrimSpace(row.ColumnName)))
	}
	return columns, nil
}

func dropMySQLIndexIfExistsTx(tx *gorm.DB, tableName, indexName string) error {
	if indexName == "" {
		return nil
	}
	var count int64
	if err := tx.Raw(
		`SELECT COUNT(*)
		FROM information_schema.statistics
		WHERE table_schema = DATABASE() AND table_name = ? AND index_name = ?`,
		tableName,
		indexName,
	).Scan(&count).Error; err != nil {
		return err
	}
	if count == 0 {
		return nil
	}
	return tx.Exec("DROP INDEX `" + indexName + "` ON `" + tableName + "`").Error
}

func dropMySQLUniqueIndexesByColumnsTx(tx *gorm.DB, tableName string, columns []string) error {
	type indexRow struct {
		IndexName  string
		ColumnName string
		SeqInIndex int
		NonUnique  int
	}

	var rows []indexRow
	if err := tx.Raw(
		`SELECT index_name, column_name, seq_in_index, non_unique
		FROM information_schema.statistics
		WHERE table_schema = DATABASE() AND table_name = ?
		ORDER BY index_name, seq_in_index`,
		tableName,
	).Scan(&rows).Error; err != nil {
		return err
	}

	normalized := make([]string, 0, len(columns))
	for _, column := range columns {
		normalized = append(normalized, strings.ToLower(strings.TrimSpace(column)))
	}

	grouped := make(map[string][]indexRow)
	for _, row := range rows {
		grouped[row.IndexName] = append(grouped[row.IndexName], row)
	}

	names := make([]string, 0, len(grouped))
	for indexName := range grouped {
		names = append(names, indexName)
	}
	sort.Strings(names)

	for _, indexName := range names {
		if strings.EqualFold(indexName, "PRIMARY") {
			continue
		}
		group := grouped[indexName]
		if len(group) == 0 || group[0].NonUnique != 0 {
			continue
		}
		sort.Slice(group, func(i, j int) bool {
			return group[i].SeqInIndex < group[j].SeqInIndex
		})
		indexColumns := make([]string, 0, len(group))
		for _, row := range group {
			indexColumns = append(indexColumns, strings.ToLower(strings.TrimSpace(row.ColumnName)))
		}
		if !stringSlicesEqual(indexColumns, normalized) {
			continue
		}
		if err := tx.Exec("DROP INDEX `" + indexName + "` ON `" + tableName + "`").Error; err != nil {
			return err
		}
	}

	return nil
}

func stringSlicesEqual(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
