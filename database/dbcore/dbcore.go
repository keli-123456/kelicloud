package dbcore

import (
	"archive/zip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/komari-monitor/komari/cmd/flags"
	"github.com/komari-monitor/komari/common"
	"github.com/komari-monitor/komari/config"
	"github.com/komari-monitor/komari/database/models"
	logutil "github.com/komari-monitor/komari/utils/log"
	"gorm.io/driver/mysql"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

const migrationKeyFailoverCooldownDefaultZero = "20260411_failover_cooldown_default_zero"

func shouldUseSQLiteForTests() bool {
	return strings.HasSuffix(filepath.Base(os.Args[0]), ".test") && flags.DatabaseType == "sqlite"
}

// zipDirectoryExcluding 将 srcDir 打包为 dstZip，exclude 是绝对路径集合需要排除
func zipDirectoryExcluding(srcDir, dstZip string, exclude map[string]struct{}) error {
	// 规范化排除路径为绝对路径
	normExclude := make(map[string]struct{}, len(exclude))
	for p := range exclude {
		abs, _ := filepath.Abs(p)
		normExclude[abs] = struct{}{}
	}

	out, err := os.Create(dstZip)
	if err != nil {
		return err
	}
	defer out.Close()

	zw := zip.NewWriter(out)
	defer zw.Close()

	absSrc, _ := filepath.Abs(srcDir)
	walkErr := filepath.Walk(absSrc, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		// 排除 backup.zip 本身
		if _, ok := normExclude[path]; ok {
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		// 计算 zip 内相对路径
		rel, err := filepath.Rel(absSrc, path)
		if err != nil {
			return err
		}
		// 根目录跳过
		if rel == "." {
			return nil
		}
		// 替换为正斜杠
		zipName := filepath.ToSlash(rel)

		if info.IsDir() {
			_, err := zw.Create(zipName + "/")
			return err
		}
		// 普通文件
		fh, err := os.Open(path)
		if err != nil {
			return err
		}
		w, err := zw.Create(zipName)
		if err != nil {
			fh.Close()
			return err
		}
		if _, err := io.Copy(w, fh); err != nil {
			fh.Close()
			return err
		}
		fh.Close()
		return nil
	})
	if walkErr != nil {
		return walkErr
	}
	return zw.Close()
}

// removeAllInDirExcept 删除 dir 下除 exclude 指定绝对路径外的所有文件和文件夹
func removeAllInDirExcept(dir string, exclude map[string]struct{}) error {
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return err
	}
	normExclude := make(map[string]struct{}, len(exclude))
	for p := range exclude {
		abs, _ := filepath.Abs(p)
		normExclude[abs] = struct{}{}
	}
	entries, err := os.ReadDir(absDir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		full := filepath.Join(absDir, e.Name())
		if _, ok := normExclude[full]; ok {
			continue
		}
		if err := os.RemoveAll(full); err != nil {
			return err
		}
	}
	return nil
}

// unzipToDir 将 zipPath 解压到 dstDir，包含路径遍历保护
func unzipToDir(zipPath, dstDir string) error {
	zr, err := zip.OpenReader(zipPath)
	if err != nil {
		return err
	}
	defer zr.Close()

	if err := os.MkdirAll(dstDir, 0755); err != nil {
		return err
	}
	absDst, _ := filepath.Abs(dstDir)

	for _, f := range zr.File {
		// 构造目标路径并做路径遍历保护
		cleanName := filepath.Clean(f.Name)
		targetPath := filepath.Join(absDst, cleanName)
		if !strings.HasPrefix(targetPath, absDst+string(os.PathSeparator)) && targetPath != absDst {
			return fmt.Errorf("illegal file path in zip: %s", f.Name)
		}
		if f.FileInfo().IsDir() {
			if err := os.MkdirAll(targetPath, 0755); err != nil {
				return err
			}
			continue
		}
		if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
			return err
		}
		rc, err := f.Open()
		if err != nil {
			return err
		}
		out, err := os.Create(targetPath)
		if err != nil {
			rc.Close()
			return err
		}
		if _, err := io.Copy(out, rc); err != nil {
			out.Close()
			rc.Close()
			return err
		}
		out.Close()
		rc.Close()
	}
	return nil
}

// mergeClientInfo 将旧版ClientInfo数据迁移到新版Client表
func mergeClientInfo(db *gorm.DB) {
	var clientInfos []common.ClientInfo
	if err := db.Find(&clientInfos).Error; err != nil {
		log.Printf("Failed to read ClientInfo table: %v", err)
		return
	}

	for _, info := range clientInfos {
		var client models.Client
		if err := db.Where("uuid = ?", info.UUID).First(&client).Error; err != nil {
			log.Printf("Could not find Client record with UUID %s: %v", info.UUID, err)
			continue
		}

		// 更新Client记录
		client.Name = info.Name
		client.CpuName = info.CpuName
		client.Virtualization = info.Virtualization
		client.Arch = info.Arch
		client.CpuCores = info.CpuCores
		client.OS = info.OS
		client.GpuName = info.GpuName
		client.IPv4 = info.IPv4
		client.IPv6 = info.IPv6
		client.Region = info.Region
		client.Remark = info.Remark
		client.PublicRemark = info.PublicRemark
		client.MemTotal = info.MemTotal
		client.SwapTotal = info.SwapTotal
		client.DiskTotal = info.DiskTotal
		client.Version = info.Version
		client.Weight = info.Weight
		client.Price = info.Price
		client.BillingCycle = info.BillingCycle
		client.ExpiredAt = models.FromTime(info.ExpiredAt)
		// Save updated Client record
		if err := db.Save(&client).Error; err != nil {
			log.Printf("Failed to update Client record: %v", err)
			continue
		}
	}

	// Backup and rename old table after migration
	if err := db.Migrator().RenameTable("client_infos", "client_infos_backup"); err != nil {
		log.Printf("Failed to backup ClientInfo table: %v", err)
		return
	}
	log.Println("Data migration completed, old table has been backed up as client_infos_backup")
}

func MergeDatabase(db *gorm.DB) {
	legacyConfigTableExists := db.Migrator().HasTable("configs") && db.Migrator().HasColumn(&models.Config{}, "id")

	if db.Migrator().HasTable("client_infos") {
		log.Println("[>0.0.5] Legacy ClientInfo table detected, starting data migration...")
		mergeClientInfo(db)
	}
	if legacyConfigTableExists && db.Migrator().HasColumn(&models.Config{}, "allow_cros") {
		log.Println("[>0.0.5a] Renaming column 'allow_cros' to 'allow_cors' in config table...")
		db.Migrator().RenameColumn(&models.Config{}, "allow_cros", "allow_cors")
	}
	if db.Migrator().HasColumn(&models.LoadNotification{}, "client") {
		log.Println("[>0.1.4] Rebuilding LoadNotification table....")
		db.Migrator().DropTable(&models.LoadNotification{})
	}
	if !db.Migrator().HasTable(&models.OidcProvider{}) && legacyConfigTableExists {
		log.Println("[>1.0.2] Merge OidcProvider table....")
		var config struct {
			OAuthClientID     string `json:"o_auth_client_id" gorm:"type:varchar(255)"`
			OAuthClientSecret string `json:"o_auth_client_secret" gorm:"type:varchar(255)"`
		}
		if err := db.Raw("SELECT * FROM configs LIMIT 1").Scan(&config).Error; err != nil {
			log.Println("Failed to get config for OIDC provider migration:", err)
		}
		db.AutoMigrate(&models.OidcProvider{})
		j, err := json.Marshal(&map[string]string{
			"client_id":     config.OAuthClientID,
			"client_secret": config.OAuthClientSecret,
		})
		if err != nil {
			log.Println("Failed to marshal OIDC provider config:", err)
			return
		}
		db.Save(&models.OidcProvider{
			Name:     "github",
			Addition: string(j),
		})
		db.AutoMigrate(&models.Config{})
		db.Model(&models.Config{}).Where("id = 1").Update("o_auth_provider", "github")
	}
	if !db.Migrator().HasTable(&models.MessageSenderProvider{}) && legacyConfigTableExists {
		log.Println("[>1.0.2] Migrate MessageSender configuration....")
		var config struct {
			TelegramBotToken   string `json:"telegram_bot_token" gorm:"type:varchar(255)"`
			TelegramChatID     string `json:"telegram_chat_id" gorm:"type:varchar(255)"`
			TelegramEndpoint   string `json:"telegram_endpoint" gorm:"type:varchar(255)"`
			EmailHost          string `json:"email_host" gorm:"type:varchar(255)"`
			EmailPort          int    `json:"email_port" gorm:"type:int"`
			EmailUsername      string `json:"email_username" gorm:"type:varchar(255)"`
			EmailPassword      string `json:"email_password" gorm:"type:varchar(255)"`
			EmailSender        string `json:"email_sender" gorm:"type:varchar(255)"`
			EmailReceiver      string `json:"email_receiver" gorm:"type:varchar(255)"`
			EmailUseSSL        bool   `json:"email_use_ssl" gorm:"type:boolean"`
			NotificationMethod string `json:"notification_method" gorm:"type:varchar(50)"`
		}
		if err := db.Raw("SELECT * FROM configs LIMIT 1").Scan(&config).Error; err != nil {
			log.Println("Failed to get config for MessageSender migration:", err)
		}

		db.AutoMigrate(&models.MessageSenderProvider{})

		// 迁移Telegram配置
		if config.NotificationMethod == "telegram" && config.TelegramBotToken != "" {
			telegramConfig := map[string]interface{}{
				"bot_token": config.TelegramBotToken,
				"chat_id":   config.TelegramChatID,
				"endpoint":  config.TelegramEndpoint,
			}
			if telegramConfig["endpoint"] == "" {
				telegramConfig["endpoint"] = "https://api.telegram.org/bot"
			}
			telegramConfigJSON, err := json.Marshal(telegramConfig)
			if err != nil {
				log.Println("Failed to marshal Telegram config:", err)
			} else {
				db.Save(&models.MessageSenderProvider{
					Name:     "telegram",
					Addition: string(telegramConfigJSON),
				})
			}
		}

		// 迁移Email配置
		if config.NotificationMethod == "email" && config.EmailHost != "" {
			emailConfig := map[string]interface{}{
				"host":     config.EmailHost,
				"port":     config.EmailPort,
				"username": config.EmailUsername,
				"password": config.EmailPassword,
				"sender":   config.EmailSender,
				"receiver": config.EmailReceiver,
				"use_ssl":  config.EmailUseSSL,
			}
			emailConfigJSON, err := json.Marshal(emailConfig)
			if err != nil {
				log.Println("Failed to marshal Email config:", err)
			} else {
				db.Save(&models.MessageSenderProvider{
					Name:     "email",
					Addition: string(emailConfigJSON),
				})
			}
		}

		// 删除旧的配置字段
		if db.Migrator().HasColumn(&models.Config{}, "telegram_bot_token") {
			db.Migrator().DropColumn(&models.Config{}, "telegram_bot_token")
		}
		if db.Migrator().HasColumn(&models.Config{}, "telegram_chat_id") {
			db.Migrator().DropColumn(&models.Config{}, "telegram_chat_id")
		}
		if db.Migrator().HasColumn(&models.Config{}, "telegram_endpoint") {
			db.Migrator().DropColumn(&models.Config{}, "telegram_endpoint")
		}
		if db.Migrator().HasColumn(&models.Config{}, "email_host") {
			db.Migrator().DropColumn(&models.Config{}, "email_host")
		}
		if db.Migrator().HasColumn(&models.Config{}, "email_port") {
			db.Migrator().DropColumn(&models.Config{}, "email_port")
		}
		if db.Migrator().HasColumn(&models.Config{}, "email_username") {
			db.Migrator().DropColumn(&models.Config{}, "email_username")
		}
		if db.Migrator().HasColumn(&models.Config{}, "email_password") {
			db.Migrator().DropColumn(&models.Config{}, "email_password")
		}
		if db.Migrator().HasColumn(&models.Config{}, "email_sender") {
			db.Migrator().DropColumn(&models.Config{}, "email_sender")
		}
		if db.Migrator().HasColumn(&models.Config{}, "email_receiver") {
			db.Migrator().DropColumn(&models.Config{}, "email_receiver")
		}
		if db.Migrator().HasColumn(&models.Config{}, "email_use_ssl") {
			db.Migrator().DropColumn(&models.Config{}, "email_use_ssl")
		}
	}
	if legacyConfigTableExists {
		if db.Migrator().HasColumn(&models.Config{}, "theme") {
			db.Migrator().DropColumn(&models.Config{}, "theme")
		}
		if db.Migrator().HasColumn(&models.Config{}, "private_site") {
			db.Migrator().DropColumn(&models.Config{}, "private_site")
		}
	}
}

func prepareMySQLSchemaCompatibility(db *gorm.DB) {
	if db == nil || db.Dialector.Name() != "mysql" {
		return
	}

	prepareMySQLOfflineNotificationCompatibility(db)
	prepareMySQLLogSchemaCompatibility(db)
	prepareMySQLClientRegionSchemaCompatibility(db)
}

func prepareFailoverV2MemberSchemaCompatibility(db *gorm.DB) {
	if db == nil {
		return
	}

	migrator := db.Migrator()
	if !migrator.HasTable(&models.FailoverV2Member{}) {
		return
	}

	indexes, err := migrator.GetIndexes(&models.FailoverV2Member{})
	if err != nil {
		log.Printf("Failed to inspect failover v2 member indexes: %v", err)
		return
	}

	rebuildLegacyUniqueIndex := func(model interface{}, modelIndexes []gorm.Index, subject string, indexName string, columns []string) {
		normalizedColumns := make([]string, len(columns))
		for i, c := range columns {
			normalizedColumns[i] = strings.ToLower(strings.TrimSpace(c))
		}

		hasTargetIndex := false
		targetIsUnique := false
		for _, index := range modelIndexes {
			if index.Name() != indexName {
				continue
			}
			hasTargetIndex = true
			if unique, ok := index.Unique(); ok {
				targetIsUnique = unique
			}
		}

		needsRecreate := false
		hasMatchingTargetColumns := func(index gorm.Index) bool {
			indexColumns := index.Columns()
			if len(indexColumns) != len(normalizedColumns) {
				return false
			}
			for i, c := range indexColumns {
				if strings.ToLower(strings.TrimSpace(c)) != normalizedColumns[i] {
					return false
				}
			}
			return true
		}

		for _, index := range modelIndexes {
			if !hasMatchingTargetColumns(index) {
				continue
			}
			unique, ok := index.Unique()
			if !ok || !unique {
				continue
			}

			if err := migrator.DropIndex(model, index.Name()); err != nil {
				log.Printf("Failed to drop legacy failover v2 %s unique index %s: %v", subject, index.Name(), err)
				return
			}
			needsRecreate = true
		}

		if needsRecreate && hasTargetIndex && targetIsUnique {
			hasTargetIndex = false
		}

		if !needsRecreate {
			return
		}

		if hasTargetIndex {
			// target index is already non-unique, keep it as-is.
			return
		}
		if err := migrator.CreateIndex(model, indexName); err != nil {
			log.Printf("Failed to recreate failover v2 %s index: %v", subject, err)
		}
	}

	rebuildLegacyUniqueIndex(&models.FailoverV2Member{}, indexes, "member watch client", "idx_failover_v2_service_client", []string{"service_id", "watch_client_uuid"})
	rebuildLegacyUniqueIndex(&models.FailoverV2Member{}, indexes, "member dns line", "idx_failover_v2_service_line", []string{"service_id", "dns_line"})

	if migrator.HasTable(&models.FailoverV2MemberLine{}) {
		lineIndexes, err := migrator.GetIndexes(&models.FailoverV2MemberLine{})
		if err != nil {
			log.Printf("Failed to inspect failover v2 member line indexes: %v", err)
		} else {
			rebuildLegacyUniqueIndex(&models.FailoverV2MemberLine{}, lineIndexes, "member line code", "idx_failover_v2_service_line_code", []string{"service_id", "line_code"})
		}

		if err := db.Model(&models.FailoverV2Member{}).
			Where("mode = '' OR mode IS NULL").
			Update("mode", models.FailoverV2MemberModeProviderTemplate).Error; err != nil {
			log.Printf("Failed to backfill failover v2 member modes: %v", err)
		}

		type legacyMemberLineRow struct {
			ID            uint
			ServiceID     uint
			DNSLine       string
			DNSRecordRefs string
		}

		var legacyRows []legacyMemberLineRow
		if err := db.Model(&models.FailoverV2Member{}).
			Select("id, service_id, dns_line, dns_record_refs").
			Order("id ASC").
			Find(&legacyRows).Error; err != nil {
			log.Printf("Failed to load failover v2 member legacy lines for backfill: %v", err)
			return
		}

		for _, row := range legacyRows {
			lineCode := strings.TrimSpace(row.DNSLine)
			if lineCode == "" {
				continue
			}

			var count int64
			if err := db.Model(&models.FailoverV2MemberLine{}).
				Where("member_id = ?", row.ID).
				Count(&count).Error; err != nil {
				log.Printf("Failed to inspect failover v2 member lines for member %d: %v", row.ID, err)
				continue
			}
			if count > 0 {
				continue
			}

			dnsRecordRefs := strings.TrimSpace(row.DNSRecordRefs)
			if dnsRecordRefs == "" {
				dnsRecordRefs = "{}"
			}

			if err := db.Create(&models.FailoverV2MemberLine{
				ServiceID:     row.ServiceID,
				MemberID:      row.ID,
				LineCode:      lineCode,
				DNSRecordRefs: dnsRecordRefs,
			}).Error; err != nil {
				log.Printf("Failed to backfill failover v2 member line for member %d line %q: %v", row.ID, lineCode, err)
			}
		}
	}
}

func applyFailoverCooldownDefaultZeroMigration(db *gorm.DB) {
	if db == nil {
		return
	}

	if err := db.AutoMigrate(&models.DBMigrationMarker{}); err != nil {
		log.Printf("Failed to migrate db migration markers table: %v", err)
		return
	}

	var marker models.DBMigrationMarker
	err := db.Where("key = ?", migrationKeyFailoverCooldownDefaultZero).First(&marker).Error
	if err == nil {
		return
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		log.Printf("Failed to inspect cooldown migration marker: %v", err)
		return
	}

	err = db.Transaction(func(tx *gorm.DB) error {
		var existing models.DBMigrationMarker
		if err := tx.Where("key = ?", migrationKeyFailoverCooldownDefaultZero).First(&existing).Error; err == nil {
			return nil
		} else if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}

		if tx.Migrator().HasTable(&models.FailoverTask{}) {
			if err := tx.Model(&models.FailoverTask{}).
				Where("cooldown_seconds = ?", 1800).
				Update("cooldown_seconds", 0).Error; err != nil {
				return err
			}
		}

		if tx.Migrator().HasTable(&models.FailoverV2Member{}) {
			if err := tx.Model(&models.FailoverV2Member{}).
				Where("cooldown_seconds = ?", 1800).
				Update("cooldown_seconds", 0).Error; err != nil {
				return err
			}
		}

		now := models.FromTime(time.Now())
		return tx.Create(&models.DBMigrationMarker{
			Key:       migrationKeyFailoverCooldownDefaultZero,
			AppliedAt: now,
		}).Error
	})
	if err != nil {
		log.Printf("Failed to apply cooldown default zero migration: %v", err)
		return
	}

	log.Printf("[migration] Applied %s", migrationKeyFailoverCooldownDefaultZero)
}

func prepareMySQLOfflineNotificationCompatibility(db *gorm.DB) {
	migrator := db.Migrator()
	if !migrator.HasTable(&models.OfflineNotification{}) {
		return
	}

	// Collapse any legacy duplicate rows so the primary key can be restored.
	if err := db.Exec(
		`CREATE TEMPORARY TABLE offline_notifications_dedup AS
		SELECT client, MAX(enable) AS enable, MAX(grace_period) AS grace_period, MAX(last_notified) AS last_notified
		FROM offline_notifications
		GROUP BY client`,
	).Error; err != nil {
		log.Printf("Failed to build MySQL offline notification dedup table: %v", err)
	} else {
		if err := db.Exec("TRUNCATE TABLE `offline_notifications`").Error; err != nil {
			log.Printf("Failed to truncate MySQL offline_notifications before dedup restore: %v", err)
		} else if err := db.Exec(
			`INSERT INTO offline_notifications (client, enable, grace_period, last_notified)
			SELECT client, enable, grace_period, last_notified FROM offline_notifications_dedup`,
		).Error; err != nil {
			log.Printf("Failed to restore deduplicated MySQL offline notifications: %v", err)
		}
		if err := db.Exec("DROP TEMPORARY TABLE IF EXISTS offline_notifications_dedup").Error; err != nil {
			log.Printf("Failed to drop MySQL offline notification dedup table: %v", err)
		}
	}

	if migrator.HasTable(&models.Client{}) {
		if err := db.Exec(
			`DELETE offline_notifications FROM offline_notifications
			LEFT JOIN clients ON clients.uuid = offline_notifications.client
			WHERE clients.uuid IS NULL`,
		).Error; err != nil {
			log.Printf("Failed to remove orphaned MySQL offline notifications: %v", err)
		}
	}

	for _, indexName := range []string{"uni_offline_notifications_client", "idx_offline_notifications_client"} {
		if migrator.HasIndex(&models.OfflineNotification{}, indexName) {
			if err := migrator.DropIndex(&models.OfflineNotification{}, indexName); err != nil {
				log.Printf("Failed to drop legacy MySQL offline notification index %s: %v", indexName, err)
			}
		}
	}

	var primaryKeyCount int64
	if err := db.Raw(
		`SELECT COUNT(*) FROM information_schema.table_constraints WHERE table_schema = DATABASE() AND table_name = ? AND constraint_type = 'PRIMARY KEY'`,
		"offline_notifications",
	).Scan(&primaryKeyCount).Error; err != nil {
		log.Printf("Failed to inspect MySQL offline notification primary key: %v", err)
		return
	}
	if primaryKeyCount == 0 {
		if err := db.Exec("ALTER TABLE `offline_notifications` ADD PRIMARY KEY (`client`)").Error; err != nil {
			log.Printf("Failed to add MySQL offline notification primary key: %v", err)
		}
	}
}

func prepareMySQLLogSchemaCompatibility(db *gorm.DB) {
	migrator := db.Migrator()
	if !migrator.HasTable(&models.Log{}) {
		return
	}

	var primaryKeyColumns string
	if err := db.Raw(
		`SELECT COALESCE(GROUP_CONCAT(kcu.column_name ORDER BY kcu.ordinal_position SEPARATOR ','), '')
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu
		  ON tc.constraint_name = kcu.constraint_name
		 AND tc.table_schema = kcu.table_schema
		 AND tc.table_name = kcu.table_name
		WHERE tc.table_schema = DATABASE() AND tc.table_name = ? AND tc.constraint_type = 'PRIMARY KEY'`,
		"logs",
	).Scan(&primaryKeyColumns).Error; err != nil {
		log.Printf("Failed to inspect MySQL log primary key: %v", err)
		return
	}

	primaryKeyColumns = strings.TrimSpace(primaryKeyColumns)
	if primaryKeyColumns == "id" {
		return
	}

	if primaryKeyColumns != "" {
		if err := db.Exec("ALTER TABLE `logs` DROP PRIMARY KEY").Error; err != nil {
			log.Printf("Failed to drop legacy MySQL log primary key (%s): %v", primaryKeyColumns, err)
			return
		}
	}

	if !migrator.HasColumn(&models.Log{}, "id") {
		if err := db.Exec("ALTER TABLE `logs` ADD COLUMN `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST").Error; err != nil {
			log.Printf("Failed to add MySQL log id column: %v", err)
		}
		return
	}

	if err := db.Exec("ALTER TABLE `logs` MODIFY COLUMN `id` BIGINT UNSIGNED NOT NULL").Error; err != nil {
		log.Printf("Failed to normalize MySQL log id column before primary key restore: %v", err)
		return
	}
	if err := db.Exec("ALTER TABLE `logs` ADD PRIMARY KEY (`id`)").Error; err != nil {
		log.Printf("Failed to add MySQL log primary key on id: %v", err)
		return
	}
	if err := db.Exec("ALTER TABLE `logs` MODIFY COLUMN `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT").Error; err != nil {
		log.Printf("Failed to restore MySQL log id auto increment: %v", err)
	}
}

func prepareMySQLClientRegionSchemaCompatibility(db *gorm.DB) {
	migrator := db.Migrator()
	if !migrator.HasTable(&models.Client{}) || !migrator.HasColumn(&models.Client{}, "region") {
		return
	}

	var columnInfo struct {
		CharacterSetName string `gorm:"column:character_set_name"`
		CollationName    string `gorm:"column:collation_name"`
	}
	if err := db.Raw(
		`SELECT COALESCE(character_set_name, '') AS character_set_name, COALESCE(collation_name, '') AS collation_name
		FROM information_schema.columns
		WHERE table_schema = DATABASE() AND table_name = ? AND column_name = ?`,
		"clients",
		"region",
	).Scan(&columnInfo).Error; err != nil {
		log.Printf("Failed to inspect MySQL clients.region charset: %v", err)
		return
	}

	if strings.EqualFold(columnInfo.CharacterSetName, "utf8mb4") && strings.HasPrefix(strings.ToLower(columnInfo.CollationName), "utf8mb4_") {
		return
	}

	if err := db.Exec(
		"ALTER TABLE `clients` MODIFY COLUMN `region` VARCHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL",
	).Error; err != nil {
		log.Printf("Failed to normalize MySQL clients.region charset: %v", err)
	}
}

var (
	instance *gorm.DB
	once     sync.Once
)

func GetDBInstance() *gorm.DB {
	once.Do(func() {

		var err error

		// 在数据库初始化前执行：如果存在 ./data/backup.zip，则进行恢复逻辑
		func() {
			backupZipPath := filepath.Join(".", "data", "backup.zip")
			if _, statErr := os.Stat(backupZipPath); statErr == nil {
				// 4. 把除了 ./data/backup.zip 之外的所有文件压缩到 ./backup/{time}.zip
				if err := os.MkdirAll("./backup", 0755); err != nil {
					log.Printf("[restore] failed to create backup dir: %v", err)
				} else {
					tsName := time.Now().Format("20060102-150405")
					bakPath := filepath.Join("./backup", fmt.Sprintf("%s.zip", tsName))
					if zipErr := zipDirectoryExcluding("./data", bakPath, map[string]struct{}{backupZipPath: {}}); zipErr != nil {
						log.Printf("[restore] failed to zip current data: %v", zipErr)
					} else {
						log.Printf("[restore] current data zipped to %s", bakPath)
					}
				}

				// 5. 删除除了 ./data/backup.zip 之外的所有文件
				if delErr := removeAllInDirExcept("./data", map[string]struct{}{backupZipPath: {}}); delErr != nil {
					log.Printf("[restore] failed to cleanup data dir: %v", delErr)
				}

				// 6. 解压 ./data/backup.zip 到 ./data
				if unzipErr := unzipToDir(backupZipPath, "./data"); unzipErr != nil {
					log.Printf("[restore] failed to unzip backup into data: %v", unzipErr)
				} else {
					log.Printf("[restore] backup.zip extracted to ./data")
				}

				// 7. 删除 ./data/backup.zip
				if rmErr := os.Remove(backupZipPath); rmErr != nil {
					log.Printf("[restore] failed to remove backup.zip: %v", rmErr)
				} else {
					log.Printf("[restore] backup.zip removed")
				}
				// 8. 删除标记
				if rmErr := os.Remove("./data/komari-backup-markup"); rmErr != nil {
					log.Printf("[restore] failed to remove komari-backup-markup: %v", rmErr)
				} else {
					log.Printf("[restore] komari-backup-markup removed")
				}
			}
		}()

		logConfig := &gorm.Config{
			Logger: logutil.NewGormLogger(),
		}

		// 运行时固定使用 MySQL；SQLite 仅保留给单元测试。
		if shouldUseSQLiteForTests() {
			instance, err = gorm.Open(sqlite.Open(flags.DatabaseFile), logConfig)
			if err != nil {
				log.Fatalf("Failed to connect to SQLite3 database: %v", err)
			}
			log.Printf("Using SQLite database file for tests: %s", flags.DatabaseFile)
			instance.Exec("PRAGMA wal = ON;")
			if err := instance.Exec("PRAGMA journal_mode = WAL;").Error; err != nil {
				log.Printf("Failed to enable WAL mode for SQLite: %v", err)
			}
			instance.Exec("VACUUM;")
			instance.Exec("PRAGMA wal_checkpoint(TRUNCATE);")
		} else {
			if flags.DatabaseType == "sqlite" {
				log.Fatalf("SQLite is no longer supported for runtime deployments; please configure MySQL instead")
			}
			dsn := fmt.Sprintf(
				"%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&collation=utf8mb4_unicode_ci&parseTime=True&loc=Local",
				flags.DatabaseUser,
				flags.DatabasePass,
				flags.DatabaseHost,
				flags.DatabasePort,
				flags.DatabaseName,
			)
			instance, err = gorm.Open(mysql.Open(dsn), logConfig)
			if err != nil {
				log.Fatalf("Failed to connect to MySQL database: %v", err)
			}
			sqlDB, sqlErr := instance.DB()
			if sqlErr != nil {
				log.Fatalf("Failed to get underlying MySQL database: %v", sqlErr)
			}
			sqlDB.SetMaxIdleConns(10)
			sqlDB.SetMaxOpenConns(50)
			sqlDB.SetConnMaxLifetime(time.Hour)
			if pingErr := sqlDB.Ping(); pingErr != nil {
				log.Fatalf("Failed to ping MySQL database: %v", pingErr)
			}
			log.Printf(
				"Using MySQL database: %s@%s:%s/%s",
				flags.DatabaseUser,
				flags.DatabaseHost,
				flags.DatabasePort,
				flags.DatabaseName,
			)
		}
		config.SetDb(instance)
		MergeDatabase(instance)
		err = instance.AutoMigrate(
			&models.User{},
			&models.Client{},
		)
		if err != nil {
			log.Fatalf("Failed to create user/client tables: %v", err)
		}
		prepareMySQLSchemaCompatibility(instance)
		// 自动迁移模型
		err = instance.AutoMigrate(
			&models.Record{},
			&models.GPURecord{},
			&models.Log{},
			&models.Clipboard{},
			&models.ClientDDNSBinding{},
			&models.LoadNotification{},
			&models.OfflineNotification{},
			&models.PingRecord{},
			&models.PingTask{},
			&models.OidcProvider{},
			&models.MessageSenderProvider{},
			&models.CloudProvider{},
			&models.CloudInstanceShare{},
			&models.AWSFollowUpTask{},
		)
		if err != nil {
			log.Fatalf("Failed to create tables: %v", err)
		}
		err = instance.Table("records_long_term").AutoMigrate(
			&models.Record{},
		)
		if err != nil {
			log.Printf("Failed to create records_long_term table, it may already exist: %v", err)
		}
		err = instance.Table("gpu_records_long_term").AutoMigrate(
			&models.GPURecord{},
		)
		if err != nil {
			log.Printf("Failed to create gpu_records_long_term table, it may already exist: %v", err)
		}
		err = instance.AutoMigrate(
			&models.Session{},
		)
		if err != nil {
			log.Printf("Failed to create Session table, it may already exist: %v", err)
		}
		err = instance.AutoMigrate(
			&models.Task{},
			&models.TaskResult{},
		)
		if err != nil {
			log.Printf("Failed to create Task and TaskResult table, it may already exist: %v", err)
		}
		err = instance.AutoMigrate(
			&models.FailoverTask{},
			&models.FailoverPlan{},
			&models.FailoverExecution{},
			&models.FailoverExecutionStep{},
			&models.FailoverPendingCleanup{},
			&models.FailoverV2Service{},
			&models.FailoverV2Member{},
			&models.FailoverV2MemberLine{},
			&models.FailoverV2Execution{},
			&models.FailoverV2ExecutionStep{},
			&models.FailoverV2PendingCleanup{},
			&models.FailoverV2RunLock{},
		)
		if err != nil {
			log.Printf("Failed to create failover tables, it may already exist: %v", err)
		}
		applyFailoverCooldownDefaultZeroMigration(instance)
		prepareFailoverV2MemberSchemaCompatibility(instance)

	})

	return instance
}
