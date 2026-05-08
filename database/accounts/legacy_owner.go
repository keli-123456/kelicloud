package accounts

import (
	"errors"
	"strings"

	"github.com/komari-monitor/komari/database/dbcore"
	"github.com/komari-monitor/komari/database/models"
	"gorm.io/gorm"
)

type LegacyUserScopedDataBackfillSummary struct {
	OwnerUUID string
	Updated   map[string]int64
	Skipped   map[string]int64
}

func (summary LegacyUserScopedDataBackfillSummary) TotalUpdated() int64 {
	var total int64
	for _, count := range summary.Updated {
		total += count
	}
	return total
}

func (summary LegacyUserScopedDataBackfillSummary) TotalSkipped() int64 {
	var total int64
	for _, count := range summary.Skipped {
		total += count
	}
	return total
}

func BackfillLegacyUserScopedData(ownerUUID string) (LegacyUserScopedDataBackfillSummary, error) {
	return backfillLegacyUserScopedDataWithDB(dbcore.GetDBInstance(), ownerUUID)
}

func backfillLegacyUserScopedDataWithDB(db *gorm.DB, ownerUUID string) (LegacyUserScopedDataBackfillSummary, error) {
	summary := LegacyUserScopedDataBackfillSummary{
		OwnerUUID: strings.TrimSpace(ownerUUID),
		Updated:   map[string]int64{},
		Skipped:   map[string]int64{},
	}
	if db == nil {
		return summary, errors.New("database is required")
	}
	if summary.OwnerUUID == "" {
		return summary, errors.New("owner uuid is required")
	}

	simpleModels := []struct {
		label string
		model interface{}
	}{
		{"clients", &models.Client{}},
		{"aws_follow_up_tasks", &models.AWSFollowUpTask{}},
		{"clipboards", &models.Clipboard{}},
		{"client_ddns_bindings", &models.ClientDDNSBinding{}},
		{"client_port_forward_rules", &models.ClientPortForwardRule{}},
		{"logs", &models.Log{}},
		{"load_notifications", &models.LoadNotification{}},
		{"ping_tasks", &models.PingTask{}},
		{"tasks", &models.Task{}},
		{"task_results", &models.TaskResult{}},
		{"failover_tasks", &models.FailoverTask{}},
		{"failover_v2_services", &models.FailoverV2Service{}},
	}

	for _, item := range simpleModels {
		if !db.Migrator().HasTable(item.model) {
			continue
		}
		updated, err := updateBlankUserID(db, item.model, summary.OwnerUUID)
		if err != nil {
			return summary, err
		}
		if updated > 0 {
			summary.Updated[item.label] = updated
		}
	}

	if err := backfillCloudProviders(db, &summary); err != nil {
		return summary, err
	}
	if err := backfillCloudInstanceShares(db, &summary); err != nil {
		return summary, err
	}
	if err := backfillFailoverShares(db, &summary); err != nil {
		return summary, err
	}
	if err := backfillFailoverV2Shares(db, &summary); err != nil {
		return summary, err
	}
	if err := backfillFailoverPendingCleanups(db, &summary); err != nil {
		return summary, err
	}
	if err := backfillFailoverV2PendingCleanups(db, &summary); err != nil {
		return summary, err
	}

	return summary, nil
}

func updateBlankUserID(db *gorm.DB, model interface{}, ownerUUID string) (int64, error) {
	result := db.Model(model).
		Where("user_id = ? OR user_id IS NULL", "").
		Update("user_id", ownerUUID)
	return result.RowsAffected, result.Error
}

func recordBackfillUpdate(summary *LegacyUserScopedDataBackfillSummary, label string, count int64) {
	if count > 0 {
		summary.Updated[label] += count
	}
}

func recordBackfillSkip(summary *LegacyUserScopedDataBackfillSummary, label string) {
	summary.Skipped[label]++
}

func hasCloudProviderForOwner(db *gorm.DB, ownerUUID, name string) (bool, error) {
	var count int64
	err := db.Model(&models.CloudProvider{}).
		Where("user_id = ? AND name = ?", ownerUUID, strings.TrimSpace(name)).
		Count(&count).Error
	return count > 0, err
}

func backfillCloudProviders(db *gorm.DB, summary *LegacyUserScopedDataBackfillSummary) error {
	if !db.Migrator().HasTable(&models.CloudProvider{}) {
		return nil
	}

	var rows []models.CloudProvider
	if err := db.Where("user_id = ? OR user_id IS NULL", "").Find(&rows).Error; err != nil {
		return err
	}

	for _, row := range rows {
		name := strings.TrimSpace(row.Name)
		if name == "" {
			recordBackfillSkip(summary, "cloud_providers")
			continue
		}
		exists, err := hasCloudProviderForOwner(db, summary.OwnerUUID, name)
		if err != nil {
			return err
		}
		if exists {
			recordBackfillSkip(summary, "cloud_providers")
			continue
		}
		result := db.Model(&models.CloudProvider{}).
			Where("(user_id = ? OR user_id IS NULL) AND name = ?", "", row.Name).
			Update("user_id", summary.OwnerUUID)
		if result.Error != nil {
			return result.Error
		}
		recordBackfillUpdate(summary, "cloud_providers", result.RowsAffected)
	}
	return nil
}

func backfillCloudInstanceShares(db *gorm.DB, summary *LegacyUserScopedDataBackfillSummary) error {
	if !db.Migrator().HasTable(&models.CloudInstanceShare{}) {
		return nil
	}

	var rows []models.CloudInstanceShare
	if err := db.Where("user_id = ? OR user_id IS NULL", "").Find(&rows).Error; err != nil {
		return err
	}

	for _, row := range rows {
		var count int64
		err := db.Model(&models.CloudInstanceShare{}).
			Where(
				"user_id = ? AND provider = ? AND resource_type = ? AND resource_id = ?",
				summary.OwnerUUID,
				row.Provider,
				row.ResourceType,
				row.ResourceID,
			).
			Count(&count).Error
		if err != nil {
			return err
		}
		if count > 0 {
			recordBackfillSkip(summary, "cloud_instance_shares")
			continue
		}
		result := db.Model(&models.CloudInstanceShare{}).
			Where("id = ?", row.ID).
			Update("user_id", summary.OwnerUUID)
		if result.Error != nil {
			return result.Error
		}
		recordBackfillUpdate(summary, "cloud_instance_shares", result.RowsAffected)
	}
	return nil
}

func backfillFailoverShares(db *gorm.DB, summary *LegacyUserScopedDataBackfillSummary) error {
	if !db.Migrator().HasTable(&models.FailoverShare{}) {
		return nil
	}

	var rows []models.FailoverShare
	if err := db.Where("user_id = ? OR user_id IS NULL", "").Find(&rows).Error; err != nil {
		return err
	}

	for _, row := range rows {
		var count int64
		err := db.Model(&models.FailoverShare{}).
			Where("user_id = ? AND task_id = ?", summary.OwnerUUID, row.TaskID).
			Count(&count).Error
		if err != nil {
			return err
		}
		if count > 0 {
			recordBackfillSkip(summary, "failover_shares")
			continue
		}
		result := db.Model(&models.FailoverShare{}).
			Where("id = ?", row.ID).
			Update("user_id", summary.OwnerUUID)
		if result.Error != nil {
			return result.Error
		}
		recordBackfillUpdate(summary, "failover_shares", result.RowsAffected)
	}
	return nil
}

func backfillFailoverV2Shares(db *gorm.DB, summary *LegacyUserScopedDataBackfillSummary) error {
	if !db.Migrator().HasTable(&models.FailoverV2Share{}) {
		return nil
	}

	var rows []models.FailoverV2Share
	if err := db.Where("user_id = ? OR user_id IS NULL", "").Find(&rows).Error; err != nil {
		return err
	}

	for _, row := range rows {
		var count int64
		err := db.Model(&models.FailoverV2Share{}).
			Where("user_id = ? AND service_id = ?", summary.OwnerUUID, row.ServiceID).
			Count(&count).Error
		if err != nil {
			return err
		}
		if count > 0 {
			recordBackfillSkip(summary, "failover_v2_shares")
			continue
		}
		result := db.Model(&models.FailoverV2Share{}).
			Where("id = ?", row.ID).
			Update("user_id", summary.OwnerUUID)
		if result.Error != nil {
			return result.Error
		}
		recordBackfillUpdate(summary, "failover_v2_shares", result.RowsAffected)
	}
	return nil
}

func backfillFailoverPendingCleanups(db *gorm.DB, summary *LegacyUserScopedDataBackfillSummary) error {
	if !db.Migrator().HasTable(&models.FailoverPendingCleanup{}) {
		return nil
	}

	var rows []models.FailoverPendingCleanup
	if err := db.Where("user_id = ? OR user_id IS NULL", "").Find(&rows).Error; err != nil {
		return err
	}

	for _, row := range rows {
		var count int64
		err := db.Model(&models.FailoverPendingCleanup{}).
			Where(
				"user_id = ? AND provider = ? AND resource_type = ? AND resource_id = ?",
				summary.OwnerUUID,
				row.Provider,
				row.ResourceType,
				row.ResourceID,
			).
			Count(&count).Error
		if err != nil {
			return err
		}
		if count > 0 {
			recordBackfillSkip(summary, "failover_pending_cleanups")
			continue
		}
		result := db.Model(&models.FailoverPendingCleanup{}).
			Where("id = ?", row.ID).
			Update("user_id", summary.OwnerUUID)
		if result.Error != nil {
			return result.Error
		}
		recordBackfillUpdate(summary, "failover_pending_cleanups", result.RowsAffected)
	}
	return nil
}

func backfillFailoverV2PendingCleanups(db *gorm.DB, summary *LegacyUserScopedDataBackfillSummary) error {
	if !db.Migrator().HasTable(&models.FailoverV2PendingCleanup{}) {
		return nil
	}

	var rows []models.FailoverV2PendingCleanup
	if err := db.Where("user_id = ? OR user_id IS NULL", "").Find(&rows).Error; err != nil {
		return err
	}

	for _, row := range rows {
		var count int64
		err := db.Model(&models.FailoverV2PendingCleanup{}).
			Where(
				"user_id = ? AND provider = ? AND resource_type = ? AND resource_id = ?",
				summary.OwnerUUID,
				row.Provider,
				row.ResourceType,
				row.ResourceID,
			).
			Count(&count).Error
		if err != nil {
			return err
		}
		if count > 0 {
			recordBackfillSkip(summary, "failover_v2_pending_cleanups")
			continue
		}
		result := db.Model(&models.FailoverV2PendingCleanup{}).
			Where("id = ?", row.ID).
			Update("user_id", summary.OwnerUUID)
		if result.Error != nil {
			return result.Error
		}
		recordBackfillUpdate(summary, "failover_v2_pending_cleanups", result.RowsAffected)
	}
	return nil
}
