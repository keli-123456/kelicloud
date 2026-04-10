package failoverv2

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/komari-monitor/komari/database/dbcore"
	"github.com/komari-monitor/komari/database/models"
	"gorm.io/gorm"
)

const (
	defaultFailureThreshold    = 2
	defaultStaleAfterSeconds   = 300
	defaultCooldownSeconds     = 1800
	defaultScriptTimeoutSec    = 600
	defaultWaitAgentTimeoutSec = 600
	maxProviderEntryGroupLen   = 100
)

func normalizeProviderEntryGroupForStorage(group string) string {
	group = strings.TrimSpace(group)
	runes := []rune(group)
	if len(runes) > maxProviderEntryGroupLen {
		group = string(runes[:maxProviderEntryGroupLen])
	}
	return group
}

func normalizeFailoverV2UserID(userUUID string) (string, error) {
	userUUID = strings.TrimSpace(userUUID)
	if userUUID == "" {
		return "", fmt.Errorf("user id is required")
	}
	return userUUID, nil
}

func serviceScopeWithDB(db *gorm.DB, userUUID string) *gorm.DB {
	userUUID = strings.TrimSpace(userUUID)
	if userUUID == "" {
		return db.Where("1 = 0")
	}
	return db.Where("user_id = ?", userUUID)
}

func preloadFailoverV2Service(db *gorm.DB) *gorm.DB {
	return db.Preload("Members", func(tx *gorm.DB) *gorm.DB {
		return tx.Order("priority ASC").
			Order("id ASC").
			Preload("Lines", func(lineTx *gorm.DB) *gorm.DB {
				return lineTx.Order("line_code ASC").Order("id ASC")
			})
	})
}

func serviceStatusForEnabled(enabled bool, current string) string {
	if !enabled {
		return models.FailoverV2ServiceStatusUnknown
	}

	switch strings.TrimSpace(current) {
	case models.FailoverV2ServiceStatusHealthy,
		models.FailoverV2ServiceStatusRunning,
		models.FailoverV2ServiceStatusFailed:
		return current
	default:
		return models.FailoverV2ServiceStatusUnknown
	}
}

func memberStatusForEnabled(enabled bool, current string) string {
	if !enabled {
		return models.FailoverV2MemberStatusDisabled
	}

	switch strings.TrimSpace(current) {
	case models.FailoverV2MemberStatusHealthy,
		models.FailoverV2MemberStatusTriggered,
		models.FailoverV2MemberStatusRunning,
		models.FailoverV2MemberStatusCooldown,
		models.FailoverV2MemberStatusFailed:
		return current
	default:
		return models.FailoverV2MemberStatusUnknown
	}
}

func applyServiceDefaults(service *models.FailoverV2Service) {
	service.Name = strings.TrimSpace(service.Name)
	service.DNSProvider = strings.ToLower(strings.TrimSpace(service.DNSProvider))
	service.DNSEntryID = strings.TrimSpace(service.DNSEntryID)
	service.DeleteStrategy = strings.TrimSpace(service.DeleteStrategy)

	if strings.TrimSpace(service.DNSPayload) == "" {
		service.DNSPayload = "{}"
	}
	if strings.TrimSpace(service.DeleteStrategy) == "" {
		service.DeleteStrategy = models.FailoverDeleteStrategyKeep
	}
	if service.ScriptTimeoutSec <= 0 {
		service.ScriptTimeoutSec = defaultScriptTimeoutSec
	}
	if service.WaitAgentTimeoutSec <= 0 {
		service.WaitAgentTimeoutSec = defaultWaitAgentTimeoutSec
	}
	if service.DeleteDelaySeconds < 0 {
		service.DeleteDelaySeconds = 0
	}
	service.LastStatus = serviceStatusForEnabled(service.Enabled, service.LastStatus)
}

func applyMemberDefaults(member *models.FailoverV2Member) {
	member.Name = strings.TrimSpace(member.Name)
	member.Mode = normalizeFailoverV2MemberModeValue(member.Mode)
	member.WatchClientUUID = strings.TrimSpace(member.WatchClientUUID)
	member.CurrentAddress = strings.TrimSpace(member.CurrentAddress)
	member.CurrentInstanceRef = strings.TrimSpace(member.CurrentInstanceRef)
	member.Provider = strings.ToLower(strings.TrimSpace(member.Provider))
	member.ProviderEntryID = strings.TrimSpace(member.ProviderEntryID)
	member.ProviderEntryGroup = normalizeProviderEntryGroupForStorage(member.ProviderEntryGroup)
	member.PlanPayload = strings.TrimSpace(member.PlanPayload)
	member.Lines = normalizeFailoverV2MemberLines(member.ServiceID, member.ID, member.DNSLine, member.DNSRecordRefs, member.Lines)
	syncFailoverV2MemberLegacyLineFields(member)

	if member.Priority <= 0 {
		member.Priority = 1
	}
	member.DNSRecordRefs = normalizeFailoverV2MemberLineRecordRefs(member.DNSRecordRefs)
	if strings.TrimSpace(member.PlanPayload) == "" {
		member.PlanPayload = "{}"
	}
	if member.FailureThreshold <= 0 {
		member.FailureThreshold = defaultFailureThreshold
	}
	if member.StaleAfterSeconds <= 0 {
		member.StaleAfterSeconds = defaultStaleAfterSeconds
	}
	if member.CooldownSeconds < 0 {
		member.CooldownSeconds = defaultCooldownSeconds
	}
	if member.TriggerFailureCount < 0 {
		member.TriggerFailureCount = 0
	}
	member.LastStatus = memberStatusForEnabled(member.Enabled, member.LastStatus)
}

func applyExecutionDefaults(execution *models.FailoverV2Execution) {
	if execution == nil {
		return
	}

	execution.Status = strings.TrimSpace(execution.Status)
	execution.TriggerReason = strings.TrimSpace(execution.TriggerReason)
	execution.TriggerSnapshot = strings.TrimSpace(execution.TriggerSnapshot)
	execution.OldClientUUID = strings.TrimSpace(execution.OldClientUUID)
	execution.OldInstanceRef = strings.TrimSpace(execution.OldInstanceRef)
	execution.OldAddresses = strings.TrimSpace(execution.OldAddresses)
	execution.DetachDNSStatus = strings.TrimSpace(execution.DetachDNSStatus)
	execution.DetachDNSResult = strings.TrimSpace(execution.DetachDNSResult)
	execution.NewClientUUID = strings.TrimSpace(execution.NewClientUUID)
	execution.NewInstanceRef = strings.TrimSpace(execution.NewInstanceRef)
	execution.NewAddresses = strings.TrimSpace(execution.NewAddresses)
	execution.AttachDNSStatus = strings.TrimSpace(execution.AttachDNSStatus)
	execution.AttachDNSResult = strings.TrimSpace(execution.AttachDNSResult)
	execution.CleanupStatus = strings.TrimSpace(execution.CleanupStatus)
	execution.CleanupResult = strings.TrimSpace(execution.CleanupResult)
	execution.ErrorMessage = strings.TrimSpace(execution.ErrorMessage)

	if execution.Status == "" {
		execution.Status = models.FailoverV2ExecutionStatusQueued
	}
	if execution.DetachDNSStatus == "" {
		execution.DetachDNSStatus = models.FailoverDNSStatusPending
	}
	if execution.AttachDNSStatus == "" {
		execution.AttachDNSStatus = models.FailoverDNSStatusPending
	}
	if execution.CleanupStatus == "" {
		execution.CleanupStatus = models.FailoverCleanupStatusPending
	}
	if execution.StartedAt.ToTime().IsZero() {
		execution.StartedAt = models.FromTime(time.Now())
	}
}

func applyPendingCleanupDefaults(cleanup *models.FailoverV2PendingCleanup) {
	if cleanup == nil {
		return
	}

	cleanup.UserID = strings.TrimSpace(cleanup.UserID)
	cleanup.Provider = strings.TrimSpace(cleanup.Provider)
	cleanup.ProviderEntryID = strings.TrimSpace(cleanup.ProviderEntryID)
	cleanup.ResourceType = strings.TrimSpace(cleanup.ResourceType)
	cleanup.ResourceID = strings.TrimSpace(cleanup.ResourceID)
	cleanup.InstanceRef = strings.TrimSpace(cleanup.InstanceRef)
	cleanup.CleanupLabel = strings.TrimSpace(cleanup.CleanupLabel)
	cleanup.LastError = strings.TrimSpace(cleanup.LastError)
	cleanup.Status = strings.TrimSpace(cleanup.Status)
	if cleanup.Status == "" {
		cleanup.Status = models.FailoverV2PendingCleanupStatusPending
	}
	if cleanup.AttemptCount < 0 {
		cleanup.AttemptCount = 0
	}
}

func listServicesByUserWithDB(db *gorm.DB, userUUID string) ([]models.FailoverV2Service, error) {
	userUUID, err := normalizeFailoverV2UserID(userUUID)
	if err != nil {
		return nil, err
	}

	var services []models.FailoverV2Service
	if err := preloadFailoverV2Service(serviceScopeWithDB(db, userUUID)).
		Order("id ASC").
		Find(&services).Error; err != nil {
		return nil, err
	}
	return services, nil
}

func ListServicesByUser(userUUID string) ([]models.FailoverV2Service, error) {
	return listServicesByUserWithDB(dbcore.GetDBInstance(), userUUID)
}

func ClaimRunLock(lockKey, owner string, ttl time.Duration) (bool, error) {
	return claimRunLockWithDB(dbcore.GetDBInstance(), lockKey, owner, ttl, time.Now())
}

func claimRunLockWithDB(db *gorm.DB, lockKey, owner string, ttl time.Duration, now time.Time) (bool, error) {
	lockKey = strings.TrimSpace(lockKey)
	owner = strings.TrimSpace(owner)
	if lockKey == "" || owner == "" {
		return false, fmt.Errorf("lock key and owner are required")
	}
	if ttl <= 0 {
		ttl = time.Minute
	}
	if now.IsZero() {
		now = time.Now()
	}

	expiresAt := models.FromTime(now.Add(ttl))
	lock := models.FailoverV2RunLock{
		LockKey:   lockKey,
		Owner:     owner,
		ExpiresAt: expiresAt,
	}
	if err := db.Create(&lock).Error; err == nil {
		return true, nil
	}

	result := db.Model(&models.FailoverV2RunLock{}).
		Where("lock_key = ? AND expires_at <= ?", lockKey, models.FromTime(now)).
		Updates(map[string]interface{}{
			"owner":      owner,
			"expires_at": expiresAt,
		})
	if result.Error != nil {
		return false, result.Error
	}
	return result.RowsAffected > 0, nil
}

func ReleaseRunLock(lockKey, owner string) error {
	return releaseRunLockWithDB(dbcore.GetDBInstance(), lockKey, owner)
}

func releaseRunLockWithDB(db *gorm.DB, lockKey, owner string) error {
	lockKey = strings.TrimSpace(lockKey)
	owner = strings.TrimSpace(owner)
	if lockKey == "" || owner == "" {
		return nil
	}
	return db.Where("lock_key = ? AND owner = ?", lockKey, owner).
		Delete(&models.FailoverV2RunLock{}).Error
}

func ReleaseExpiredRunLocks(before time.Time) (int64, error) {
	return releaseExpiredRunLocksWithDB(dbcore.GetDBInstance(), before)
}

func releaseExpiredRunLocksWithDB(db *gorm.DB, before time.Time) (int64, error) {
	if before.IsZero() {
		before = time.Now()
	}
	result := db.Where("expires_at <= ?", models.FromTime(before)).
		Delete(&models.FailoverV2RunLock{})
	if result.Error != nil {
		return 0, result.Error
	}
	return result.RowsAffected, nil
}

func listEnabledServicesWithDB(db *gorm.DB) ([]models.FailoverV2Service, error) {
	var services []models.FailoverV2Service
	if err := preloadFailoverV2Service(db).
		Where("enabled = ?", true).
		Order("id ASC").
		Find(&services).Error; err != nil {
		return nil, err
	}
	return services, nil
}

func ListEnabledServices() ([]models.FailoverV2Service, error) {
	return listEnabledServicesWithDB(dbcore.GetDBInstance())
}

func getServiceByIDForUserWithDB(db *gorm.DB, userUUID string, serviceID uint) (*models.FailoverV2Service, error) {
	userUUID, err := normalizeFailoverV2UserID(userUUID)
	if err != nil {
		return nil, err
	}

	var service models.FailoverV2Service
	if err := preloadFailoverV2Service(serviceScopeWithDB(db, userUUID)).
		Where("id = ?", serviceID).
		First(&service).Error; err != nil {
		return nil, err
	}
	return &service, nil
}

func GetServiceByIDForUser(userUUID string, serviceID uint) (*models.FailoverV2Service, error) {
	return getServiceByIDForUserWithDB(dbcore.GetDBInstance(), userUUID, serviceID)
}

func getMemberByIDForServiceForUserWithDB(db *gorm.DB, userUUID string, serviceID, memberID uint) (*models.FailoverV2Member, error) {
	userUUID, err := normalizeFailoverV2UserID(userUUID)
	if err != nil {
		return nil, err
	}

	var service models.FailoverV2Service
	if err := serviceScopeWithDB(db, userUUID).
		Where("id = ?", serviceID).
		First(&service).Error; err != nil {
		return nil, err
	}

	var member models.FailoverV2Member
	if err := db.Preload("Lines", func(tx *gorm.DB) *gorm.DB {
		return tx.Order("line_code ASC").Order("id ASC")
	}).Where("service_id = ? AND id = ?", service.ID, memberID).
		First(&member).Error; err != nil {
		return nil, err
	}
	return &member, nil
}

func getExecutionByIDForServiceForUserWithDB(db *gorm.DB, userUUID string, serviceID, executionID uint) (*models.FailoverV2Execution, error) {
	userUUID, err := normalizeFailoverV2UserID(userUUID)
	if err != nil {
		return nil, err
	}

	var service models.FailoverV2Service
	if err := serviceScopeWithDB(db, userUUID).
		Where("id = ?", serviceID).
		First(&service).Error; err != nil {
		return nil, err
	}

	var execution models.FailoverV2Execution
	if err := db.Preload("Steps", func(tx *gorm.DB) *gorm.DB {
		return tx.Order("sort ASC").Order("id ASC")
	}).Where("service_id = ? AND id = ?", service.ID, executionID).
		First(&execution).Error; err != nil {
		return nil, err
	}
	return &execution, nil
}

func createServiceForUserWithDB(db *gorm.DB, userUUID string, service *models.FailoverV2Service) (*models.FailoverV2Service, error) {
	userUUID, err := normalizeFailoverV2UserID(userUUID)
	if err != nil {
		return nil, err
	}
	if service == nil {
		return nil, fmt.Errorf("service is required")
	}

	var created *models.FailoverV2Service
	err = db.Transaction(func(tx *gorm.DB) error {
		service.UserID = userUUID
		applyServiceDefaults(service)
		if err := tx.Create(service).Error; err != nil {
			return err
		}

		loaded, err := getServiceByIDForUserWithDB(tx, userUUID, service.ID)
		if err != nil {
			return err
		}
		created = loaded
		return nil
	})
	if err != nil {
		return nil, err
	}
	return created, nil
}

func CreateServiceForUser(userUUID string, service *models.FailoverV2Service) (*models.FailoverV2Service, error) {
	return createServiceForUserWithDB(dbcore.GetDBInstance(), userUUID, service)
}

func updateServiceForUserWithDB(db *gorm.DB, userUUID string, serviceID uint, service *models.FailoverV2Service) (*models.FailoverV2Service, error) {
	userUUID, err := normalizeFailoverV2UserID(userUUID)
	if err != nil {
		return nil, err
	}
	if service == nil {
		return nil, fmt.Errorf("service is required")
	}

	var updated *models.FailoverV2Service
	err = db.Transaction(func(tx *gorm.DB) error {
		existing, err := getServiceByIDForUserWithDB(tx, userUUID, serviceID)
		if err != nil {
			return err
		}
		active, err := hasActiveExecutionForServiceWithDB(tx, userUUID, serviceID)
		if err != nil {
			return err
		}
		if active {
			return fmt.Errorf("cannot update failover v2 service while an execution is active")
		}

		applyServiceDefaults(service)
		updates := map[string]interface{}{
			"name":                   service.Name,
			"enabled":                service.Enabled,
			"dns_provider":           service.DNSProvider,
			"dns_entry_id":           service.DNSEntryID,
			"dns_payload":            service.DNSPayload,
			"script_clipboard_ids":   service.ScriptClipboardIDs,
			"script_timeout_sec":     service.ScriptTimeoutSec,
			"wait_agent_timeout_sec": service.WaitAgentTimeoutSec,
			"delete_strategy":        service.DeleteStrategy,
			"delete_delay_seconds":   service.DeleteDelaySeconds,
			"last_status":            serviceStatusForEnabled(service.Enabled, existing.LastStatus),
		}
		if err := serviceScopeWithDB(tx.Model(&models.FailoverV2Service{}), userUUID).
			Where("id = ?", serviceID).
			Updates(updates).Error; err != nil {
			return err
		}

		loaded, err := getServiceByIDForUserWithDB(tx, userUUID, serviceID)
		if err != nil {
			return err
		}
		updated = loaded
		return nil
	})
	if err != nil {
		return nil, err
	}
	return updated, nil
}

func UpdateServiceForUser(userUUID string, serviceID uint, service *models.FailoverV2Service) (*models.FailoverV2Service, error) {
	return updateServiceForUserWithDB(dbcore.GetDBInstance(), userUUID, serviceID, service)
}

func deleteServiceForUserWithDB(db *gorm.DB, userUUID string, serviceID uint) error {
	userUUID, err := normalizeFailoverV2UserID(userUUID)
	if err != nil {
		return err
	}

	return db.Transaction(func(tx *gorm.DB) error {
		if _, err := getServiceByIDForUserWithDB(tx, userUUID, serviceID); err != nil {
			return err
		}
		active, err := hasActiveExecutionForServiceWithDB(tx, userUUID, serviceID)
		if err != nil {
			return err
		}
		if active {
			return fmt.Errorf("cannot delete failover v2 service while an execution is active")
		}
		return serviceScopeWithDB(tx, userUUID).
			Where("id = ?", serviceID).
			Delete(&models.FailoverV2Service{}).Error
	})
}

func DeleteServiceForUser(userUUID string, serviceID uint) error {
	return deleteServiceForUserWithDB(dbcore.GetDBInstance(), userUUID, serviceID)
}

func createMemberForUserWithDB(db *gorm.DB, userUUID string, serviceID uint, member *models.FailoverV2Member) (*models.FailoverV2Member, error) {
	userUUID, err := normalizeFailoverV2UserID(userUUID)
	if err != nil {
		return nil, err
	}
	if member == nil {
		return nil, fmt.Errorf("member is required")
	}

	var created *models.FailoverV2Member
	err = db.Transaction(func(tx *gorm.DB) error {
		service, err := getServiceByIDForUserWithDB(tx, userUUID, serviceID)
		if err != nil {
			return err
		}
		active, err := hasActiveExecutionForServiceWithDB(tx, userUUID, service.ID)
		if err != nil {
			return err
		}
		if active {
			return fmt.Errorf("cannot create failover v2 member while an execution is active")
		}

		member.ServiceID = service.ID
		applyMemberDefaults(member)
		lines := cloneFailoverV2MemberLines(member.Lines)
		member.Lines = nil
		if err := tx.Create(member).Error; err != nil {
			return err
		}
		if err := replaceFailoverV2MemberLinesWithDB(tx, service.ID, member.ID, lines, nil); err != nil {
			return err
		}

		loaded, err := getMemberByIDForServiceForUserWithDB(tx, userUUID, service.ID, member.ID)
		if err != nil {
			return err
		}
		created = loaded
		return nil
	})
	if err != nil {
		return nil, err
	}
	return created, nil
}

func CreateMemberForUser(userUUID string, serviceID uint, member *models.FailoverV2Member) (*models.FailoverV2Member, error) {
	return createMemberForUserWithDB(dbcore.GetDBInstance(), userUUID, serviceID, member)
}

func updateMemberForUserWithDB(db *gorm.DB, userUUID string, serviceID, memberID uint, member *models.FailoverV2Member) (*models.FailoverV2Member, error) {
	userUUID, err := normalizeFailoverV2UserID(userUUID)
	if err != nil {
		return nil, err
	}
	if member == nil {
		return nil, fmt.Errorf("member is required")
	}

	var updated *models.FailoverV2Member
	err = db.Transaction(func(tx *gorm.DB) error {
		service, err := getServiceByIDForUserWithDB(tx, userUUID, serviceID)
		if err != nil {
			return err
		}
		existing, err := getMemberByIDForServiceForUserWithDB(tx, userUUID, service.ID, memberID)
		if err != nil {
			return err
		}
		active, err := hasActiveExecutionForServiceWithDB(tx, userUUID, service.ID)
		if err != nil {
			return err
		}
		if active {
			return fmt.Errorf("cannot update failover v2 member while an execution is active")
		}

		member.ServiceID = service.ID
		applyMemberDefaults(member)
		lines := cloneFailoverV2MemberLines(member.Lines)
		updates := map[string]interface{}{
			"name":                 member.Name,
			"enabled":              member.Enabled,
			"priority":             member.Priority,
			"mode":                 member.Mode,
			"watch_client_uuid":    member.WatchClientUUID,
			"dns_line":             member.DNSLine,
			"dns_record_refs":      member.DNSRecordRefs,
			"current_address":      member.CurrentAddress,
			"current_instance_ref": member.CurrentInstanceRef,
			"provider":             member.Provider,
			"provider_entry_id":    member.ProviderEntryID,
			"provider_entry_group": member.ProviderEntryGroup,
			"plan_payload":         member.PlanPayload,
			"failure_threshold":    member.FailureThreshold,
			"stale_after_seconds":  member.StaleAfterSeconds,
			"cooldown_seconds":     member.CooldownSeconds,
			"last_status":          memberStatusForEnabled(member.Enabled, existing.LastStatus),
		}
		if err := tx.Model(&models.FailoverV2Member{}).
			Where("service_id = ? AND id = ?", service.ID, memberID).
			Updates(updates).Error; err != nil {
			return err
		}
		if err := replaceFailoverV2MemberLinesWithDB(tx, service.ID, memberID, lines, existing.Lines); err != nil {
			return err
		}

		loaded, err := getMemberByIDForServiceForUserWithDB(tx, userUUID, service.ID, memberID)
		if err != nil {
			return err
		}
		updated = loaded
		return nil
	})
	if err != nil {
		return nil, err
	}
	return updated, nil
}

func UpdateMemberForUser(userUUID string, serviceID, memberID uint, member *models.FailoverV2Member) (*models.FailoverV2Member, error) {
	return updateMemberForUserWithDB(dbcore.GetDBInstance(), userUUID, serviceID, memberID, member)
}

func setServiceEnabledForUserWithDB(db *gorm.DB, userUUID string, serviceID uint, enabled bool) (*models.FailoverV2Service, error) {
	userUUID, err := normalizeFailoverV2UserID(userUUID)
	if err != nil {
		return nil, err
	}

	var updated *models.FailoverV2Service
	err = db.Transaction(func(tx *gorm.DB) error {
		existing, err := getServiceByIDForUserWithDB(tx, userUUID, serviceID)
		if err != nil {
			return err
		}

		active, err := hasActiveExecutionForServiceWithDB(tx, userUUID, serviceID)
		if err != nil {
			return err
		}
		if active {
			return fmt.Errorf("cannot change failover v2 service state while an execution is active")
		}

		if existing.Enabled == enabled {
			updated = existing
			return nil
		}

		message := "service resumed by operator; automatic scheduling is enabled"
		if !enabled {
			message = "service paused by operator; automatic scheduling is disabled"
		}

		if err := serviceScopeWithDB(tx.Model(&models.FailoverV2Service{}), userUUID).
			Where("id = ?", serviceID).
			Updates(map[string]interface{}{
				"enabled":      enabled,
				"last_status":  serviceStatusForEnabled(enabled, existing.LastStatus),
				"last_message": strings.TrimSpace(message),
			}).Error; err != nil {
			return err
		}

		loaded, err := getServiceByIDForUserWithDB(tx, userUUID, serviceID)
		if err != nil {
			return err
		}
		updated = loaded
		return nil
	})
	if err != nil {
		return nil, err
	}
	return updated, nil
}

func SetServiceEnabledForUser(userUUID string, serviceID uint, enabled bool) (*models.FailoverV2Service, error) {
	return setServiceEnabledForUserWithDB(dbcore.GetDBInstance(), userUUID, serviceID, enabled)
}

func setMemberEnabledForUserWithDB(db *gorm.DB, userUUID string, serviceID, memberID uint, enabled bool) (*models.FailoverV2Member, error) {
	userUUID, err := normalizeFailoverV2UserID(userUUID)
	if err != nil {
		return nil, err
	}

	var updated *models.FailoverV2Member
	err = db.Transaction(func(tx *gorm.DB) error {
		service, err := getServiceByIDForUserWithDB(tx, userUUID, serviceID)
		if err != nil {
			return err
		}
		existing, err := getMemberByIDForServiceForUserWithDB(tx, userUUID, service.ID, memberID)
		if err != nil {
			return err
		}

		active, err := hasActiveExecutionForServiceWithDB(tx, userUUID, service.ID)
		if err != nil {
			return err
		}
		if active {
			return fmt.Errorf("cannot change failover v2 member state while an execution is active")
		}

		if existing.Enabled == enabled {
			updated = existing
			return nil
		}

		message := "member resumed by operator; automatic scheduling is enabled"
		if !enabled {
			message = "member paused by operator; automatic scheduling is disabled"
		}

		if err := tx.Model(&models.FailoverV2Member{}).
			Where("service_id = ? AND id = ?", service.ID, memberID).
			Updates(map[string]interface{}{
				"enabled":               enabled,
				"last_status":           memberStatusForEnabled(enabled, existing.LastStatus),
				"last_message":          strings.TrimSpace(message),
				"trigger_failure_count": 0,
			}).Error; err != nil {
			return err
		}

		loaded, err := getMemberByIDForServiceForUserWithDB(tx, userUUID, service.ID, memberID)
		if err != nil {
			return err
		}
		updated = loaded
		return nil
	})
	if err != nil {
		return nil, err
	}
	return updated, nil
}

func SetMemberEnabledForUser(userUUID string, serviceID, memberID uint, enabled bool) (*models.FailoverV2Member, error) {
	return setMemberEnabledForUserWithDB(dbcore.GetDBInstance(), userUUID, serviceID, memberID, enabled)
}

func deleteMemberForUserWithDB(db *gorm.DB, userUUID string, serviceID, memberID uint) error {
	userUUID, err := normalizeFailoverV2UserID(userUUID)
	if err != nil {
		return err
	}

	return db.Transaction(func(tx *gorm.DB) error {
		service, err := getServiceByIDForUserWithDB(tx, userUUID, serviceID)
		if err != nil {
			return err
		}
		if _, err := getMemberByIDForServiceForUserWithDB(tx, userUUID, service.ID, memberID); err != nil {
			return err
		}
		active, err := hasActiveExecutionForServiceWithDB(tx, userUUID, service.ID)
		if err != nil {
			return err
		}
		if active {
			return fmt.Errorf("cannot delete failover v2 member while an execution is active")
		}
		return tx.Where("service_id = ? AND id = ?", service.ID, memberID).
			Delete(&models.FailoverV2Member{}).Error
	})
}

func DeleteMemberForUser(userUUID string, serviceID, memberID uint) error {
	return deleteMemberForUserWithDB(dbcore.GetDBInstance(), userUUID, serviceID, memberID)
}

func listExecutionsByServiceForUserWithDB(db *gorm.DB, userUUID string, serviceID uint, limit int) ([]models.FailoverV2Execution, error) {
	userUUID, err := normalizeFailoverV2UserID(userUUID)
	if err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}

	var service models.FailoverV2Service
	if err := serviceScopeWithDB(db, userUUID).
		Where("id = ?", serviceID).
		First(&service).Error; err != nil {
		return nil, err
	}

	var executions []models.FailoverV2Execution
	if err := db.Where("service_id = ?", service.ID).
		Order("started_at DESC").
		Order("id DESC").
		Limit(limit).
		Find(&executions).Error; err != nil {
		return nil, err
	}
	return executions, nil
}

func ListExecutionsByServiceForUser(userUUID string, serviceID uint, limit int) ([]models.FailoverV2Execution, error) {
	return listExecutionsByServiceForUserWithDB(dbcore.GetDBInstance(), userUUID, serviceID, limit)
}

func CreateExecutionForUser(userUUID string, serviceID, memberID uint, execution *models.FailoverV2Execution) (*models.FailoverV2Execution, error) {
	return createExecutionForUserWithDB(dbcore.GetDBInstance(), userUUID, serviceID, memberID, execution)
}

func createExecutionForUserWithDB(db *gorm.DB, userUUID string, serviceID, memberID uint, execution *models.FailoverV2Execution) (*models.FailoverV2Execution, error) {
	userUUID, err := normalizeFailoverV2UserID(userUUID)
	if err != nil {
		return nil, err
	}
	if execution == nil {
		return nil, fmt.Errorf("execution is required")
	}

	var created *models.FailoverV2Execution
	err = db.Transaction(func(tx *gorm.DB) error {
		service, err := getServiceByIDForUserWithDB(tx, userUUID, serviceID)
		if err != nil {
			return err
		}
		member, err := getMemberByIDForServiceForUserWithDB(tx, userUUID, service.ID, memberID)
		if err != nil {
			return err
		}

		execution.ServiceID = service.ID
		execution.MemberID = member.ID
		applyExecutionDefaults(execution)
		if err := tx.Create(execution).Error; err != nil {
			return err
		}

		loaded, err := getExecutionByIDForServiceForUserWithDB(tx, userUUID, service.ID, execution.ID)
		if err != nil {
			return err
		}
		created = loaded
		return nil
	})
	if err != nil {
		return nil, err
	}
	return created, nil
}

func GetExecutionByIDForUser(userUUID string, serviceID, executionID uint) (*models.FailoverV2Execution, error) {
	return getExecutionByIDForServiceForUserWithDB(dbcore.GetDBInstance(), userUUID, serviceID, executionID)
}

func UpdateExecutionFields(executionID uint, fields map[string]interface{}) error {
	if len(fields) == 0 {
		return nil
	}
	result := dbcore.GetDBInstance().Model(&models.FailoverV2Execution{}).
		Where("id = ?", executionID).
		Updates(fields)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}
	return nil
}

func UpdateActiveExecutionFields(executionID uint, fields map[string]interface{}) (bool, error) {
	if len(fields) == 0 {
		return true, nil
	}
	result := dbcore.GetDBInstance().Model(&models.FailoverV2Execution{}).
		Where("id = ?", executionID).
		Where("finished_at IS NULL").
		Where("status IN ?", activeFailoverV2ExecutionStatuses).
		Updates(fields)
	if result.Error != nil {
		return false, result.Error
	}
	return result.RowsAffected > 0, nil
}

func CreateExecutionStep(step *models.FailoverV2ExecutionStep) (*models.FailoverV2ExecutionStep, error) {
	if step == nil {
		return nil, fmt.Errorf("step is required")
	}
	if strings.TrimSpace(step.Status) == "" {
		step.Status = models.FailoverStepStatusPending
	}
	if err := dbcore.GetDBInstance().Create(step).Error; err != nil {
		return nil, err
	}
	return step, nil
}

func UpdateExecutionStepFields(stepID uint, fields map[string]interface{}) error {
	if len(fields) == 0 {
		return nil
	}
	result := dbcore.GetDBInstance().Model(&models.FailoverV2ExecutionStep{}).
		Where("id = ?", stepID).
		Updates(fields)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}
	return nil
}

func UpdateRunningExecutionStepFields(stepID uint, fields map[string]interface{}) (bool, error) {
	if len(fields) == 0 {
		return true, nil
	}
	result := dbcore.GetDBInstance().Model(&models.FailoverV2ExecutionStep{}).
		Where("id = ?", stepID).
		Where("status = ?", models.FailoverStepStatusRunning).
		Updates(fields)
	if result.Error != nil {
		return false, result.Error
	}
	return result.RowsAffected > 0, nil
}

func UpdateServiceFieldsForUser(userUUID string, serviceID uint, fields map[string]interface{}) error {
	userUUID, err := normalizeFailoverV2UserID(userUUID)
	if err != nil {
		return err
	}
	if len(fields) == 0 {
		return nil
	}
	result := serviceScopeWithDB(dbcore.GetDBInstance().Model(&models.FailoverV2Service{}), userUUID).
		Where("id = ?", serviceID).
		Updates(fields)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}
	return nil
}

func UpdateMemberFieldsForUser(userUUID string, serviceID, memberID uint, fields map[string]interface{}) error {
	userUUID, err := normalizeFailoverV2UserID(userUUID)
	if err != nil {
		return err
	}
	if len(fields) == 0 {
		return nil
	}

	return dbcore.GetDBInstance().Transaction(func(tx *gorm.DB) error {
		service, err := getServiceByIDForUserWithDB(tx, userUUID, serviceID)
		if err != nil {
			return err
		}
		result := tx.Model(&models.FailoverV2Member{}).
			Where("service_id = ? AND id = ?", service.ID, memberID).
			Updates(fields)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return gorm.ErrRecordNotFound
		}
		return nil
	})
}

func UpdateMemberFieldsAndLineRecordRefsForUser(
	userUUID string,
	serviceID uint,
	memberID uint,
	fields map[string]interface{},
	lineRecordRefs map[string]map[string]string,
) error {
	userUUID, err := normalizeFailoverV2UserID(userUUID)
	if err != nil {
		return err
	}
	if len(fields) == 0 && lineRecordRefs == nil {
		return nil
	}

	return dbcore.GetDBInstance().Transaction(func(tx *gorm.DB) error {
		service, err := getServiceByIDForUserWithDB(tx, userUUID, serviceID)
		if err != nil {
			return err
		}
		member, err := getMemberByIDForServiceForUserWithDB(tx, userUUID, service.ID, memberID)
		if err != nil {
			return err
		}

		if len(fields) > 0 {
			result := tx.Model(&models.FailoverV2Member{}).
				Where("service_id = ? AND id = ?", service.ID, memberID).
				Updates(fields)
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected == 0 {
				return gorm.ErrRecordNotFound
			}
		}

		if lineRecordRefs == nil {
			return nil
		}

		lines := effectiveFailoverV2MemberLines(member)
		if len(lines) == 0 {
			return nil
		}

		if err := tx.Where("service_id = ? AND member_id = ?", service.ID, memberID).
			Delete(&models.FailoverV2MemberLine{}).Error; err != nil {
			return err
		}

		normalizedLines := make([]models.FailoverV2MemberLine, 0, len(lines))
		for _, line := range lines {
			lineCode := normalizeFailoverV2MemberLineCode(line.LineCode)
			if lineCode == "" {
				continue
			}
			refs := lineRecordRefs[lineCode]
			normalizedLines = append(normalizedLines, models.FailoverV2MemberLine{
				ServiceID:     service.ID,
				MemberID:      memberID,
				LineCode:      lineCode,
				DNSRecordRefs: normalizeFailoverV2MemberLineRecordRefs(encodeMemberLineRecordRefsJSON(refs)),
			})
		}
		if len(normalizedLines) > 0 {
			if err := tx.Create(&normalizedLines).Error; err != nil {
				return err
			}
		}

		legacyRecordRefs := "{}"
		if len(normalizedLines) > 0 {
			legacyRecordRefs = normalizedLines[0].DNSRecordRefs
		}
		return tx.Model(&models.FailoverV2Member{}).
			Where("service_id = ? AND id = ?", service.ID, memberID).
			Update("dns_record_refs", legacyRecordRefs).Error
	})
}

func HasActiveExecutionForService(userUUID string, serviceID uint) (bool, error) {
	return hasActiveExecutionForServiceWithDB(dbcore.GetDBInstance(), userUUID, serviceID)
}

func hasActiveExecutionForServiceWithDB(db *gorm.DB, userUUID string, serviceID uint) (bool, error) {
	userUUID, err := normalizeFailoverV2UserID(userUUID)
	if err != nil {
		return false, err
	}

	var service models.FailoverV2Service
	if err := serviceScopeWithDB(db, userUUID).
		Where("id = ?", serviceID).
		First(&service).Error; err != nil {
		return false, err
	}

	var count int64
	if err := db.Model(&models.FailoverV2Execution{}).
		Where("service_id = ?", service.ID).
		Where("finished_at IS NULL").
		Where("status NOT IN ?", []string{
			models.FailoverV2ExecutionStatusSuccess,
			models.FailoverV2ExecutionStatusFailed,
		}).
		Count(&count).Error; err != nil {
		return false, err
	}
	return count > 0, nil
}

func RecoverInterruptedExecutionsForService(userUUID string, serviceID uint, message string) (int64, error) {
	return recoverInterruptedExecutionsForServiceWithDB(dbcore.GetDBInstance(), userUUID, serviceID, message)
}

func RecoverInterruptedExecutions(message string) (int64, error) {
	return recoverInterruptedExecutionsWithDB(dbcore.GetDBInstance(), "", 0, message, time.Now())
}

func recoverInterruptedExecutionsForServiceWithDB(db *gorm.DB, userUUID string, serviceID uint, message string) (int64, error) {
	userUUID, err := normalizeFailoverV2UserID(userUUID)
	if err != nil {
		return 0, err
	}
	return recoverInterruptedExecutionsWithDB(db, userUUID, serviceID, message, time.Now())
}

func recoverInterruptedExecutionsWithDB(db *gorm.DB, userUUID string, serviceID uint, message string, now time.Time) (int64, error) {
	if db == nil {
		return 0, fmt.Errorf("db is required")
	}

	message = strings.TrimSpace(message)
	if message == "" {
		message = "failover v2 execution was interrupted before completion"
	}

	var service *models.FailoverV2Service
	if strings.TrimSpace(userUUID) != "" || serviceID > 0 {
		if strings.TrimSpace(userUUID) == "" {
			return 0, fmt.Errorf("user id is required")
		}
		loaded, err := getServiceByIDForUserWithDB(db, userUUID, serviceID)
		if err != nil {
			return 0, err
		}
		service = loaded
	}

	query := db.Where("status IN ?", activeFailoverV2ExecutionStatuses)
	if service != nil {
		query = query.Where("service_id = ?", service.ID)
	}

	var executions []models.FailoverV2Execution
	if err := query.Find(&executions).Error; err != nil {
		return 0, err
	}
	if len(executions) == 0 {
		return 0, nil
	}

	executionIDs := make([]uint, 0, len(executions))
	for _, execution := range executions {
		executionIDs = append(executionIDs, execution.ID)
	}

	finishedAt := models.FromTime(now)
	if err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&models.FailoverV2Execution{}).
			Where("id IN ?", executionIDs).
			Updates(map[string]interface{}{
				"status":        models.FailoverV2ExecutionStatusFailed,
				"error_message": message,
				"finished_at":   finishedAt,
			}).Error; err != nil {
			return err
		}

		if err := failRunningExecutionStepsWithDB(tx, executionIDs, message, now); err != nil {
			return err
		}

		for _, execution := range executions {
			if err := tx.Model(&models.FailoverV2Service{}).
				Where("id = ? AND last_execution_id = ?", execution.ServiceID, execution.ID).
				Updates(map[string]interface{}{
					"last_status":  models.FailoverV2ServiceStatusFailed,
					"last_message": message,
				}).Error; err != nil {
				return err
			}
			if err := tx.Model(&models.FailoverV2Member{}).
				Where("service_id = ? AND id = ? AND last_execution_id = ?", execution.ServiceID, execution.MemberID, execution.ID).
				Updates(map[string]interface{}{
					"last_status":    models.FailoverV2MemberStatusFailed,
					"last_message":   message,
					"last_failed_at": finishedAt,
				}).Error; err != nil {
				return err
			}
		}

		return nil
	}); err != nil {
		return 0, err
	}

	return int64(len(executions)), nil
}

func CreateOrUpdatePendingCleanup(cleanup *models.FailoverV2PendingCleanup) (*models.FailoverV2PendingCleanup, error) {
	return createOrUpdatePendingCleanupWithDB(dbcore.GetDBInstance(), cleanup)
}

func createOrUpdatePendingCleanupWithDB(db *gorm.DB, cleanup *models.FailoverV2PendingCleanup) (*models.FailoverV2PendingCleanup, error) {
	if cleanup == nil {
		return nil, fmt.Errorf("pending cleanup is required")
	}
	if _, err := normalizeFailoverV2UserID(cleanup.UserID); err != nil {
		return nil, err
	}

	applyPendingCleanupDefaults(cleanup)
	if cleanup.Provider == "" {
		return nil, fmt.Errorf("provider is required")
	}
	if cleanup.ResourceType == "" {
		return nil, fmt.Errorf("resource type is required")
	}
	if cleanup.ResourceID == "" {
		return nil, fmt.Errorf("resource id is required")
	}

	var saved models.FailoverV2PendingCleanup
	err := db.Transaction(func(tx *gorm.DB) error {
		var existing models.FailoverV2PendingCleanup
		err := tx.Where(
			"user_id = ? AND provider = ? AND resource_type = ? AND resource_id = ?",
			cleanup.UserID,
			cleanup.Provider,
			cleanup.ResourceType,
			cleanup.ResourceID,
		).First(&existing).Error
		switch {
		case errors.Is(err, gorm.ErrRecordNotFound):
			if err := tx.Create(cleanup).Error; err != nil {
				return err
			}
			saved = *cleanup
			return nil
		case err != nil:
			return err
		}

		updates := map[string]interface{}{
			"service_id":        cleanup.ServiceID,
			"member_id":         cleanup.MemberID,
			"execution_id":      cleanup.ExecutionID,
			"provider_entry_id": cleanup.ProviderEntryID,
			"instance_ref":      cleanup.InstanceRef,
			"cleanup_label":     cleanup.CleanupLabel,
			"status":            cleanup.Status,
			"attempt_count":     cleanup.AttemptCount,
			"last_error":        cleanup.LastError,
			"last_attempted_at": cleanup.LastAttemptedAt,
			"next_retry_at":     cleanup.NextRetryAt,
			"resolved_at":       cleanup.ResolvedAt,
		}
		if err := tx.Model(&models.FailoverV2PendingCleanup{}).
			Where("id = ?", existing.ID).
			Updates(updates).Error; err != nil {
			return err
		}
		return tx.First(&saved, existing.ID).Error
	})
	if err != nil {
		return nil, err
	}
	return &saved, nil
}

func ListDuePendingCleanups(limit int, before time.Time) ([]models.FailoverV2PendingCleanup, error) {
	return listDuePendingCleanupsWithDB(dbcore.GetDBInstance(), limit, before)
}

func normalizePendingCleanupStatuses(statuses []string) []string {
	if len(statuses) == 0 {
		return nil
	}

	seen := make(map[string]struct{}, len(statuses))
	normalized := make([]string, 0, len(statuses))
	for _, status := range statuses {
		value := strings.TrimSpace(strings.ToLower(status))
		switch value {
		case models.FailoverV2PendingCleanupStatusPending,
			models.FailoverV2PendingCleanupStatusRunning,
			models.FailoverV2PendingCleanupStatusSucceeded,
			models.FailoverV2PendingCleanupStatusManualReview:
		default:
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		normalized = append(normalized, value)
	}
	return normalized
}

func getPendingCleanupByIDForServiceForUserWithDB(db *gorm.DB, userUUID string, serviceID, cleanupID uint) (*models.FailoverV2PendingCleanup, error) {
	userUUID, err := normalizeFailoverV2UserID(userUUID)
	if err != nil {
		return nil, err
	}

	service, err := getServiceByIDForUserWithDB(db, userUUID, serviceID)
	if err != nil {
		return nil, err
	}

	var cleanup models.FailoverV2PendingCleanup
	if err := db.Where("service_id = ? AND id = ?", service.ID, cleanupID).
		First(&cleanup).Error; err != nil {
		return nil, err
	}
	return &cleanup, nil
}

func GetPendingCleanupByIDForUser(userUUID string, serviceID, cleanupID uint) (*models.FailoverV2PendingCleanup, error) {
	return getPendingCleanupByIDForServiceForUserWithDB(dbcore.GetDBInstance(), userUUID, serviceID, cleanupID)
}

func listPendingCleanupsByServiceForUserWithDB(db *gorm.DB, userUUID string, serviceID uint, limit int, statuses []string) ([]models.FailoverV2PendingCleanup, error) {
	userUUID, err := normalizeFailoverV2UserID(userUUID)
	if err != nil {
		return nil, err
	}

	service, err := getServiceByIDForUserWithDB(db, userUUID, serviceID)
	if err != nil {
		return nil, err
	}

	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}

	query := db.Where("service_id = ?", service.ID)
	if normalized := normalizePendingCleanupStatuses(statuses); len(normalized) > 0 {
		query = query.Where("status IN ?", normalized)
	}

	var cleanups []models.FailoverV2PendingCleanup
	if err := query.
		Order("updated_at DESC").
		Order("id DESC").
		Limit(limit).
		Find(&cleanups).Error; err != nil {
		return nil, err
	}
	return cleanups, nil
}

func ListPendingCleanupsByServiceForUser(userUUID string, serviceID uint, limit int, statuses []string) ([]models.FailoverV2PendingCleanup, error) {
	return listPendingCleanupsByServiceForUserWithDB(dbcore.GetDBInstance(), userUUID, serviceID, limit, statuses)
}

func listDuePendingCleanupsWithDB(db *gorm.DB, limit int, before time.Time) ([]models.FailoverV2PendingCleanup, error) {
	if limit <= 0 {
		limit = 20
	}
	if limit > 200 {
		limit = 200
	}
	if before.IsZero() {
		before = time.Now()
	}

	var cleanups []models.FailoverV2PendingCleanup
	if err := db.Where("status = ?", models.FailoverV2PendingCleanupStatusPending).
		Where("next_retry_at IS NULL OR next_retry_at <= ?", before).
		Order("next_retry_at ASC").
		Order("id ASC").
		Limit(limit).
		Find(&cleanups).Error; err != nil {
		return nil, err
	}
	return cleanups, nil
}

func MarkPendingCleanupSucceeded(cleanupID uint) error {
	return markPendingCleanupSucceededWithDB(dbcore.GetDBInstance(), cleanupID)
}

func MarkPendingCleanupSucceededIfNotRunning(cleanupID uint) error {
	return markPendingCleanupSucceededIfNotRunningWithDB(dbcore.GetDBInstance(), cleanupID)
}

func MarkPendingCleanupRunning(cleanupID uint, message string) error {
	return markPendingCleanupRunningWithDB(dbcore.GetDBInstance(), cleanupID, message)
}

func MarkPendingCleanupSucceededByResource(userUUID, provider, resourceType, resourceID string) error {
	return markPendingCleanupSucceededByResourceWithDB(dbcore.GetDBInstance(), userUUID, provider, resourceType, resourceID)
}

func markPendingCleanupRunningWithDB(db *gorm.DB, cleanupID uint, message string) error {
	if cleanupID == 0 {
		return nil
	}
	message = strings.TrimSpace(message)
	if message == "" {
		message = "pending cleanup retry is running"
	}
	now := models.FromTime(time.Now())
	result := db.Model(&models.FailoverV2PendingCleanup{}).
		Where("id = ? AND status IN ?", cleanupID, []string{
			models.FailoverV2PendingCleanupStatusPending,
			models.FailoverV2PendingCleanupStatusManualReview,
		}).
		Updates(map[string]interface{}{
			"status":            models.FailoverV2PendingCleanupStatusRunning,
			"last_error":        message,
			"last_attempted_at": now,
			"next_retry_at":     nil,
			"resolved_at":       nil,
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}
	return nil
}

func markPendingCleanupSucceededWithDB(db *gorm.DB, cleanupID uint) error {
	if cleanupID == 0 {
		return nil
	}
	now := models.FromTime(time.Now())
	result := db.Model(&models.FailoverV2PendingCleanup{}).
		Where("id = ?", cleanupID).
		Updates(map[string]interface{}{
			"status":            models.FailoverV2PendingCleanupStatusSucceeded,
			"last_error":        "",
			"next_retry_at":     nil,
			"last_attempted_at": now,
			"resolved_at":       now,
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}
	return nil
}

func markPendingCleanupSucceededIfNotRunningWithDB(db *gorm.DB, cleanupID uint) error {
	if cleanupID == 0 {
		return nil
	}
	now := models.FromTime(time.Now())
	result := db.Model(&models.FailoverV2PendingCleanup{}).
		Where("id = ? AND status != ?", cleanupID, models.FailoverV2PendingCleanupStatusRunning).
		Updates(map[string]interface{}{
			"status":            models.FailoverV2PendingCleanupStatusSucceeded,
			"last_error":        "",
			"next_retry_at":     nil,
			"last_attempted_at": now,
			"resolved_at":       now,
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}
	return nil
}

func markPendingCleanupSucceededByResourceWithDB(db *gorm.DB, userUUID, provider, resourceType, resourceID string) error {
	userUUID = strings.TrimSpace(userUUID)
	provider = strings.TrimSpace(provider)
	resourceType = strings.TrimSpace(resourceType)
	resourceID = strings.TrimSpace(resourceID)
	if userUUID == "" || provider == "" || resourceType == "" || resourceID == "" {
		return nil
	}

	now := models.FromTime(time.Now())
	result := db.Model(&models.FailoverV2PendingCleanup{}).
		Where("user_id = ? AND provider = ? AND resource_type = ? AND resource_id = ?", userUUID, provider, resourceType, resourceID).
		Updates(map[string]interface{}{
			"status":            models.FailoverV2PendingCleanupStatusSucceeded,
			"last_error":        "",
			"next_retry_at":     nil,
			"last_attempted_at": now,
			"resolved_at":       now,
		})
	if result.Error != nil {
		return result.Error
	}
	return nil
}

func SchedulePendingCleanupRetry(cleanupID uint, attemptCount int, lastError string, nextRetryAt time.Time) error {
	return schedulePendingCleanupRetryWithDB(dbcore.GetDBInstance(), cleanupID, attemptCount, lastError, nextRetryAt)
}

func schedulePendingCleanupRetryWithDB(db *gorm.DB, cleanupID uint, attemptCount int, lastError string, nextRetryAt time.Time) error {
	if cleanupID == 0 {
		return nil
	}
	if attemptCount < 0 {
		attemptCount = 0
	}
	now := models.FromTime(time.Now())
	next := models.FromTime(nextRetryAt)
	result := db.Model(&models.FailoverV2PendingCleanup{}).
		Where("id = ?", cleanupID).
		Updates(map[string]interface{}{
			"status":            models.FailoverV2PendingCleanupStatusPending,
			"attempt_count":     attemptCount,
			"last_error":        strings.TrimSpace(lastError),
			"last_attempted_at": now,
			"next_retry_at":     next,
			"resolved_at":       nil,
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}
	return nil
}

func MarkPendingCleanupManualReview(cleanupID uint, attemptCount int, lastError string) error {
	return markPendingCleanupManualReviewWithDB(dbcore.GetDBInstance(), cleanupID, attemptCount, lastError)
}

func MarkPendingCleanupManualReviewIfNotRunning(cleanupID uint, attemptCount int, lastError string) error {
	return markPendingCleanupManualReviewIfNotRunningWithDB(dbcore.GetDBInstance(), cleanupID, attemptCount, lastError)
}

func RecoverStaleRunningPendingCleanups(staleBefore time.Time, message string) (int64, error) {
	return recoverStaleRunningPendingCleanupsWithDB(dbcore.GetDBInstance(), staleBefore, message)
}

func recoverStaleRunningPendingCleanupsWithDB(db *gorm.DB, staleBefore time.Time, message string) (int64, error) {
	if db == nil {
		return 0, fmt.Errorf("db is required")
	}
	if staleBefore.IsZero() {
		return 0, nil
	}
	message = strings.TrimSpace(message)
	if message == "" {
		message = "pending cleanup retry worker was interrupted"
	}
	result := db.Model(&models.FailoverV2PendingCleanup{}).
		Where("status = ?", models.FailoverV2PendingCleanupStatusRunning).
		Where("updated_at <= ?", models.FromTime(staleBefore)).
		Updates(map[string]interface{}{
			"status":        models.FailoverV2PendingCleanupStatusPending,
			"last_error":    message,
			"next_retry_at": nil,
			"resolved_at":   nil,
		})
	if result.Error != nil {
		return 0, result.Error
	}
	return result.RowsAffected, nil
}

func markPendingCleanupManualReviewWithDB(db *gorm.DB, cleanupID uint, attemptCount int, lastError string) error {
	return markPendingCleanupManualReviewWithScope(db, cleanupID, attemptCount, lastError, false)
}

func markPendingCleanupManualReviewIfNotRunningWithDB(db *gorm.DB, cleanupID uint, attemptCount int, lastError string) error {
	return markPendingCleanupManualReviewWithScope(db, cleanupID, attemptCount, lastError, true)
}

func markPendingCleanupManualReviewWithScope(db *gorm.DB, cleanupID uint, attemptCount int, lastError string, excludeRunning bool) error {
	if cleanupID == 0 {
		return nil
	}
	if attemptCount < 0 {
		attemptCount = 0
	}
	now := models.FromTime(time.Now())
	query := db.Model(&models.FailoverV2PendingCleanup{}).Where("id = ?", cleanupID)
	if excludeRunning {
		query = query.Where("status != ?", models.FailoverV2PendingCleanupStatusRunning)
	}
	result := query.Updates(map[string]interface{}{
		"status":            models.FailoverV2PendingCleanupStatusManualReview,
		"attempt_count":     attemptCount,
		"last_error":        strings.TrimSpace(lastError),
		"last_attempted_at": now,
		"next_retry_at":     nil,
		"resolved_at":       nil,
	})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}
	return nil
}
