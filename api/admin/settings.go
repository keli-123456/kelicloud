package admin

import (
	"encoding/json"
	"errors"

	"github.com/komari-monitor/komari/api"
	"github.com/komari-monitor/komari/config"
	"github.com/komari-monitor/komari/database"
	"github.com/komari-monitor/komari/database/records"
	"github.com/komari-monitor/komari/database/tasks"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

var systemSettingKeys = map[string]struct{}{
	config.AllowCorsKey:                  {},
	config.ApiKeyKey:                     {},
	config.CNConnectivityEnabledKey:      {},
	config.CNConnectivityTargetKey:       {},
	config.CNConnectivityIntervalKey:     {},
	config.EulaAcceptedKey:               {},
	config.GeoIpEnabledKey:               {},
	config.GeoIpProviderKey:              {},
	config.NezhaCompatEnabledKey:         {},
	config.NezhaCompatListenKey:          {},
	config.OAuthEnabledKey:               {},
	config.OAuthProviderKey:              {},
	config.DisablePasswordLoginKey:       {},
	config.NotificationEnabledKey:        {},
	config.NotificationMethodKey:         {},
	config.NotificationTemplateKey:       {},
	config.ExpireNotificationEnabledKey:  {},
	config.ExpireNotificationLeadDaysKey: {},
	config.LoginNotificationKey:          {},
	config.TrafficLimitPercentageKey:     {},
	config.RecordEnabledKey:              {},
	config.RecordPreserveTimeKey:         {},
	config.PingRecordPreserveTimeKey:     {},
}

func isSystemSettingKey(key string) bool {
	_, ok := systemSettingKeys[key]
	return ok
}

func filterSystemSettings(cfg map[string]any) map[string]any {
	result := make(map[string]any)
	for key, value := range cfg {
		if isSystemSettingKey(key) {
			result[key] = value
		}
	}
	return result
}

func splitEditableSettings(cfg map[string]any) (map[string]any, map[string]any, []string) {
	tenantValues := make(map[string]any)
	systemValues := make(map[string]any)
	ignored := make([]string, 0)

	for key, value := range cfg {
		switch {
		case config.IsTenantScopedKey(key):
			tenantValues[key] = value
		case isSystemSettingKey(key):
			systemValues[key] = value
		default:
			ignored = append(ignored, key)
		}
	}

	return tenantValues, systemValues, ignored
}

func isPlatformAdmin(c *gin.Context) (bool, error) {
	if _, ok := c.Get("api_key"); ok {
		return true, nil
	}

	userUUID, ok := c.Get("uuid")
	if !ok {
		return false, nil
	}

	defaultTenantID, err := database.GetDefaultTenantID()
	if err != nil {
		return false, err
	}

	member, err := database.GetAccessibleTenantByUser(userUUID.(string), defaultTenantID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return false, nil
		}
		return false, err
	}

	return database.IsTenantRoleAtLeast(member.Role, database.RoleAdmin), nil
}

func requirePlatformAdmin(c *gin.Context) bool {
	allowed, err := isPlatformAdmin(c)
	if err != nil {
		api.RespondError(c, 500, "Failed to resolve platform admin permissions: "+err.Error())
		return false
	}
	if !allowed {
		api.RespondError(c, 403, "Platform admin permission is required")
		return false
	}
	return true
}

func RequirePlatformAdminMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if !requirePlatformAdmin(c) {
			c.Abort()
			return
		}
		c.Next()
	}
}

func EnsurePlatformAdmin(c *gin.Context) bool {
	return requirePlatformAdmin(c)
}

func loadSystemSettings() (map[string]any, error) {
	legacy, err := config.GetManyAs[config.Legacy]()
	if err != nil {
		return nil, err
	}

	bytes, err := json.Marshal(legacy)
	if err != nil {
		return nil, err
	}

	values := make(map[string]any)
	if err := json.Unmarshal(bytes, &values); err != nil {
		return nil, err
	}
	return filterSystemSettings(values), nil
}

func GetSystemSettings(c *gin.Context) {
	if !requirePlatformAdmin(c) {
		return
	}

	cst, err := loadSystemSettings()
	if err != nil {
		api.RespondError(c, 500, "Failed to get system settings: "+err.Error())
		return
	}
	api.RespondSuccess(c, cst)
}

// GetSettings 获取自定义配置
func GetSettings(c *gin.Context) {
	tenantID, ok := requireCurrentTenantID(c)
	if !ok {
		return
	}

	cst, err := config.GetAllForTenant(tenantID)
	if err != nil {
		api.RespondError(c, 500, "Failed to get settings: "+err.Error())
		return
	}
	allowed, err := isPlatformAdmin(c)
	if err != nil {
		api.RespondError(c, 500, "Failed to resolve platform admin permissions: "+err.Error())
		return
	}
	if !allowed {
		for key := range systemSettingKeys {
			delete(cst, key)
		}
	}
	api.RespondSuccess(c, cst)
}

// EditSettings 更新自定义配置
func EditSettings(c *gin.Context) {
	cfg := make(map[string]interface{})
	if err := c.ShouldBindJSON(&cfg); err != nil {
		api.RespondError(c, 400, "Invalid or missing request body: "+err.Error())
		return
	}

	tenantID, ok := requireCurrentTenantID(c)
	if !ok {
		return
	}

	tenantValues, systemValues, ignoredKeys := splitEditableSettings(cfg)
	if len(tenantValues) > 0 {
		if err := config.SetManyForTenant(tenantID, tenantValues); err != nil {
			api.RespondError(c, 500, "Failed to update tenant settings: "+err.Error())
			return
		}
	}

	updatedSystemKeys := make([]string, 0)
	ignoredSystemKeys := make([]string, 0)
	if len(systemValues) > 0 {
		allowed, err := isPlatformAdmin(c)
		if err != nil {
			api.RespondError(c, 500, "Failed to resolve platform admin permissions: "+err.Error())
			return
		}
		if allowed {
			if err := config.SetMany(systemValues); err != nil {
				api.RespondError(c, 500, "Failed to update system settings: "+err.Error())
				return
			}
			for key := range systemValues {
				updatedSystemKeys = append(updatedSystemKeys, key)
			}
		} else {
			for key := range systemValues {
				ignoredSystemKeys = append(ignoredSystemKeys, key)
			}
		}
	}

	message := "update settings"
	if len(tenantValues) > 0 || len(updatedSystemKeys) > 0 {
		message += ": "
	}
	for key := range tenantValues {
		message += key + ", "
	}
	for _, key := range updatedSystemKeys {
		message += key + ", "
	}
	if len(message) > 2 && message[len(message)-2:] == ", " {
		message = message[:len(message)-2]
	}

	uuid, exists := c.Get("uuid")
	if exists {
		api.AuditLogForCurrentTenant(c, uuid.(string), message, "info")
	}
	api.RespondSuccess(c, gin.H{
		"updated_tenant_keys":  tenantValues,
		"updated_system_keys":  updatedSystemKeys,
		"ignored_system_keys":  ignoredSystemKeys,
		"ignored_unknown_keys": ignoredKeys,
	})
}

func EditSystemSettings(c *gin.Context) {
	if !requirePlatformAdmin(c) {
		return
	}

	cfg := make(map[string]any)
	if err := c.ShouldBindJSON(&cfg); err != nil {
		api.RespondError(c, 400, "Invalid or missing request body: "+err.Error())
		return
	}

	filtered := filterSystemSettings(cfg)
	if err := config.SetMany(filtered); err != nil {
		api.RespondError(c, 500, "Failed to update system settings: "+err.Error())
		return
	}

	uuid, exists := c.Get("uuid")
	if exists {
		message := "update system settings"
		if len(filtered) > 0 {
			message += ": "
		}
		for key := range filtered {
			message += key + ", "
		}
		if len(message) > 2 && message[len(message)-2:] == ", " {
			message = message[:len(message)-2]
		}
		api.AuditLogForCurrentTenant(c, uuid.(string), message, "info")
	}
	api.RespondSuccess(c, nil)
}

func ClearAllRecords(c *gin.Context) {
	tenantID, ok := currentTenantID(c)
	if !ok {
		api.RespondError(c, 403, "Tenant context is required")
		return
	}
	if err := records.DeleteAllByTenant(tenantID); err != nil {
		api.RespondError(c, 500, "Failed to clear records: "+err.Error())
		return
	}
	if err := tasks.DeletePingRecordsByTenant(tenantID); err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		api.RespondError(c, 500, "Failed to clear ping records: "+err.Error())
		return
	}
	uuid, _ := c.Get("uuid")
	api.AuditLogForCurrentTenant(c, uuid.(string), "clear all records", "info")
	api.RespondSuccess(c, nil)
}
