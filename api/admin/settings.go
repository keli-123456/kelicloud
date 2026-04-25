package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/komari-monitor/komari/api"
	"github.com/komari-monitor/komari/config"
	"github.com/komari-monitor/komari/database/accounts"
	"github.com/komari-monitor/komari/database/records"
	"github.com/komari-monitor/komari/database/tasks"
	"github.com/komari-monitor/komari/utils/offlinecleanup"
	"github.com/komari-monitor/komari/utils/outboundproxy"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

var systemSettingKeys = map[string]struct{}{
	config.SitenameKey:                        {},
	config.DescriptionKey:                     {},
	config.SiteSubtitleKey:                    {},
	config.GithubURLKey:                       {},
	config.AllowCorsKey:                       {},
	config.ApiKeyKey:                          {},
	config.CustomHeadKey:                      {},
	config.CustomBodyKey:                      {},
	config.CNConnectivityEnabledKey:           {},
	config.CNConnectivityTargetKey:            {},
	config.CNConnectivityIntervalKey:          {},
	config.CNConnectivityRetryAttemptsKey:     {},
	config.CNConnectivityRetryDelaySecondsKey: {},
	config.CNConnectivityTimeoutSecondsKey:    {},
	config.OutboundProxyEnabledKey:            {},
	config.OutboundProxyProtocolKey:           {},
	config.OutboundProxyHostKey:               {},
	config.OutboundProxyPortKey:               {},
	config.OutboundProxyUsernameKey:           {},
	config.OutboundProxyPasswordKey:           {},
	config.EulaAcceptedKey:                    {},
	config.GeoIpEnabledKey:                    {},
	config.GeoIpProviderKey:                   {},
	config.NezhaCompatEnabledKey:              {},
	config.NezhaCompatListenKey:               {},
	config.OAuthEnabledKey:                    {},
	config.OAuthProviderKey:                   {},
	config.DisablePasswordLoginKey:            {},
	config.NotificationEnabledKey:             {},
	config.NotificationMethodKey:              {},
	config.NotificationTemplateKey:            {},
	config.ExpireNotificationEnabledKey:       {},
	config.ExpireNotificationLeadDaysKey:      {},
	config.LoginNotificationKey:               {},
	config.TrafficLimitPercentageKey:          {},
	config.RecordEnabledKey:                   {},
	config.RecordPreserveTimeKey:              {},
	config.PingRecordPreserveTimeKey:          {},
	config.OfflineCleanupEnabledKey:           {},
	config.OfflineCleanupTimeKey:              {},
	config.OfflineCleanupGraceHoursKey:        {},
	config.OfflineCleanupLastRunAtKey:         {},
	config.FailoverV2SchedulerEnabledKey:      {},
}

var readableSystemSettingKeys = map[string]struct{}{
	config.CNConnectivityEnabledKey:           {},
	config.CNConnectivityTargetKey:            {},
	config.CNConnectivityIntervalKey:          {},
	config.CNConnectivityRetryAttemptsKey:     {},
	config.CNConnectivityRetryDelaySecondsKey: {},
	config.CNConnectivityTimeoutSecondsKey:    {},
	config.NotificationEnabledKey:             {},
	config.NotificationMethodKey:              {},
}

var delegatedSystemSettingFeatures = map[string]string{
	config.CNConnectivityEnabledKey:           config.UserFeatureCNConnectivity,
	config.CNConnectivityTargetKey:            config.UserFeatureCNConnectivity,
	config.CNConnectivityIntervalKey:          config.UserFeatureCNConnectivity,
	config.CNConnectivityRetryAttemptsKey:     config.UserFeatureCNConnectivity,
	config.CNConnectivityRetryDelaySecondsKey: config.UserFeatureCNConnectivity,
	config.CNConnectivityTimeoutSecondsKey:    config.UserFeatureCNConnectivity,
}

func isSystemSettingKey(key string) bool {
	_, ok := systemSettingKeys[key]
	return ok
}

func isReadableSystemSettingKey(key string) bool {
	_, ok := readableSystemSettingKeys[key]
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

func canEditSystemSettingKey(c *gin.Context, key string) (bool, error) {
	allowed, err := isPlatformAdmin(c)
	if err != nil {
		return false, err
	}
	if allowed {
		return true, nil
	}

	feature, ok := delegatedSystemSettingFeatures[key]
	if !ok {
		return false, nil
	}

	userUUID, ok := currentUserUUID(c)
	if !ok {
		return false, nil
	}
	return config.IsUserFeatureAllowed(userUUID, feature)
}

func splitEditableSettings(cfg map[string]any) (map[string]any, map[string]any, []string) {
	userValues := make(map[string]any)
	systemValues := make(map[string]any)
	ignored := make([]string, 0)

	for key, value := range cfg {
		switch {
		case config.IsUserScopedKey(key):
			userValues[key] = value
		case isSystemSettingKey(key):
			systemValues[key] = value
		default:
			ignored = append(ignored, key)
		}
	}

	return userValues, systemValues, ignored
}

func isPlatformAdmin(c *gin.Context) (bool, error) {
	if _, ok := c.Get("api_key"); ok {
		return true, nil
	}
	roleValue, ok := c.Get("user_role")
	if !ok {
		return false, nil
	}
	role, _ := roleValue.(string)
	return accounts.IsUserRoleAtLeast(role, accounts.RoleAdmin), nil
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

type outboundProxyTestRequest struct {
	OutboundProxyEnabled  bool   `json:"outbound_proxy_enabled"`
	OutboundProxyProtocol string `json:"outbound_proxy_protocol"`
	OutboundProxyHost     string `json:"outbound_proxy_host"`
	OutboundProxyPort     int    `json:"outbound_proxy_port"`
	OutboundProxyUsername string `json:"outbound_proxy_username"`
	OutboundProxyPassword string `json:"outbound_proxy_password"`
}

func parseOutboundProxyTestRequest(c *gin.Context) (*outboundproxy.Settings, error) {
	body, err := c.GetRawData()
	if err != nil {
		return nil, err
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return nil, nil
	}

	var payload outboundProxyTestRequest
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, err
	}

	return &outboundproxy.Settings{
		Enabled:  payload.OutboundProxyEnabled,
		Protocol: payload.OutboundProxyProtocol,
		Host:     payload.OutboundProxyHost,
		Port:     payload.OutboundProxyPort,
		Username: payload.OutboundProxyUsername,
		Password: payload.OutboundProxyPassword,
	}, nil
}

func TestOutboundProxy(c *gin.Context) {
	if !requirePlatformAdmin(c) {
		return
	}

	settings, err := parseOutboundProxyTestRequest(c)
	if err != nil {
		api.RespondError(c, 400, "Invalid outbound proxy test payload: "+err.Error())
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 20*time.Second)
	defer cancel()

	var result *outboundproxy.ProbeResult
	if settings == nil {
		result, err = outboundproxy.Probe(ctx)
	} else {
		result, err = outboundproxy.ProbeWithSettings(ctx, settings)
	}
	if err != nil {
		api.RespondError(c, 500, "Failed to test outbound proxy: "+err.Error())
		return
	}
	api.RespondSuccess(c, result)
}

// GetSettings 获取自定义配置
func GetSettings(c *gin.Context) {
	cst, err := config.GetAll()
	if err != nil {
		api.RespondError(c, 500, "Failed to get settings: "+err.Error())
		return
	}
	for _, key := range config.UserScopedKeys() {
		delete(cst, key)
	}
	if userUUID, ok := currentUserUUID(c); ok {
		userSettings, err := config.GetAllForUser(userUUID)
		if err != nil {
			api.RespondError(c, 500, "Failed to get user settings: "+err.Error())
			return
		}
		for key, value := range userSettings {
			if config.IsUserScopedKey(key) {
				cst[key] = value
			}
		}
		if autoDiscoveryKey, err := config.EnsureAutoDiscoveryKeyForUser(userUUID); err == nil {
			cst[config.AutoDiscoveryKeyKey] = autoDiscoveryKey
		} else {
			api.RespondError(c, 500, "Failed to ensure user auto discovery key: "+err.Error())
			return
		}
	}
	allowed, err := isPlatformAdmin(c)
	if err != nil {
		api.RespondError(c, 500, "Failed to resolve platform admin permissions: "+err.Error())
		return
	}
	if !allowed {
		for key := range systemSettingKeys {
			if !isReadableSystemSettingKey(key) {
				delete(cst, key)
			}
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

	userValues, systemValues, ignoredKeys := splitEditableSettings(cfg)
	if len(userValues) > 0 {
		userUUID, ok := currentUserUUID(c)
		if !ok {
			api.RespondError(c, 403, "User context is required to update user settings")
			return
		}
		if err := config.SetManyForUser(userUUID, userValues); err != nil {
			api.RespondError(c, 500, "Failed to update user settings: "+err.Error())
			return
		}
	}
	updatedSystemKeys := make([]string, 0)
	ignoredSystemKeys := make([]string, 0)
	if len(systemValues) > 0 {
		editableSystemValues := make(map[string]any)
		for key, value := range systemValues {
			allowed, err := canEditSystemSettingKey(c, key)
			if err != nil {
				api.RespondError(c, 500, "Failed to resolve system setting permissions: "+err.Error())
				return
			}
			if !allowed {
				ignoredSystemKeys = append(ignoredSystemKeys, key)
				continue
			}
			editableSystemValues[key] = value
		}
		if len(editableSystemValues) > 0 {
			if err := validateSystemSettingUpdates(editableSystemValues); err != nil {
				api.RespondError(c, 400, "Invalid system settings: "+err.Error())
				return
			}
			if err := config.SetMany(editableSystemValues); err != nil {
				api.RespondError(c, 500, "Failed to update system settings: "+err.Error())
				return
			}
			for key := range editableSystemValues {
				updatedSystemKeys = append(updatedSystemKeys, key)
			}
		}
	}

	message := "update settings"
	if len(userValues) > 0 || len(updatedSystemKeys) > 0 {
		message += ": "
	}
	for key := range userValues {
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
		api.AuditLogForCurrentUser(c, uuid.(string), message, "info")
	}
	api.RespondSuccess(c, gin.H{
		"updated_user_keys":    userValues,
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
	if err := validateSystemSettingUpdates(filtered); err != nil {
		api.RespondError(c, 400, "Invalid system settings: "+err.Error())
		return
	}
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
		api.AuditLogForCurrentUser(c, uuid.(string), message, "info")
	}
	api.RespondSuccess(c, nil)
}

func validateSystemSettingUpdates(values map[string]any) error {
	if rawTime, exists := values[config.OfflineCleanupTimeKey]; exists {
		rawString, ok := rawTime.(string)
		if !ok {
			return fmt.Errorf("%s must be a string", config.OfflineCleanupTimeKey)
		}
		normalized, err := offlinecleanup.NormalizeDailyCleanupTime(rawString)
		if err != nil {
			return err
		}
		values[config.OfflineCleanupTimeKey] = normalized
	}

	if rawEnabled, exists := values[config.OfflineCleanupEnabledKey]; exists {
		if _, ok := rawEnabled.(bool); !ok {
			return fmt.Errorf("%s must be a boolean", config.OfflineCleanupEnabledKey)
		}
	}
	if rawEnabled, exists := values[config.FailoverV2SchedulerEnabledKey]; exists {
		if _, ok := rawEnabled.(bool); !ok {
			return fmt.Errorf("%s must be a boolean", config.FailoverV2SchedulerEnabledKey)
		}
	}
	if err := validateLoginMethodSettings(values); err != nil {
		return err
	}

	for _, integerKey := range []string{
		config.CNConnectivityIntervalKey,
		config.CNConnectivityRetryAttemptsKey,
		config.CNConnectivityRetryDelaySecondsKey,
		config.CNConnectivityTimeoutSecondsKey,
	} {
		rawValue, exists := values[integerKey]
		if !exists {
			continue
		}
		switch value := rawValue.(type) {
		case float64:
			if value < 1 || value != float64(int(value)) {
				return fmt.Errorf("%s must be an integer greater than 0", integerKey)
			}
			values[integerKey] = int(value)
		case int:
			if value < 1 {
				return fmt.Errorf("%s must be greater than 0", integerKey)
			}
		default:
			return fmt.Errorf("%s must be an integer", integerKey)
		}
	}

	if rawGraceHours, exists := values[config.OfflineCleanupGraceHoursKey]; exists {
		switch value := rawGraceHours.(type) {
		case float64:
			if value < 1 || value != float64(int(value)) {
				return fmt.Errorf("%s must be an integer greater than 0", config.OfflineCleanupGraceHoursKey)
			}
			values[config.OfflineCleanupGraceHoursKey] = int(value)
		case int:
			if value < 1 {
				return fmt.Errorf("%s must be greater than 0", config.OfflineCleanupGraceHoursKey)
			}
		default:
			return fmt.Errorf("%s must be an integer", config.OfflineCleanupGraceHoursKey)
		}
	}

	return nil
}

func validateLoginMethodSettings(values map[string]any) error {
	_, touchesPasswordLogin := values[config.DisablePasswordLoginKey]
	_, touchesOAuth := values[config.OAuthEnabledKey]
	if !touchesPasswordLogin && !touchesOAuth {
		return nil
	}

	currentDisablePasswordLogin, err := config.GetAs[bool](config.DisablePasswordLoginKey, false)
	if err != nil {
		return fmt.Errorf("failed to read current %s: %w", config.DisablePasswordLoginKey, err)
	}
	currentOAuthEnabled, err := config.GetAs[bool](config.OAuthEnabledKey, false)
	if err != nil {
		return fmt.Errorf("failed to read current %s: %w", config.OAuthEnabledKey, err)
	}

	disablePasswordLogin, err := boolSettingValue(values, config.DisablePasswordLoginKey, currentDisablePasswordLogin)
	if err != nil {
		return err
	}
	oauthEnabled, err := boolSettingValue(values, config.OAuthEnabledKey, currentOAuthEnabled)
	if err != nil {
		return err
	}

	if !disablePasswordLogin {
		return nil
	}
	if !oauthEnabled {
		return errors.New("at least one login method must be enabled (password/oauth)")
	}

	hasBoundUser, err := accounts.HasAnySSOBoundUser()
	if err != nil {
		return fmt.Errorf("failed to verify SSO-bound accounts: %w", err)
	}
	if !hasBoundUser {
		return errors.New("cannot disable password login when no SSO-bound account exists")
	}
	return nil
}

func boolSettingValue(values map[string]any, key string, fallback bool) (bool, error) {
	rawValue, exists := values[key]
	if !exists {
		return fallback, nil
	}
	value, ok := rawValue.(bool)
	if !ok {
		return false, fmt.Errorf("%s must be a boolean", key)
	}
	return value, nil
}

func ClearAllRecords(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}
	err := records.DeleteAllByUser(scope.UserUUID)
	if err != nil {
		api.RespondError(c, 500, "Failed to clear records: "+err.Error())
		return
	}
	err = tasks.DeletePingRecordsByUser(scope.UserUUID)
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		api.RespondError(c, 500, "Failed to clear ping records: "+err.Error())
		return
	}
	userUUID, _ := currentUserUUID(c)
	api.AuditLogForCurrentUser(c, userUUID, "clear all records", "info")
	api.RespondSuccess(c, nil)
}
