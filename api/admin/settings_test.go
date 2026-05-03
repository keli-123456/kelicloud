package admin

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/komari-monitor/komari/config"
)

type settingsTestEnvelope struct {
	Status string         `json:"status"`
	Data   map[string]any `json:"data"`
}

func setupAdminSettingsTestDB(t *testing.T) {
	t.Helper()

	db := configureAdminTestDB()
	for _, table := range []string{"user_configs", "configs"} {
		if err := db.Exec("DELETE FROM " + table).Error; err != nil {
			t.Fatalf("failed to clear %s: %v", table, err)
		}
	}
}

func decodeSettingsTestEnvelope(t *testing.T, recorder *httptest.ResponseRecorder) settingsTestEnvelope {
	t.Helper()
	var resp settingsTestEnvelope
	if err := json.Unmarshal(recorder.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	return resp
}

func newSettingsTestContext(t *testing.T, method string, body []byte, userUUID, role string) (*gin.Context, *httptest.ResponseRecorder) {
	t.Helper()
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(method, "/api/admin/settings", bytes.NewReader(body))
	if len(body) > 0 {
		c.Request.Header.Set("Content-Type", "application/json")
	}
	if userUUID != "" {
		c.Set("uuid", userUUID)
	}
	if role != "" {
		c.Set("user_role", role)
	}
	return c, recorder
}

func TestGetSettingsShowsPerUserAutoDiscoveryAndReadableCNProbeConfig(t *testing.T) {
	gin.SetMode(gin.TestMode)
	setupAdminSettingsTestDB(t)

	if err := config.SetMany(map[string]any{
		config.CNConnectivityEnabledKey:           true,
		config.CNConnectivityTargetKey:            "223.5.5.5",
		config.CNConnectivityIntervalKey:          90,
		config.CNConnectivityRetryAttemptsKey:     4,
		config.CNConnectivityRetryDelaySecondsKey: 2,
		config.CNConnectivityTimeoutSecondsKey:    7,
	}); err != nil {
		t.Fatalf("failed to seed cn connectivity settings: %v", err)
	}

	firstCtx, firstRecorder := newSettingsTestContext(t, http.MethodGet, nil, "settings-user-a", "user")
	GetSettings(firstCtx)
	if firstRecorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", firstRecorder.Code)
	}

	firstResp := decodeSettingsTestEnvelope(t, firstRecorder)
	firstKey, _ := firstResp.Data[config.AutoDiscoveryKeyKey].(string)
	if len(firstKey) < 12 {
		t.Fatalf("expected auto discovery key to be generated, got %q", firstKey)
	}
	if firstResp.Data[config.CNConnectivityEnabledKey] != true {
		t.Fatalf("expected CN connectivity flag to be readable, got %#v", firstResp.Data[config.CNConnectivityEnabledKey])
	}
	if firstResp.Data[config.CNConnectivityTargetKey] != "223.5.5.5" {
		t.Fatalf("expected CN connectivity target to be readable, got %#v", firstResp.Data[config.CNConnectivityTargetKey])
	}
	if firstResp.Data[config.CNConnectivityRetryAttemptsKey] != float64(4) {
		t.Fatalf("expected CN connectivity retry attempts to be readable, got %#v", firstResp.Data[config.CNConnectivityRetryAttemptsKey])
	}
	if firstResp.Data[config.CNConnectivityRetryDelaySecondsKey] != float64(2) {
		t.Fatalf("expected CN connectivity retry delay seconds to be readable, got %#v", firstResp.Data[config.CNConnectivityRetryDelaySecondsKey])
	}
	if firstResp.Data[config.CNConnectivityTimeoutSecondsKey] != float64(7) {
		t.Fatalf("expected CN connectivity timeout seconds to be readable, got %#v", firstResp.Data[config.CNConnectivityTimeoutSecondsKey])
	}

	secondCtx, secondRecorder := newSettingsTestContext(t, http.MethodGet, nil, "settings-user-b", "user")
	GetSettings(secondCtx)
	if secondRecorder.Code != http.StatusOK {
		t.Fatalf("expected second request to return 200, got %d", secondRecorder.Code)
	}

	secondResp := decodeSettingsTestEnvelope(t, secondRecorder)
	secondKey, _ := secondResp.Data[config.AutoDiscoveryKeyKey].(string)
	if len(secondKey) < 12 {
		t.Fatalf("expected second auto discovery key to be generated, got %q", secondKey)
	}
	if firstKey == secondKey {
		t.Fatalf("expected per-user auto discovery keys, got identical key %q", firstKey)
	}
}

func TestEditSettingsOnlyAllowsDelegatedCNConnectivityEditors(t *testing.T) {
	gin.SetMode(gin.TestMode)
	setupAdminSettingsTestDB(t)

	if err := config.SetMany(map[string]any{
		config.CNConnectivityEnabledKey:           true,
		config.CNConnectivityTargetKey:            "223.5.5.5",
		config.CNConnectivityIntervalKey:          60,
		config.CNConnectivityRetryAttemptsKey:     3,
		config.CNConnectivityRetryDelaySecondsKey: 1,
		config.CNConnectivityTimeoutSecondsKey:    5,
	}); err != nil {
		t.Fatalf("failed to seed cn connectivity settings: %v", err)
	}

	plainCtx, plainRecorder := newSettingsTestContext(
		t,
		http.MethodPost,
		[]byte(`{"cn_connectivity_enabled":false}`),
		"plain-user",
		"user",
	)
	EditSettings(plainCtx)
	if plainRecorder.Code != http.StatusOK {
		t.Fatalf("expected plain user update to return 200, got %d", plainRecorder.Code)
	}

	plainResp := decodeSettingsTestEnvelope(t, plainRecorder)
	ignored, _ := plainResp.Data["ignored_system_keys"].([]any)
	if len(ignored) != 1 || ignored[0] != config.CNConnectivityEnabledKey {
		t.Fatalf("expected plain user change to be ignored, got %#v", plainResp.Data["ignored_system_keys"])
	}
	enabledAfterPlain, err := config.GetAs[bool](config.CNConnectivityEnabledKey)
	if err != nil {
		t.Fatalf("failed to reload cn connectivity flag: %v", err)
	}
	if !enabledAfterPlain {
		t.Fatal("expected plain user edit to leave CN connectivity enabled")
	}

	features := []string{config.UserFeatureCNConnectivity}
	if err := config.SetUserPolicy("delegated-user", nil, &features); err != nil {
		t.Fatalf("failed to delegate cn connectivity feature: %v", err)
	}

	delegatedCtx, delegatedRecorder := newSettingsTestContext(
		t,
		http.MethodPost,
		[]byte(`{"cn_connectivity_enabled":false,"cn_connectivity_target":"1.1.1.1","cn_connectivity_interval":15,"cn_connectivity_retry_attempts":4,"cn_connectivity_retry_delay_seconds":2,"cn_connectivity_timeout_seconds":8}`),
		"delegated-user",
		"user",
	)
	EditSettings(delegatedCtx)
	if delegatedRecorder.Code != http.StatusOK {
		t.Fatalf("expected delegated update to return 200, got %d", delegatedRecorder.Code)
	}

	delegatedResp := decodeSettingsTestEnvelope(t, delegatedRecorder)
	updated, _ := delegatedResp.Data["updated_system_keys"].([]any)
	if len(updated) != 6 {
		t.Fatalf("expected delegated user to update all CN keys, got %#v", delegatedResp.Data["updated_system_keys"])
	}

	enabledAfterDelegated, err := config.GetAs[bool](config.CNConnectivityEnabledKey)
	if err != nil {
		t.Fatalf("failed to reload delegated cn connectivity flag: %v", err)
	}
	if enabledAfterDelegated {
		t.Fatal("expected delegated user to disable CN connectivity")
	}
	targetAfterDelegated, err := config.GetAs[string](config.CNConnectivityTargetKey)
	if err != nil {
		t.Fatalf("failed to reload delegated cn connectivity target: %v", err)
	}
	if targetAfterDelegated != "1.1.1.1" {
		t.Fatalf("expected delegated user to update cn target, got %q", targetAfterDelegated)
	}
	intervalAfterDelegated, err := config.GetAs[int](config.CNConnectivityIntervalKey)
	if err != nil {
		t.Fatalf("failed to reload delegated cn connectivity interval: %v", err)
	}
	if intervalAfterDelegated != 15 {
		t.Fatalf("expected delegated user to update interval to 15, got %d", intervalAfterDelegated)
	}
	retryAfterDelegated, err := config.GetAs[int](config.CNConnectivityRetryAttemptsKey)
	if err != nil {
		t.Fatalf("failed to reload delegated cn connectivity retry attempts: %v", err)
	}
	if retryAfterDelegated != 4 {
		t.Fatalf("expected delegated user to update retry attempts to 4, got %d", retryAfterDelegated)
	}
	retryDelayAfterDelegated, err := config.GetAs[int](config.CNConnectivityRetryDelaySecondsKey)
	if err != nil {
		t.Fatalf("failed to reload delegated cn connectivity retry delay seconds: %v", err)
	}
	if retryDelayAfterDelegated != 2 {
		t.Fatalf("expected delegated user to update retry delay seconds to 2, got %d", retryDelayAfterDelegated)
	}
	timeoutAfterDelegated, err := config.GetAs[int](config.CNConnectivityTimeoutSecondsKey)
	if err != nil {
		t.Fatalf("failed to reload delegated cn connectivity timeout seconds: %v", err)
	}
	if timeoutAfterDelegated != 8 {
		t.Fatalf("expected delegated user to update timeout seconds to 8, got %d", timeoutAfterDelegated)
	}
}

func TestEditSystemSettingsUpdatesFailoverV2SchedulerFlag(t *testing.T) {
	gin.SetMode(gin.TestMode)
	setupAdminSettingsTestDB(t)

	ctx, recorder := newSettingsTestContext(
		t,
		http.MethodPost,
		[]byte(`{"failover_v2_scheduler_enabled":true}`),
		"platform-admin",
		"admin",
	)
	EditSystemSettings(ctx)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", recorder.Code)
	}

	enabled, err := config.GetAs[bool](config.FailoverV2SchedulerEnabledKey)
	if err != nil {
		t.Fatalf("failed to reload failover v2 scheduler flag: %v", err)
	}
	if !enabled {
		t.Fatal("expected failover v2 scheduler flag to be enabled")
	}
}

func TestEditSettingsValidatesOfflineCleanupSchedule(t *testing.T) {
	gin.SetMode(gin.TestMode)
	setupAdminSettingsTestDB(t)

	validCtx, validRecorder := newSettingsTestContext(
		t,
		http.MethodPost,
		[]byte(`{"offline_cleanup_enabled":true,"offline_cleanup_time":" 04:30 ","offline_cleanup_grace_hours":48}`),
		"platform-admin",
		"admin",
	)
	EditSettings(validCtx)
	if validRecorder.Code != http.StatusOK {
		t.Fatalf("expected valid offline cleanup update to return 200, got %d", validRecorder.Code)
	}

	enabled, err := config.GetAs[bool](config.OfflineCleanupEnabledKey)
	if err != nil {
		t.Fatalf("failed to reload offline cleanup enabled flag: %v", err)
	}
	if !enabled {
		t.Fatal("expected offline cleanup to be enabled")
	}
	runAt, err := config.GetAs[string](config.OfflineCleanupTimeKey)
	if err != nil {
		t.Fatalf("failed to reload offline cleanup time: %v", err)
	}
	if runAt != "04:30" {
		t.Fatalf("expected normalized offline cleanup time, got %q", runAt)
	}
	graceHours, err := config.GetAs[int](config.OfflineCleanupGraceHoursKey)
	if err != nil {
		t.Fatalf("failed to reload offline cleanup grace hours: %v", err)
	}
	if graceHours != 48 {
		t.Fatalf("expected grace hours to be 48, got %d", graceHours)
	}

	invalidCtx, invalidRecorder := newSettingsTestContext(
		t,
		http.MethodPost,
		[]byte(`{"offline_cleanup_time":"25:00","offline_cleanup_grace_hours":0}`),
		"platform-admin",
		"admin",
	)
	EditSettings(invalidCtx)
	if invalidRecorder.Code != http.StatusBadRequest {
		t.Fatalf("expected invalid offline cleanup update to return 400, got %d", invalidRecorder.Code)
	}
}
