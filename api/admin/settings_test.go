package admin

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/komari-monitor/komari/cmd/flags"
	"github.com/komari-monitor/komari/config"
	"github.com/komari-monitor/komari/database/dbcore"
)

type settingsTestEnvelope struct {
	Status string         `json:"status"`
	Data   map[string]any `json:"data"`
}

func setupAdminSettingsTestDB(t *testing.T) {
	t.Helper()
	flags.DatabaseType = "sqlite"
	flags.DatabaseFile = filepath.Join(t.TempDir(), "komari-admin-settings.db")

	db := dbcore.GetDBInstance()
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
		config.CNConnectivityEnabledKey:  true,
		config.CNConnectivityTargetKey:   "223.5.5.5",
		config.CNConnectivityIntervalKey: 90,
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
		config.CNConnectivityEnabledKey:  true,
		config.CNConnectivityTargetKey:   "223.5.5.5",
		config.CNConnectivityIntervalKey: 60,
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
		[]byte(`{"cn_connectivity_enabled":false,"cn_connectivity_target":"1.1.1.1","cn_connectivity_interval":15}`),
		"delegated-user",
		"user",
	)
	EditSettings(delegatedCtx)
	if delegatedRecorder.Code != http.StatusOK {
		t.Fatalf("expected delegated update to return 200, got %d", delegatedRecorder.Code)
	}

	delegatedResp := decodeSettingsTestEnvelope(t, delegatedRecorder)
	updated, _ := delegatedResp.Data["updated_system_keys"].([]any)
	if len(updated) != 3 {
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
}
