package admin

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/komari-monitor/komari/cmd/flags"
	"github.com/komari-monitor/komari/database/dbcore"
	"github.com/komari-monitor/komari/database/models"
	"gorm.io/gorm"
)

type failoverV2ValidationTestEnvelope struct {
	Status  string                   `json:"status"`
	Message string                   `json:"message"`
	Data    failoverV2ValidationView `json:"data"`
}

type failoverV2BulkValidationTestEnvelope struct {
	Status  string                       `json:"status"`
	Message string                       `json:"message"`
	Data    failoverV2BulkValidationView `json:"data"`
}

func setupFailoverV2APITestDB(t *testing.T) *gorm.DB {
	t.Helper()

	flags.DatabaseType = "sqlite"
	flags.DatabaseFile = filepath.Join(t.TempDir(), "komari-failover-v2-api.db")

	db := dbcore.GetDBInstance()
	if err := db.AutoMigrate(
		&models.CloudProvider{},
		&models.Client{},
		&models.FailoverTask{},
		&models.FailoverPlan{},
		&models.FailoverExecution{},
		&models.FailoverV2Service{},
		&models.FailoverV2Member{},
		&models.FailoverV2Execution{},
		&models.FailoverV2ExecutionStep{},
		&models.FailoverV2PendingCleanup{},
		&models.FailoverV2RunLock{},
	); err != nil {
		t.Fatalf("failed to migrate failover v2 api test tables: %v", err)
	}

	session := db.Session(&gorm.Session{AllowGlobalUpdate: true})
	for _, model := range []interface{}{
		&models.FailoverV2RunLock{},
		&models.FailoverV2PendingCleanup{},
		&models.FailoverV2ExecutionStep{},
		&models.FailoverV2Execution{},
		&models.FailoverV2Member{},
		&models.FailoverV2Service{},
		&models.FailoverExecution{},
		&models.FailoverPlan{},
		&models.FailoverTask{},
		&models.CloudProvider{},
		&models.Client{},
	} {
		if err := session.Delete(model).Error; err != nil {
			t.Fatalf("failed to clear failover v2 api test table: %v", err)
		}
	}

	return db
}

func seedFailoverV2ValidationProviders(t *testing.T, db *gorm.DB) {
	t.Helper()

	providers := []models.CloudProvider{
		{
			UserID: "user-a",
			Name:   models.FailoverDNSProviderCloudflare,
			Addition: `{
				"entries": [{
					"id": "cf-entry",
					"name": "Cloudflare",
					"values": {
						"api_token": "cf-token",
						"zone_name": "example.com"
					}
				}]
			}`,
		},
		{
			UserID: "user-a",
			Name:   digitalOceanProviderName,
			Addition: `{
				"active_token_id": "do-entry",
				"tokens": [{
					"id": "do-entry",
					"name": "DigitalOcean",
					"token": "do-token"
				}]
			}`,
		},
	}
	if err := db.Create(&providers).Error; err != nil {
		t.Fatalf("failed to seed failover v2 api providers: %v", err)
	}
}

func seedFailoverV2ValidationClient(t *testing.T, db *gorm.DB) {
	t.Helper()

	client := models.Client{
		UUID:   "client-a",
		Token:  "client-token-a",
		UserID: "user-a",
		Name:   "client-a",
		IPv4:   "203.0.113.10",
	}
	if err := db.Create(&client).Error; err != nil {
		t.Fatalf("failed to seed failover v2 api client: %v", err)
	}
}

func newFailoverV2ValidationTestContext(t *testing.T, method, path string, body interface{}, params ...gin.Param) (*gin.Context, *httptest.ResponseRecorder) {
	t.Helper()

	payload, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("failed to marshal failover v2 api test request: %v", err)
	}

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(method, path, bytes.NewReader(payload))
	c.Request.Header.Set("Content-Type", "application/json")
	c.Set("uuid", "user-a")
	c.Params = params
	return c, recorder
}

func decodeFailoverV2ValidationTestEnvelope(t *testing.T, recorder *httptest.ResponseRecorder) failoverV2ValidationTestEnvelope {
	t.Helper()

	var resp failoverV2ValidationTestEnvelope
	if err := json.Unmarshal(recorder.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode failover v2 validation response: %v", err)
	}
	return resp
}

func decodeFailoverV2BulkValidationTestEnvelope(t *testing.T, recorder *httptest.ResponseRecorder) failoverV2BulkValidationTestEnvelope {
	t.Helper()

	var resp failoverV2BulkValidationTestEnvelope
	if err := json.Unmarshal(recorder.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode failover v2 bulk validation response: %v", err)
	}
	return resp
}

func requireFailoverV2ValidationCheck(t *testing.T, checks []failoverV2ValidationCheckView, key string) failoverV2ValidationCheckView {
	t.Helper()

	for _, check := range checks {
		if check.Key == key {
			return check
		}
	}
	t.Fatalf("expected validation check %q in %#v", key, checks)
	return failoverV2ValidationCheckView{}
}

func testFailoverV2ServiceValidationRequest() failoverV2ServiceRequest {
	enabled := true
	return failoverV2ServiceRequest{
		Name:                "v2-service",
		Enabled:             &enabled,
		DNSProvider:         models.FailoverDNSProviderCloudflare,
		DNSEntryID:          "cf-entry",
		DNSPayload:          json.RawMessage(`{"zone_name":"example.com","record_name":"api","record_type":"A","ttl":60}`),
		ScriptTimeoutSec:    600,
		WaitAgentTimeoutSec: 600,
		DeleteStrategy:      models.FailoverDeleteStrategyKeep,
	}
}

func testFailoverV2MemberValidationRequest() failoverV2MemberRequest {
	enabled := true
	return failoverV2MemberRequest{
		Name:               "member-a",
		Enabled:            &enabled,
		Priority:           1,
		WatchClientUUID:    "client-a",
		DNSLine:            "telecom",
		DNSRecordRefs:      json.RawMessage(`{}`),
		CurrentInstanceRef: json.RawMessage(`null`),
		Provider:           digitalOceanProviderName,
		ProviderEntryID:    "do-entry",
		PlanPayload:        json.RawMessage(`{"region":"nyc1","size":"s-1vcpu-1gb","image":"ubuntu-24-04-x64"}`),
		FailureThreshold:   2,
		StaleAfterSeconds:  300,
		CooldownSeconds:    1800,
	}
}

func TestValidateFailoverV2ServicePassesForCleanCreate(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := setupFailoverV2APITestDB(t)
	seedFailoverV2ValidationProviders(t, db)

	c, recorder := newFailoverV2ValidationTestContext(
		t,
		http.MethodPost,
		"/api/admin/failover-v2/services/validate",
		testFailoverV2ServiceValidationRequest(),
	)
	ValidateFailoverV2Service(c)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", recorder.Code, recorder.Body.String())
	}
	resp := decodeFailoverV2ValidationTestEnvelope(t, recorder)
	if resp.Status != "success" || !resp.Data.OK {
		t.Fatalf("expected clean service validation to pass, got %+v", resp)
	}
	if check := requireFailoverV2ValidationCheck(t, resp.Data.Checks, "dns_ownership"); check.Status != "pass" {
		t.Fatalf("expected dns_ownership to pass, got %+v", check)
	}
}

func TestValidateFailoverV2ServiceReportsActiveV1DNSConflict(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := setupFailoverV2APITestDB(t)
	seedFailoverV2ValidationProviders(t, db)

	if err := db.Create(&models.FailoverTask{
		UserID:          "user-a",
		Name:            "v1-prod",
		Enabled:         true,
		WatchClientUUID: "client-v1",
		DNSProvider:     models.FailoverDNSProviderCloudflare,
		DNSEntryID:      "cf-entry",
		DNSPayload:      `{"zone_name":"example.com","record_name":"api","record_type":"A","ttl":60}`,
		DeleteStrategy:  models.FailoverDeleteStrategyKeep,
		LastStatus:      models.FailoverTaskStatusHealthy,
	}).Error; err != nil {
		t.Fatalf("failed to seed v1 failover task: %v", err)
	}

	c, recorder := newFailoverV2ValidationTestContext(
		t,
		http.MethodPost,
		"/api/admin/failover-v2/services/validate",
		testFailoverV2ServiceValidationRequest(),
	)
	ValidateFailoverV2Service(c)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", recorder.Code, recorder.Body.String())
	}
	resp := decodeFailoverV2ValidationTestEnvelope(t, recorder)
	if resp.Data.OK {
		t.Fatalf("expected v1 dns conflict validation to fail, got %+v", resp)
	}
	check := requireFailoverV2ValidationCheck(t, resp.Data.Checks, "dns_ownership")
	if check.Status != "fail" || !strings.Contains(check.Message, "active v1 failover task") {
		t.Fatalf("expected active v1 dns conflict, got %+v", check)
	}
}

func TestValidateAllFailoverV2ServicesReportsActiveV1DNSConflict(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := setupFailoverV2APITestDB(t)
	seedFailoverV2ValidationProviders(t, db)

	service := models.FailoverV2Service{
		UserID:              "user-a",
		Name:                "v2-service",
		Enabled:             true,
		DNSProvider:         models.FailoverDNSProviderCloudflare,
		DNSEntryID:          "cf-entry",
		DNSPayload:          `{"zone_name":"example.com","record_name":"api","record_type":"A","ttl":60}`,
		ScriptTimeoutSec:    600,
		WaitAgentTimeoutSec: 600,
		DeleteStrategy:      models.FailoverDeleteStrategyKeep,
		LastStatus:          models.FailoverV2ServiceStatusHealthy,
	}
	if err := db.Create(&service).Error; err != nil {
		t.Fatalf("failed to seed v2 service: %v", err)
	}
	if err := db.Create(&models.FailoverTask{
		UserID:          "user-a",
		Name:            "v1-prod",
		Enabled:         true,
		WatchClientUUID: "client-v1",
		DNSProvider:     models.FailoverDNSProviderCloudflare,
		DNSEntryID:      "cf-entry",
		DNSPayload:      `{"zone_name":"example.com","record_name":"api","record_type":"A","ttl":60}`,
		DeleteStrategy:  models.FailoverDeleteStrategyKeep,
		LastStatus:      models.FailoverTaskStatusHealthy,
	}).Error; err != nil {
		t.Fatalf("failed to seed v1 failover task: %v", err)
	}

	c, recorder := newFailoverV2ValidationTestContext(
		t,
		http.MethodPost,
		"/api/admin/failover-v2/services/validate-all",
		nil,
	)
	ValidateAllFailoverV2Services(c)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", recorder.Code, recorder.Body.String())
	}
	resp := decodeFailoverV2BulkValidationTestEnvelope(t, recorder)
	if resp.Data.OK || resp.Data.Failed != 1 || len(resp.Data.Services) != 1 {
		t.Fatalf("expected bulk validation to fail one service, got %+v", resp)
	}
	check := requireFailoverV2ValidationCheck(t, resp.Data.Services[0].Checks, "dns_ownership")
	if check.Status != "fail" || !strings.Contains(check.Message, "active v1 failover task") {
		t.Fatalf("expected active v1 dns conflict, got %+v", check)
	}
}

func TestValidateFailoverV2MemberReportsActiveV1WatchClientConflict(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := setupFailoverV2APITestDB(t)
	seedFailoverV2ValidationProviders(t, db)
	seedFailoverV2ValidationClient(t, db)

	service := models.FailoverV2Service{
		UserID:              "user-a",
		Name:                "v2-service",
		Enabled:             true,
		DNSProvider:         models.FailoverDNSProviderCloudflare,
		DNSEntryID:          "cf-entry",
		DNSPayload:          `{"zone_name":"example.com","record_name":"api-v2","record_type":"A","ttl":60}`,
		ScriptTimeoutSec:    600,
		WaitAgentTimeoutSec: 600,
		DeleteStrategy:      models.FailoverDeleteStrategyKeep,
		LastStatus:          models.FailoverV2ServiceStatusHealthy,
	}
	if err := db.Create(&service).Error; err != nil {
		t.Fatalf("failed to seed v2 service: %v", err)
	}
	if err := db.Create(&models.FailoverTask{
		UserID:          "user-a",
		Name:            "v1-prod",
		Enabled:         true,
		WatchClientUUID: "client-a",
		DNSProvider:     models.FailoverDNSProviderCloudflare,
		DNSEntryID:      "cf-entry",
		DNSPayload:      `{"zone_name":"example.com","record_name":"api-v1","record_type":"A","ttl":60}`,
		DeleteStrategy:  models.FailoverDeleteStrategyKeep,
		LastStatus:      models.FailoverTaskStatusHealthy,
	}).Error; err != nil {
		t.Fatalf("failed to seed v1 failover task: %v", err)
	}

	c, recorder := newFailoverV2ValidationTestContext(
		t,
		http.MethodPost,
		"/api/admin/failover-v2/services/"+strconv.FormatUint(uint64(service.ID), 10)+"/members/validate",
		testFailoverV2MemberValidationRequest(),
		gin.Param{Key: "id", Value: strconv.FormatUint(uint64(service.ID), 10)},
	)
	ValidateFailoverV2Member(c)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", recorder.Code, recorder.Body.String())
	}
	resp := decodeFailoverV2ValidationTestEnvelope(t, recorder)
	if resp.Data.OK {
		t.Fatalf("expected v1 watch client conflict validation to fail, got %+v", resp)
	}
	check := requireFailoverV2ValidationCheck(t, resp.Data.Checks, "v1_target")
	if check.Status != "fail" || !strings.Contains(check.Message, "same watch_client_uuid") {
		t.Fatalf("expected active v1 watch client conflict, got %+v", check)
	}
}
