package clients

import (
	"testing"
	"time"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"

	"github.com/komari-monitor/komari/database/models"
)

func TestDeleteClientWithDBClearsOfflineNotifications(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}

	if err := db.Exec("PRAGMA foreign_keys = ON").Error; err != nil {
		t.Fatalf("failed to enable foreign keys: %v", err)
	}
	if err := db.AutoMigrate(&models.Client{}, &models.OfflineNotification{}); err != nil {
		t.Fatalf("failed to migrate test schema: %v", err)
	}

	now := models.FromTime(time.Now())
	client := models.Client{
		UUID:      "test-client-1",
		Token:     "test-token-1",
		Name:      "Test Client",
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := db.Create(&client).Error; err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	notification := models.OfflineNotification{
		Client: client.UUID,
		Enable: true,
	}
	if err := db.Create(&notification).Error; err != nil {
		t.Fatalf("failed to create offline notification: %v", err)
	}

	if err := deleteClientWithDB(db, client.UUID); err != nil {
		t.Fatalf("deleteClientWithDB returned error: %v", err)
	}

	var clientCount int64
	if err := db.Model(&models.Client{}).Where("uuid = ?", client.UUID).Count(&clientCount).Error; err != nil {
		t.Fatalf("failed to count clients: %v", err)
	}
	if clientCount != 0 {
		t.Fatalf("expected client to be deleted, found %d rows", clientCount)
	}

	var notificationCount int64
	if err := db.Model(&models.OfflineNotification{}).Where("client = ?", client.UUID).Count(&notificationCount).Error; err != nil {
		t.Fatalf("failed to count offline notifications: %v", err)
	}
	if notificationCount != 0 {
		t.Fatalf("expected offline notification to be deleted, found %d rows", notificationCount)
	}
}

func TestTenantScopedClientQueriesAndUpdates(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}

	if err := db.AutoMigrate(&models.Client{}); err != nil {
		t.Fatalf("failed to migrate test schema: %v", err)
	}

	now := models.FromTime(time.Now())
	tenantAClient := models.Client{
		UUID:      "tenant-a-client",
		Token:     "tenant-a-token",
		TenantID:  "tenant-a",
		Name:      "Tenant A Client",
		CreatedAt: now,
		UpdatedAt: now,
	}
	tenantBClient := models.Client{
		UUID:      "tenant-b-client",
		Token:     "tenant-b-token",
		TenantID:  "tenant-b",
		Name:      "Tenant B Client",
		CreatedAt: now,
		UpdatedAt: now,
	}

	if err := db.Create(&tenantAClient).Error; err != nil {
		t.Fatalf("failed to create tenant A client: %v", err)
	}
	if err := db.Create(&tenantBClient).Error; err != nil {
		t.Fatalf("failed to create tenant B client: %v", err)
	}

	if _, err := getClientByUUIDForTenantWithDB(db, tenantAClient.UUID, tenantAClient.TenantID); err != nil {
		t.Fatalf("expected tenant-scoped lookup to succeed: %v", err)
	}

	if _, err := getClientByUUIDForTenantWithDB(db, tenantAClient.UUID, tenantBClient.TenantID); err == nil {
		t.Fatalf("expected cross-tenant lookup to fail")
	}

	if err := saveClientForTenantWithDB(db, tenantBClient.TenantID, map[string]interface{}{
		"uuid": tenantAClient.UUID,
		"name": "Cross Tenant Update",
	}); err == nil {
		t.Fatalf("expected cross-tenant update to fail")
	}

	if err := saveClientForTenantWithDB(db, tenantAClient.TenantID, map[string]interface{}{
		"uuid": tenantAClient.UUID,
		"name": "Tenant A Updated",
	}); err != nil {
		t.Fatalf("expected tenant-scoped update to succeed: %v", err)
	}

	updatedClient, err := getClientByUUIDForTenantWithDB(db, tenantAClient.UUID, tenantAClient.TenantID)
	if err != nil {
		t.Fatalf("failed to load updated client: %v", err)
	}
	if updatedClient.Name != "Tenant A Updated" {
		t.Fatalf("expected updated client name, got %q", updatedClient.Name)
	}

	tenantAClients, err := getAllClientBasicInfoWithDB(db, tenantAClient.TenantID)
	if err != nil {
		t.Fatalf("failed to list tenant A clients: %v", err)
	}
	if len(tenantAClients) != 1 || tenantAClients[0].UUID != tenantAClient.UUID {
		t.Fatalf("expected only tenant A client, got %+v", tenantAClients)
	}
}
