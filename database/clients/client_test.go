package clients

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/komari-monitor/komari/config"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"

	"github.com/komari-monitor/komari/database/models"
)

func openClientTestDB(t *testing.T) *gorm.DB {
	t.Helper()

	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared", strings.ReplaceAll(t.Name(), "/", "_"))
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}
	return db
}

func TestDeleteClientWithDBClearsOfflineNotifications(t *testing.T) {
	db := openClientTestDB(t)

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

func TestUserScopedClientQueriesIgnoreOwnerlessData(t *testing.T) {
	db := openClientTestDB(t)

	if err := db.AutoMigrate(&models.User{}, &models.Client{}); err != nil {
		t.Fatalf("failed to migrate test schema: %v", err)
	}

	now := models.FromTime(time.Now())
	user := models.User{UUID: "user-a", Username: "alice", Passwd: "hashed", CreatedAt: now, UpdatedAt: now}
	otherUser := models.User{UUID: "user-b", Username: "bob", Passwd: "hashed", CreatedAt: now, UpdatedAt: now}

	if err := db.Create(&user).Error; err != nil {
		t.Fatalf("failed to create user: %v", err)
	}
	if err := db.Create(&otherUser).Error; err != nil {
		t.Fatalf("failed to create other user: %v", err)
	}

	legacyClient := models.Client{
		UUID:      "legacy-client",
		Token:     "legacy-token",
		Name:      "Legacy Client",
		CreatedAt: now,
		UpdatedAt: now,
	}
	userScopedClient := models.Client{
		UUID:      "user-client",
		Token:     "user-token",
		UserID:    user.UUID,
		Name:      "User Client",
		CreatedAt: now,
		UpdatedAt: now,
	}

	if err := db.Create(&legacyClient).Error; err != nil {
		t.Fatalf("failed to create legacy client: %v", err)
	}
	if err := db.Create(&userScopedClient).Error; err != nil {
		t.Fatalf("failed to create user-scoped client: %v", err)
	}

	ownedClients, err := getAllClientBasicInfoByUserWithDB(db, user.UUID)
	if err != nil {
		t.Fatalf("failed to list user-scoped clients: %v", err)
	}
	if len(ownedClients) != 1 || ownedClients[0].UUID != userScopedClient.UUID {
		t.Fatalf("expected only user-scoped client, got %+v", ownedClients)
	}

	if _, err := getClientByUUIDForUserWithDB(db, legacyClient.UUID, user.UUID); err == nil {
		t.Fatalf("expected ownerless client to be hidden from user scope")
	}
	if _, err := getClientByUUIDForUserWithDB(db, legacyClient.UUID, otherUser.UUID); err == nil {
		t.Fatalf("expected cross-user lookup to fail")
	}

	if err := saveClientForUserWithDB(db, user.UUID, map[string]interface{}{
		"uuid": legacyClient.UUID,
		"name": "Updated By Owner",
	}); err == nil {
		t.Fatalf("expected ownerless client update to fail under user scope")
	}
	if err := saveClientForUserWithDB(db, otherUser.UUID, map[string]interface{}{
		"uuid": legacyClient.UUID,
		"name": "Updated By Other User",
	}); err == nil {
		t.Fatalf("expected cross-user update to fail")
	}

	var normalized []string
	if err := ClientUUIDScopeByUserWithDB(db, user.UUID).
		Where("uuid = ?", userScopedClient.UUID).
		Pluck("uuid", &normalized).Error; err != nil {
		t.Fatalf("expected user scope query to return owned client, got %v", err)
	}
	if len(normalized) != 1 || normalized[0] != userScopedClient.UUID {
		t.Fatalf("expected normalized user client list, got %+v", normalized)
	}
}

func TestCreateClientWithUserWithDBPersistsOwner(t *testing.T) {
	db := openClientTestDB(t)

	config.SetDb(db)

	if err := db.AutoMigrate(&models.User{}, &models.Client{}); err != nil {
		t.Fatalf("failed to migrate test schema: %v", err)
	}

	now := models.FromTime(time.Now())
	if err := db.Create(&models.User{UUID: "user-a", Username: "alice", Passwd: "hashed", CreatedAt: now, UpdatedAt: now}).Error; err != nil {
		t.Fatalf("failed to create owner user: %v", err)
	}

	clientUUID, token, err := createClientWithUserWithDB(db, "user-a", "User Client", "edge")
	if err != nil {
		t.Fatalf("failed to create user-owned client: %v", err)
	}
	if clientUUID == "" || token == "" {
		t.Fatalf("expected generated identifiers, got uuid=%q token=%q", clientUUID, token)
	}

	client, err := getClientByUUIDWithDB(db, clientUUID)
	if err != nil {
		t.Fatalf("failed to load created client: %v", err)
	}
	if client.UserID != "user-a" {
		t.Fatalf("expected user-owned client to keep user_id, got %q", client.UserID)
	}
	if client.Name != "User Client" || client.Group != "edge" {
		t.Fatalf("unexpected created client payload: %+v", client)
	}
}

func TestCreateClientWithUserWithDBHonorsQuota(t *testing.T) {
	db := openClientTestDB(t)
	config.SetDb(db)

	if err := db.AutoMigrate(&models.User{}, &models.Client{}); err != nil {
		t.Fatalf("failed to migrate test schema: %v", err)
	}

	now := models.FromTime(time.Now())
	if err := db.Create(&models.User{UUID: "user-a", Username: "alice", Passwd: "hashed", CreatedAt: now, UpdatedAt: now}).Error; err != nil {
		t.Fatalf("failed to create owner user: %v", err)
	}

	quota := 1
	if err := config.SetUserPolicy("user-a", &quota, nil); err != nil {
		t.Fatalf("failed to set user quota: %v", err)
	}

	if _, _, err := createClientWithUserWithDB(db, "user-a", "First", ""); err != nil {
		t.Fatalf("failed to create first client under quota: %v", err)
	}
	if _, _, err := createClientWithUserWithDB(db, "user-a", "Second", ""); err == nil {
		t.Fatal("expected second client creation to be blocked by quota")
	}
}
