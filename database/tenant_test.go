package database

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/komari-monitor/komari/config"
	"github.com/komari-monitor/komari/database/models"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func setupTenantTestDB(t *testing.T, migrateModels ...interface{}) *gorm.DB {
	t.Helper()

	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared", t.Name())
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}
	if err := db.Exec("PRAGMA foreign_keys = ON").Error; err != nil {
		t.Fatalf("failed to enable foreign keys: %v", err)
	}
	if len(migrateModels) > 0 {
		if err := db.AutoMigrate(migrateModels...); err != nil {
			t.Fatalf("failed to migrate tenant test schema: %v", err)
		}
	}
	return db
}

func createTenantTestUser(t *testing.T, db *gorm.DB, uuid, username string) models.User {
	t.Helper()

	user := models.User{
		UUID:     uuid,
		Username: username,
		Passwd:   "hashed",
	}
	if err := db.Create(&user).Error; err != nil {
		t.Fatalf("failed to create tenant test user %s: %v", username, err)
	}
	return user
}

func TestCreateTenantAndListMembers(t *testing.T) {
	db := setupTenantTestDB(t, &models.User{}, &models.Tenant{}, &models.TenantMember{})

	user := createTenantTestUser(t, db, "user-1", "owner")

	tenant, err := createTenantWithDB(db, "Example Workspace", "", "demo", user.UUID)
	if err != nil {
		t.Fatalf("failed to create tenant: %v", err)
	}
	if tenant.Slug != "example-workspace" {
		t.Fatalf("expected normalized slug, got %q", tenant.Slug)
	}

	members, err := listTenantMembersWithDB(db, tenant.ID)
	if err != nil {
		t.Fatalf("failed to list tenant members: %v", err)
	}
	if len(members) != 1 {
		t.Fatalf("expected one tenant member, got %d", len(members))
	}
	if members[0].UserUUID != user.UUID || members[0].Role != RoleOwner {
		t.Fatalf("expected owner membership, got %+v", members[0])
	}
}

func TestLeaveTenantWithDBMovesSessionToFallbackTenant(t *testing.T) {
	db := setupTenantTestDB(t, &models.User{}, &models.Tenant{}, &models.TenantMember{}, &models.Session{})

	user := createTenantTestUser(t, db, "user-owner", "owner")
	coOwner := createTenantTestUser(t, db, "user-co-owner", "co-owner")
	currentTenant, err := createTenantWithDB(db, "Alpha Workspace", "", "", user.UUID)
	if err != nil {
		t.Fatalf("failed to create current tenant: %v", err)
	}
	fallbackTenant, err := createTenantWithDB(db, "Beta Workspace", "", "", user.UUID)
	if err != nil {
		t.Fatalf("failed to create fallback tenant: %v", err)
	}
	if err := ensureTenantMemberTx(db, currentTenant.ID, coOwner.UUID, RoleOwner); err != nil {
		t.Fatalf("failed to add co-owner to current tenant: %v", err)
	}

	session := models.Session{
		UUID:            user.UUID,
		Session:         "session-1",
		CurrentTenantID: currentTenant.ID,
		Expires:         models.FromTime(time.Now().Add(time.Hour)),
	}
	if err := db.Create(&session).Error; err != nil {
		t.Fatalf("failed to create session: %v", err)
	}

	nextTenant, err := leaveTenantWithDB(db, currentTenant.ID, user.UUID)
	if err != nil {
		t.Fatalf("leaveTenantWithDB returned error: %v", err)
	}
	if nextTenant == nil || nextTenant.ID != fallbackTenant.ID {
		t.Fatalf("expected fallback tenant %q, got %+v", fallbackTenant.ID, nextTenant)
	}

	if _, err := getTenantMemberWithDB(db, currentTenant.ID, user.UUID); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("expected current tenant membership to be deleted, got %v", err)
	}

	var updatedSession models.Session
	if err := db.Where("session = ?", session.Session).First(&updatedSession).Error; err != nil {
		t.Fatalf("failed to reload session: %v", err)
	}
	if updatedSession.CurrentTenantID != fallbackTenant.ID {
		t.Fatalf("expected session current tenant to switch to fallback tenant, got %q", updatedSession.CurrentTenantID)
	}
}

func TestLeaveTenantWithDBRejectsLastAccessibleTenant(t *testing.T) {
	db := setupTenantTestDB(t, &models.User{}, &models.Tenant{}, &models.TenantMember{}, &models.Session{})

	owner := createTenantTestUser(t, db, "user-owner", "owner")
	member := createTenantTestUser(t, db, "user-member", "member")
	tenant, err := createTenantWithDB(db, "Solo Workspace", "", "", owner.UUID)
	if err != nil {
		t.Fatalf("failed to create tenant: %v", err)
	}
	if err := ensureTenantMemberTx(db, tenant.ID, member.UUID, RoleAdmin); err != nil {
		t.Fatalf("failed to add secondary member: %v", err)
	}

	_, err = leaveTenantWithDB(db, tenant.ID, member.UUID)
	if !errors.Is(err, ErrCannotLeaveLastAccessibleTenant) {
		t.Fatalf("expected ErrCannotLeaveLastAccessibleTenant, got %v", err)
	}
}

func TestTransferTenantOwnershipWithDBPromotesTargetAndDemotesSource(t *testing.T) {
	db := setupTenantTestDB(t, &models.User{}, &models.Tenant{}, &models.TenantMember{})

	owner := createTenantTestUser(t, db, "user-owner", "owner")
	target := createTenantTestUser(t, db, "user-target", "target")
	tenant, err := createTenantWithDB(db, "Transfer Workspace", "", "", owner.UUID)
	if err != nil {
		t.Fatalf("failed to create tenant: %v", err)
	}
	if err := ensureTenantMemberTx(db, tenant.ID, target.UUID, RoleAdmin); err != nil {
		t.Fatalf("failed to add target member: %v", err)
	}

	if err := transferTenantOwnershipWithDB(db, tenant.ID, owner.UUID, target.UUID); err != nil {
		t.Fatalf("transferTenantOwnershipWithDB returned error: %v", err)
	}

	updatedOwner, err := getTenantMemberWithDB(db, tenant.ID, owner.UUID)
	if err != nil {
		t.Fatalf("failed to reload current owner membership: %v", err)
	}
	if updatedOwner.Role != RoleAdmin {
		t.Fatalf("expected former owner to be demoted to admin, got %q", updatedOwner.Role)
	}

	updatedTarget, err := getTenantMemberWithDB(db, tenant.ID, target.UUID)
	if err != nil {
		t.Fatalf("failed to reload target membership: %v", err)
	}
	if updatedTarget.Role != RoleOwner {
		t.Fatalf("expected target user to become owner, got %q", updatedTarget.Role)
	}
}

func TestDeleteTenantWithDBCleansTenantScopedDataAndSessions(t *testing.T) {
	db := setupTenantTestDB(
		t,
		&models.User{},
		&models.Tenant{},
		&models.TenantMember{},
		&models.Session{},
		&models.Client{},
		&models.OfflineNotification{},
		&models.Clipboard{},
		&models.PingTask{},
		&models.LoadNotification{},
		&models.Task{},
		&models.TaskResult{},
		&models.Log{},
		&models.CloudProvider{},
		&models.CloudInstanceShare{},
		&models.ThemeConfiguration{},
		&config.TenantConfigItem{},
	)

	user := createTenantTestUser(t, db, "user-owner", "owner")
	doomedTenant, err := createTenantWithDB(db, "Delete Workspace", "", "", user.UUID)
	if err != nil {
		t.Fatalf("failed to create tenant to delete: %v", err)
	}
	fallbackTenant, err := createTenantWithDB(db, "Fallback Workspace", "", "", user.UUID)
	if err != nil {
		t.Fatalf("failed to create fallback tenant: %v", err)
	}

	session := models.Session{
		UUID:            user.UUID,
		Session:         "session-delete",
		CurrentTenantID: doomedTenant.ID,
		Expires:         models.FromTime(time.Now().Add(time.Hour)),
	}
	if err := db.Create(&session).Error; err != nil {
		t.Fatalf("failed to create session: %v", err)
	}

	now := models.FromTime(time.Now())
	client := models.Client{
		UUID:      "client-1",
		Token:     "token-1",
		TenantID:  doomedTenant.ID,
		Name:      "Tenant Client",
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := db.Create(&client).Error; err != nil {
		t.Fatalf("failed to create tenant client: %v", err)
	}
	if err := db.Create(&models.OfflineNotification{Client: client.UUID, Enable: true}).Error; err != nil {
		t.Fatalf("failed to create offline notification: %v", err)
	}
	if err := db.Create(&models.Clipboard{TenantID: doomedTenant.ID, Name: "Script", Text: "echo test"}).Error; err != nil {
		t.Fatalf("failed to create clipboard: %v", err)
	}
	if err := db.Create(&models.PingTask{TenantID: doomedTenant.ID, Name: "Ping", Target: "1.1.1.1", Interval: 60}).Error; err != nil {
		t.Fatalf("failed to create ping task: %v", err)
	}
	if err := db.Create(&models.LoadNotification{TenantID: doomedTenant.ID, Name: "CPU", Metric: "cpu", Threshold: 90, Ratio: 0.9, Interval: 15}).Error; err != nil {
		t.Fatalf("failed to create load notification: %v", err)
	}
	if err := db.Create(&models.Task{TenantID: doomedTenant.ID, TaskId: "task-1", Clients: models.StringArray{client.UUID}}).Error; err != nil {
		t.Fatalf("failed to create task: %v", err)
	}
	if err := db.Create(&models.TaskResult{TenantID: doomedTenant.ID, TaskId: "task-1", Client: client.UUID}).Error; err != nil {
		t.Fatalf("failed to create task result: %v", err)
	}
	if err := db.Create(&models.Log{TenantID: doomedTenant.ID, UUID: user.UUID, Message: "test", MsgType: "info"}).Error; err != nil {
		t.Fatalf("failed to create log: %v", err)
	}
	if err := db.Create(&models.CloudProvider{TenantID: doomedTenant.ID, Name: "cloudflare", Addition: "{}"}).Error; err != nil {
		t.Fatalf("failed to create cloud provider: %v", err)
	}
	if err := db.Create(&models.CloudInstanceShare{
		TenantID:     doomedTenant.ID,
		ShareToken:   "share-token",
		Provider:     "digitalocean",
		ResourceType: "droplet",
		ResourceID:   "123",
	}).Error; err != nil {
		t.Fatalf("failed to create cloud instance share: %v", err)
	}
	if err := db.Create(&models.ThemeConfiguration{TenantID: doomedTenant.ID, Short: "default", Data: `{"accent":"red"}`}).Error; err != nil {
		t.Fatalf("failed to create theme configuration: %v", err)
	}
	if err := db.Create(&config.TenantConfigItem{TenantID: doomedTenant.ID, Key: config.SitenameKey, Value: `"Tenant"`}).Error; err != nil {
		t.Fatalf("failed to create tenant config item: %v", err)
	}

	if err := db.Create(&models.CloudProvider{TenantID: fallbackTenant.ID, Name: "aliyun", Addition: "{}"}).Error; err != nil {
		t.Fatalf("failed to create fallback tenant cloud provider: %v", err)
	}

	if err := deleteTenantWithDB(db, doomedTenant.ID); err != nil {
		t.Fatalf("deleteTenantWithDB returned error: %v", err)
	}

	assertCount := func(model interface{}, where string, args ...interface{}) {
		t.Helper()
		var count int64
		if err := db.Model(model).Where(where, args...).Count(&count).Error; err != nil {
			t.Fatalf("failed to count %T: %v", model, err)
		}
		if count != 0 {
			t.Fatalf("expected %T cleanup for %q, found %d rows", model, where, count)
		}
	}

	assertCount(&models.Tenant{}, "id = ?", doomedTenant.ID)
	assertCount(&models.TenantMember{}, "tenant_id = ?", doomedTenant.ID)
	assertCount(&models.Client{}, "tenant_id = ?", doomedTenant.ID)
	assertCount(&models.OfflineNotification{}, "client = ?", client.UUID)
	assertCount(&models.Clipboard{}, "tenant_id = ?", doomedTenant.ID)
	assertCount(&models.PingTask{}, "tenant_id = ?", doomedTenant.ID)
	assertCount(&models.LoadNotification{}, "tenant_id = ?", doomedTenant.ID)
	assertCount(&models.Task{}, "tenant_id = ?", doomedTenant.ID)
	assertCount(&models.TaskResult{}, "tenant_id = ?", doomedTenant.ID)
	assertCount(&models.Log{}, "tenant_id = ?", doomedTenant.ID)
	assertCount(&models.CloudProvider{}, "tenant_id = ? AND name = ?", doomedTenant.ID, "cloudflare")
	assertCount(&models.CloudInstanceShare{}, "tenant_id = ?", doomedTenant.ID)
	assertCount(&models.ThemeConfiguration{}, "tenant_id = ?", doomedTenant.ID)
	assertCount(&config.TenantConfigItem{}, "tenant_id = ?", doomedTenant.ID)

	var updatedSession models.Session
	if err := db.Where("session = ?", session.Session).First(&updatedSession).Error; err != nil {
		t.Fatalf("failed to reload session: %v", err)
	}
	if updatedSession.CurrentTenantID != fallbackTenant.ID {
		t.Fatalf("expected session fallback tenant %q, got %q", fallbackTenant.ID, updatedSession.CurrentTenantID)
	}

	var fallbackProviderCount int64
	if err := db.Model(&models.CloudProvider{}).
		Where("tenant_id = ? AND name = ?", fallbackTenant.ID, "aliyun").
		Count(&fallbackProviderCount).Error; err != nil {
		t.Fatalf("failed to count fallback tenant provider: %v", err)
	}
	if fallbackProviderCount != 1 {
		t.Fatalf("expected fallback tenant data to remain, found %d rows", fallbackProviderCount)
	}
}

func TestDeleteTenantWithDBRejectsDefaultTenant(t *testing.T) {
	db := setupTenantTestDB(t, &models.Tenant{}, &models.TenantMember{})

	defaultTenant := models.Tenant{
		ID:        "tenant-default",
		Slug:      DefaultTenantSlug,
		Name:      DefaultTenantName,
		IsDefault: true,
	}
	if err := db.Create(&defaultTenant).Error; err != nil {
		t.Fatalf("failed to create default tenant: %v", err)
	}

	err := deleteTenantWithDB(db, defaultTenant.ID)
	if !errors.Is(err, ErrCannotDeleteDefaultTenant) {
		t.Fatalf("expected ErrCannotDeleteDefaultTenant, got %v", err)
	}
}
