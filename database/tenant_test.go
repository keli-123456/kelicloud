package database

import (
	"fmt"
	"testing"

	"github.com/komari-monitor/komari/database/models"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestCreateTenantAndListMembers(t *testing.T) {
	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared", t.Name())
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}

	if err := db.AutoMigrate(&models.User{}, &models.Tenant{}, &models.TenantMember{}); err != nil {
		t.Fatalf("failed to migrate tenant schema: %v", err)
	}

	user := models.User{
		UUID:     "user-1",
		Username: "owner",
		Passwd:   "hashed",
	}
	if err := db.Create(&user).Error; err != nil {
		t.Fatalf("failed to create owner user: %v", err)
	}

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
