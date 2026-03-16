package config

import (
	"fmt"
	"testing"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func setupTenantConfigTestDB(t *testing.T) *gorm.DB {
	t.Helper()

	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared", t.Name())
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}
	SetDb(db)
	return db
}

func TestTenantScopedConfigIsolation(t *testing.T) {
	setupTenantConfigTestDB(t)

	if err := Set(SitenameKey, "Legacy Site"); err != nil {
		t.Fatalf("failed to seed global sitename: %v", err)
	}
	if err := Set(AllowCorsKey, true); err != nil {
		t.Fatalf("failed to seed global allow_cors: %v", err)
	}

	if err := SetForTenant("tenant-a", SitenameKey, "Tenant A"); err != nil {
		t.Fatalf("failed to set tenant A sitename: %v", err)
	}
	if err := SetForTenant("tenant-b", SitenameKey, "Tenant B"); err != nil {
		t.Fatalf("failed to set tenant B sitename: %v", err)
	}

	tenantAName, err := GetAsForTenant[string]("tenant-a", SitenameKey)
	if err != nil {
		t.Fatalf("failed to read tenant A sitename: %v", err)
	}
	if tenantAName != "Tenant A" {
		t.Fatalf("expected tenant A sitename, got %q", tenantAName)
	}

	tenantBName, err := GetAsForTenant[string]("tenant-b", SitenameKey)
	if err != nil {
		t.Fatalf("failed to read tenant B sitename: %v", err)
	}
	if tenantBName != "Tenant B" {
		t.Fatalf("expected tenant B sitename, got %q", tenantBName)
	}

	allowCors, err := GetAsForTenant[bool]("tenant-a", AllowCorsKey)
	if err != nil {
		t.Fatalf("failed to read tenant A global key: %v", err)
	}
	if !allowCors {
		t.Fatal("expected global allow_cors to remain visible through tenant read")
	}

	allTenantA, err := GetAllForTenant("tenant-a")
	if err != nil {
		t.Fatalf("failed to get tenant A config snapshot: %v", err)
	}
	if allTenantA[SitenameKey] != "Tenant A" {
		t.Fatalf("expected tenant A snapshot to use tenant sitename, got %#v", allTenantA[SitenameKey])
	}
}

func TestBackfillTenantScopedConfigs(t *testing.T) {
	setupTenantConfigTestDB(t)

	if err := Set(SitenameKey, "Legacy Site"); err != nil {
		t.Fatalf("failed to seed global sitename: %v", err)
	}
	if err := BackfillTenantScopedConfigs("tenant-default"); err != nil {
		t.Fatalf("failed to backfill tenant config: %v", err)
	}

	name, err := GetAsForTenant[string]("tenant-default", SitenameKey)
	if err != nil {
		t.Fatalf("failed to read backfilled sitename: %v", err)
	}
	if name != "Legacy Site" {
		t.Fatalf("expected backfilled sitename, got %q", name)
	}

	if err := SetForTenant("tenant-default", SitenameKey, "Tenant Override"); err != nil {
		t.Fatalf("failed to override tenant sitename: %v", err)
	}
	if err := BackfillTenantScopedConfigs("tenant-default"); err != nil {
		t.Fatalf("failed to rerun backfill tenant config: %v", err)
	}

	name, err = GetAsForTenant[string]("tenant-default", SitenameKey)
	if err != nil {
		t.Fatalf("failed to read overridden sitename: %v", err)
	}
	if name != "Tenant Override" {
		t.Fatalf("expected tenant override to be preserved, got %q", name)
	}
}

func TestFindTenantIDByConfigValue(t *testing.T) {
	setupTenantConfigTestDB(t)

	if err := SetForTenant("tenant-a", AutoDiscoveryKeyKey, "key-a-123456"); err != nil {
		t.Fatalf("failed to set tenant auto discovery key: %v", err)
	}

	tenantID, err := FindTenantIDByConfigValue(AutoDiscoveryKeyKey, "key-a-123456")
	if err != nil {
		t.Fatalf("failed to resolve tenant by auto discovery key: %v", err)
	}
	if tenantID != "tenant-a" {
		t.Fatalf("expected tenant-a, got %q", tenantID)
	}
}
