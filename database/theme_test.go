package database

import (
	"fmt"
	"testing"

	"github.com/komari-monitor/komari/database/models"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestThemeConfigurationIsTenantScoped(t *testing.T) {
	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared", t.Name())
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}

	if err := db.AutoMigrate(&models.ThemeConfiguration{}); err != nil {
		t.Fatalf("failed to migrate theme configuration schema: %v", err)
	}

	if err := upsertThemeConfigurationForTenantWithDB(db, "tenant-a", "default", `{"accent":"red"}`); err != nil {
		t.Fatalf("failed to save tenant A theme configuration: %v", err)
	}
	if err := upsertThemeConfigurationForTenantWithDB(db, "tenant-b", "default", `{"accent":"blue"}`); err != nil {
		t.Fatalf("failed to save tenant B theme configuration: %v", err)
	}

	tenantA, err := getThemeConfigurationByTenantAndShortWithDB(db, "tenant-a", "default")
	if err != nil {
		t.Fatalf("failed to load tenant A theme configuration: %v", err)
	}
	if tenantA.Data != `{"accent":"red"}` {
		t.Fatalf("expected tenant A theme configuration to be isolated, got %s", tenantA.Data)
	}

	tenantB, err := getThemeConfigurationByTenantAndShortWithDB(db, "tenant-b", "default")
	if err != nil {
		t.Fatalf("failed to load tenant B theme configuration: %v", err)
	}
	if tenantB.Data != `{"accent":"blue"}` {
		t.Fatalf("expected tenant B theme configuration to be isolated, got %s", tenantB.Data)
	}
}
