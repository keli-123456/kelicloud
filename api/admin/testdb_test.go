package admin

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/komari-monitor/komari/cmd/flags"
	"github.com/komari-monitor/komari/database/dbcore"
	"gorm.io/gorm"
)

var (
	adminTestDBPath = filepath.Join(os.TempDir(), fmt.Sprintf("komari-admin-tests-%d.db", os.Getpid()))
	adminTestDBOnce sync.Once
	adminTestDB     *gorm.DB
)

func configureAdminTestDB() *gorm.DB {
	adminTestDBOnce.Do(func() {
		_ = os.Remove(adminTestDBPath)
		flags.DatabaseType = "sqlite"
		flags.DatabaseFile = adminTestDBPath
		adminTestDB = dbcore.GetDBInstance()
	})
	return adminTestDB
}

func TestMain(m *testing.M) {
	code := m.Run()
	if adminTestDB != nil {
		if sqlDB, err := adminTestDB.DB(); err == nil {
			_ = sqlDB.Close()
		}
	}
	_ = os.Remove(adminTestDBPath)
	os.Exit(code)
}
