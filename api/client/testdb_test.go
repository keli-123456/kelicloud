package client

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/komari-monitor/komari/cmd/flags"
	"github.com/komari-monitor/komari/database/dbcore"
	"gorm.io/gorm"
)

var (
	clientTestDBPath = filepath.Join(os.TempDir(), "komari-api-client-tests.db")
	clientTestDBOnce sync.Once
)

func configureClientTestDB(t *testing.T) *gorm.DB {
	t.Helper()

	clientTestDBOnce.Do(func() {
		_ = os.Remove(clientTestDBPath)
	})

	flags.DatabaseType = "sqlite"
	flags.DatabaseFile = clientTestDBPath
	return dbcore.GetDBInstance()
}
