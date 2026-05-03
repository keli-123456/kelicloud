package jsonRpc

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
	jsonRPCTestDBPath = filepath.Join(os.TempDir(), fmt.Sprintf("komari-jsonrpc-tests-%d.db", os.Getpid()))
	jsonRPCTestDBOnce sync.Once
	jsonRPCTestDB     *gorm.DB
)

func configureJSONRPCTestDB() *gorm.DB {
	jsonRPCTestDBOnce.Do(func() {
		_ = os.Remove(jsonRPCTestDBPath)
		flags.DatabaseType = "sqlite"
		flags.DatabaseFile = jsonRPCTestDBPath
		jsonRPCTestDB = dbcore.GetDBInstance()
	})
	return jsonRPCTestDB
}

func TestMain(m *testing.M) {
	code := m.Run()
	if jsonRPCTestDB != nil {
		if sqlDB, err := jsonRPCTestDB.DB(); err == nil {
			_ = sqlDB.Close()
		}
	}
	_ = os.Remove(jsonRPCTestDBPath)
	os.Exit(code)
}
