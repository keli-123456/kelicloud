package api

import (
	"os"
	"path/filepath"
	"sync"

	"github.com/komari-monitor/komari/cmd/flags"
)

var (
	apiTestDBPath = filepath.Join(os.TempDir(), "komari-api-tests.db")
	apiTestDBOnce sync.Once
)

func configureAPITestDB() {
	apiTestDBOnce.Do(func() {
		_ = os.Remove(apiTestDBPath)
	})
	flags.DatabaseType = "sqlite"
	flags.DatabaseFile = apiTestDBPath
}
