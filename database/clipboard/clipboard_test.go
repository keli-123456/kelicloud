package clipboard

import (
	"fmt"
	"strings"
	"testing"

	"github.com/komari-monitor/komari/database/models"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func openClipboardTestDB(t *testing.T) *gorm.DB {
	t.Helper()

	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared", strings.ReplaceAll(t.Name(), "/", "_"))
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}
	return db
}

func TestUserScopedClipboardQueriesStayWithinUser(t *testing.T) {
	db := openClipboardTestDB(t)

	if err := db.AutoMigrate(&models.Clipboard{}); err != nil {
		t.Fatalf("failed to migrate test schema: %v", err)
	}

	userEntry := models.Clipboard{UserID: "user-a", Name: "Personal", Text: "echo personal"}
	secondUserEntry := models.Clipboard{UserID: "user-a", Name: "Personal B", Text: "echo personal b"}
	otherUserEntry := models.Clipboard{UserID: "user-b", Name: "Other", Text: "echo other"}

	for _, item := range []*models.Clipboard{&userEntry, &secondUserEntry, &otherUserEntry} {
		if err := db.Create(item).Error; err != nil {
			t.Fatalf("failed to create clipboard entry: %v", err)
		}
	}

	got, err := getClipboardByIDForUserWithDB(db, userEntry.Id, "user-a")
	if err != nil {
		t.Fatalf("failed to load user entry: %v", err)
	}
	if got.Name != "Personal" {
		t.Fatalf("expected personal entry, got %q", got.Name)
	}

	if _, err := getClipboardByIDForUserWithDB(db, otherUserEntry.Id, "user-a"); err == nil {
		t.Fatal("expected cross-user clipboard lookup to fail")
	}

	list, err := listClipboardByUserWithDB(db, "user-a")
	if err != nil {
		t.Fatalf("failed to list clipboard entries: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("expected both personal entries, got %+v", list)
	}

	if err := updateClipboardFieldsForUserWithDB(db, otherUserEntry.Id, "user-a", map[string]interface{}{"name": "Updated Other"}); err == nil {
		t.Fatal("expected cross-user clipboard update to fail")
	}

	if err := deleteClipboardForUserWithDB(db, userEntry.Id, "user-a"); err != nil {
		t.Fatalf("failed to delete personal entry: %v", err)
	}
	if _, err := getClipboardByIDForUserWithDB(db, userEntry.Id, "user-a"); err == nil {
		t.Fatal("expected deleted user entry to be gone")
	}

	remaining, err := listClipboardByUserWithDB(db, "user-a")
	if err != nil {
		t.Fatalf("failed to list remaining clipboard entries: %v", err)
	}
	if len(remaining) != 1 || remaining[0].Name != "Personal B" {
		t.Fatalf("expected second personal entry to remain, got %+v", remaining)
	}
}

func TestCreateClipboardForUserRequiresUser(t *testing.T) {
	err := CreateClipboardForUser("", &models.Clipboard{Name: "Loose", Text: "echo loose"})
	if err == nil {
		t.Fatal("expected user-scoped clipboard create to require user id")
	}
}
