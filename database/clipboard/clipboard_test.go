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

func TestListClipboardPageByUserRespectsPaginationAndOrder(t *testing.T) {
	db := openClipboardTestDB(t)

	if err := db.AutoMigrate(&models.Clipboard{}); err != nil {
		t.Fatalf("failed to migrate test schema: %v", err)
	}

	entries := []models.Clipboard{
		{UserID: "user-a", Name: "Weight 10", Text: "echo a", Weight: 10},
		{UserID: "user-a", Name: "Weight 7", Text: "echo b", Weight: 7},
		{UserID: "user-a", Name: "Weight 3", Text: "echo c", Weight: 3},
		{UserID: "user-b", Name: "Other User", Text: "echo other", Weight: 99},
	}
	for i := range entries {
		if err := db.Create(&entries[i]).Error; err != nil {
			t.Fatalf("failed to create clipboard entry: %v", err)
		}
	}

	pageOne, total, err := listClipboardPageByUserWithDB(db, "user-a", 1, 2, "")
	if err != nil {
		t.Fatalf("failed to list first page: %v", err)
	}
	if total != 3 {
		t.Fatalf("expected total 3 for user-a, got %d", total)
	}
	if len(pageOne) != 2 {
		t.Fatalf("expected 2 entries on first page, got %d", len(pageOne))
	}
	if pageOne[0].Name != "Weight 10" || pageOne[1].Name != "Weight 7" {
		t.Fatalf("unexpected first page order: %+v", pageOne)
	}

	pageTwo, total, err := listClipboardPageByUserWithDB(db, "user-a", 2, 2, "")
	if err != nil {
		t.Fatalf("failed to list second page: %v", err)
	}
	if total != 3 {
		t.Fatalf("expected total 3 for user-a on second page, got %d", total)
	}
	if len(pageTwo) != 1 || pageTwo[0].Name != "Weight 3" {
		t.Fatalf("unexpected second page contents: %+v", pageTwo)
	}
}

func TestListClipboardPageByUserSearchesByTokenAndKeepsStableOrderAfterEdit(t *testing.T) {
	db := openClipboardTestDB(t)

	if err := db.AutoMigrate(&models.Clipboard{}); err != nil {
		t.Fatalf("failed to migrate test schema: %v", err)
	}

	older := models.Clipboard{
		UserID: "user-a",
		Name:   "Singapore Bootstrap",
		Text:   "systemctl restart komari-agent",
		Remark: "sg1 edge rollout",
		Weight: 5,
	}
	newer := models.Clipboard{
		UserID: "user-a",
		Name:   "Tokyo Bootstrap",
		Text:   "curl -fsSL https://example.com/install.sh | bash",
		Remark: "jp1 edge rollout",
		Weight: 5,
	}

	for _, item := range []*models.Clipboard{&older, &newer} {
		if err := db.Create(item).Error; err != nil {
			t.Fatalf("failed to create clipboard entry: %v", err)
		}
	}

	searchResult, total, err := listClipboardPageByUserWithDB(db, "user-a", 1, 20, "sg1 rollout")
	if err != nil {
		t.Fatalf("failed to search clipboard entries: %v", err)
	}
	if total != 1 {
		t.Fatalf("expected exactly one token search match, got %d", total)
	}
	if len(searchResult) != 1 || searchResult[0].Id != older.Id {
		t.Fatalf("unexpected token search result: %+v", searchResult)
	}

	if err := updateClipboardFieldsForUserWithDB(db, older.Id, "user-a", map[string]interface{}{
		"remark": "sg1 edge rollout updated",
	}); err != nil {
		t.Fatalf("failed to update clipboard entry: %v", err)
	}

	page, total, err := listClipboardPageByUserWithDB(db, "user-a", 1, 20, "")
	if err != nil {
		t.Fatalf("failed to list clipboard entries after update: %v", err)
	}
	if total != 2 {
		t.Fatalf("expected total 2 after update, got %d", total)
	}
	if len(page) != 2 {
		t.Fatalf("expected 2 entries after update, got %d", len(page))
	}
	if page[0].Id != newer.Id || page[1].Id != older.Id {
		t.Fatalf("expected stable id-based order after edit, got %+v", page)
	}
}

func TestListClipboardPageByUserSearchDoesNotMatchCharacterSequenceOnly(t *testing.T) {
	db := openClipboardTestDB(t)

	if err := db.AutoMigrate(&models.Clipboard{}); err != nil {
		t.Fatalf("failed to migrate test schema: %v", err)
	}

	exact := models.Clipboard{
		UserID: "user-a",
		Name:   "Singapore Bootstrap",
		Text:   "echo deploy",
		Remark: "sg1 edge rollout",
		Weight: 5,
	}
	falsePositive := models.Clipboard{
		UserID: "user-a",
		Name:   "Staging Zone 1",
		Text:   "echo staging",
		Remark: "regional bootstrap",
		Weight: 4,
	}

	for _, item := range []*models.Clipboard{&exact, &falsePositive} {
		if err := db.Create(item).Error; err != nil {
			t.Fatalf("failed to create clipboard entry: %v", err)
		}
	}

	searchResult, total, err := listClipboardPageByUserWithDB(db, "user-a", 1, 20, "sg1")
	if err != nil {
		t.Fatalf("failed to search clipboard entries: %v", err)
	}
	if total != 1 {
		t.Fatalf("expected exact token match only, got %d", total)
	}
	if len(searchResult) != 1 || searchResult[0].Id != exact.Id {
		t.Fatalf("unexpected exact token search result: %+v", searchResult)
	}
}

func TestListClipboardPageByUserSearchTreatsLikeWildcardsLiterally(t *testing.T) {
	db := openClipboardTestDB(t)

	if err := db.AutoMigrate(&models.Clipboard{}); err != nil {
		t.Fatalf("failed to migrate test schema: %v", err)
	}

	items := []models.Clipboard{
		{
			UserID: "user-a",
			Name:   "100% rollout_script",
			Text:   "echo percent underscore",
			Remark: "literal wildcard sample",
			Weight: 3,
		},
		{
			UserID: "user-a",
			Name:   "100 rollout script",
			Text:   "echo plain text",
			Remark: "control sample",
			Weight: 2,
		},
	}
	for i := range items {
		if err := db.Create(&items[i]).Error; err != nil {
			t.Fatalf("failed to create clipboard entry: %v", err)
		}
	}

	searchResult, total, err := listClipboardPageByUserWithDB(db, "user-a", 1, 20, "% _")
	if err != nil {
		t.Fatalf("failed to search clipboard entries with literal wildcard characters: %v", err)
	}
	if total != 1 {
		t.Fatalf("expected exactly one literal wildcard search match, got %d", total)
	}
	if len(searchResult) != 1 || searchResult[0].Id != items[0].Id {
		t.Fatalf("unexpected literal wildcard search result: %+v", searchResult)
	}
}
