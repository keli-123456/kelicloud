package failover

import (
	"testing"
	"time"

	"github.com/komari-monitor/komari/database/models"
)

func TestFailoverShareIsUserScoped(t *testing.T) {
	db := openFailoverTestDB(t)
	if err := db.AutoMigrate(&models.FailoverShare{}); err != nil {
		t.Fatalf("failed to migrate share schema: %v", err)
	}

	shareA := &models.FailoverShare{
		UserID:     "user-a",
		ShareToken: "token-a",
		TaskID:     1001,
	}
	shareB := &models.FailoverShare{
		UserID:     "user-b",
		ShareToken: "token-b",
		TaskID:     1001,
	}
	if err := saveShareWithDB(db, shareA); err != nil {
		t.Fatalf("failed to save first share: %v", err)
	}
	if err := saveShareWithDB(db, shareB); err != nil {
		t.Fatalf("failed to save second share: %v", err)
	}

	loadedA, err := getShareByTaskForUserWithDB(db, "user-a", 1001)
	if err != nil {
		t.Fatalf("failed to load scoped share for user a: %v", err)
	}
	if loadedA.ShareToken != "token-a" {
		t.Fatalf("expected token-a, got %q", loadedA.ShareToken)
	}

	loadedB, err := getShareByTaskForUserWithDB(db, "user-b", 1001)
	if err != nil {
		t.Fatalf("failed to load scoped share for user b: %v", err)
	}
	if loadedB.ShareToken != "token-b" {
		t.Fatalf("expected token-b, got %q", loadedB.ShareToken)
	}

	tokenLookup, err := getShareByTokenWithDB(db, "token-b")
	if err != nil {
		t.Fatalf("failed to load share by token: %v", err)
	}
	if tokenLookup.UserID != "user-b" {
		t.Fatalf("expected token lookup to belong to user-b, got %q", tokenLookup.UserID)
	}
}

func TestRecordFailoverShareSingleUseConsumesOnce(t *testing.T) {
	db := openFailoverTestDB(t)
	if err := db.AutoMigrate(&models.FailoverShare{}); err != nil {
		t.Fatalf("failed to migrate share schema: %v", err)
	}

	share := &models.FailoverShare{
		UserID:       "user-a",
		ShareToken:   "token-a",
		TaskID:       1001,
		AccessPolicy: "single_use",
	}
	if err := saveShareWithDB(db, share); err != nil {
		t.Fatalf("failed to save share: %v", err)
	}

	firstAccess := time.Date(2026, time.May, 8, 10, 0, 0, 0, time.UTC)
	recorded, err := recordShareAccessWithDB(db, share, true, firstAccess)
	if err != nil {
		t.Fatalf("failed to record first access: %v", err)
	}
	if !recorded {
		t.Fatal("expected first single-use access to be recorded")
	}

	secondAccess := firstAccess.Add(time.Minute)
	recorded, err = recordShareAccessWithDB(db, share, true, secondAccess)
	if err != nil {
		t.Fatalf("failed to record second access: %v", err)
	}
	if recorded {
		t.Fatal("expected second single-use access to be rejected")
	}

	loaded, err := getShareByTokenWithDB(db, "token-a")
	if err != nil {
		t.Fatalf("failed to reload share: %v", err)
	}
	if loaded.AccessCount != 1 {
		t.Fatalf("expected access count 1, got %d", loaded.AccessCount)
	}
	if loaded.ConsumedAt == nil || !loaded.ConsumedAt.Equal(firstAccess) {
		t.Fatalf("expected consumed_at to be first access time, got %v", loaded.ConsumedAt)
	}
}
