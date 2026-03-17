package tasks

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/komari-monitor/komari/database/models"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func openPingTaskTestDB(t *testing.T) *gorm.DB {
	t.Helper()

	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared", strings.ReplaceAll(t.Name(), "/", "_"))
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}
	return db
}

func TestPingQueriesAreStrictlyUserScoped(t *testing.T) {
	db := openPingTaskTestDB(t)

	if err := db.AutoMigrate(&models.Client{}, &models.PingTask{}, &models.PingRecord{}); err != nil {
		t.Fatalf("failed to migrate test schema: %v", err)
	}

	now := models.FromTime(time.Now())
	start := time.Unix(0, 0)
	end := time.Now().Add(24 * time.Hour)

	legacyClient := models.Client{UUID: "legacy-client", Token: "legacy-token", Name: "Legacy", CreatedAt: now, UpdatedAt: now}
	userClient := models.Client{UUID: "user-client", Token: "user-token", UserID: "user-a", Name: "User", CreatedAt: now, UpdatedAt: now}
	userSecondClient := models.Client{UUID: "user-client-b", Token: "user-token-b", UserID: "user-a", Name: "User B", CreatedAt: now, UpdatedAt: now}
	otherClient := models.Client{UUID: "other-client", Token: "other-token", UserID: "user-b", Name: "Other", CreatedAt: now, UpdatedAt: now}
	for _, client := range []models.Client{legacyClient, userClient, userSecondClient, otherClient} {
		if err := db.Create(&client).Error; err != nil {
			t.Fatalf("failed to create client %s: %v", client.UUID, err)
		}
	}
	legacyTask := models.PingTask{Name: "Legacy Task", Clients: models.StringArray{legacyClient.UUID}, Type: "icmp", Target: "1.1.1.1", Interval: 60}
	userTask := models.PingTask{UserID: "user-a", Name: "User Task", Clients: models.StringArray{userClient.UUID}, Type: "icmp", Target: "8.8.8.8", Interval: 60}
	userSecondTask := models.PingTask{UserID: "user-a", Name: "User Task B", Clients: models.StringArray{userSecondClient.UUID}, Type: "icmp", Target: "4.4.4.4", Interval: 60}
	otherTask := models.PingTask{UserID: "user-b", Name: "Other Task", Clients: models.StringArray{otherClient.UUID}, Type: "icmp", Target: "9.9.9.9", Interval: 60}
	for _, task := range []models.PingTask{legacyTask, userTask, userSecondTask, otherTask} {
		if err := db.Create(&task).Error; err != nil {
			t.Fatalf("failed to create ping task %s: %v", task.Name, err)
		}
	}

	var createdTasks []models.PingTask
	if err := db.Order("id asc").Find(&createdTasks).Error; err != nil {
		t.Fatalf("failed to reload ping tasks: %v", err)
	}

	records := []models.PingRecord{
		{TaskId: createdTasks[0].Id, Client: legacyClient.UUID, Time: now, Value: 10},
		{TaskId: createdTasks[1].Id, Client: userClient.UUID, Time: now, Value: 20},
		{TaskId: createdTasks[2].Id, Client: userSecondClient.UUID, Time: now, Value: 25},
		{TaskId: createdTasks[3].Id, Client: otherClient.UUID, Time: now, Value: 30},
	}
	if err := db.Create(&records).Error; err != nil {
		t.Fatalf("failed to create ping records: %v", err)
	}

	userTasks, err := getAllPingTasksByUserWithDB(db, "user-a")
	if err != nil {
		t.Fatalf("failed to load user-scoped ping tasks: %v", err)
	}
	if len(userTasks) != 2 {
		t.Fatalf("expected both personal ping tasks across tenants, got %+v", userTasks)
	}
	for _, task := range userTasks {
		if task.UserID != "user-a" {
			t.Fatalf("expected only user-a tasks, got %+v", userTasks)
		}
	}

	var legacyTasks []models.PingTask
	if err := db.Where("COALESCE(user_id, '') = ''").Find(&legacyTasks).Error; err != nil {
		t.Fatalf("failed to load legacy ping tasks: %v", err)
	}
	if len(legacyTasks) != 1 || legacyTasks[0].Name != "Legacy Task" {
		t.Fatalf("expected legacy scope to expose only unowned ping tasks, got %+v", legacyTasks)
	}

	userRecords, err := getPingRecordsByUserWithDB(db, "user-a", "", -1, start, end)
	if err != nil {
		t.Fatalf("failed to load user-scoped ping records: %v", err)
	}
	if len(userRecords) != 2 {
		t.Fatalf("expected personal ping records across tenants, got %+v", userRecords)
	}
	for _, record := range userRecords {
		if record.Client == otherClient.UUID || record.Client == legacyClient.UUID {
			t.Fatalf("expected non-user ping records to be excluded, got %+v", userRecords)
		}
	}
}

func TestNormalizePingTaskOwnerScopePrefersUser(t *testing.T) {
	userUUID, tenantID, err := normalizePingTaskOwnerScope("user-a", "tenant-a")
	if err != nil {
		t.Fatalf("normalizePingTaskOwnerScope returned error: %v", err)
	}
	if userUUID != "user-a" || tenantID != "" {
		t.Fatalf("expected user scope to clear tenant, got user=%q tenant=%q", userUUID, tenantID)
	}
}
