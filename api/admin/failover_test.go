package admin

import (
	"testing"

	"github.com/komari-monitor/komari/database/models"
	"gorm.io/gorm"
)

func setupFailoverV1APITestDB(t *testing.T) *gorm.DB {
	t.Helper()

	db := configureAdminTestDB()
	if err := db.AutoMigrate(
		&models.CloudProvider{},
		&models.Client{},
		&models.FailoverTask{},
		&models.FailoverPlan{},
		&models.FailoverExecution{},
	); err != nil {
		t.Fatalf("failed to migrate failover v1 api test tables: %v", err)
	}

	session := db.Session(&gorm.Session{AllowGlobalUpdate: true})
	for _, model := range []interface{}{
		&models.FailoverExecution{},
		&models.FailoverPlan{},
		&models.FailoverTask{},
		&models.CloudProvider{},
		&models.Client{},
	} {
		if err := session.Delete(model).Error; err != nil {
			t.Fatalf("failed to clear failover v1 api test table: %v", err)
		}
	}

	return db
}

func TestValidateFailoverTaskRequestAllowsMissingCurrentClient(t *testing.T) {
	db := setupFailoverV1APITestDB(t)
	if err := db.Create(&models.CloudProvider{
		UserID: "user-a",
		Name:   digitalOceanProviderName,
		Addition: `{
			"active_token_id": "do-entry",
			"tokens": [{
				"id": "do-entry",
				"name": "DigitalOcean",
				"token": "do-token"
			}]
		}`,
	}).Error; err != nil {
		t.Fatalf("failed to seed cloud provider: %v", err)
	}

	task, plans, err := validateFailoverTaskRequest(ownerScope{UserUUID: "user-a"}, &failoverTaskRequest{
		Name: "Provision later",
		Plans: []failoverPlanRequest{
			{
				Provider:        digitalOceanProviderName,
				ProviderEntryID: "do-entry",
				ActionType:      models.FailoverActionProvisionInstance,
			},
		},
	})
	if err != nil {
		t.Fatalf("expected task validation to allow a missing current client: %v", err)
	}
	if task.WatchClientUUID != "" {
		t.Fatalf("expected watch client to remain empty, got %q", task.WatchClientUUID)
	}
	if task.CurrentAddress != "" {
		t.Fatalf("expected current address to remain empty, got %q", task.CurrentAddress)
	}
	if len(plans) != 1 {
		t.Fatalf("expected one validated plan, got %d", len(plans))
	}
}

func TestNormalizeFailoverDeleteStrategyKeepsRebindPlans(t *testing.T) {
	plans := []models.FailoverPlan{
		{
			Enabled:    true,
			ActionType: models.FailoverActionRebindPublicIP,
		},
	}

	if got := normalizeFailoverDeleteStrategy(models.FailoverDeleteStrategyDeleteAfterSuccess, plans); got != models.FailoverDeleteStrategyKeep {
		t.Fatalf("expected rebind-only plans to keep the old instance, got %q", got)
	}
}

func TestNormalizeFailoverDeleteStrategyForcesProvisionPlansToDelete(t *testing.T) {
	plans := []models.FailoverPlan{
		{
			Enabled:    true,
			ActionType: models.FailoverActionProvisionInstance,
		},
	}

	if got := normalizeFailoverDeleteStrategy(models.FailoverDeleteStrategyKeep, plans); got != models.FailoverDeleteStrategyDeleteAfterSuccess {
		t.Fatalf("expected provision plans to delete old instances by default, got %q", got)
	}
}

func TestNormalizeFailoverDeleteStrategyPreservesDelayedDeletion(t *testing.T) {
	plans := []models.FailoverPlan{
		{
			Enabled:    true,
			ActionType: models.FailoverActionProvisionInstance,
		},
	}

	if got := normalizeFailoverDeleteStrategy(models.FailoverDeleteStrategyDeleteAfterSuccessDelay, plans); got != models.FailoverDeleteStrategyDeleteAfterSuccessDelay {
		t.Fatalf("expected delayed deletion strategy to be preserved, got %q", got)
	}
}

func TestNormalizeFailoverDeleteStrategyIgnoresDisabledProvisionPlans(t *testing.T) {
	plans := []models.FailoverPlan{
		{
			Enabled:    false,
			ActionType: models.FailoverActionProvisionInstance,
		},
		{
			Enabled:    true,
			ActionType: models.FailoverActionRebindPublicIP,
		},
	}

	if got := normalizeFailoverDeleteStrategy("", plans); got != models.FailoverDeleteStrategyKeep {
		t.Fatalf("expected disabled provision plans to be ignored, got %q", got)
	}
}

func TestNormalizeRequestedFailoverScriptClipboardIDsPrefersArray(t *testing.T) {
	legacyID := 5

	got := normalizeRequestedFailoverScriptClipboardIDs(&legacyID, []int{9, 5, 9, 12})
	expected := []int{9, 5, 12}
	if len(got) != len(expected) {
		t.Fatalf("expected %d ids, got %#v", len(expected), got)
	}
	for index := range expected {
		if got[index] != expected[index] {
			t.Fatalf("expected %#v, got %#v", expected, got)
		}
	}
}

func TestNormalizeRequestedFailoverScriptClipboardIDsFallsBackToLegacyField(t *testing.T) {
	legacyID := 5

	got := normalizeRequestedFailoverScriptClipboardIDs(&legacyID, nil)
	if len(got) != 1 || got[0] != legacyID {
		t.Fatalf("expected legacy clipboard id to be preserved, got %#v", got)
	}
}
