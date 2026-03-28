package admin

import (
	"testing"

	"github.com/komari-monitor/komari/database/models"
)

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
