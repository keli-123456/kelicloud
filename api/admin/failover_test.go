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
