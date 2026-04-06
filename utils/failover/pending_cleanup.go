package failover

import (
	"context"
	"errors"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	failoverdb "github.com/komari-monitor/komari/database/failover"
	"github.com/komari-monitor/komari/database/models"
)

const (
	pendingRollbackCleanupBaseBackoff       = 2 * time.Minute
	pendingRollbackCleanupAssessmentBackoff = 30 * time.Minute
	pendingRollbackCleanupMaxAttempts       = 8
)

var (
	pendingRollbackCleanupBatchLimit  = 10
	pendingRollbackCleanupResolveFunc = resolveCurrentInstanceCleanupFromRef

	pendingRollbackCleanupRunMu     sync.Mutex
	pendingRollbackCleanupRunActive bool
)

func runPendingRollbackCleanupRetries() error {
	if !claimPendingRollbackCleanupRun() {
		return nil
	}
	defer releasePendingRollbackCleanupRun()

	items, err := failoverdb.ListDuePendingCleanups(pendingRollbackCleanupBatchLimit, time.Now())
	if err != nil {
		return err
	}

	var firstErr error
	for _, item := range items {
		if err := retryPendingRollbackCleanup(item); err != nil {
			log.Printf("failover: pending rollback cleanup %d failed: %v", item.ID, err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

func claimPendingRollbackCleanupRun() bool {
	pendingRollbackCleanupRunMu.Lock()
	defer pendingRollbackCleanupRunMu.Unlock()
	if pendingRollbackCleanupRunActive {
		return false
	}
	pendingRollbackCleanupRunActive = true
	return true
}

func releasePendingRollbackCleanupRun() {
	pendingRollbackCleanupRunMu.Lock()
	defer pendingRollbackCleanupRunMu.Unlock()
	pendingRollbackCleanupRunActive = false
}

func retryPendingRollbackCleanup(item models.FailoverPendingCleanup) error {
	ref := parseJSONMap(item.InstanceRef)
	attemptCount := item.AttemptCount + 1
	if len(ref) == 0 {
		return failoverdb.MarkPendingCleanupManualReview(
			item.ID,
			attemptCount,
			"saved instance reference is empty; manual cleanup review required",
		)
	}

	cleanup, err := pendingRollbackCleanupResolveFunc(context.Background(), item.UserID, ref)
	if err != nil {
		return reschedulePendingRollbackCleanup(item, attemptCount, err)
	}
	if cleanup == nil {
		return reschedulePendingRollbackCleanup(
			item,
			attemptCount,
			errors.New("cleanup action is unavailable for the saved instance reference"),
		)
	}
	if cleanup.Assessment != nil && cleanup.Cleanup == nil {
		classification := strings.TrimSpace(stringMapValue(cleanup.Assessment.Result, "classification"))
		if classification == cleanupClassificationInstanceMissing {
			return failoverdb.MarkPendingCleanupSucceeded(item.ID)
		}

		message := firstNonEmpty(
			strings.TrimSpace(cleanup.Assessment.StepMessage),
			strings.TrimSpace(stringMapValue(cleanup.Assessment.Result, "summary")),
			"pending cleanup requires manual review",
		)
		if attemptCount >= pendingRollbackCleanupMaxAttempts {
			return failoverdb.MarkPendingCleanupManualReview(item.ID, attemptCount, message)
		}
		return failoverdb.SchedulePendingCleanupRetry(
			item.ID,
			attemptCount,
			message,
			time.Now().Add(pendingRollbackCleanupAssessmentBackoff),
		)
	}
	if cleanup.Cleanup == nil {
		return reschedulePendingRollbackCleanup(
			item,
			attemptCount,
			errors.New("cleanup action is unavailable for the saved instance reference"),
		)
	}

	if err := normalizeExecutionStopError(cleanup.Cleanup(context.Background())); err != nil {
		return reschedulePendingRollbackCleanup(item, attemptCount, err)
	}

	resolvedRef := cleanup.Ref
	if len(resolvedRef) == 0 {
		resolvedRef = ref
	}
	provider := strings.TrimSpace(stringMapValue(resolvedRef, "provider"))
	entryID := strings.TrimSpace(providerEntryIDFromRef(resolvedRef))
	if provider != "" && entryID != "" {
		invalidateProviderEntrySnapshot(item.UserID, provider, entryID)
	}
	return failoverdb.MarkPendingCleanupSucceeded(item.ID)
}

func reschedulePendingRollbackCleanup(item models.FailoverPendingCleanup, attemptCount int, err error) error {
	message := "pending cleanup retry failed"
	if err != nil && strings.TrimSpace(err.Error()) != "" {
		message = strings.TrimSpace(err.Error())
	}
	if attemptCount >= pendingRollbackCleanupMaxAttempts {
		return failoverdb.MarkPendingCleanupManualReview(item.ID, attemptCount, message)
	}
	return failoverdb.SchedulePendingCleanupRetry(
		item.ID,
		attemptCount,
		message,
		time.Now().Add(pendingCleanupRetryBackoff(item.Provider, err, attemptCount)),
	)
}

func pendingCleanupRetryBackoff(provider string, err error, attemptCount int) time.Duration {
	if err != nil {
		decision := classifyProviderFailure(provider, err)
		if decision.Cooldown > 0 {
			return decision.Cooldown
		}
	}

	if attemptCount <= 1 {
		return pendingRollbackCleanupBaseBackoff
	}

	backoff := pendingRollbackCleanupBaseBackoff
	for step := 1; step < attemptCount && step < 6; step++ {
		backoff *= 2
	}
	if backoff > 2*time.Hour {
		return 2 * time.Hour
	}
	return backoff
}

func pendingCleanupIdentityFromRef(ref map[string]interface{}) (string, string, string, string) {
	if len(ref) == 0 {
		return "", "", "", ""
	}

	provider := strings.ToLower(strings.TrimSpace(stringMapValue(ref, "provider")))
	entryID := strings.TrimSpace(providerEntryIDFromRef(ref))
	switch provider {
	case "digitalocean":
		if dropletID := intMapValue(ref, "droplet_id"); dropletID > 0 {
			return provider, "droplet", strconv.Itoa(dropletID), entryID
		}
	case "linode":
		if instanceID := intMapValue(ref, "instance_id"); instanceID > 0 {
			return provider, "instance", strconv.Itoa(instanceID), entryID
		}
	case "aws":
		service := strings.ToLower(strings.TrimSpace(firstNonEmpty(stringMapValue(ref, "service"), "ec2")))
		switch service {
		case "lightsail":
			if instanceName := strings.TrimSpace(stringMapValue(ref, "instance_name")); instanceName != "" {
				return provider, "lightsail_instance", instanceName, entryID
			}
		default:
			if instanceID := strings.TrimSpace(stringMapValue(ref, "instance_id")); instanceID != "" {
				return provider, "ec2_instance", instanceID, entryID
			}
		}
	}

	return "", "", "", ""
}
