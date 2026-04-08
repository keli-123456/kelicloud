package failoverv2

import (
	"context"
	"errors"
	"log"
	"strings"
	"sync"
	"time"

	failoverv2db "github.com/komari-monitor/komari/database/failoverv2"
	"github.com/komari-monitor/komari/database/models"
	"gorm.io/gorm"
)

const (
	pendingFailoverV2CleanupBaseBackoff = 2 * time.Minute
	pendingFailoverV2CleanupMaxAttempts = 8
)

var (
	pendingFailoverV2CleanupBatchLimit = 10
	pendingFailoverV2CleanupTimeout    = 25 * time.Second

	pendingFailoverV2CleanupRunMu     sync.Mutex
	pendingFailoverV2CleanupRunActive bool

	activePendingCleanupRunsMu sync.Mutex
	activePendingCleanupRuns   = map[uint]struct{}{}
)

func runPendingFailoverV2CleanupRetries() error {
	if !claimPendingFailoverV2CleanupRun() {
		return nil
	}
	defer releasePendingFailoverV2CleanupRun()

	if recovered, err := failoverv2db.RecoverStaleRunningPendingCleanups(
		time.Now().Add(-2*pendingFailoverV2CleanupTimeout),
		"pending cleanup retry worker was interrupted",
	); err != nil {
		log.Printf("failoverv2: failed to recover stale pending cleanup retries: %v", err)
	} else if recovered > 0 {
		log.Printf("failoverv2: recovered %d stale pending cleanup retry job(s)", recovered)
	}

	items, err := failoverv2db.ListDuePendingCleanups(pendingFailoverV2CleanupBatchLimit, time.Now())
	if err != nil {
		return err
	}

	var firstErr error
	for _, item := range items {
		claimed, claimErr := claimPendingFailoverV2CleanupItemForRetry(item.ID, "pending cleanup retry started by scheduler")
		if claimErr != nil {
			log.Printf("failoverv2: pending cleanup %d claim failed: %v", item.ID, claimErr)
			if firstErr == nil {
				firstErr = claimErr
			}
			continue
		}
		if !claimed {
			continue
		}
		if err := retryPendingFailoverV2Cleanup(item); err != nil {
			log.Printf("failoverv2: pending cleanup %d failed: %v", item.ID, err)
			if firstErr == nil {
				firstErr = err
			}
		}
		releasePendingFailoverV2CleanupItem(item.ID)
	}
	return firstErr
}

func claimPendingFailoverV2CleanupRun() bool {
	pendingFailoverV2CleanupRunMu.Lock()
	defer pendingFailoverV2CleanupRunMu.Unlock()
	if pendingFailoverV2CleanupRunActive {
		return false
	}
	pendingFailoverV2CleanupRunActive = true
	return true
}

func releasePendingFailoverV2CleanupRun() {
	pendingFailoverV2CleanupRunMu.Lock()
	defer pendingFailoverV2CleanupRunMu.Unlock()
	pendingFailoverV2CleanupRunActive = false
}

func claimPendingFailoverV2CleanupItem(cleanupID uint) bool {
	if cleanupID == 0 {
		return false
	}

	activePendingCleanupRunsMu.Lock()
	defer activePendingCleanupRunsMu.Unlock()
	if _, exists := activePendingCleanupRuns[cleanupID]; exists {
		return false
	}
	activePendingCleanupRuns[cleanupID] = struct{}{}
	return true
}

func releasePendingFailoverV2CleanupItem(cleanupID uint) {
	if cleanupID == 0 {
		return
	}

	activePendingCleanupRunsMu.Lock()
	delete(activePendingCleanupRuns, cleanupID)
	activePendingCleanupRunsMu.Unlock()
}

func claimPendingFailoverV2CleanupItemForRetry(cleanupID uint, message string) (bool, error) {
	if !claimPendingFailoverV2CleanupItem(cleanupID) {
		return false, nil
	}
	if err := failoverv2db.MarkPendingCleanupRunning(cleanupID, message); err != nil {
		releasePendingFailoverV2CleanupItem(cleanupID)
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func runStandalonePendingFailoverV2Cleanup(cleanup *oldInstanceCleanup) error {
	if cleanup == nil || cleanup.Cleanup == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), pendingFailoverV2CleanupTimeout)
	defer cancel()
	return cleanup.Cleanup(ctx)
}

func retryPendingFailoverV2Cleanup(item models.FailoverV2PendingCleanup) error {
	attemptCount := item.AttemptCount + 1
	ref := parseJSONMap(item.InstanceRef)
	if len(ref) == 0 {
		err := failoverv2db.MarkPendingCleanupManualReview(
			item.ID,
			attemptCount,
			"saved instance reference is empty; manual cleanup review required",
		)
		if err == nil {
			notifyPendingCleanupManualReviewByID(item.UserID, item.ServiceID, item.ID, "saved instance reference is empty; manual cleanup review required")
		}
		return err
	}

	cleanup, err := failoverV2ResolveOldInstanceCleanupFromRefFunc(item.UserID, ref)
	if err != nil {
		return reschedulePendingFailoverV2Cleanup(item, attemptCount, err)
	}
	if cleanup == nil || cleanup.Cleanup == nil {
		err := failoverv2db.MarkPendingCleanupManualReview(
			item.ID,
			attemptCount,
			"cleanup action is unavailable for the saved instance reference",
		)
		if err == nil {
			notifyPendingCleanupManualReviewByID(item.UserID, item.ServiceID, item.ID, "cleanup action is unavailable for the saved instance reference")
		}
		return err
	}

	if err := runStandalonePendingFailoverV2Cleanup(cleanup); err != nil {
		return reschedulePendingFailoverV2Cleanup(item, attemptCount, err)
	}
	return failoverv2db.MarkPendingCleanupSucceeded(item.ID)
}

func reschedulePendingFailoverV2Cleanup(item models.FailoverV2PendingCleanup, attemptCount int, err error) error {
	message := "pending cleanup retry failed"
	if err != nil && strings.TrimSpace(err.Error()) != "" {
		message = strings.TrimSpace(err.Error())
	}
	if attemptCount >= pendingFailoverV2CleanupMaxAttempts {
		err := failoverv2db.MarkPendingCleanupManualReview(item.ID, attemptCount, message)
		if err == nil {
			notifyPendingCleanupManualReviewByID(item.UserID, item.ServiceID, item.ID, message)
		}
		return err
	}
	return failoverv2db.SchedulePendingCleanupRetry(
		item.ID,
		attemptCount,
		message,
		time.Now().Add(pendingFailoverV2CleanupRetryBackoff(item.Provider, err, attemptCount)),
	)
}

func pendingFailoverV2CleanupRetryBackoff(provider string, err error, attemptCount int) time.Duration {
	if attemptCount <= 1 {
		return pendingFailoverV2CleanupBaseBackoff
	}

	backoff := pendingFailoverV2CleanupBaseBackoff
	for step := 1; step < attemptCount && step < 6; step++ {
		backoff *= 2
	}
	if backoff > 2*time.Hour {
		return 2 * time.Hour
	}
	return backoff
}
