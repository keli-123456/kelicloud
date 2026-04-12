package failoverv2

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestClaimDNSProviderEntryRunLockWaitsUntilReleased(t *testing.T) {
	configureFailoverV2RunnerTestDB(t)

	service, _ := createTestRunnerServiceAndMember(t)

	firstLock, err := claimDNSProviderEntryRunLock(context.Background(), "user-a", service)
	if err != nil {
		t.Fatalf("failed to claim first dns api lock: %v", err)
	}
	if firstLock == nil {
		t.Fatal("expected first dns api lock handle")
	}
	defer firstLock.release()

	type lockResult struct {
		lock *failoverV2RunLockHandle
		err  error
	}
	resultCh := make(chan lockResult, 1)

	waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go func() {
		lock, lockErr := claimDNSProviderEntryRunLock(waitCtx, "user-a", service)
		resultCh <- lockResult{lock: lock, err: lockErr}
	}()

	select {
	case result := <-resultCh:
		if result.lock != nil {
			result.lock.release()
		}
		t.Fatalf("expected second dns api lock claim to wait, got early result err=%v", result.err)
	case <-time.After(120 * time.Millisecond):
	}

	firstLock.release()

	select {
	case result := <-resultCh:
		if result.err != nil {
			t.Fatalf("expected second dns api lock claim to succeed after release, got %v", result.err)
		}
		if result.lock == nil {
			t.Fatal("expected second dns api lock handle after release")
		}
		result.lock.release()
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for second dns api lock claim")
	}
}

func TestClaimDNSProviderEntryRunLockHonorsContextDeadline(t *testing.T) {
	configureFailoverV2RunnerTestDB(t)

	service, _ := createTestRunnerServiceAndMember(t)

	firstLock, err := claimDNSProviderEntryRunLock(context.Background(), "user-a", service)
	if err != nil {
		t.Fatalf("failed to claim first dns api lock: %v", err)
	}
	if firstLock == nil {
		t.Fatal("expected first dns api lock handle")
	}
	defer firstLock.release()

	waitCtx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()

	secondLock, secondErr := claimDNSProviderEntryRunLock(waitCtx, "user-a", service)
	if secondLock != nil {
		secondLock.release()
		t.Fatal("expected second dns api lock to not be acquired before deadline")
	}
	if !errors.Is(secondErr, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", secondErr)
	}
}

