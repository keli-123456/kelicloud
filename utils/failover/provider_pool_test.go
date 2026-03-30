package failover

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/komari-monitor/komari/database/models"
)

func TestSortProviderPoolCandidates(t *testing.T) {
	candidates := []providerPoolCandidate{
		{EntryID: "entry-c", EntryName: "C"},
		{EntryID: "entry-b", EntryName: "B", Active: true},
		{EntryID: "entry-a", EntryName: "A", Preferred: true},
	}

	sorted := sortProviderPoolCandidates(candidates)
	if len(sorted) != 3 {
		t.Fatalf("expected 3 candidates, got %d", len(sorted))
	}
	if sorted[0].EntryID != "entry-a" {
		t.Fatalf("expected preferred candidate first, got %s", sorted[0].EntryID)
	}
	if sorted[1].EntryID != "entry-b" {
		t.Fatalf("expected active candidate second, got %s", sorted[1].EntryID)
	}
	if sorted[2].EntryID != "entry-c" {
		t.Fatalf("expected remaining candidate last, got %s", sorted[2].EntryID)
	}
}

func TestAvailableSlotsUsesQuotaAndReservations(t *testing.T) {
	state := &providerEntryRuntimeState{
		reserved:         2,
		provisionedDelta: 1,
	}
	plan := models.FailoverPlan{
		Provider:   "digitalocean",
		ActionType: models.FailoverActionProvisionInstance,
	}
	snapshot := &providerEntryCapacitySnapshot{
		Mode:  providerEntryCapacityModeQuota,
		Limit: 10,
		Used:  3,
	}

	free := state.availableSlots(snapshot, plan)
	if free != 4 {
		t.Fatalf("expected 4 free slots, got %d", free)
	}
}

func TestAvailableSlotsFallsBackToSerializedMode(t *testing.T) {
	state := &providerEntryRuntimeState{
		reserved: 1,
	}
	plan := models.FailoverPlan{
		Provider:   "aws",
		ActionType: models.FailoverActionRebindPublicIP,
	}

	free := state.availableSlots(nil, plan)
	if free != 0 {
		t.Fatalf("expected serialized mode to block when one reservation exists, got %d", free)
	}
}

func TestAvailableSlotsAllowsQueuedSerializedProvisioning(t *testing.T) {
	state := &providerEntryRuntimeState{
		reserved: 1,
	}
	plan := models.FailoverPlan{
		Provider:   "linode",
		ActionType: models.FailoverActionProvisionInstance,
	}
	snapshot := &providerEntryCapacitySnapshot{
		Mode:  providerEntryCapacityModeSerialized,
		Limit: 1,
		Used:  3,
	}

	free := state.availableSlots(snapshot, plan)
	if free != 1 {
		t.Fatalf("expected serialized provisioning to stay queueable, got %d", free)
	}
}

func TestBeginSerializedOperationQueuesRequests(t *testing.T) {
	state := &providerEntryRuntimeState{}
	state.cond = sync.NewCond(&state.mu)

	firstLease := &providerEntryLease{state: state}
	secondLease := &providerEntryLease{state: state}

	finishFirst, err := firstLease.BeginSerializedOperation(0)
	if err != nil {
		t.Fatalf("expected first operation to start, got %v", err)
	}

	startedSecond := make(chan struct{})
	secondDone := make(chan struct{})
	go func() {
		finishSecond, beginErr := secondLease.BeginSerializedOperation(0)
		if beginErr != nil {
			t.Errorf("expected second operation to wait instead of failing: %v", beginErr)
			close(secondDone)
			return
		}
		close(startedSecond)
		finishSecond()
		close(secondDone)
	}()

	select {
	case <-startedSecond:
		t.Fatal("expected second operation to wait for the first one")
	case <-time.After(80 * time.Millisecond):
	}

	finishFirst()

	select {
	case <-startedSecond:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected second operation to start after the first one finished")
	}

	select {
	case <-secondDone:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected second operation to complete")
	}
}

func TestBeginSerializedOperationAppliesMinimumSpacing(t *testing.T) {
	state := &providerEntryRuntimeState{}
	state.cond = sync.NewCond(&state.mu)

	firstLease := &providerEntryLease{state: state}
	secondLease := &providerEntryLease{state: state}
	spacing := 90 * time.Millisecond

	finishFirst, err := firstLease.BeginSerializedOperation(spacing)
	if err != nil {
		t.Fatalf("expected first operation to start, got %v", err)
	}
	finishFirst()

	startedAt := time.Now()
	finishSecond, err := secondLease.BeginSerializedOperation(spacing)
	if err != nil {
		t.Fatalf("expected second operation to start, got %v", err)
	}
	elapsed := time.Since(startedAt)
	finishSecond()

	if elapsed < 70*time.Millisecond {
		t.Fatalf("expected serialized spacing to delay the second operation, got %s", elapsed)
	}
}

func TestWaitForProviderEntryCapacityAfterRecyclePollsUntilFree(t *testing.T) {
	originalScheduler := failoverProviderEntryScheduler
	originalLoader := providerEntryCapacitySnapshotLoader
	originalTimeout := providerEntryRecycleWaitTimeout
	originalPollInterval := providerEntryRecycleWaitPollInterval
	failoverProviderEntryScheduler = &providerEntryScheduler{
		states: map[string]*providerEntryRuntimeState{},
	}
	providerEntryRecycleWaitTimeout = 80 * time.Millisecond
	providerEntryRecycleWaitPollInterval = 1 * time.Millisecond
	defer func() {
		failoverProviderEntryScheduler = originalScheduler
		providerEntryCapacitySnapshotLoader = originalLoader
		providerEntryRecycleWaitTimeout = originalTimeout
		providerEntryRecycleWaitPollInterval = originalPollInterval
	}()

	loadCalls := 0
	providerEntryCapacitySnapshotLoader = func(userUUID string, plan models.FailoverPlan, candidate providerPoolCandidate) (*providerEntryCapacitySnapshot, error) {
		loadCalls++
		used := 3
		if loadCalls >= 3 {
			used = 2
		}
		return &providerEntryCapacitySnapshot{
			FetchedAt: time.Now(),
			Mode:      providerEntryCapacityModeQuota,
			Limit:     3,
			Used:      used,
		}, nil
	}

	availability := waitForProviderEntryCapacityAfterRecycle(
		context.Background(),
		"user-1",
		models.FailoverPlan{
			Provider:   "digitalocean",
			ActionType: models.FailoverActionProvisionInstance,
		},
		providerPoolCandidate{EntryID: "entry-1", EntryName: "Token 1"},
	)

	if loadCalls < 3 {
		t.Fatalf("expected at least 3 snapshot refreshes, got %d", loadCalls)
	}
	if got := intMapValue(availability, "free"); got != 1 {
		t.Fatalf("expected 1 free slot after recycle, got %d", got)
	}
	if got := stringMapValue(availability, "status"); got != "available" {
		t.Fatalf("expected availability status to become available, got %q", got)
	}
}
