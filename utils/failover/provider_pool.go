package failover

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	smithy "github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/komari-monitor/komari/database/models"
	awscloud "github.com/komari-monitor/komari/utils/cloudprovider/aws"
	"github.com/komari-monitor/komari/utils/cloudprovider/digitalocean"
	linodecloud "github.com/komari-monitor/komari/utils/cloudprovider/linode"
)

const (
	providerEntryCapacityModeSerialized = "serialized"
	providerEntryCapacityModeQuota      = "quota"

	providerEntrySnapshotTTL         = 45 * time.Second
	providerEntryCooldownTransient   = 15 * time.Second
	providerEntryCooldownRateLimited = 2 * time.Minute
	providerEntryCooldownQuota       = 5 * time.Minute
	providerEntryCooldownHardFailure = 30 * time.Minute
	providerEntryMinCreateInterval   = 3 * time.Second
)

type providerPoolCandidate struct {
	EntryID    string
	EntryName  string
	EntryGroup string
	Preferred  bool
	Active     bool
}

type providerEntryCapacitySnapshot struct {
	FetchedAt time.Time
	Mode      string
	Limit     int
	Used      int
	Detail    map[string]interface{}
}

type providerEntryLease struct {
	state *providerEntryRuntimeState
}

type providerEntryRuntimeState struct {
	mu               sync.Mutex
	cond             *sync.Cond
	nextTicket       uint64
	servingTicket    uint64
	nextOpTicket     uint64
	servingOpTicket  uint64
	opInFlight       bool
	nextAllowedAt    time.Time
	reserved         int
	provisionedDelta int
	cooldownUntil    time.Time
	cooldownReason   string
	snapshot         *providerEntryCapacitySnapshot
}

type providerEntryScheduler struct {
	mu     sync.Mutex
	states map[string]*providerEntryRuntimeState
}

type providerFailureDecision struct {
	Class    string
	Cooldown time.Duration
}

var failoverProviderEntryScheduler = &providerEntryScheduler{
	states: map[string]*providerEntryRuntimeState{},
}

func (scheduler *providerEntryScheduler) stateFor(key string) *providerEntryRuntimeState {
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()

	if state := scheduler.states[key]; state != nil {
		return state
	}

	state := &providerEntryRuntimeState{}
	state.cond = sync.NewCond(&state.mu)
	scheduler.states[key] = state
	return state
}

func (state *providerEntryRuntimeState) beginTurn() func() {
	state.mu.Lock()
	ticket := state.nextTicket
	state.nextTicket++
	for ticket != state.servingTicket {
		state.cond.Wait()
	}
	return func() {
		state.servingTicket++
		state.cond.Broadcast()
		state.mu.Unlock()
	}
}

func (lease *providerEntryLease) Release(provisioned bool) {
	if lease == nil || lease.state == nil {
		return
	}

	lease.state.mu.Lock()
	defer lease.state.mu.Unlock()

	if lease.state.reserved > 0 {
		lease.state.reserved--
	}
	if provisioned {
		lease.state.provisionedDelta++
	}
}

func (lease *providerEntryLease) BeginSerializedOperation(spacing time.Duration) (func(), error) {
	if lease == nil || lease.state == nil {
		return func() {}, nil
	}

	state := lease.state
	state.mu.Lock()
	ticket := state.nextOpTicket
	state.nextOpTicket++
	for ticket != state.servingOpTicket || state.opInFlight {
		state.cond.Wait()
	}

	now := time.Now()
	if !state.cooldownUntil.IsZero() && state.cooldownUntil.After(now) {
		state.servingOpTicket++
		state.cond.Broadcast()
		state.mu.Unlock()
		return nil, fmt.Errorf("provider entry is cooling down until %s", state.cooldownUntil.UTC().Format(time.RFC3339))
	}

	startAt := now
	if spacing > 0 && state.nextAllowedAt.After(startAt) {
		startAt = state.nextAllowedAt
	}
	if spacing > 0 {
		state.nextAllowedAt = startAt.Add(spacing)
	}

	state.opInFlight = true
	state.mu.Unlock()

	if delay := time.Until(startAt); delay > 0 {
		time.Sleep(delay)
	}

	return func() {
		state.mu.Lock()
		state.opInFlight = false
		state.servingOpTicket++
		state.cond.Broadcast()
		state.mu.Unlock()
	}, nil
}

func listProviderPoolCandidates(userUUID string, plan models.FailoverPlan) ([]providerPoolCandidate, error) {
	entryGroup := normalizeProviderEntryGroup(plan.ProviderEntryGroup)
	switch strings.ToLower(strings.TrimSpace(plan.Provider)) {
	case "aws":
		raw, err := loadProviderAddition(userUUID, "aws")
		if err != nil {
			return nil, fmt.Errorf("AWS provider is not configured")
		}
		addition := &awscloud.Addition{}
		if strings.TrimSpace(raw) == "" {
			raw = "{}"
		}
		if err := json.Unmarshal([]byte(raw), addition); err != nil {
			return nil, fmt.Errorf("AWS configuration is invalid: %w", err)
		}
		addition.Normalize()
		if len(addition.Credentials) == 0 {
			return nil, errors.New("AWS credential is not configured")
		}
		preferredID := resolvePreferredEntryID(strings.TrimSpace(plan.ProviderEntryID), addition.ActiveCredentialID)
		candidates := make([]providerPoolCandidate, 0, len(addition.Credentials))
		for _, credential := range addition.Credentials {
			credentialGroup := normalizeProviderEntryGroup(credential.Group)
			if entryGroup != "" && credentialGroup != entryGroup {
				continue
			}
			candidates = append(candidates, providerPoolCandidate{
				EntryID:    strings.TrimSpace(credential.ID),
				EntryName:  strings.TrimSpace(credential.Name),
				EntryGroup: credentialGroup,
				Preferred:  strings.TrimSpace(credential.ID) == preferredID,
				Active:     strings.TrimSpace(credential.ID) == strings.TrimSpace(addition.ActiveCredentialID),
			})
		}
		if entryGroup != "" && len(candidates) == 0 {
			return nil, fmt.Errorf("AWS credential group not found: %s", entryGroup)
		}
		return sortProviderPoolCandidates(candidates), nil
	case "digitalocean":
		raw, err := loadProviderAddition(userUUID, "digitalocean")
		if err != nil {
			return nil, fmt.Errorf("DigitalOcean provider is not configured")
		}
		addition := &digitalocean.Addition{}
		if strings.TrimSpace(raw) == "" {
			raw = "{}"
		}
		if err := json.Unmarshal([]byte(raw), addition); err != nil {
			return nil, fmt.Errorf("DigitalOcean configuration is invalid: %w", err)
		}
		addition.Normalize()
		if len(addition.Tokens) == 0 {
			return nil, errors.New("DigitalOcean token is not configured")
		}
		preferredID := resolvePreferredEntryID(strings.TrimSpace(plan.ProviderEntryID), addition.ActiveTokenID)
		candidates := make([]providerPoolCandidate, 0, len(addition.Tokens))
		for _, token := range addition.Tokens {
			tokenGroup := normalizeProviderEntryGroup(token.Group)
			if entryGroup != "" && tokenGroup != entryGroup {
				continue
			}
			candidates = append(candidates, providerPoolCandidate{
				EntryID:    strings.TrimSpace(token.ID),
				EntryName:  strings.TrimSpace(token.Name),
				EntryGroup: tokenGroup,
				Preferred:  strings.TrimSpace(token.ID) == preferredID,
				Active:     strings.TrimSpace(token.ID) == strings.TrimSpace(addition.ActiveTokenID),
			})
		}
		if entryGroup != "" && len(candidates) == 0 {
			return nil, fmt.Errorf("DigitalOcean token group not found: %s", entryGroup)
		}
		return sortProviderPoolCandidates(candidates), nil
	case "linode":
		raw, err := loadProviderAddition(userUUID, "linode")
		if err != nil {
			return nil, fmt.Errorf("Linode provider is not configured")
		}
		addition := &linodecloud.Addition{}
		if strings.TrimSpace(raw) == "" {
			raw = "{}"
		}
		if err := json.Unmarshal([]byte(raw), addition); err != nil {
			return nil, fmt.Errorf("Linode configuration is invalid: %w", err)
		}
		addition.Normalize()
		if len(addition.Tokens) == 0 {
			return nil, errors.New("Linode token is not configured")
		}
		preferredID := resolvePreferredEntryID(strings.TrimSpace(plan.ProviderEntryID), addition.ActiveTokenID)
		candidates := make([]providerPoolCandidate, 0, len(addition.Tokens))
		for _, token := range addition.Tokens {
			tokenGroup := normalizeProviderEntryGroup(token.Group)
			if entryGroup != "" && tokenGroup != entryGroup {
				continue
			}
			candidates = append(candidates, providerPoolCandidate{
				EntryID:    strings.TrimSpace(token.ID),
				EntryName:  strings.TrimSpace(token.Name),
				EntryGroup: tokenGroup,
				Preferred:  strings.TrimSpace(token.ID) == preferredID,
				Active:     strings.TrimSpace(token.ID) == strings.TrimSpace(addition.ActiveTokenID),
			})
		}
		if entryGroup != "" && len(candidates) == 0 {
			return nil, fmt.Errorf("Linode token group not found: %s", entryGroup)
		}
		return sortProviderPoolCandidates(candidates), nil
	default:
		return nil, fmt.Errorf("unsupported provider pool: %s", plan.Provider)
	}
}

func resolvePreferredEntryID(requestedID, activeID string) string {
	requestedID = strings.TrimSpace(requestedID)
	activeID = strings.TrimSpace(activeID)
	if requestedID == "" || requestedID == activeProviderEntryID {
		return activeID
	}
	return requestedID
}

func sortProviderPoolCandidates(candidates []providerPoolCandidate) []providerPoolCandidate {
	if len(candidates) <= 1 {
		return candidates
	}

	scored := make([]providerPoolCandidate, 0, len(candidates))
	seen := make(map[string]struct{}, len(candidates))
	for _, candidate := range candidates {
		candidate.EntryID = strings.TrimSpace(candidate.EntryID)
		candidate.EntryName = strings.TrimSpace(candidate.EntryName)
		if candidate.EntryID == "" {
			continue
		}
		if candidate.EntryName == "" {
			candidate.EntryName = candidate.EntryID
		}
		if _, exists := seen[candidate.EntryID]; exists {
			continue
		}
		seen[candidate.EntryID] = struct{}{}
		scored = append(scored, candidate)
	}

	for i := 0; i < len(scored); i++ {
		for j := i + 1; j < len(scored); j++ {
			if candidateRank(scored[j]) < candidateRank(scored[i]) {
				scored[i], scored[j] = scored[j], scored[i]
			}
		}
	}
	return scored
}

func candidateRank(candidate providerPoolCandidate) int {
	switch {
	case candidate.Preferred:
		return 0
	case candidate.Active:
		return 1
	default:
		return 2
	}
}

func acquireProviderEntryLease(userUUID string, plan models.FailoverPlan, candidate providerPoolCandidate) (*providerEntryLease, map[string]interface{}, error) {
	key := providerEntryStateKey(userUUID, plan.Provider, candidate.EntryID)
	state := failoverProviderEntryScheduler.stateFor(key)
	finishTurn := state.beginTurn()
	defer finishTurn()

	now := time.Now()
	if !state.cooldownUntil.IsZero() && state.cooldownUntil.After(now) {
		availability := state.buildAvailability(nil, plan)
		availability["status"] = "cooldown"
		availability["reason"] = state.cooldownReason
		availability["cooldown_until"] = state.cooldownUntil.UTC().Format(time.RFC3339)
		return nil, availability, fmt.Errorf("provider entry is cooling down until %s", state.cooldownUntil.UTC().Format(time.RFC3339))
	}

	snapshot, err := state.loadSnapshot(userUUID, plan, candidate, now)
	if err != nil {
		return nil, state.buildAvailability(snapshot, plan), err
	}

	freeSlots := state.availableSlots(snapshot, plan)
	availability := state.buildAvailability(snapshot, plan)
	availability["free"] = freeSlots
	if freeSlots <= 0 {
		availability["status"] = "full"
		return nil, availability, errors.New("provider entry has no available capacity")
	}

	state.reserved++
	availability = state.buildAvailability(snapshot, plan)
	availability["status"] = "reserved"
	return &providerEntryLease{state: state}, availability, nil
}

func (state *providerEntryRuntimeState) loadSnapshot(userUUID string, plan models.FailoverPlan, candidate providerPoolCandidate, now time.Time) (*providerEntryCapacitySnapshot, error) {
	if !shouldCheckProviderCapacity(plan) {
		return &providerEntryCapacitySnapshot{
			FetchedAt: now,
			Mode:      providerEntryCapacityModeSerialized,
			Limit:     1,
			Detail: map[string]interface{}{
				"policy": "serialized",
			},
		}, nil
	}

	if state.snapshot != nil && now.Sub(state.snapshot.FetchedAt) < providerEntrySnapshotTTL {
		return state.snapshot, nil
	}

	snapshot, err := loadProviderEntryCapacitySnapshot(userUUID, plan, candidate)
	if err != nil {
		return nil, err
	}

	state.snapshot = snapshot
	state.provisionedDelta = 0
	return snapshot, nil
}

func (state *providerEntryRuntimeState) availableSlots(snapshot *providerEntryCapacitySnapshot, plan models.FailoverPlan) int {
	if snapshot == nil {
		if shouldCheckProviderCapacity(plan) {
			return 0
		}
		free := 1 - state.reserved
		if free < 0 {
			return 0
		}
		return free
	}

	if !shouldCheckProviderCapacity(plan) || snapshot.Mode == providerEntryCapacityModeSerialized {
		if snapshot.Mode == providerEntryCapacityModeSerialized &&
			strings.TrimSpace(plan.ActionType) == models.FailoverActionProvisionInstance {
			// Providers without reliable quota data should still allow multiple failover
			// tasks to queue on the same entry; BeginSerializedOperation enforces that
			// only one create operation runs at a time.
			return 1
		}
		free := 1 - state.reserved
		if free < 0 {
			return 0
		}
		return free
	}

	limit := snapshot.Limit
	if limit <= 0 {
		free := 1 - state.reserved
		if free < 0 {
			return 0
		}
		return free
	}

	free := limit - snapshot.Used - state.provisionedDelta - state.reserved
	if free < 0 {
		return 0
	}
	return free
}

func (state *providerEntryRuntimeState) buildAvailability(snapshot *providerEntryCapacitySnapshot, plan models.FailoverPlan) map[string]interface{} {
	availability := map[string]interface{}{
		"reserved":          state.reserved,
		"provisioned_delta": state.provisionedDelta,
		"capacity_tracked":  shouldCheckProviderCapacity(plan),
	}
	if snapshot == nil {
		return availability
	}

	availability["mode"] = snapshot.Mode
	availability["limit"] = snapshot.Limit
	availability["used"] = snapshot.Used
	availability["fetched_at"] = snapshot.FetchedAt.UTC().Format(time.RFC3339)
	if snapshot.Detail != nil {
		availability["detail"] = snapshot.Detail
	}
	availability["free"] = state.availableSlots(snapshot, plan)
	return availability
}

func providerEntryStateKey(userUUID, provider, entryID string) string {
	return strings.TrimSpace(userUUID) + "|" + strings.ToLower(strings.TrimSpace(provider)) + "|" + strings.TrimSpace(entryID)
}

func shouldCheckProviderCapacity(plan models.FailoverPlan) bool {
	if strings.TrimSpace(plan.ActionType) != models.FailoverActionProvisionInstance {
		return false
	}

	if strings.ToLower(strings.TrimSpace(plan.Provider)) != "aws" {
		return true
	}

	service := resolveAWSPlanService(plan)
	return service == "ec2"
}

func shouldSerializeProviderOperation(plan models.FailoverPlan) bool {
	return strings.TrimSpace(plan.ActionType) == models.FailoverActionProvisionInstance
}

func providerEntryOperationSpacing(plan models.FailoverPlan) time.Duration {
	if !shouldSerializeProviderOperation(plan) {
		return 0
	}
	return providerEntryMinCreateInterval
}

func resolveAWSPlanService(plan models.FailoverPlan) string {
	switch strings.TrimSpace(plan.ActionType) {
	case models.FailoverActionProvisionInstance:
		var payload awsProvisionPayload
		if err := json.Unmarshal([]byte(plan.Payload), &payload); err == nil {
			service := strings.ToLower(strings.TrimSpace(payload.Service))
			if service != "" {
				return service
			}
		}
		return "ec2"
	case models.FailoverActionRebindPublicIP:
		var payload awsRebindPayload
		if err := json.Unmarshal([]byte(plan.Payload), &payload); err == nil {
			service := strings.ToLower(strings.TrimSpace(payload.Service))
			if service != "" {
				return service
			}
		}
		return "ec2"
	default:
		return "ec2"
	}
}

func loadProviderEntryCapacitySnapshot(userUUID string, plan models.FailoverPlan, candidate providerPoolCandidate) (*providerEntryCapacitySnapshot, error) {
	switch strings.ToLower(strings.TrimSpace(plan.Provider)) {
	case "aws":
		return loadAWSCapacitySnapshot(userUUID, plan, candidate.EntryID)
	case "digitalocean":
		return loadDigitalOceanCapacitySnapshot(userUUID, candidate.EntryID)
	case "linode":
		return loadLinodeCapacitySnapshot(userUUID, candidate.EntryID)
	default:
		return &providerEntryCapacitySnapshot{
			FetchedAt: time.Now(),
			Mode:      providerEntryCapacityModeSerialized,
			Limit:     1,
		}, nil
	}
}

func loadAWSCapacitySnapshot(userUUID string, plan models.FailoverPlan, entryID string) (*providerEntryCapacitySnapshot, error) {
	addition, credential, err := loadAWSCredential(userUUID, entryID)
	if err != nil {
		return nil, err
	}

	region := resolveAWSPlanRegion(plan, addition, credential)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	instances, err := awscloud.ListInstances(ctx, credential, region)
	if err != nil {
		return nil, err
	}

	activeInstances := 0
	for _, instance := range instances {
		state := strings.ToLower(strings.TrimSpace(instance.State))
		if state == "terminated" {
			continue
		}
		activeInstances++
	}

	quota := credential.EC2Quota
	if quota == nil || strings.TrimSpace(quota.Region) != region {
		quota, _ = awscloud.GetEC2QuotaSummary(ctx, credential, region)
	}

	mode := providerEntryCapacityModeSerialized
	limit := 1
	if quota != nil && quota.MaxInstances > 0 {
		mode = providerEntryCapacityModeQuota
		limit = quota.MaxInstances
	}

	return &providerEntryCapacitySnapshot{
		FetchedAt: time.Now(),
		Mode:      mode,
		Limit:     limit,
		Used:      activeInstances,
		Detail: map[string]interface{}{
			"provider":  "aws",
			"region":    region,
			"account":   strings.TrimSpace(credential.AccountID),
			"quota_max": limit,
			"service":   resolveAWSPlanService(plan),
		},
	}, nil
}

func resolveAWSPlanRegion(plan models.FailoverPlan, addition *awscloud.Addition, credential *awscloud.CredentialRecord) string {
	switch strings.TrimSpace(plan.ActionType) {
	case models.FailoverActionProvisionInstance:
		var payload awsProvisionPayload
		if err := json.Unmarshal([]byte(plan.Payload), &payload); err == nil {
			if region := strings.TrimSpace(payload.Region); region != "" {
				return region
			}
		}
	case models.FailoverActionRebindPublicIP:
		var payload awsRebindPayload
		if err := json.Unmarshal([]byte(plan.Payload), &payload); err == nil {
			if region := strings.TrimSpace(payload.Region); region != "" {
				return region
			}
		}
	}

	if addition != nil {
		if region := strings.TrimSpace(addition.ActiveRegion); region != "" {
			return region
		}
	}
	if credential != nil {
		if region := strings.TrimSpace(credential.DefaultRegion); region != "" {
			return region
		}
	}
	return awscloud.DefaultRegion
}

func loadDigitalOceanCapacitySnapshot(userUUID, entryID string) (*providerEntryCapacitySnapshot, error) {
	_, token, err := loadDigitalOceanToken(userUUID, entryID)
	if err != nil {
		return nil, err
	}

	client, err := digitalocean.NewClientFromToken(token.Token)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	account, err := client.GetAccount(ctx)
	if err != nil {
		return nil, err
	}

	tokenCopy := *token
	tokenCopy.SetCheckResult(time.Now(), account, nil)
	if strings.TrimSpace(tokenCopy.LastStatus) == digitalocean.TokenStatusError && strings.TrimSpace(tokenCopy.LastError) != "" {
		return nil, errors.New(tokenCopy.LastError)
	}

	droplets, err := client.ListDroplets(ctx)
	if err != nil {
		return nil, err
	}

	limit := token.DropletLimit
	if account != nil && account.DropletLimit > 0 {
		limit = account.DropletLimit
	}

	mode := providerEntryCapacityModeSerialized
	if limit > 0 {
		mode = providerEntryCapacityModeQuota
	}

	snapshotLimit := 1
	if limit > 0 {
		snapshotLimit = limit
	}

	return &providerEntryCapacitySnapshot{
		FetchedAt: time.Now(),
		Mode:      mode,
		Limit:     snapshotLimit,
		Used:      len(droplets),
		Detail: map[string]interface{}{
			"provider":      "digitalocean",
			"account_email": strings.TrimSpace(account.Email),
			"account_uuid":  strings.TrimSpace(account.UUID),
			"droplet_limit": limit,
			"account_status": func() string {
				if account == nil {
					return ""
				}
				return strings.TrimSpace(account.Status)
			}(),
		},
	}, nil
}

func loadLinodeCapacitySnapshot(userUUID, entryID string) (*providerEntryCapacitySnapshot, error) {
	_, token, err := loadLinodeToken(userUUID, entryID)
	if err != nil {
		return nil, err
	}

	client, err := linodecloud.NewClientFromToken(token.Token)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	instances, err := client.ListInstances(ctx)
	if err != nil {
		return nil, err
	}

	return &providerEntryCapacitySnapshot{
		FetchedAt: time.Now(),
		Mode:      providerEntryCapacityModeSerialized,
		Limit:     1,
		Used:      len(instances),
		Detail: map[string]interface{}{
			"provider":   "linode",
			"instances":  len(instances),
			"token_name": strings.TrimSpace(token.Name),
		},
	}, nil
}

func clearProviderEntryCooldown(userUUID, provider, entryID string) {
	key := providerEntryStateKey(userUUID, provider, entryID)
	state := failoverProviderEntryScheduler.stateFor(key)
	state.mu.Lock()
	defer state.mu.Unlock()

	state.cooldownUntil = time.Time{}
	state.cooldownReason = ""
}

func invalidateProviderEntrySnapshot(userUUID, provider, entryID string) {
	key := providerEntryStateKey(userUUID, provider, entryID)
	state := failoverProviderEntryScheduler.stateFor(key)
	state.mu.Lock()
	defer state.mu.Unlock()

	state.snapshot = nil
	state.provisionedDelta = 0
}

func applyProviderEntryFailure(userUUID, provider, entryID string, decision providerFailureDecision, err error) {
	if decision.Cooldown <= 0 {
		return
	}

	key := providerEntryStateKey(userUUID, provider, entryID)
	state := failoverProviderEntryScheduler.stateFor(key)
	state.mu.Lock()
	defer state.mu.Unlock()

	until := time.Now().Add(decision.Cooldown)
	if until.After(state.cooldownUntil) {
		state.cooldownUntil = until
	}
	if err != nil {
		state.cooldownReason = strings.TrimSpace(err.Error())
	} else {
		state.cooldownReason = decision.Class
	}
}

func classifyProviderFailure(provider string, err error) providerFailureDecision {
	provider = strings.ToLower(strings.TrimSpace(provider))
	message := strings.ToLower(strings.TrimSpace(providerFailureMessage(err)))
	switch provider {
	case "digitalocean":
		var apiErr *digitalocean.APIError
		if errors.As(err, &apiErr) {
			switch apiErr.StatusCode {
			case 401, 403:
				return providerFailureDecision{Class: "auth_invalid", Cooldown: providerEntryCooldownHardFailure}
			case 429:
				return providerFailureDecision{Class: "rate_limited", Cooldown: providerEntryCooldownRateLimited}
			case 422:
				if isQuotaMessage(message) {
					return providerFailureDecision{Class: "quota_exhausted", Cooldown: providerEntryCooldownQuota}
				}
				if isBillingMessage(message) {
					return providerFailureDecision{Class: "billing_locked", Cooldown: providerEntryCooldownHardFailure}
				}
			}
		}
	case "linode":
		var apiErr *linodecloud.APIError
		if errors.As(err, &apiErr) {
			switch apiErr.StatusCode {
			case 401, 403:
				return providerFailureDecision{Class: "auth_invalid", Cooldown: providerEntryCooldownHardFailure}
			case 429:
				return providerFailureDecision{Class: "rate_limited", Cooldown: providerEntryCooldownRateLimited}
			case 400, 402, 422:
				if isQuotaMessage(message) {
					return providerFailureDecision{Class: "quota_exhausted", Cooldown: providerEntryCooldownQuota}
				}
				if isBillingMessage(message) {
					return providerFailureDecision{Class: "billing_locked", Cooldown: providerEntryCooldownHardFailure}
				}
			}
		}
	case "aws":
		var responseErr *smithyhttp.ResponseError
		if errors.As(err, &responseErr) {
			switch responseErr.HTTPStatusCode() {
			case 401, 403:
				return providerFailureDecision{Class: "auth_invalid", Cooldown: providerEntryCooldownHardFailure}
			case 429:
				return providerFailureDecision{Class: "rate_limited", Cooldown: providerEntryCooldownRateLimited}
			}
		}

		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			code := strings.ToLower(strings.TrimSpace(apiErr.ErrorCode()))
			apiMessage := strings.ToLower(strings.TrimSpace(apiErr.ErrorMessage()))
			switch {
			case strings.Contains(code, "requestlimit"), strings.Contains(code, "thrott"), strings.Contains(apiMessage, "too many requests"):
				return providerFailureDecision{Class: "rate_limited", Cooldown: providerEntryCooldownRateLimited}
			case strings.Contains(code, "auth"), strings.Contains(code, "unauthor"), strings.Contains(code, "accessdenied"), strings.Contains(code, "invalidclienttokenid"):
				return providerFailureDecision{Class: "auth_invalid", Cooldown: providerEntryCooldownHardFailure}
			case strings.Contains(code, "quota"), strings.Contains(code, "limitexceeded"), strings.Contains(apiMessage, "quota"), strings.Contains(apiMessage, "limit"):
				return providerFailureDecision{Class: "quota_exhausted", Cooldown: providerEntryCooldownQuota}
			}
		}
	}

	switch {
	case strings.Contains(message, "too many requests"), strings.Contains(message, "rate limit"), strings.Contains(message, "thrott"):
		return providerFailureDecision{Class: "rate_limited", Cooldown: providerEntryCooldownRateLimited}
	case isQuotaMessage(message):
		return providerFailureDecision{Class: "quota_exhausted", Cooldown: providerEntryCooldownQuota}
	case isBillingMessage(message):
		return providerFailureDecision{Class: "billing_locked", Cooldown: providerEntryCooldownHardFailure}
	case strings.Contains(message, "unauthorized"), strings.Contains(message, "forbidden"), strings.Contains(message, "invalid token"), strings.Contains(message, "invalid access key"):
		return providerFailureDecision{Class: "auth_invalid", Cooldown: providerEntryCooldownHardFailure}
	default:
		return providerFailureDecision{Class: "transient_error", Cooldown: providerEntryCooldownTransient}
	}
}

func isQuotaMessage(message string) bool {
	return strings.Contains(message, "quota") ||
		strings.Contains(message, "limit") ||
		strings.Contains(message, "droplet limit") ||
		strings.Contains(message, "resource limit")
}

func isBillingMessage(message string) bool {
	return strings.Contains(message, "billing") ||
		strings.Contains(message, "payment") ||
		strings.Contains(message, "balance") ||
		strings.Contains(message, "locked") ||
		strings.Contains(message, "past due")
}

func providerFailureMessage(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
