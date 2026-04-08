package failoverv2

import (
	"fmt"
	"log"
	"strings"
	"sync"

	failoverdb "github.com/komari-monitor/komari/database/failover"
	failoverv2db "github.com/komari-monitor/komari/database/failoverv2"
	"github.com/komari-monitor/komari/database/models"
)

type ServiceDNSOwnership struct {
	Provider   string
	EntryID    string
	DomainName string
	RR         string
	Key        string

	serviceRunLock *failoverV2RunLockHandle
	dnsRunLock     *failoverV2RunLockHandle
}

type serviceDNSOwnershipConflictError struct {
	Ownership         ServiceDNSOwnership
	ServiceID         uint
	ServiceName       string
	ConflictingID     uint
	ConflictingName   string
	ConflictingUserID string
}

type activeDNSRunConflictError struct {
	Ownership       ServiceDNSOwnership
	ServiceID       uint
	ActiveServiceID uint
}

type legacyFailoverDNSOwnershipConflictError struct {
	Ownership       ServiceDNSOwnership
	ConflictingID   uint
	ConflictingName string
}

type legacyFailoverMemberTargetConflictError struct {
	ConflictingID   uint
	ConflictingName string
	Reason          string
}

var (
	activeDNSRunsMu sync.Mutex
	activeDNSRuns   = map[string]uint{}
)

func (e *serviceDNSOwnershipConflictError) Error() string {
	if e == nil {
		return "failover v2 dns ownership conflict"
	}

	domainRR := strings.TrimSpace(e.Ownership.DomainName)
	if rr := strings.TrimSpace(e.Ownership.RR); rr != "" {
		domainRR = strings.TrimSpace(rr + "." + domainRR)
		if rr == "@" {
			domainRR = strings.TrimSpace(e.Ownership.DomainName)
		}
	}

	parts := []string{"failover v2 dns ownership conflict"}
	if domainRR != "" {
		parts = append(parts, fmt.Sprintf("%s %s", strings.TrimSpace(e.Ownership.Provider), domainRR))
	}
	if e.ConflictingID != 0 {
		conflictLabel := fmt.Sprintf("service %d", e.ConflictingID)
		if strings.TrimSpace(e.ConflictingName) != "" {
			conflictLabel = fmt.Sprintf("service %d (%s)", e.ConflictingID, strings.TrimSpace(e.ConflictingName))
		}
		parts = append(parts, "is already owned by "+conflictLabel)
	}
	return strings.Join(parts, ": ")
}

func (e *activeDNSRunConflictError) Error() string {
	if e == nil {
		return "failover v2 dns target is already being modified"
	}

	target := strings.TrimSpace(e.Ownership.DomainName)
	if rr := strings.TrimSpace(e.Ownership.RR); rr != "" && rr != "@" {
		target = strings.TrimSpace(rr + "." + target)
	}
	if target == "" {
		target = strings.TrimSpace(e.Ownership.Key)
	}
	if e.ActiveServiceID == 0 {
		return "failover v2 dns target is already being modified: " + target
	}
	return fmt.Sprintf(
		"failover v2 dns target %s is already being modified by service %d",
		target,
		e.ActiveServiceID,
	)
}

func (e *legacyFailoverDNSOwnershipConflictError) Error() string {
	if e == nil {
		return "failover v2 dns ownership conflicts with active v1 failover task"
	}
	label := strings.TrimSpace(e.ConflictingName)
	if label == "" {
		label = fmt.Sprintf("#%d", e.ConflictingID)
	}
	return fmt.Sprintf(
		"failover v2 dns target conflicts with active v1 failover task %d (%s); disable or migrate the v1 task first",
		e.ConflictingID,
		label,
	)
}

func (e *legacyFailoverMemberTargetConflictError) Error() string {
	if e == nil {
		return "failover v2 member target conflicts with active v1 failover task"
	}
	label := strings.TrimSpace(e.ConflictingName)
	if label == "" {
		label = fmt.Sprintf("#%d", e.ConflictingID)
	}
	reason := strings.TrimSpace(e.Reason)
	if reason == "" {
		reason = "same managed target"
	}
	return fmt.Sprintf(
		"failover v2 member target conflicts with active v1 failover task %d (%s): %s; disable or migrate the v1 task first",
		e.ConflictingID,
		label,
		reason,
	)
}

func ResolveServiceDNSOwnership(userUUID string, service *models.FailoverV2Service) (*ServiceDNSOwnership, error) {
	if strings.TrimSpace(userUUID) == "" {
		return nil, fmt.Errorf("user id is required")
	}
	if service == nil {
		return nil, fmt.Errorf("service is required")
	}

	provider := strings.ToLower(strings.TrimSpace(service.DNSProvider))
	switch provider {
	case models.FailoverDNSProviderAliyun:
		configValue, err := loadAliyunDNSConfigFunc(userUUID, service.DNSEntryID)
		if err != nil {
			return nil, err
		}
		if configValue == nil {
			return nil, fmt.Errorf("aliyun dns config is required")
		}

		payload, err := parseAliyunMemberDNSPayload(service.DNSPayload)
		if err != nil {
			return nil, err
		}

		domainName := normalizeServiceDNSDomainName(firstNonEmpty(payload.DomainName, configValue.DomainName))
		if domainName == "" {
			return nil, fmt.Errorf("aliyun domain_name is required")
		}

		rr, err := validateAliyunRR(domainName, payload.RR)
		if err != nil {
			return nil, err
		}
		rr = normalizeServiceDNSRR(rr)
		return &ServiceDNSOwnership{
			Provider:   provider,
			EntryID:    strings.TrimSpace(service.DNSEntryID),
			DomainName: domainName,
			RR:         rr,
			Key:        buildServiceDNSOwnershipKey(userUUID, provider, domainName, rr),
		}, nil
	case models.FailoverDNSProviderCloudflare:
		configValue, err := loadCloudflareDNSConfigFunc(userUUID, service.DNSEntryID)
		if err != nil {
			return nil, err
		}
		if configValue == nil {
			return nil, fmt.Errorf("cloudflare dns config is required")
		}

		payload, err := parseCloudflareMemberDNSPayload(service.DNSPayload)
		if err != nil {
			return nil, err
		}

		zoneID := strings.TrimSpace(firstNonEmpty(payload.ZoneID, configValue.ZoneID))
		zoneName := normalizeServiceDNSDomainName(firstNonEmpty(payload.ZoneName, configValue.ZoneName))
		if zoneID == "" && zoneName == "" {
			return nil, fmt.Errorf("cloudflare zone_id or zone_name is required")
		}

		recordName := normalizeCloudflareRecordName(firstNonEmpty(strings.TrimSpace(payload.RecordName), zoneName), zoneName)
		if recordName == "" {
			return nil, fmt.Errorf("cloudflare record_name is required")
		}

		domainTarget := firstNonEmpty(zoneName, zoneID)
		rr := normalizeCloudflareOwnershipRR(recordName, zoneName)
		return &ServiceDNSOwnership{
			Provider:   provider,
			EntryID:    strings.TrimSpace(service.DNSEntryID),
			DomainName: domainTarget,
			RR:         rr,
			Key:        buildServiceDNSOwnershipKey(userUUID, provider, domainTarget, rr),
		}, nil
	default:
		return nil, fmt.Errorf("unsupported failover v2 dns provider: %s", service.DNSProvider)
	}
}

func resolveLegacyFailoverTaskDNSOwnership(userUUID string, task *models.FailoverTask) (*ServiceDNSOwnership, bool, error) {
	if task == nil {
		return nil, false, nil
	}
	provider := strings.ToLower(strings.TrimSpace(task.DNSProvider))
	switch provider {
	case models.FailoverDNSProviderAliyun, models.FailoverDNSProviderCloudflare:
	default:
		return nil, false, nil
	}
	ownership, err := ResolveServiceDNSOwnership(userUUID, &models.FailoverV2Service{
		DNSProvider: provider,
		DNSEntryID:  strings.TrimSpace(task.DNSEntryID),
		DNSPayload:  strings.TrimSpace(task.DNSPayload),
	})
	if err != nil {
		return nil, true, err
	}
	return ownership, true, nil
}

func isLegacyFailoverTaskActive(task *models.FailoverTask) (bool, error) {
	if task == nil || task.ID == 0 {
		return false, nil
	}
	if task.Enabled {
		return true, nil
	}
	return failoverdb.HasActiveExecution(task.ID)
}

func ensureServiceDNSOwnershipAvailableFromLegacyFailover(userUUID string, ownership *ServiceDNSOwnership) error {
	if ownership == nil || strings.TrimSpace(ownership.Key) == "" {
		return nil
	}

	tasks, err := failoverdb.ListTasksByUser(userUUID)
	if err != nil {
		return err
	}

	for index := range tasks {
		task := &tasks[index]
		active, activeErr := isLegacyFailoverTaskActive(task)
		if activeErr != nil {
			return activeErr
		}
		if !active {
			continue
		}

		legacyOwnership, ok, ownershipErr := resolveLegacyFailoverTaskDNSOwnership(userUUID, task)
		if ownershipErr != nil {
			return fmt.Errorf("failed to resolve v1 failover task %d dns ownership: %w", task.ID, ownershipErr)
		}
		if ok && legacyOwnership != nil && legacyOwnership.Key == ownership.Key {
			return &legacyFailoverDNSOwnershipConflictError{
				Ownership:       *ownership,
				ConflictingID:   task.ID,
				ConflictingName: strings.TrimSpace(task.Name),
			}
		}
	}

	return nil
}

func ensureMemberTargetAvailableFromLegacyFailover(userUUID string, member *models.FailoverV2Member) error {
	if member == nil {
		return fmt.Errorf("member is required")
	}

	memberWatchClient := strings.TrimSpace(member.WatchClientUUID)
	memberAddress := normalizeIPAddress(member.CurrentAddress)
	memberRef := resolvedMemberCurrentInstanceRef(member)
	if memberWatchClient == "" && memberAddress == "" && len(memberRef) == 0 {
		return nil
	}

	tasks, err := failoverdb.ListTasksByUser(userUUID)
	if err != nil {
		return err
	}

	for index := range tasks {
		task := &tasks[index]
		active, activeErr := isLegacyFailoverTaskActive(task)
		if activeErr != nil {
			return activeErr
		}
		if !active {
			continue
		}

		reason := ""
		if memberWatchClient != "" && memberWatchClient == strings.TrimSpace(task.WatchClientUUID) {
			reason = "same watch_client_uuid " + memberWatchClient
		}
		if reason == "" && memberAddress != "" && memberAddress == normalizeIPAddress(task.CurrentAddress) {
			reason = "same current_address " + memberAddress
		}
		if reason == "" && sameManagedResource(parseJSONMap(task.CurrentInstanceRef), memberRef) {
			reason = "same cloud resource"
		}
		if reason == "" {
			continue
		}

		return &legacyFailoverMemberTargetConflictError{
			ConflictingID:   task.ID,
			ConflictingName: strings.TrimSpace(task.Name),
			Reason:          reason,
		}
	}

	return nil
}

func EnsureMemberTargetAvailable(userUUID string, member *models.FailoverV2Member) error {
	return ensureMemberTargetAvailableFromLegacyFailover(userUUID, member)
}

func EnsureServiceDNSOwnershipAvailable(userUUID string, serviceID uint, service *models.FailoverV2Service) (*ServiceDNSOwnership, error) {
	ownership, err := ResolveServiceDNSOwnership(userUUID, service)
	if err != nil {
		return nil, err
	}

	services, err := failoverv2db.ListServicesByUser(userUUID)
	if err != nil {
		return nil, err
	}

	for index := range services {
		candidate := &services[index]
		if candidate.ID == serviceID {
			continue
		}

		candidateOwnership, resolveErr := ResolveServiceDNSOwnership(userUUID, candidate)
		if resolveErr != nil {
			log.Printf("failoverv2: skipping dns ownership comparison for service %d: %v", candidate.ID, resolveErr)
			continue
		}
		if candidateOwnership == nil || candidateOwnership.Key != ownership.Key {
			continue
		}
		return ownership, &serviceDNSOwnershipConflictError{
			Ownership:         *ownership,
			ServiceID:         serviceID,
			ServiceName:       strings.TrimSpace(service.Name),
			ConflictingID:     candidate.ID,
			ConflictingName:   strings.TrimSpace(candidate.Name),
			ConflictingUserID: strings.TrimSpace(candidate.UserID),
		}
	}

	if err := ensureServiceDNSOwnershipAvailableFromLegacyFailover(userUUID, ownership); err != nil {
		return nil, err
	}

	return ownership, nil
}

func claimDNSRun(key string, serviceID uint) (uint, bool) {
	key = strings.TrimSpace(key)
	if key == "" || serviceID == 0 {
		return 0, true
	}

	activeDNSRunsMu.Lock()
	defer activeDNSRunsMu.Unlock()
	if activeServiceID, exists := activeDNSRuns[key]; exists {
		return activeServiceID, false
	}
	activeDNSRuns[key] = serviceID
	return 0, true
}

func releaseDNSRun(key string, serviceID uint) {
	key = strings.TrimSpace(key)
	if key == "" || serviceID == 0 {
		return
	}

	activeDNSRunsMu.Lock()
	defer activeDNSRunsMu.Unlock()
	if activeDNSRuns[key] == serviceID {
		delete(activeDNSRuns, key)
	}
}

func claimServiceExecutionLocks(userUUID string, service *models.FailoverV2Service) (*ServiceDNSOwnership, error) {
	ownership, err := EnsureServiceDNSOwnershipAvailable(userUUID, service.ID, service)
	if err != nil {
		return nil, err
	}

	serviceRunLock, err := claimServiceRunLock(service.ID, failoverV2ServiceRunLockTTL(service))
	if err != nil {
		return nil, err
	}
	ownership.serviceRunLock = serviceRunLock

	activeServiceID, claimed := claimDNSRun(ownership.Key, service.ID)
	if !claimed {
		serviceRunLock.release()
		return nil, &activeDNSRunConflictError{
			Ownership:       *ownership,
			ServiceID:       service.ID,
			ActiveServiceID: activeServiceID,
		}
	}
	dnsRunLock, err := claimFailoverV2RunLock(
		failoverV2DNSRunLockKey(ownership.Key),
		failoverV2ServiceRunLockTTL(service),
		func() {
			releaseDNSRun(ownership.Key, service.ID)
		},
	)
	if err != nil {
		serviceRunLock.release()
		return nil, &activeDNSRunConflictError{
			Ownership:       *ownership,
			ServiceID:       service.ID,
			ActiveServiceID: activeServiceID,
		}
	}
	ownership.dnsRunLock = dnsRunLock

	return ownership, nil
}

func releaseServiceExecutionLocks(serviceID uint, ownership *ServiceDNSOwnership) {
	if ownership != nil {
		ownership.dnsRunLock.release()
		ownership.serviceRunLock.release()
		return
	}
	releaseServiceRun(serviceID)
}

func buildServiceDNSOwnershipKey(userUUID, provider, domainName, rr string) string {
	return strings.Join([]string{
		strings.ToLower(strings.TrimSpace(userUUID)),
		strings.ToLower(strings.TrimSpace(provider)),
		normalizeServiceDNSDomainName(domainName),
		normalizeServiceDNSRR(rr),
	}, "|")
}

func normalizeServiceDNSDomainName(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	value = strings.TrimSuffix(value, ".")
	return value
}

func normalizeServiceDNSRR(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return "@"
	}
	return value
}
