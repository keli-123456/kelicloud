package failoverv2

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sort"
	"strings"

	"github.com/komari-monitor/komari/database/models"
)

type AliyunMemberDNSPayload struct {
	DomainName string `json:"domain_name,omitempty"`
	RR         string `json:"rr,omitempty"`
	RecordType string `json:"record_type,omitempty"`
	SyncIPv6   bool   `json:"sync_ipv6,omitempty"`
	TTL        int    `json:"ttl,omitempty"`
}

type AliyunMemberDNSRecord struct {
	Provider string `json:"provider,omitempty"`
	ID       string `json:"id,omitempty"`
	Name     string `json:"name,omitempty"`
	Type     string `json:"type,omitempty"`
	Value    string `json:"value,omitempty"`
	Domain   string `json:"domain,omitempty"`
	RR       string `json:"rr,omitempty"`
	Line     string `json:"line,omitempty"`
}

type AliyunMemberDNSResult struct {
	Provider     string                       `json:"provider"`
	Domain       string                       `json:"domain"`
	RR           string                       `json:"rr"`
	Line         string                       `json:"line"`
	Records      []AliyunMemberDNSRecord      `json:"records,omitempty"`
	Removed      []AliyunMemberDNSRecord      `json:"removed_records,omitempty"`
	SkippedTypes []string                     `json:"skipped_types,omitempty"`
	PrunedTypes  []string                     `json:"pruned_types,omitempty"`
	RecordRefs   map[string]string            `json:"record_refs,omitempty"`
	Payload      *AliyunMemberDNSPayload      `json:"payload,omitempty"`
	Observed     []AliyunMemberDNSRecord      `json:"observed_records,omitempty"`
	ManagedTypes []string                     `json:"managed_types,omitempty"`
	Metadata     map[string]map[string]string `json:"metadata,omitempty"`
}

type AliyunMemberDNSVerification struct {
	Provider   string                  `json:"provider"`
	Domain     string                  `json:"domain"`
	RR         string                  `json:"rr"`
	Line       string                  `json:"line"`
	Success    bool                    `json:"success"`
	Expected   []AliyunMemberDNSRecord `json:"expected_records,omitempty"`
	Observed   []AliyunMemberDNSRecord `json:"observed_records,omitempty"`
	Missing    []AliyunMemberDNSRecord `json:"missing_records,omitempty"`
	Unexpected []AliyunMemberDNSRecord `json:"unexpected_records,omitempty"`
}

type aliyunDNSApplyPlan struct {
	RecordTypes  []string
	SkippedTypes []string
	PrunedTypes  []string
}

type aliyunMemberDNSOperation struct {
	payload    AliyunMemberDNSPayload
	config     *aliyunDNSConfig
	client     aliyunRecordClient
	domainName string
	rr         string
	line       string
	ttl        int
	recordRefs map[string]string
}

var newAliyunDNSClientFunc = func(configValue *aliyunDNSConfig) aliyunRecordClient {
	if configValue == nil {
		return nil
	}
	return newAliyunDNSClient(configValue.AccessKeyID, configValue.AccessKeySecret, configValue.RegionID)
}

func ApplyAliyunMemberDNSAttach(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member, ipv4, ipv6 string) (*AliyunMemberDNSResult, error) {
	operation, err := newAliyunMemberDNSOperation(userUUID, service, member)
	if err != nil {
		return nil, err
	}

	plan, err := buildAliyunDNSApplyPlan(operation.payload.RecordType, operation.payload.SyncIPv6, ipv4, ipv6)
	if err != nil {
		return nil, err
	}

	existingRecords, err := operation.client.listRecords(contextOrBackground(ctx), operation.domainName)
	if err != nil {
		return nil, err
	}

	removed := make([]AliyunMemberDNSRecord, 0)
	removedRecordIDs := map[string]struct{}{}
	recordRefs := make(map[string]string, len(plan.RecordTypes))
	records := make([]AliyunMemberDNSRecord, 0, len(plan.RecordTypes))

	for _, recordType := range plan.RecordTypes {
		recordType = strings.ToUpper(strings.TrimSpace(recordType))
		targetValue, err := selectAliyunRecordValue(recordType, ipv4, ipv6)
		if err != nil {
			return nil, err
		}

		expectedValues, err := buildServiceMemberExpectedRecordValues(
			service,
			member,
			recordType,
			ipv4,
			ipv6,
			func(candidate *models.FailoverV2Member) bool {
				return memberHasAliyunLine(candidate, operation.line)
			},
		)
		if err != nil {
			return nil, err
		}
		expectedValues = augmentExpectedRecordValuesWithMemberRefs(
			expectedValues,
			service,
			member,
			recordType,
			func(candidate *models.FailoverV2Member) bool {
				return memberHasAliyunLine(candidate, operation.line)
			},
			func(candidate *models.FailoverV2Member, resolvedType string) (string, bool) {
				return resolveAliyunMemberExpectedRecordValueFromRefs(candidate, resolvedType, operation.rr, operation.line, existingRecords)
			},
		)

		selectedRecordIDs := map[string]struct{}{}
		targetRecordID := ""
		ownedRecordID := strings.TrimSpace(operation.recordRefs[recordType])
		if ownedRecordID != "" && detachRecordReferencedByOtherMember(
			service,
			member,
			recordType,
			ownedRecordID,
			func(candidate *models.FailoverV2Member) bool {
				return memberHasAliyunLine(candidate, operation.line)
			},
		) {
			ownedRecordID = ""
		}

		for _, expectedValue := range expectedValues {
			isTargetExpected := targetValue != "" && sameAddress(expectedValue, targetValue)
			existingMatch := aliyunDNSRecord{}
			if isTargetExpected && ownedRecordID != "" {
				owned := findAliyunDNSRecordByID(existingRecords, ownedRecordID)
				if strings.TrimSpace(owned.RecordID) != "" &&
					sameAliyunRecordIdentity(owned, operation.rr, recordType) &&
					sameAliyunRecordLine(owned.Line, operation.line) {
					if _, selected := selectedRecordIDs[ownedRecordID]; !selected {
						existingMatch = owned
					}
				}
			}
			if strings.TrimSpace(existingMatch.RecordID) == "" {
				existingMatch = findAliyunDNSRecordExactMatchForAssignment(existingRecords, selectedRecordIDs, operation.rr, recordType, operation.line, expectedValue)
			}
			if strings.TrimSpace(existingMatch.RecordID) == "" {
				if !isTargetExpected {
					existingMatch = findAliyunDNSRecordForAssignment(existingRecords, selectedRecordIDs, operation.rr, recordType, operation.line)
				}
			}
			recordID := strings.TrimSpace(existingMatch.RecordID)
			if recordID == "" || !sameAliyunRecordValue(existingMatch.Value, expectedValue) || existingMatch.TTL != operation.ttl {
				recordID, err = operation.client.upsertRecord(
					contextOrBackground(ctx),
					recordID,
					operation.domainName,
					operation.rr,
					recordType,
					expectedValue,
					operation.ttl,
					operation.line,
				)
				if err != nil {
					return nil, err
				}
			}
			recordID = strings.TrimSpace(recordID)
			if recordID == "" {
				return nil, fmt.Errorf("aliyun %s record id is empty after upsert", recordType)
			}
			selectedRecordIDs[recordID] = struct{}{}
			if targetRecordID == "" && sameAddress(expectedValue, targetValue) {
				targetRecordID = recordID
			}
		}

		for _, existingRecord := range existingRecords {
			if !sameAliyunRecordIdentity(existingRecord, operation.rr, recordType) || !sameAliyunRecordLine(existingRecord.Line, operation.line) {
				continue
			}
			existingRecordID := strings.TrimSpace(existingRecord.RecordID)
			if existingRecordID == "" {
				continue
			}
			if _, ok := selectedRecordIDs[existingRecordID]; ok {
				continue
			}
			if _, ok := removedRecordIDs[existingRecordID]; ok {
				continue
			}
			if detachRecordReferencedByOtherMember(
				service,
				member,
				recordType,
				existingRecordID,
				func(candidate *models.FailoverV2Member) bool {
					return memberHasAliyunLine(candidate, operation.line)
				},
			) {
				continue
			}
			if detachRecordBelongsToAnotherMember(
				service,
				member,
				recordType,
				existingRecord.Value,
				func(candidate *models.FailoverV2Member) bool {
					return memberHasAliyunLine(candidate, operation.line)
				},
			) {
				continue
			}
			if err := operation.client.deleteRecord(contextOrBackground(ctx), existingRecordID); err != nil {
				return nil, err
			}
			removedRecordIDs[existingRecordID] = struct{}{}
			removed = append(removed, buildAliyunMemberDNSRecordFromExisting(operation.domainName, existingRecord))
		}

		if targetValue != "" && targetRecordID == "" {
			return nil, fmt.Errorf("aliyun target %s record not found for line %s", recordType, operation.line)
		}
		if targetRecordID != "" {
			recordRefs[recordType] = targetRecordID
			records = append(records, buildAliyunMemberDNSRecord(operation.domainName, operation.rr, operation.line, recordType, targetValue, targetRecordID))
		}
	}

	for _, recordType := range plan.PrunedTypes {
		recordType = strings.ToUpper(strings.TrimSpace(recordType))
		for _, existingRecord := range existingRecords {
			if !sameAliyunRecordIdentity(existingRecord, operation.rr, recordType) || !sameAliyunRecordLine(existingRecord.Line, operation.line) {
				continue
			}
			existingRecordID := strings.TrimSpace(existingRecord.RecordID)
			if existingRecordID == "" {
				continue
			}
			if _, ok := removedRecordIDs[existingRecordID]; ok {
				continue
			}
			if detachRecordReferencedByOtherMember(
				service,
				member,
				recordType,
				existingRecordID,
				func(candidate *models.FailoverV2Member) bool {
					return memberHasAliyunLine(candidate, operation.line)
				},
			) {
				continue
			}
			if detachRecordBelongsToAnotherMember(
				service,
				member,
				recordType,
				existingRecord.Value,
				func(candidate *models.FailoverV2Member) bool {
					return memberHasAliyunLine(candidate, operation.line)
				},
			) {
				continue
			}
			if err := operation.client.deleteRecord(contextOrBackground(ctx), existingRecordID); err != nil {
				return nil, err
			}
			removedRecordIDs[existingRecordID] = struct{}{}
			removed = append(removed, buildAliyunMemberDNSRecordFromExisting(operation.domainName, existingRecord))
		}
	}

	return &AliyunMemberDNSResult{
		Provider:     models.FailoverDNSProviderAliyun,
		Domain:       operation.domainName,
		RR:           operation.rr,
		Line:         operation.line,
		Records:      records,
		Removed:      removed,
		SkippedTypes: append([]string(nil), plan.SkippedTypes...),
		PrunedTypes:  append([]string(nil), plan.PrunedTypes...),
		RecordRefs:   recordRefs,
		Payload:      cloneAliyunMemberDNSPayload(operation.payload),
		ManagedTypes: managedAliyunRecordTypes(operation.payload.RecordType),
	}, nil
}

func ApplyAliyunMemberDNSDetach(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member) (*AliyunMemberDNSResult, error) {
	operation, err := newAliyunMemberDNSOperation(userUUID, service, member)
	if err != nil {
		return nil, err
	}

	managedTypes := managedAliyunRecordTypes(operation.payload.RecordType)
	recordRefs, err := selectManagedRecordRefsForDetach(operation.recordRefs, managedTypes)
	if err != nil {
		return nil, err
	}
	existingRecords, err := operation.client.listRecords(contextOrBackground(ctx), operation.domainName)
	if err != nil {
		return nil, err
	}

	removed := make([]AliyunMemberDNSRecord, 0)
	removedRecordIDs := make(map[string]struct{}, len(recordRefs))
	currentAddress := normalizeIPAddress(member.CurrentAddress)

	for _, managedType := range managedTypes {
		recordType := strings.ToUpper(strings.TrimSpace(managedType))
		if recordType == "" {
			continue
		}
		recordID := strings.TrimSpace(recordRefs[recordType])
		record := findAliyunDNSRecordByID(existingRecords, recordID)
		if strings.TrimSpace(record.RecordID) != "" &&
			(!sameAliyunRecordIdentity(record, operation.rr, recordType) || !sameAliyunRecordLine(record.Line, operation.line)) {
			// The stored ref may become stale after service domain/RR/line changes.
			// Fallback to current-address lookup and avoid deleting the mismatched referenced record.
			record = aliyunDNSRecord{}
		}
		if strings.TrimSpace(record.RecordID) == "" {
			if currentAddress == "" {
				continue
			}
			typedCurrentAddress, hasTypedCurrentAddress := normalizeMemberAddressForRecordType(recordType, currentAddress)
			if !hasTypedCurrentAddress {
				continue
			}
			record = findAliyunDNSRecordExactMatch(existingRecords, operation.rr, recordType, operation.line, typedCurrentAddress)
			if strings.TrimSpace(record.RecordID) == "" {
				continue
			}
		}

		typedCurrentAddress, hasTypedCurrentAddress := normalizeMemberAddressForRecordType(recordType, currentAddress)
		if strings.TrimSpace(record.RecordID) != "" && recordID != "" {
			if detachRecordReferencedByOtherMember(
				service,
				member,
				recordType,
				recordID,
				func(candidate *models.FailoverV2Member) bool {
					return memberHasAliyunLine(candidate, operation.line)
				},
			) {
				continue
			}
			if !hasTypedCurrentAddress && detachRecordBelongsToAnotherMember(
				service,
				member,
				recordType,
				record.Value,
				func(candidate *models.FailoverV2Member) bool {
					return memberHasAliyunLine(candidate, operation.line)
				},
			) {
				continue
			}
			if hasTypedCurrentAddress && !sameAddress(record.Value, typedCurrentAddress) {
				// stale ref: continue to detach as long as this record is not explicitly owned by another member
			}
		}
		existingRecordID := strings.TrimSpace(record.RecordID)
		if existingRecordID == "" {
			continue
		}
		if _, ok := removedRecordIDs[existingRecordID]; ok {
			continue
		}
		if err := operation.client.deleteRecord(contextOrBackground(ctx), existingRecordID); err != nil {
			return nil, err
		}
		removedRecordIDs[existingRecordID] = struct{}{}
		removed = append(removed, buildAliyunMemberDNSRecordFromExisting(operation.domainName, record))
	}

	return &AliyunMemberDNSResult{
		Provider:     models.FailoverDNSProviderAliyun,
		Domain:       operation.domainName,
		RR:           operation.rr,
		Line:         operation.line,
		Removed:      removed,
		RecordRefs:   map[string]string{},
		Payload:      cloneAliyunMemberDNSPayload(operation.payload),
		ManagedTypes: managedTypes,
	}, nil
}

func VerifyAliyunMemberDNSAttached(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member, ipv4, ipv6 string) (*AliyunMemberDNSVerification, error) {
	operation, err := newAliyunMemberDNSOperation(userUUID, service, member)
	if err != nil {
		return nil, err
	}

	plan, err := buildAliyunDNSApplyPlan(operation.payload.RecordType, operation.payload.SyncIPv6, ipv4, ipv6)
	if err != nil {
		return nil, err
	}

	existingRecords, err := operation.client.listRecords(contextOrBackground(ctx), operation.domainName)
	if err != nil {
		return nil, err
	}

	expected := make([]AliyunMemberDNSRecord, 0, len(plan.RecordTypes))
	observed := make([]AliyunMemberDNSRecord, 0)
	for _, recordType := range plan.RecordTypes {
		recordType = strings.ToUpper(strings.TrimSpace(recordType))
		expectedValues, err := buildServiceMemberExpectedRecordValues(
			service,
			member,
			recordType,
			ipv4,
			ipv6,
			func(candidate *models.FailoverV2Member) bool {
				return memberHasAliyunLine(candidate, operation.line)
			},
		)
		if err != nil {
			return nil, err
		}
		expectedValues = augmentExpectedRecordValuesWithMemberRefs(
			expectedValues,
			service,
			member,
			recordType,
			func(candidate *models.FailoverV2Member) bool {
				return memberHasAliyunLine(candidate, operation.line)
			},
			func(candidate *models.FailoverV2Member, resolvedType string) (string, bool) {
				return resolveAliyunMemberExpectedRecordValueFromRefs(candidate, resolvedType, operation.rr, operation.line, existingRecords)
			},
		)
		for _, expectedValue := range expectedValues {
			expected = append(expected, buildAliyunMemberDNSRecord(operation.domainName, operation.rr, operation.line, recordType, expectedValue, ""))
		}
		for _, record := range existingRecords {
			if !sameAliyunRecordIdentity(record, operation.rr, recordType) || !sameAliyunRecordLine(record.Line, operation.line) {
				continue
			}
			observed = append(observed, buildAliyunMemberDNSRecordFromExisting(operation.domainName, record))
		}
	}

	for _, recordType := range plan.PrunedTypes {
		recordType = strings.ToUpper(strings.TrimSpace(recordType))
		for _, record := range existingRecords {
			if !sameAliyunRecordIdentity(record, operation.rr, recordType) || !sameAliyunRecordLine(record.Line, operation.line) {
				continue
			}
			observed = append(observed, buildAliyunMemberDNSRecordFromExisting(operation.domainName, record))
		}
	}

	verification := evaluateAliyunMemberDNSVerification(operation.domainName, operation.rr, operation.line, expected, observed)
	filteredUnexpected := make([]AliyunMemberDNSRecord, 0, len(verification.Unexpected))
	for _, unexpectedRecord := range verification.Unexpected {
		recordID := strings.TrimSpace(unexpectedRecord.ID)
		recordType := strings.ToUpper(strings.TrimSpace(unexpectedRecord.Type))
		if recordID != "" && recordType != "" && detachRecordReferencedByOtherMember(
			service,
			member,
			recordType,
			recordID,
			func(candidate *models.FailoverV2Member) bool {
				return memberHasAliyunLine(candidate, operation.line)
			},
		) {
			continue
		}
		filteredUnexpected = append(filteredUnexpected, unexpectedRecord)
	}
	verification.Unexpected = filteredUnexpected
	verification.Success = len(verification.Missing) == 0 && len(verification.Unexpected) == 0
	return verification, nil
}

func VerifyAliyunMemberDNSDetached(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member) (*AliyunMemberDNSVerification, error) {
	operation, err := newAliyunMemberDNSOperation(userUUID, service, member)
	if err != nil {
		return nil, err
	}
	managedTypes := managedAliyunRecordTypes(operation.payload.RecordType)
	recordRefs, err := selectManagedRecordRefsForDetach(operation.recordRefs, managedTypes)
	if err != nil {
		return nil, err
	}

	existingRecords, err := operation.client.listRecords(contextOrBackground(ctx), operation.domainName)
	if err != nil {
		return nil, err
	}

	observed := make([]AliyunMemberDNSRecord, 0)
	currentAddress := normalizeIPAddress(member.CurrentAddress)
	for _, managedType := range managedTypes {
		recordType := strings.ToUpper(strings.TrimSpace(managedType))
		if recordType == "" {
			continue
		}
		recordID := strings.TrimSpace(recordRefs[recordType])
		record := findAliyunDNSRecordByID(existingRecords, recordID)
		if strings.TrimSpace(record.RecordID) != "" &&
			(!sameAliyunRecordIdentity(record, operation.rr, recordType) || !sameAliyunRecordLine(record.Line, operation.line)) {
			// The stored ref may become stale after service domain/RR/line changes.
			// Fallback to current-address lookup and avoid treating stale refs as fatal errors.
			record = aliyunDNSRecord{}
		}
		if strings.TrimSpace(record.RecordID) == "" {
			if currentAddress == "" {
				continue
			}
			typedCurrentAddress, hasTypedCurrentAddress := normalizeMemberAddressForRecordType(recordType, currentAddress)
			if !hasTypedCurrentAddress {
				continue
			}
			record = findAliyunDNSRecordExactMatch(existingRecords, operation.rr, recordType, operation.line, typedCurrentAddress)
			if strings.TrimSpace(record.RecordID) == "" {
				continue
			}
		}

		typedCurrentAddress, hasTypedCurrentAddress := normalizeMemberAddressForRecordType(recordType, currentAddress)
		if strings.TrimSpace(record.RecordID) != "" && recordID != "" {
			if detachRecordReferencedByOtherMember(
				service,
				member,
				recordType,
				recordID,
				func(candidate *models.FailoverV2Member) bool {
					return memberHasAliyunLine(candidate, operation.line)
				},
			) {
				continue
			}
			if !hasTypedCurrentAddress && detachRecordBelongsToAnotherMember(
				service,
				member,
				recordType,
				record.Value,
				func(candidate *models.FailoverV2Member) bool {
					return memberHasAliyunLine(candidate, operation.line)
				},
			) {
				continue
			}
			if hasTypedCurrentAddress && !sameAddress(record.Value, typedCurrentAddress) {
				// stale ref: allow verification to proceed unless explicitly owned by another member
			}
		}
		observed = append(observed, buildAliyunMemberDNSRecordFromExisting(operation.domainName, record))
	}

	return evaluateAliyunMemberDNSVerification(operation.domainName, operation.rr, operation.line, nil, observed), nil
}

func DecodeAliyunMemberDNSRecordRefs(raw string) map[string]string {
	return decodeMemberDNSRecordRefs(raw)
}

func EncodeAliyunMemberDNSRecordRefs(recordRefs map[string]string) string {
	return encodeMemberDNSRecordRefs(recordRefs)
}

func newAliyunMemberDNSOperation(userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member) (*aliyunMemberDNSOperation, error) {
	if strings.TrimSpace(userUUID) == "" {
		return nil, errors.New("user id is required")
	}
	if service == nil {
		return nil, errors.New("service is required")
	}
	if member == nil {
		return nil, errors.New("member is required")
	}
	if strings.ToLower(strings.TrimSpace(service.DNSProvider)) != models.FailoverDNSProviderAliyun {
		return nil, errors.New("failover v2 service must use aliyun dns")
	}

	configValue, err := loadAliyunDNSConfigFunc(userUUID, service.DNSEntryID)
	if err != nil {
		return nil, err
	}
	if configValue == nil {
		return nil, errors.New("aliyun dns config is required")
	}

	payload, err := parseAliyunMemberDNSPayload(service.DNSPayload)
	if err != nil {
		return nil, err
	}

	domainName := strings.TrimSpace(payload.DomainName)
	if domainName == "" {
		domainName = strings.TrimSpace(configValue.DomainName)
	}
	if domainName == "" {
		return nil, errors.New("aliyun domain_name is required")
	}

	rr, err := validateAliyunRR(domainName, payload.RR)
	if err != nil {
		return nil, err
	}

	line := canonicalAliyunLineValue(member.DNSLine)
	if line == "" {
		return nil, errors.New("member dns_line is required")
	}

	ttl := payload.TTL
	if ttl <= 0 {
		ttl = 600
	}

	client := newAliyunDNSClientFunc(configValue)
	if client == nil {
		return nil, errors.New("aliyun dns client is not configured")
	}

	return &aliyunMemberDNSOperation{
		payload:    payload,
		config:     configValue,
		client:     client,
		domainName: domainName,
		rr:         rr,
		line:       line,
		ttl:        ttl,
		recordRefs: DecodeAliyunMemberDNSRecordRefs(member.DNSRecordRefs),
	}, nil
}

func parseAliyunMemberDNSPayload(raw string) (AliyunMemberDNSPayload, error) {
	payload := AliyunMemberDNSPayload{}
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "null" {
		return payload, nil
	}

	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return AliyunMemberDNSPayload{}, fmt.Errorf("aliyun dns payload is invalid: %w", err)
	}
	return payload, nil
}

func cloneAliyunMemberDNSPayload(payload AliyunMemberDNSPayload) *AliyunMemberDNSPayload {
	cloned := payload
	return &cloned
}

func buildAliyunDNSApplyPlan(recordType string, syncIPv6 bool, ipv4, ipv6 string) (*aliyunDNSApplyPlan, error) {
	normalized := strings.ToUpper(strings.TrimSpace(recordType))
	if normalized == "" {
		normalized = "A"
	}
	counterpart := counterpartDNSRecordType(normalized)

	if !syncIPv6 {
		if _, err := selectAliyunRecordValue(normalized, ipv4, ipv6); err != nil {
			return nil, err
		}
		return &aliyunDNSApplyPlan{
			RecordTypes: []string{normalized},
			PrunedTypes: []string{counterpart},
		}, nil
	}

	if _, err := selectAliyunRecordValue(normalized, ipv4, ipv6); err != nil {
		return nil, err
	}

	plan := &aliyunDNSApplyPlan{
		RecordTypes: []string{normalized},
	}
	if _, err := selectAliyunRecordValue(counterpart, ipv4, ipv6); err == nil {
		plan.RecordTypes = append(plan.RecordTypes, counterpart)
	} else {
		plan.SkippedTypes = append(plan.SkippedTypes, counterpart)
		plan.PrunedTypes = append(plan.PrunedTypes, counterpart)
	}
	return plan, nil
}

func counterpartDNSRecordType(recordType string) string {
	if strings.EqualFold(strings.TrimSpace(recordType), "AAAA") {
		return "A"
	}
	return "AAAA"
}

func managedAliyunRecordTypes(recordType string) []string {
	normalized := strings.ToUpper(strings.TrimSpace(recordType))
	if normalized == "" {
		normalized = "A"
	}
	return []string{normalized, counterpartDNSRecordType(normalized)}
}

func stringSliceSet(values []string) map[string]struct{} {
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.ToUpper(strings.TrimSpace(value))
		if value == "" {
			continue
		}
		result[value] = struct{}{}
	}
	return result
}

func selectAliyunRecordValue(recordType, ipv4, ipv6 string) (string, error) {
	switch strings.ToUpper(strings.TrimSpace(recordType)) {
	case "A":
		return normalizeDNSRecordIPValue("ipv4", "A", ipv4, true)
	case "AAAA":
		return normalizeDNSRecordIPValue("ipv6", "AAAA", ipv6, false)
	default:
		return "", fmt.Errorf("unsupported dns record type: %s", recordType)
	}
}

func normalizeDNSRecordIPValue(addressLabel, recordType, value string, wantIPv4 bool) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", fmt.Errorf("%s address is empty for %s record", addressLabel, recordType)
	}

	normalized := normalizeIPAddress(value)
	if normalized == "" {
		return "", fmt.Errorf("%s address is invalid for %s record: %q", addressLabel, recordType, value)
	}

	ip := net.ParseIP(normalized)
	if ip == nil {
		return "", fmt.Errorf("%s address is invalid for %s record: %q", addressLabel, recordType, value)
	}
	if wantIPv4 {
		if ip.To4() == nil {
			return "", fmt.Errorf("%s address is invalid for %s record: %q", addressLabel, recordType, value)
		}
		return ip.To4().String(), nil
	}
	if ip.To4() != nil {
		return "", fmt.Errorf("%s address is invalid for %s record: %q", addressLabel, recordType, value)
	}
	return ip.String(), nil
}

func evaluateAliyunMemberDNSVerification(domainName, rr, line string, expected, observed []AliyunMemberDNSRecord) *AliyunMemberDNSVerification {
	expected = cloneAliyunMemberDNSResultRecords(expected)
	observed = cloneAliyunMemberDNSResultRecords(observed)
	sortAliyunMemberDNSRecords(expected)
	sortAliyunMemberDNSRecords(observed)

	matchedObserved := make([]bool, len(observed))
	missing := make([]AliyunMemberDNSRecord, 0)
	for _, expectedRecord := range expected {
		found := false
		for index, observedRecord := range observed {
			if matchedObserved[index] {
				continue
			}
			if !aliyunMemberDNSRecordsMatch(expectedRecord, observedRecord) {
				continue
			}
			matchedObserved[index] = true
			found = true
			break
		}
		if !found {
			missing = append(missing, expectedRecord)
		}
	}

	unexpected := make([]AliyunMemberDNSRecord, 0)
	for index, observedRecord := range observed {
		if matchedObserved[index] {
			continue
		}
		unexpected = append(unexpected, observedRecord)
	}

	return &AliyunMemberDNSVerification{
		Provider:   models.FailoverDNSProviderAliyun,
		Domain:     strings.TrimSpace(domainName),
		RR:         strings.TrimSpace(rr),
		Line:       canonicalAliyunLineValue(line),
		Success:    len(missing) == 0 && len(unexpected) == 0,
		Expected:   expected,
		Observed:   observed,
		Missing:    missing,
		Unexpected: unexpected,
	}
}

func cloneAliyunMemberDNSResultRecords(records []AliyunMemberDNSRecord) []AliyunMemberDNSRecord {
	if len(records) == 0 {
		return nil
	}
	cloned := make([]AliyunMemberDNSRecord, len(records))
	copy(cloned, records)
	return cloned
}

func sortAliyunMemberDNSRecords(records []AliyunMemberDNSRecord) {
	sort.Slice(records, func(i, j int) bool {
		left := strings.Join([]string{
			strings.ToLower(strings.TrimSpace(records[i].Domain)),
			strings.ToLower(strings.TrimSpace(records[i].RR)),
			strings.ToLower(strings.TrimSpace(records[i].Line)),
			strings.ToLower(strings.TrimSpace(records[i].Type)),
			strings.TrimSpace(records[i].Value),
			strings.TrimSpace(records[i].ID),
		}, "|")
		right := strings.Join([]string{
			strings.ToLower(strings.TrimSpace(records[j].Domain)),
			strings.ToLower(strings.TrimSpace(records[j].RR)),
			strings.ToLower(strings.TrimSpace(records[j].Line)),
			strings.ToLower(strings.TrimSpace(records[j].Type)),
			strings.TrimSpace(records[j].Value),
			strings.TrimSpace(records[j].ID),
		}, "|")
		return left < right
	})
}

func containsAliyunRecord(records []AliyunMemberDNSRecord, recordID string) bool {
	recordID = strings.TrimSpace(recordID)
	if recordID == "" {
		return false
	}
	for _, record := range records {
		if strings.TrimSpace(record.ID) == recordID {
			return true
		}
	}
	return false
}

func aliyunMemberDNSRecordsMatch(expected, observed AliyunMemberDNSRecord) bool {
	return strings.EqualFold(strings.TrimSpace(expected.Domain), strings.TrimSpace(observed.Domain)) &&
		sameAliyunRecordRR(expected.RR, observed.RR) &&
		sameAliyunRecordLine(expected.Line, observed.Line) &&
		strings.EqualFold(strings.TrimSpace(expected.Type), strings.TrimSpace(observed.Type)) &&
		sameAliyunRecordValue(expected.Value, observed.Value)
}

func buildAliyunMemberDNSRecord(domainName, rr, line, recordType, value, recordID string) AliyunMemberDNSRecord {
	return AliyunMemberDNSRecord{
		Provider: models.FailoverDNSProviderAliyun,
		ID:       strings.TrimSpace(recordID),
		Name:     joinAliyunRecordName(domainName, rr),
		Type:     strings.ToUpper(strings.TrimSpace(recordType)),
		Value:    strings.TrimSpace(value),
		Domain:   strings.TrimSpace(domainName),
		RR:       strings.TrimSpace(rr),
		Line:     canonicalAliyunLineValue(line),
	}
}

func buildAliyunMemberDNSRecordFromExisting(domainName string, record aliyunDNSRecord) AliyunMemberDNSRecord {
	return buildAliyunMemberDNSRecord(domainName, record.RR, record.Line, record.Type, record.Value, record.RecordID)
}

func normalizeAliyunRR(domainName, rr string) string {
	normalizedDomain := strings.Trim(strings.TrimSpace(domainName), ".")
	normalizedRR := strings.Trim(strings.TrimSpace(rr), ".")
	if normalizedRR == "" || normalizedRR == "@" {
		return "@"
	}
	if normalizedDomain == "" {
		return normalizedRR
	}
	if strings.EqualFold(normalizedRR, normalizedDomain) {
		return "@"
	}
	if len(normalizedRR) > len(normalizedDomain)+1 && normalizedRR[len(normalizedRR)-len(normalizedDomain)-1] == '.' && strings.EqualFold(normalizedRR[len(normalizedRR)-len(normalizedDomain):], normalizedDomain) {
		normalizedRR = strings.TrimSpace(normalizedRR[:len(normalizedRR)-len(normalizedDomain)-1])
		if normalizedRR == "" || normalizedRR == "@" {
			return "@"
		}
	}
	return normalizedRR
}

func validateAliyunRR(domainName, rr string) (string, error) {
	normalizedRR := normalizeAliyunRR(domainName, rr)
	if strings.Contains(normalizedRR, "://") {
		return "", errors.New("aliyun rr must be a host record like @, www, or api; do not enter a URL")
	}
	if strings.ContainsAny(normalizedRR, "/\\ \t\r\n") {
		return "", errors.New("aliyun rr must be a host record like @, www, or api; do not include spaces or path separators")
	}
	if strings.HasPrefix(normalizedRR, ".") || strings.HasSuffix(normalizedRR, ".") || strings.Contains(normalizedRR, "..") {
		return "", errors.New("aliyun rr is invalid; use only the host record such as @, www, or api")
	}
	return normalizedRR, nil
}

func canonicalAliyunLineValue(value string) string {
	value = strings.TrimSpace(value)
	switch normalizeAliyunLineIdentity(value) {
	case "default":
		return "default"
	case "telecom":
		return "telecom"
	case "unicom":
		return "unicom"
	case "mobile":
		return "mobile"
	case "edu":
		return "edu"
	case "oversea":
		return "oversea"
	case "search":
		return "search"
	case "school":
		return "school"
	default:
		return value
	}
}

func normalizeAliyunLineIdentity(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "default", "默认":
		return "default"
	case "telecom", "电信":
		return "telecom"
	case "unicom", "联通":
		return "unicom"
	case "mobile", "移动":
		return "mobile"
	case "edu", "教育网":
		return "edu"
	case "oversea", "境外":
		return "oversea"
	case "search", "搜索引擎":
		return "search"
	case "school", "校园网":
		return "school"
	default:
		return strings.ToLower(strings.TrimSpace(value))
	}
}

func sameAliyunRecordLine(left, right string) bool {
	return normalizeAliyunLineIdentity(left) == normalizeAliyunLineIdentity(right)
}

func sameAliyunRecordRR(left, right string) bool {
	return strings.EqualFold(strings.TrimSpace(left), strings.TrimSpace(right))
}

func sameAliyunRecordIdentity(record aliyunDNSRecord, rr, recordType string) bool {
	return sameAliyunRecordRR(record.RR, rr) && strings.EqualFold(strings.TrimSpace(record.Type), strings.TrimSpace(recordType))
}

func sameAliyunRecordValue(left, right string) bool {
	if sameAddress(left, right) {
		return true
	}
	return strings.EqualFold(strings.TrimSpace(left), strings.TrimSpace(right))
}

func findAliyunDNSRecord(records []aliyunDNSRecord, rr, recordType, line string) aliyunDNSRecord {
	for _, record := range records {
		if !sameAliyunRecordIdentity(record, rr, recordType) || !sameAliyunRecordLine(record.Line, line) {
			continue
		}
		return record
	}
	return aliyunDNSRecord{}
}

func findAliyunDNSRecordExactMatch(records []aliyunDNSRecord, rr, recordType, line, value string) aliyunDNSRecord {
	for _, record := range records {
		if !sameAliyunRecordIdentity(record, rr, recordType) || !sameAliyunRecordLine(record.Line, line) {
			continue
		}
		if !sameAliyunRecordValue(record.Value, value) {
			continue
		}
		return record
	}
	return aliyunDNSRecord{}
}

func findAliyunDNSRecordExactMatchForAssignment(records []aliyunDNSRecord, selectedRecordIDs map[string]struct{}, rr, recordType, line, value string) aliyunDNSRecord {
	for _, record := range records {
		if !sameAliyunRecordIdentity(record, rr, recordType) || !sameAliyunRecordLine(record.Line, line) {
			continue
		}
		if !sameAliyunRecordValue(record.Value, value) {
			continue
		}
		recordID := strings.TrimSpace(record.RecordID)
		if recordID == "" {
			continue
		}
		if selectedRecordIDs != nil {
			if _, ok := selectedRecordIDs[recordID]; ok {
				continue
			}
		}
		return record
	}
	return aliyunDNSRecord{}
}

func findAliyunDNSRecordForAssignment(records []aliyunDNSRecord, selectedRecordIDs map[string]struct{}, rr, recordType, line string) aliyunDNSRecord {
	for _, record := range records {
		if !sameAliyunRecordIdentity(record, rr, recordType) || !sameAliyunRecordLine(record.Line, line) {
			continue
		}
		recordID := strings.TrimSpace(record.RecordID)
		if recordID == "" {
			continue
		}
		if selectedRecordIDs != nil {
			if _, ok := selectedRecordIDs[recordID]; ok {
				continue
			}
		}
		return record
	}
	return aliyunDNSRecord{}
}

func findOwnedOrMatchingAliyunRecord(records []aliyunDNSRecord, ownedRecordID, rr, recordType, line, value string) aliyunDNSRecord {
	ownedRecordID = strings.TrimSpace(ownedRecordID)
	if ownedRecordID != "" {
		owned := findAliyunDNSRecordByID(records, ownedRecordID)
		if strings.TrimSpace(owned.RecordID) != "" &&
			sameAliyunRecordIdentity(owned, rr, recordType) &&
			sameAliyunRecordLine(owned.Line, line) &&
			sameAliyunRecordValue(owned.Value, value) {
			return owned
		}
	}
	return findAliyunDNSRecordExactMatch(records, rr, recordType, line, value)
}

func findAliyunDNSRecordByID(records []aliyunDNSRecord, recordID string) aliyunDNSRecord {
	recordID = strings.TrimSpace(recordID)
	if recordID == "" {
		return aliyunDNSRecord{}
	}
	for _, record := range records {
		if strings.TrimSpace(record.RecordID) == recordID {
			return record
		}
	}
	return aliyunDNSRecord{}
}

func resolveAliyunMemberExpectedRecordValueFromRefs(
	member *models.FailoverV2Member,
	recordType string,
	rr string,
	line string,
	records []aliyunDNSRecord,
) (string, bool) {
	recordType = strings.ToUpper(strings.TrimSpace(recordType))
	if member == nil || recordType == "" {
		return "", false
	}

	for _, memberLine := range effectiveMemberLines(member) {
		if !sameAliyunRecordLine(memberLine.LineCode, line) {
			continue
		}
		recordRefs := decodeMemberDNSRecordRefs(memberLine.DNSRecordRefs)
		recordID := strings.TrimSpace(recordRefs[recordType])
		if recordID == "" {
			continue
		}
		record := findAliyunDNSRecordByID(records, recordID)
		if strings.TrimSpace(record.RecordID) == "" {
			continue
		}
		if !sameAliyunRecordIdentity(record, rr, recordType) || !sameAliyunRecordLine(record.Line, line) {
			continue
		}
		normalized, err := normalizeDNSRecordIPValue("record_value", recordType, record.Value, recordType == "A")
		if err != nil {
			continue
		}
		return normalized, true
	}

	return "", false
}

func joinAliyunRecordName(domainName, rr string) string {
	domainName = strings.TrimSpace(domainName)
	rr = strings.TrimSpace(rr)
	if rr == "" || rr == "@" {
		return domainName
	}
	if domainName == "" {
		return rr
	}
	return rr + "." + domainName
}

func sameAddress(target string, values ...string) bool {
	target = strings.TrimSpace(target)
	if target == "" {
		return false
	}
	normalizedTarget := normalizeIPAddress(target)
	for _, value := range values {
		trimmedValue := strings.TrimSpace(value)
		if trimmedValue == target {
			return true
		}
		if normalizedTarget == "" {
			continue
		}
		if normalizedValue := normalizeIPAddress(trimmedValue); normalizedValue != "" && normalizedValue == normalizedTarget {
			return true
		}
	}
	return false
}

func normalizeIPAddress(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	ip := net.ParseIP(value)
	if ip != nil {
		return ip.String()
	}
	ip, _, err := net.ParseCIDR(value)
	if err != nil || ip == nil {
		return ""
	}
	return ip.String()
}

func contextOrBackground(ctx context.Context) context.Context {
	if ctx != nil {
		return ctx
	}
	return context.Background()
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
