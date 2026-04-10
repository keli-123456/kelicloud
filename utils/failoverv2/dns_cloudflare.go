package failoverv2

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/komari-monitor/komari/database/models"
	"github.com/komari-monitor/komari/utils/outboundproxy"
)

type CloudflareMemberDNSPayload struct {
	ZoneID     string `json:"zone_id,omitempty"`
	ZoneName   string `json:"zone_name,omitempty"`
	RecordName string `json:"record_name,omitempty"`
	RecordType string `json:"record_type,omitempty"`
	SyncIPv6   bool   `json:"sync_ipv6,omitempty"`
	TTL        int    `json:"ttl,omitempty"`
	Proxied    *bool  `json:"proxied,omitempty"`
}

type CloudflareMemberDNSRecord struct {
	Provider string `json:"provider,omitempty"`
	ID       string `json:"id,omitempty"`
	Name     string `json:"name,omitempty"`
	Type     string `json:"type,omitempty"`
	Value    string `json:"value,omitempty"`
	ZoneID   string `json:"zone_id,omitempty"`
	ZoneName string `json:"zone_name,omitempty"`
	Line     string `json:"line,omitempty"`
	TTL      int    `json:"ttl,omitempty"`
	Proxied  *bool  `json:"proxied,omitempty"`
}

type CloudflareMemberDNSResult struct {
	Provider     string                      `json:"provider"`
	ZoneID       string                      `json:"zone_id"`
	ZoneName     string                      `json:"zone_name"`
	RecordName   string                      `json:"record_name"`
	Line         string                      `json:"line"`
	Records      []CloudflareMemberDNSRecord `json:"records,omitempty"`
	Removed      []CloudflareMemberDNSRecord `json:"removed_records,omitempty"`
	SkippedTypes []string                    `json:"skipped_types,omitempty"`
	PrunedTypes  []string                    `json:"pruned_types,omitempty"`
	RecordRefs   map[string]string           `json:"record_refs,omitempty"`
	Payload      *CloudflareMemberDNSPayload `json:"payload,omitempty"`
	ManagedTypes []string                    `json:"managed_types,omitempty"`
}

type CloudflareMemberDNSVerification struct {
	Provider   string                      `json:"provider"`
	ZoneID     string                      `json:"zone_id"`
	ZoneName   string                      `json:"zone_name"`
	RecordName string                      `json:"record_name"`
	Line       string                      `json:"line"`
	Success    bool                        `json:"success"`
	Expected   []CloudflareMemberDNSRecord `json:"expected_records,omitempty"`
	Observed   []CloudflareMemberDNSRecord `json:"observed_records,omitempty"`
	Missing    []CloudflareMemberDNSRecord `json:"missing_records,omitempty"`
	Unexpected []CloudflareMemberDNSRecord `json:"unexpected_records,omitempty"`
}

type cloudflareMemberDNSOperation struct {
	payload    CloudflareMemberDNSPayload
	config     *cloudflareDNSConfig
	client     *cloudflareDNSClient
	zoneID     string
	zoneName   string
	recordName string
	line       string
	ttl        int
	proxied    bool
	recordRefs map[string]string
}

type cloudflareDNSClient struct {
	token      string
	baseURL    string
	httpClient *http.Client
}

type cloudflareAPIEnvelope[T any] struct {
	Success bool `json:"success"`
	Errors  []struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"errors"`
	Result     T                    `json:"result"`
	ResultInfo cloudflareResultInfo `json:"result_info"`
}

type cloudflareResultInfo struct {
	Page       int `json:"page"`
	PerPage    int `json:"per_page"`
	Count      int `json:"count"`
	TotalCount int `json:"total_count"`
	TotalPages int `json:"total_pages"`
}

type cloudflareZone struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type cloudflareDNSRecord struct {
	ID      string `json:"id"`
	Type    string `json:"type"`
	Name    string `json:"name"`
	Content string `json:"content"`
	TTL     int    `json:"ttl"`
	Proxied bool   `json:"proxied"`
}

var newCloudflareDNSClientFunc = func(configValue *cloudflareDNSConfig) *cloudflareDNSClient {
	if configValue == nil {
		return nil
	}
	return newCloudflareDNSClient(configValue.APIToken)
}

func ApplyCloudflareMemberDNSAttach(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member, ipv4, ipv6 string) (*CloudflareMemberDNSResult, error) {
	operation, err := newCloudflareMemberDNSOperation(userUUID, service, member)
	if err != nil {
		return nil, err
	}

	plan, err := buildAliyunDNSApplyPlan(operation.payload.RecordType, operation.payload.SyncIPv6, ipv4, ipv6)
	if err != nil {
		return nil, err
	}

	existingRecords, err := operation.client.listRecords(contextOrBackground(ctx), operation.zoneID)
	if err != nil {
		return nil, err
	}

	recordRefs := make(map[string]string, len(plan.RecordTypes))
	records := make([]CloudflareMemberDNSRecord, 0, len(plan.RecordTypes))
	removed := make([]CloudflareMemberDNSRecord, 0)
	removedIDs := map[string]struct{}{}

	for _, recordType := range plan.RecordTypes {
		recordValue, err := selectAliyunRecordValue(recordType, ipv4, ipv6)
		if err != nil {
			return nil, err
		}

		existingRecordID := strings.TrimSpace(operation.recordRefs[strings.ToUpper(strings.TrimSpace(recordType))])
		record, err := operation.client.upsertRecord(
			contextOrBackground(ctx),
			operation.zoneID,
			existingRecordID,
			operation.recordName,
			recordType,
			recordValue,
			operation.ttl,
			operation.proxied,
		)
		if err != nil {
			return nil, err
		}

		recordRefs[strings.ToUpper(strings.TrimSpace(recordType))] = strings.TrimSpace(record.ID)
		records = append(records, buildCloudflareMemberDNSRecord(operation.zoneID, operation.zoneName, operation.recordName, operation.line, *record))
	}

	for _, recordType := range plan.PrunedTypes {
		recordType = strings.ToUpper(strings.TrimSpace(recordType))
		recordID := strings.TrimSpace(operation.recordRefs[recordType])
		if recordID == "" {
			continue
		}
		record := findCloudflareDNSRecordByID(existingRecords, recordID)
		if strings.TrimSpace(record.ID) == "" {
			continue
		}
		if err := operation.client.deleteRecord(contextOrBackground(ctx), operation.zoneID, record.ID); err != nil {
			return nil, err
		}
		removedIDs[record.ID] = struct{}{}
		removed = append(removed, buildCloudflareMemberDNSRecord(operation.zoneID, operation.zoneName, operation.recordName, operation.line, record))
	}

	for recordType, recordID := range operation.recordRefs {
		recordType = strings.ToUpper(strings.TrimSpace(recordType))
		recordID = strings.TrimSpace(recordID)
		if recordID == "" {
			continue
		}
		if _, ok := removedIDs[recordID]; ok {
			continue
		}
		if _, ok := recordRefs[recordType]; ok {
			continue
		}
		record := findCloudflareDNSRecordByID(existingRecords, recordID)
		if strings.TrimSpace(record.ID) == "" {
			continue
		}
		if err := operation.client.deleteRecord(contextOrBackground(ctx), operation.zoneID, record.ID); err != nil {
			return nil, err
		}
		removedIDs[record.ID] = struct{}{}
		removed = append(removed, buildCloudflareMemberDNSRecord(operation.zoneID, operation.zoneName, operation.recordName, operation.line, record))
	}

	return &CloudflareMemberDNSResult{
		Provider:     models.FailoverDNSProviderCloudflare,
		ZoneID:       operation.zoneID,
		ZoneName:     operation.zoneName,
		RecordName:   operation.recordName,
		Line:         operation.line,
		Records:      records,
		Removed:      removed,
		SkippedTypes: append([]string(nil), plan.SkippedTypes...),
		PrunedTypes:  append([]string(nil), plan.PrunedTypes...),
		RecordRefs:   recordRefs,
		Payload:      cloneCloudflareMemberDNSPayload(operation.payload),
		ManagedTypes: managedAliyunRecordTypes(operation.payload.RecordType),
	}, nil
}

func ApplyCloudflareMemberDNSDetach(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member) (*CloudflareMemberDNSResult, error) {
	operation, err := newCloudflareMemberDNSOperation(userUUID, service, member)
	if err != nil {
		return nil, err
	}

	existingRecords, err := operation.client.listRecords(contextOrBackground(ctx), operation.zoneID)
	if err != nil {
		return nil, err
	}

	managedTypes := managedAliyunRecordTypes(operation.payload.RecordType)
	managedTypeSet := stringSliceSet(managedTypes)
	removed := make([]CloudflareMemberDNSRecord, 0)
	removedIDs := map[string]struct{}{}
	currentAddress := normalizeIPAddress(member.CurrentAddress)

	for recordType, recordID := range operation.recordRefs {
		recordType = strings.ToUpper(strings.TrimSpace(recordType))
		if _, ok := managedTypeSet[recordType]; !ok {
			continue
		}
		record := findCloudflareDNSRecordByID(existingRecords, recordID)
		if strings.TrimSpace(record.ID) == "" {
			continue
		}
		if !sameCloudflareRecordIdentity(record, operation.recordName, recordType) {
			continue
		}
		if currentAddress != "" && !sameAddress(record.Content, currentAddress) {
			continue
		}
		if err := operation.client.deleteRecord(contextOrBackground(ctx), operation.zoneID, record.ID); err != nil {
			return nil, err
		}
		removedIDs[record.ID] = struct{}{}
		removed = append(removed, buildCloudflareMemberDNSRecord(operation.zoneID, operation.zoneName, operation.recordName, operation.line, record))
	}

	if currentAddress != "" {
		for _, record := range existingRecords {
			recordType := strings.ToUpper(strings.TrimSpace(record.Type))
			if _, ok := managedTypeSet[recordType]; !ok {
				continue
			}
			if !sameCloudflareRecordIdentity(record, operation.recordName, recordType) || !sameAddress(record.Content, currentAddress) {
				continue
			}
			recordID := strings.TrimSpace(record.ID)
			if recordID == "" {
				continue
			}
			if _, ok := removedIDs[recordID]; ok {
				continue
			}
			if err := operation.client.deleteRecord(contextOrBackground(ctx), operation.zoneID, recordID); err != nil {
				return nil, err
			}
			removedIDs[recordID] = struct{}{}
			removed = append(removed, buildCloudflareMemberDNSRecord(operation.zoneID, operation.zoneName, operation.recordName, operation.line, record))
		}
	}

	return &CloudflareMemberDNSResult{
		Provider:     models.FailoverDNSProviderCloudflare,
		ZoneID:       operation.zoneID,
		ZoneName:     operation.zoneName,
		RecordName:   operation.recordName,
		Line:         operation.line,
		Removed:      removed,
		RecordRefs:   map[string]string{},
		Payload:      cloneCloudflareMemberDNSPayload(operation.payload),
		ManagedTypes: managedTypes,
	}, nil
}

func VerifyCloudflareMemberDNSAttached(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member, ipv4, ipv6 string) (*CloudflareMemberDNSVerification, error) {
	operation, err := newCloudflareMemberDNSOperation(userUUID, service, member)
	if err != nil {
		return nil, err
	}

	plan, err := buildAliyunDNSApplyPlan(operation.payload.RecordType, operation.payload.SyncIPv6, ipv4, ipv6)
	if err != nil {
		return nil, err
	}

	existingRecords, err := operation.client.listRecords(contextOrBackground(ctx), operation.zoneID)
	if err != nil {
		return nil, err
	}

	expected := make([]CloudflareMemberDNSRecord, 0, len(plan.RecordTypes))
	observed := make([]CloudflareMemberDNSRecord, 0, len(plan.RecordTypes))
	missing := make([]CloudflareMemberDNSRecord, 0)
	unexpected := make([]CloudflareMemberDNSRecord, 0)

	for _, recordType := range plan.RecordTypes {
		recordValue, err := selectAliyunRecordValue(recordType, ipv4, ipv6)
		if err != nil {
			return nil, err
		}
		expectedRecord := CloudflareMemberDNSRecord{
			Provider: models.FailoverDNSProviderCloudflare,
			Name:     operation.recordName,
			Type:     strings.ToUpper(strings.TrimSpace(recordType)),
			Value:    recordValue,
			ZoneID:   operation.zoneID,
			ZoneName: operation.zoneName,
			Line:     operation.line,
			TTL:      operation.ttl,
			Proxied:  ptrBool(operation.proxied),
		}
		expected = append(expected, expectedRecord)

		if record := findOwnedOrMatchingCloudflareRecord(existingRecords, operation.recordRefs[recordType], operation.recordName, recordType, recordValue); strings.TrimSpace(record.ID) != "" {
			observed = append(observed, buildCloudflareMemberDNSRecord(operation.zoneID, operation.zoneName, operation.recordName, operation.line, record))
		} else {
			missing = append(missing, expectedRecord)
		}
	}

	for _, recordType := range managedAliyunRecordTypes(operation.payload.RecordType) {
		recordType = strings.ToUpper(strings.TrimSpace(recordType))
		recordID := strings.TrimSpace(operation.recordRefs[recordType])
		if recordID == "" {
			continue
		}
		record := findCloudflareDNSRecordByID(existingRecords, recordID)
		if strings.TrimSpace(record.ID) == "" {
			continue
		}
		if !sameCloudflareRecordIdentity(record, operation.recordName, recordType) {
			unexpected = append(unexpected, buildCloudflareMemberDNSRecord(operation.zoneID, operation.zoneName, operation.recordName, operation.line, record))
			continue
		}
		if containsCloudflareRecord(observed, record.ID) {
			continue
		}
		if containsRecordType(plan.RecordTypes, recordType) {
			expectedValue, valueErr := selectAliyunRecordValue(recordType, ipv4, ipv6)
			if valueErr == nil && sameAddress(record.Content, expectedValue) {
				observed = append(observed, buildCloudflareMemberDNSRecord(operation.zoneID, operation.zoneName, operation.recordName, operation.line, record))
				continue
			}
		}
		unexpected = append(unexpected, buildCloudflareMemberDNSRecord(operation.zoneID, operation.zoneName, operation.recordName, operation.line, record))
	}

	sortCloudflareMemberDNSRecords(expected)
	sortCloudflareMemberDNSRecords(observed)
	sortCloudflareMemberDNSRecords(missing)
	sortCloudflareMemberDNSRecords(unexpected)

	return &CloudflareMemberDNSVerification{
		Provider:   models.FailoverDNSProviderCloudflare,
		ZoneID:     operation.zoneID,
		ZoneName:   operation.zoneName,
		RecordName: operation.recordName,
		Line:       operation.line,
		Success:    len(missing) == 0 && len(unexpected) == 0,
		Expected:   expected,
		Observed:   observed,
		Missing:    missing,
		Unexpected: unexpected,
	}, nil
}

func VerifyCloudflareMemberDNSDetached(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member) (*CloudflareMemberDNSVerification, error) {
	operation, err := newCloudflareMemberDNSOperation(userUUID, service, member)
	if err != nil {
		return nil, err
	}

	existingRecords, err := operation.client.listRecords(contextOrBackground(ctx), operation.zoneID)
	if err != nil {
		return nil, err
	}

	managedTypes := stringSliceSet(managedAliyunRecordTypes(operation.payload.RecordType))
	currentAddress := normalizeIPAddress(member.CurrentAddress)
	observed := make([]CloudflareMemberDNSRecord, 0)

	for _, record := range existingRecords {
		recordType := strings.ToUpper(strings.TrimSpace(record.Type))
		if _, ok := managedTypes[recordType]; !ok {
			continue
		}
		recordID := strings.TrimSpace(record.ID)
		if recordID != "" {
			if storedID := strings.TrimSpace(operation.recordRefs[recordType]); storedID != "" && storedID == recordID {
				observed = append(observed, buildCloudflareMemberDNSRecord(operation.zoneID, operation.zoneName, operation.recordName, operation.line, record))
				continue
			}
		}
		if currentAddress != "" && sameCloudflareRecordIdentity(record, operation.recordName, recordType) && sameAddress(record.Content, currentAddress) {
			observed = append(observed, buildCloudflareMemberDNSRecord(operation.zoneID, operation.zoneName, operation.recordName, operation.line, record))
		}
	}

	sortCloudflareMemberDNSRecords(observed)
	return &CloudflareMemberDNSVerification{
		Provider:   models.FailoverDNSProviderCloudflare,
		ZoneID:     operation.zoneID,
		ZoneName:   operation.zoneName,
		RecordName: operation.recordName,
		Line:       operation.line,
		Success:    len(observed) == 0,
		Observed:   observed,
		Unexpected: observed,
	}, nil
}

func DecodeCloudflareMemberDNSRecordRefs(raw string) map[string]string {
	return decodeMemberDNSRecordRefs(raw)
}

func EncodeCloudflareMemberDNSRecordRefs(recordRefs map[string]string) string {
	return encodeMemberDNSRecordRefs(recordRefs)
}

func parseCloudflareMemberDNSPayload(raw string) (CloudflareMemberDNSPayload, error) {
	payload := CloudflareMemberDNSPayload{}
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "null" {
		return payload, nil
	}
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return CloudflareMemberDNSPayload{}, fmt.Errorf("cloudflare dns payload is invalid: %w", err)
	}
	return payload, nil
}

func cloneCloudflareMemberDNSPayload(payload CloudflareMemberDNSPayload) *CloudflareMemberDNSPayload {
	cloned := payload
	return &cloned
}

func newCloudflareMemberDNSOperation(userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member) (*cloudflareMemberDNSOperation, error) {
	if strings.TrimSpace(userUUID) == "" {
		return nil, errors.New("user id is required")
	}
	if service == nil {
		return nil, errors.New("service is required")
	}
	if member == nil {
		return nil, errors.New("member is required")
	}
	if strings.ToLower(strings.TrimSpace(service.DNSProvider)) != models.FailoverDNSProviderCloudflare {
		return nil, errors.New("failover v2 service must use cloudflare dns")
	}

	configValue, err := loadCloudflareDNSConfigFunc(userUUID, service.DNSEntryID)
	if err != nil {
		return nil, err
	}
	if configValue == nil {
		return nil, errors.New("cloudflare dns config is required")
	}

	payload, err := parseCloudflareMemberDNSPayload(service.DNSPayload)
	if err != nil {
		return nil, err
	}

	zoneID := strings.TrimSpace(firstNonEmpty(payload.ZoneID, configValue.ZoneID))
	zoneName := normalizeServiceDNSDomainName(firstNonEmpty(payload.ZoneName, configValue.ZoneName))
	recordName := normalizeCloudflareRecordName(firstNonEmpty(strings.TrimSpace(payload.RecordName), zoneName), zoneName)
	if recordName == "" {
		return nil, errors.New("cloudflare record_name is required")
	}

	ttl := payload.TTL
	if ttl <= 0 {
		ttl = 120
	}

	proxied := configValue.Proxied
	if payload.Proxied != nil {
		proxied = *payload.Proxied
	}

	client := newCloudflareDNSClientFunc(configValue)
	if client == nil {
		return nil, errors.New("cloudflare dns client is not configured")
	}
	if zoneID == "" {
		if zoneName == "" {
			return nil, errors.New("cloudflare zone_id or zone_name is required")
		}
		resolvedZoneID, resolveErr := client.resolveZoneID(context.Background(), zoneName)
		if resolveErr != nil {
			return nil, resolveErr
		}
		zoneID = resolvedZoneID
	}

	line := strings.TrimSpace(member.DNSLine)
	if line == "" {
		line = "default"
	}

	return &cloudflareMemberDNSOperation{
		payload:    payload,
		config:     configValue,
		client:     client,
		zoneID:     zoneID,
		zoneName:   zoneName,
		recordName: recordName,
		line:       line,
		ttl:        ttl,
		proxied:    proxied,
		recordRefs: DecodeCloudflareMemberDNSRecordRefs(member.DNSRecordRefs),
	}, nil
}

func normalizeCloudflareRecordName(recordName, zoneName string) string {
	recordName = strings.TrimSpace(recordName)
	zoneName = normalizeServiceDNSDomainName(zoneName)
	if recordName == "@" {
		return zoneName
	}
	if recordName != "" && zoneName != "" && !strings.Contains(recordName, ".") {
		return strings.TrimSpace(recordName + "." + zoneName)
	}
	return normalizeServiceDNSDomainName(recordName)
}

func normalizeCloudflareOwnershipRR(recordName, zoneName string) string {
	recordName = normalizeCloudflareRecordName(recordName, zoneName)
	zoneName = normalizeServiceDNSDomainName(zoneName)
	if recordName == "" {
		return "@"
	}
	if zoneName == "" || recordName == zoneName {
		return "@"
	}
	suffix := "." + zoneName
	if strings.HasSuffix(recordName, suffix) {
		recordName = strings.TrimSuffix(recordName, suffix)
	}
	recordName = strings.Trim(recordName, ".")
	if recordName == "" {
		return "@"
	}
	return strings.ToLower(recordName)
}

func buildCloudflareMemberDNSRecord(zoneID, zoneName, recordName, line string, record cloudflareDNSRecord) CloudflareMemberDNSRecord {
	proxied := record.Proxied
	return CloudflareMemberDNSRecord{
		Provider: models.FailoverDNSProviderCloudflare,
		ID:       strings.TrimSpace(record.ID),
		Name:     firstNonEmpty(strings.TrimSpace(record.Name), recordName),
		Type:     strings.ToUpper(strings.TrimSpace(record.Type)),
		Value:    strings.TrimSpace(record.Content),
		ZoneID:   strings.TrimSpace(zoneID),
		ZoneName: strings.TrimSpace(zoneName),
		Line:     strings.TrimSpace(line),
		TTL:      record.TTL,
		Proxied:  &proxied,
	}
}

func sortCloudflareMemberDNSRecords(records []CloudflareMemberDNSRecord) {
	sort.Slice(records, func(i, j int) bool {
		left := strings.Join([]string{
			strings.ToLower(strings.TrimSpace(records[i].ZoneName)),
			strings.ToLower(strings.TrimSpace(records[i].Name)),
			strings.ToLower(strings.TrimSpace(records[i].Line)),
			strings.ToLower(strings.TrimSpace(records[i].Type)),
			strings.TrimSpace(records[i].Value),
			strings.TrimSpace(records[i].ID),
		}, "|")
		right := strings.Join([]string{
			strings.ToLower(strings.TrimSpace(records[j].ZoneName)),
			strings.ToLower(strings.TrimSpace(records[j].Name)),
			strings.ToLower(strings.TrimSpace(records[j].Line)),
			strings.ToLower(strings.TrimSpace(records[j].Type)),
			strings.TrimSpace(records[j].Value),
			strings.TrimSpace(records[j].ID),
		}, "|")
		return left < right
	})
}

func containsCloudflareRecord(records []CloudflareMemberDNSRecord, recordID string) bool {
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

func containsRecordType(values []string, target string) bool {
	target = strings.ToUpper(strings.TrimSpace(target))
	for _, value := range values {
		if strings.ToUpper(strings.TrimSpace(value)) == target {
			return true
		}
	}
	return false
}

func sameCloudflareRecordIdentity(record cloudflareDNSRecord, name, recordType string) bool {
	return strings.EqualFold(strings.TrimSpace(record.Name), strings.TrimSpace(name)) &&
		strings.EqualFold(strings.TrimSpace(record.Type), strings.TrimSpace(recordType))
}

func findCloudflareDNSRecordByID(records []cloudflareDNSRecord, recordID string) cloudflareDNSRecord {
	recordID = strings.TrimSpace(recordID)
	if recordID == "" {
		return cloudflareDNSRecord{}
	}
	for _, record := range records {
		if strings.TrimSpace(record.ID) == recordID {
			return record
		}
	}
	return cloudflareDNSRecord{}
}

func findCloudflareMatchingRecord(records []cloudflareDNSRecord, name, recordType, content string) cloudflareDNSRecord {
	for _, record := range records {
		if !sameCloudflareRecordIdentity(record, name, recordType) || !sameAddress(record.Content, content) {
			continue
		}
		return record
	}
	return cloudflareDNSRecord{}
}

func findOwnedOrMatchingCloudflareRecord(records []cloudflareDNSRecord, recordID, name, recordType, content string) cloudflareDNSRecord {
	record := findCloudflareDNSRecordByID(records, recordID)
	if strings.TrimSpace(record.ID) != "" {
		return record
	}
	return findCloudflareMatchingRecord(records, name, recordType, content)
}

func newCloudflareDNSClient(token string) *cloudflareDNSClient {
	return &cloudflareDNSClient{
		token:      strings.TrimSpace(token),
		baseURL:    "https://api.cloudflare.com/client/v4",
		httpClient: outboundproxy.NewDirectHTTPClient(20 * time.Second),
	}
}

func (c *cloudflareDNSClient) resolveZoneID(ctx context.Context, zoneName string) (string, error) {
	query := url.Values{}
	query.Set("name", strings.TrimSpace(zoneName))
	query.Set("status", "active")
	query.Set("per_page", "1")

	var response cloudflareAPIEnvelope[[]cloudflareZone]
	if err := c.do(ctx, http.MethodGet, strings.TrimRight(c.baseURL, "/")+"/zones?"+query.Encode(), nil, &response); err != nil {
		return "", err
	}
	if len(response.Result) == 0 {
		return "", fmt.Errorf("cloudflare zone not found: %s", zoneName)
	}
	return strings.TrimSpace(response.Result[0].ID), nil
}

func (c *cloudflareDNSClient) listRecords(ctx context.Context, zoneID string) ([]cloudflareDNSRecord, error) {
	const perPage = 100
	page := 1
	records := []cloudflareDNSRecord{}
	for {
		query := url.Values{}
		query.Set("page", fmt.Sprintf("%d", page))
		query.Set("per_page", fmt.Sprintf("%d", perPage))

		var response cloudflareAPIEnvelope[[]cloudflareDNSRecord]
		if err := c.do(ctx, http.MethodGet, fmt.Sprintf("%s/zones/%s/dns_records?%s", strings.TrimRight(c.baseURL, "/"), strings.TrimSpace(zoneID), query.Encode()), nil, &response); err != nil {
			return nil, err
		}
		records = append(records, response.Result...)
		if response.ResultInfo.TotalPages > 0 {
			if page >= response.ResultInfo.TotalPages {
				break
			}
		} else if len(response.Result) < perPage {
			break
		}
		page++
	}
	return records, nil
}

func (c *cloudflareDNSClient) upsertRecord(ctx context.Context, zoneID, existingRecordID, name, recordType, content string, ttl int, proxied bool) (*cloudflareDNSRecord, error) {
	requestBody := map[string]interface{}{
		"type":    strings.ToUpper(strings.TrimSpace(recordType)),
		"name":    strings.TrimSpace(name),
		"content": strings.TrimSpace(content),
		"ttl":     ttl,
		"proxied": proxied,
	}

	if strings.TrimSpace(existingRecordID) != "" {
		var updateResponse cloudflareAPIEnvelope[cloudflareDNSRecord]
		if err := c.do(ctx, http.MethodPut, fmt.Sprintf("%s/zones/%s/dns_records/%s", strings.TrimRight(c.baseURL, "/"), strings.TrimSpace(zoneID), strings.TrimSpace(existingRecordID)), requestBody, &updateResponse); err != nil {
			return nil, err
		}
		return &updateResponse.Result, nil
	}

	var createResponse cloudflareAPIEnvelope[cloudflareDNSRecord]
	if err := c.do(ctx, http.MethodPost, fmt.Sprintf("%s/zones/%s/dns_records", strings.TrimRight(c.baseURL, "/"), strings.TrimSpace(zoneID)), requestBody, &createResponse); err != nil {
		return nil, err
	}
	return &createResponse.Result, nil
}

func (c *cloudflareDNSClient) deleteRecord(ctx context.Context, zoneID, recordID string) error {
	return c.do(ctx, http.MethodDelete, fmt.Sprintf("%s/zones/%s/dns_records/%s", strings.TrimRight(c.baseURL, "/"), strings.TrimSpace(zoneID), strings.TrimSpace(recordID)), nil, &cloudflareAPIEnvelope[map[string]interface{}]{})
}

func (c *cloudflareDNSClient) do(ctx context.Context, method, targetURL string, payload any, out any) error {
	var body io.Reader
	if payload != nil {
		bodyBytes, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		body = bytes.NewReader(bodyBytes)
	}

	request, err := http.NewRequestWithContext(contextOrBackground(ctx), method, targetURL, body)
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+c.token)
	request.Header.Set("Accept", "application/json")
	if payload != nil {
		request.Header.Set("Content-Type", "application/json")
	}

	response, err := c.httpClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		return err
	}
	if out != nil {
		if err := json.Unmarshal(responseBody, out); err != nil {
			return err
		}
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("cloudflare api request failed: %s", response.Status)
	}

	var envelope cloudflareAPIEnvelope[json.RawMessage]
	if err := json.Unmarshal(responseBody, &envelope); err == nil && !envelope.Success && len(envelope.Errors) > 0 {
		return errors.New(envelope.Errors[0].Message)
	}
	return nil
}

func ptrBool(value bool) *bool {
	result := value
	return &result
}
