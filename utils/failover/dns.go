package failover

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/komari-monitor/komari/utils/outboundproxy"
)

type dnsUpdateResult struct {
	Provider     string                 `json:"provider"`
	ID           string                 `json:"id,omitempty"`
	Name         string                 `json:"name,omitempty"`
	Type         string                 `json:"type,omitempty"`
	Value        string                 `json:"value,omitempty"`
	ZoneID       string                 `json:"zone_id,omitempty"`
	ZoneName     string                 `json:"zone_name,omitempty"`
	Domain       string                 `json:"domain,omitempty"`
	RR           string                 `json:"rr,omitempty"`
	Line         string                 `json:"line,omitempty"`
	Proxied      *bool                  `json:"proxied,omitempty"`
	Records      []dnsUpdateResult      `json:"records,omitempty"`
	SkippedTypes []string               `json:"skipped_types,omitempty"`
	PrunedTypes  []string               `json:"pruned_types,omitempty"`
	Removed      []dnsUpdateResult      `json:"removed_records,omitempty"`
	Verification *dnsVerificationResult `json:"verification,omitempty"`
}

type DNSUpdateResult = dnsUpdateResult

type dnsVerificationResult struct {
	Provider   string            `json:"provider,omitempty"`
	Success    bool              `json:"success"`
	Attempts   int               `json:"attempts,omitempty"`
	Expected   []dnsUpdateResult `json:"expected_records,omitempty"`
	Observed   []dnsUpdateResult `json:"observed_records,omitempty"`
	Missing    []dnsUpdateResult `json:"missing_records,omitempty"`
	Unexpected []dnsUpdateResult `json:"unexpected_records,omitempty"`
}

type dnsVerificationError struct {
	Result  *dnsVerificationResult
	Message string
}

func (e *dnsVerificationError) Error() string {
	if e == nil {
		return ""
	}
	return strings.TrimSpace(e.Message)
}

type DNSCatalog struct {
	Provider string             `json:"provider"`
	Defaults DNSCatalogDefaults `json:"defaults"`
	Zones    []DNSOption        `json:"zones"`
	Domains  []DNSOption        `json:"domains"`
	Records  []DNSRecordOption  `json:"records"`
	Lines    []DNSOption        `json:"lines"`
	TTLs     []DNSOption        `json:"ttls"`
}

type DNSCatalogDefaults struct {
	ZoneID     string `json:"zone_id,omitempty"`
	ZoneName   string `json:"zone_name,omitempty"`
	DomainName string `json:"domain_name,omitempty"`
	Proxied    *bool  `json:"proxied,omitempty"`
}

type DNSRecordOption struct {
	ID         string `json:"id,omitempty"`
	Name       string `json:"name,omitempty"`
	Type       string `json:"type,omitempty"`
	Value      string `json:"value,omitempty"`
	TTL        int    `json:"ttl,omitempty"`
	ZoneID     string `json:"zone_id,omitempty"`
	ZoneName   string `json:"zone_name,omitempty"`
	DomainName string `json:"domain_name,omitempty"`
	RR         string `json:"rr,omitempty"`
	Line       string `json:"line,omitempty"`
	Proxied    *bool  `json:"proxied,omitempty"`
}

type DNSOption struct {
	Value string `json:"value"`
	Label string `json:"label"`
}

type cloudflareDNSPayload struct {
	ZoneID     string `json:"zone_id,omitempty"`
	ZoneName   string `json:"zone_name,omitempty"`
	RecordName string `json:"record_name,omitempty"`
	RecordType string `json:"record_type,omitempty"`
	SyncIPv6   bool   `json:"sync_ipv6,omitempty"`
	TTL        int    `json:"ttl,omitempty"`
	Proxied    *bool  `json:"proxied,omitempty"`
}

type aliyunRecordPayload struct {
	DomainName string   `json:"domain_name,omitempty"`
	RR         string   `json:"rr,omitempty"`
	RecordType string   `json:"record_type,omitempty"`
	SyncIPv6   bool     `json:"sync_ipv6,omitempty"`
	TTL        int      `json:"ttl,omitempty"`
	Line       string   `json:"line,omitempty"`
	Lines      []string `json:"lines,omitempty"`
}

func buildDNSTTLOptions(providerName string, records []DNSRecordOption) []DNSOption {
	base := []int{60, 120, 300, 600, 900, 1800, 3600, 7200}
	switch strings.ToLower(strings.TrimSpace(providerName)) {
	case cloudflareProviderName, aliyunProviderName:
		base = append([]int{1}, base...)
	}

	return buildDNSTTLOptionsFromValues(base, records)
}

func buildDNSTTLOptionsFromValues(base []int, records []DNSRecordOption) []DNSOption {
	if len(base) == 0 && len(records) == 0 {
		return nil
	}

	values := make([]int, 0, len(base)+len(records))
	values = append(values, base...)
	for _, record := range records {
		if record.TTL > 0 {
			values = append(values, record.TTL)
		}
	}

	seen := make(map[int]struct{}, len(values))
	options := make([]DNSOption, 0, len(values))
	for _, ttl := range values {
		if ttl <= 0 {
			continue
		}
		if _, ok := seen[ttl]; ok {
			continue
		}
		seen[ttl] = struct{}{}
		options = append(options, DNSOption{
			Value: strconv.Itoa(ttl),
			Label: strconv.Itoa(ttl),
		})
	}

	sort.Slice(options, func(i, j int) bool {
		left, _ := strconv.Atoi(options[i].Value)
		right, _ := strconv.Atoi(options[j].Value)
		return left < right
	})
	return options
}

func parseAliyunTTLValues(values []string) []int {
	ttls := make([]int, 0, len(values))
	for _, value := range values {
		text := strings.TrimSpace(value)
		if text == "" {
			continue
		}
		text = strings.TrimPrefix(text, "[")
		text = strings.TrimSuffix(text, "]")
		for _, item := range strings.Split(text, ",") {
			ttl, err := strconv.Atoi(strings.TrimSpace(item))
			if err != nil || ttl <= 0 {
				continue
			}
			ttls = append(ttls, ttl)
		}
	}
	return ttls
}

func buildDNSTTLOptionsFromMinTTL(minTTL int) []DNSOption {
	if minTTL <= 0 {
		return nil
	}

	candidates := []int{1, 60, 120, 300, 600, 900, 1800, 3600, 7200, 43200, 86400}
	values := make([]int, 0, len(candidates))
	for _, ttl := range candidates {
		if ttl >= minTTL {
			values = append(values, ttl)
		}
	}
	if len(values) == 0 {
		values = append(values, minTTL)
	}
	return buildDNSTTLOptionsFromValues(values, nil)
}

func LoadDNSCatalog(userUUID, providerName, entryID, zoneName, domainName string) (*DNSCatalog, error) {
	providerName = strings.ToLower(strings.TrimSpace(providerName))
	entryID = strings.TrimSpace(entryID)
	zoneName = strings.TrimSpace(zoneName)
	domainName = strings.TrimSpace(domainName)

	return loadCatalogWithCache(
		fmt.Sprintf("dns:%s:%s:%s:%s:%s", strings.TrimSpace(userUUID), providerName, entryID, zoneName, domainName),
		func() (*DNSCatalog, error) {
			switch providerName {
			case cloudflareProviderName:
				return loadCloudflareDNSCatalog(userUUID, entryID, zoneName)
			case aliyunProviderName:
				return loadAliyunDNSCatalog(userUUID, entryID, domainName)
			default:
				return nil, fmt.Errorf("unsupported dns provider: %s", providerName)
			}
		},
	)
}

func applyDNSRecord(ctx context.Context, userUUID, providerName, entryID, payloadJSON, ipv4, ipv6 string) (*dnsUpdateResult, error) {
	switch strings.ToLower(strings.TrimSpace(providerName)) {
	case cloudflareProviderName:
		return applyCloudflareDNSRecord(ctx, userUUID, entryID, payloadJSON, ipv4, ipv6)
	case aliyunProviderName:
		return applyAliyunDNSRecord(ctx, userUUID, entryID, payloadJSON, ipv4, ipv6)
	default:
		return nil, fmt.Errorf("unsupported dns provider: %s", providerName)
	}
}

func ApplyDNSRecord(userUUID, providerName, entryID, payloadJSON, ipv4, ipv6 string) (*DNSUpdateResult, error) {
	return applyDNSRecord(context.Background(), userUUID, providerName, entryID, payloadJSON, ipv4, ipv6)
}

const (
	dnsVerificationMaxAttempts = 3
	dnsVerificationRetryDelay  = 2 * time.Second
)

func verifyDNSRecord(ctx context.Context, userUUID, providerName, entryID, payloadJSON, ipv4, ipv6 string) (*dnsVerificationResult, error) {
	providerName = strings.ToLower(strings.TrimSpace(providerName))
	var (
		lastResult *dnsVerificationResult
		lastErr    error
	)

	for attempt := 1; attempt <= dnsVerificationMaxAttempts; attempt++ {
		var (
			result *dnsVerificationResult
			err    error
		)
		switch providerName {
		case cloudflareProviderName:
			result, err = evaluateCloudflareDNSVerification(ctx, userUUID, entryID, payloadJSON, ipv4, ipv6)
		case aliyunProviderName:
			result, err = evaluateAliyunDNSVerification(ctx, userUUID, entryID, payloadJSON, ipv4, ipv6)
		default:
			return nil, fmt.Errorf("unsupported dns provider: %s", providerName)
		}

		if result != nil {
			result.Attempts = attempt
			lastResult = result
		}
		if err == nil && result != nil && result.Success {
			return result, nil
		}
		if err != nil {
			lastErr = err
		} else if result != nil {
			lastErr = &dnsVerificationError{
				Result:  result,
				Message: buildDNSVerificationErrorMessage(result),
			}
		}
		if attempt < dnsVerificationMaxAttempts {
			if waitErr := waitContextOrDelay(ctx, dnsVerificationRetryDelay); waitErr != nil {
				return lastResult, waitErr
			}
		}
	}

	if verifyErr, ok := lastErr.(*dnsVerificationError); ok {
		verifyErr.Result = lastResult
	}
	return lastResult, lastErr
}

func buildDNSVerificationErrorMessage(result *dnsVerificationResult) string {
	if result == nil {
		return "dns verification failed"
	}

	parts := make([]string, 0, len(result.Missing)+len(result.Unexpected))
	if len(result.Missing) > 0 {
		parts = append(parts, "missing "+joinDNSRecordDescriptions(result.Missing))
	}
	if len(result.Unexpected) > 0 {
		parts = append(parts, "unexpected "+joinDNSRecordDescriptions(result.Unexpected))
	}
	if len(parts) == 0 {
		return "dns verification failed"
	}
	return "dns verification failed: " + strings.Join(parts, "; ")
}

func joinDNSRecordDescriptions(records []dnsUpdateResult) string {
	if len(records) == 0 {
		return ""
	}
	descriptions := make([]string, 0, len(records))
	for _, record := range records {
		description := strings.ToUpper(strings.TrimSpace(record.Type))
		target := firstNonEmpty(strings.TrimSpace(record.Name), strings.TrimSpace(record.RR))
		if target != "" {
			description += " " + target
		}
		if line := strings.TrimSpace(record.Line); line != "" {
			description += " [" + line + "]"
		}
		if value := strings.TrimSpace(record.Value); value != "" {
			description += " -> " + value
		}
		if description != "" {
			descriptions = append(descriptions, description)
		}
	}
	return strings.Join(descriptions, ", ")
}

type dnsApplyPlan struct {
	RecordTypes  []string
	SkippedTypes []string
	PrunedTypes  []string
}

func buildDNSApplyPlan(recordType string, syncIPv6 bool, ipv4, ipv6 string) (*dnsApplyPlan, error) {
	normalized := strings.ToUpper(strings.TrimSpace(recordType))
	if normalized == "" {
		normalized = "A"
	}
	counterpart := counterpartDNSRecordType(normalized)

	if !syncIPv6 {
		if _, err := selectRecordValue(normalized, ipv4, ipv6); err != nil {
			return nil, err
		}
		return &dnsApplyPlan{
			RecordTypes: []string{normalized},
			PrunedTypes: []string{counterpart},
		}, nil
	}

	if _, err := selectRecordValue(normalized, ipv4, ipv6); err != nil {
		return nil, err
	}

	plan := &dnsApplyPlan{
		RecordTypes: []string{normalized},
	}
	if _, err := selectRecordValue(counterpart, ipv4, ipv6); err == nil {
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

func summarizeDNSResults(results []dnsUpdateResult, skippedTypes, prunedTypes []string, removed []dnsUpdateResult) *dnsUpdateResult {
	if len(results) == 0 && len(removed) == 0 {
		return nil
	}
	if len(results) == 1 && len(skippedTypes) == 0 && len(prunedTypes) == 0 && len(removed) == 0 {
		result := results[0]
		return &result
	}
	summary := dnsUpdateResult{}
	if len(results) > 0 {
		summary = results[0]
	}
	summary.Records = append([]dnsUpdateResult(nil), results...)
	summary.SkippedTypes = append([]string(nil), skippedTypes...)
	summary.PrunedTypes = append([]string(nil), prunedTypes...)
	summary.Removed = append([]dnsUpdateResult(nil), removed...)
	return &summary
}

func cloneDNSUpdateResults(records []dnsUpdateResult) []dnsUpdateResult {
	if len(records) == 0 {
		return nil
	}
	cloned := make([]dnsUpdateResult, len(records))
	copy(cloned, records)
	return cloned
}

func sortDNSUpdateResults(records []dnsUpdateResult) {
	sort.Slice(records, func(i, j int) bool {
		left := strings.Join([]string{
			strings.ToLower(strings.TrimSpace(records[i].Name)),
			strings.ToLower(strings.TrimSpace(records[i].Domain)),
			strings.ToLower(strings.TrimSpace(records[i].RR)),
			strings.ToLower(strings.TrimSpace(records[i].Type)),
			strings.ToLower(strings.TrimSpace(records[i].Line)),
			strings.TrimSpace(records[i].Value),
			strings.TrimSpace(records[i].ID),
		}, "|")
		right := strings.Join([]string{
			strings.ToLower(strings.TrimSpace(records[j].Name)),
			strings.ToLower(strings.TrimSpace(records[j].Domain)),
			strings.ToLower(strings.TrimSpace(records[j].RR)),
			strings.ToLower(strings.TrimSpace(records[j].Type)),
			strings.ToLower(strings.TrimSpace(records[j].Line)),
			strings.TrimSpace(records[j].Value),
			strings.TrimSpace(records[j].ID),
		}, "|")
		return left < right
	})
}

func evaluateDNSVerificationRecords(provider string, expected, observed []dnsUpdateResult, match func(dnsUpdateResult, dnsUpdateResult) bool) *dnsVerificationResult {
	expected = cloneDNSUpdateResults(expected)
	observed = cloneDNSUpdateResults(observed)
	sortDNSUpdateResults(expected)
	sortDNSUpdateResults(observed)

	matchedObserved := make([]bool, len(observed))
	missing := make([]dnsUpdateResult, 0)
	for _, expectedRecord := range expected {
		found := false
		for index, observedRecord := range observed {
			if matchedObserved[index] {
				continue
			}
			if !match(expectedRecord, observedRecord) {
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

	unexpected := make([]dnsUpdateResult, 0)
	for index, observedRecord := range observed {
		if matchedObserved[index] {
			continue
		}
		unexpected = append(unexpected, observedRecord)
	}

	return &dnsVerificationResult{
		Provider:   strings.TrimSpace(provider),
		Success:    len(missing) == 0 && len(unexpected) == 0,
		Expected:   expected,
		Observed:   observed,
		Missing:    missing,
		Unexpected: unexpected,
	}
}

func cloudflareDNSRecordsMatch(expected, observed dnsUpdateResult) bool {
	if !strings.EqualFold(strings.TrimSpace(expected.Name), strings.TrimSpace(observed.Name)) ||
		!strings.EqualFold(strings.TrimSpace(expected.Type), strings.TrimSpace(observed.Type)) ||
		strings.TrimSpace(expected.Value) != strings.TrimSpace(observed.Value) {
		return false
	}
	if expected.Proxied != nil && observed.Proxied != nil && *expected.Proxied != *observed.Proxied {
		return false
	}
	if expected.Proxied != nil && observed.Proxied == nil {
		return false
	}
	return true
}

func aliyunDNSRecordsMatch(expected, observed dnsUpdateResult) bool {
	return sameAliyunRecordRR(expected.RR, observed.RR) &&
		strings.EqualFold(strings.TrimSpace(expected.Type), strings.TrimSpace(observed.Type)) &&
		sameAliyunRecordLine(expected.Line, observed.Line) &&
		strings.TrimSpace(expected.Value) == strings.TrimSpace(observed.Value)
}

func dnsRelevantRecordTypes(plan *dnsApplyPlan) map[string]struct{} {
	relevant := make(map[string]struct{}, len(plan.RecordTypes)+len(plan.PrunedTypes))
	if plan == nil {
		return relevant
	}
	for _, recordType := range plan.RecordTypes {
		relevant[strings.ToUpper(strings.TrimSpace(recordType))] = struct{}{}
	}
	for _, recordType := range plan.PrunedTypes {
		relevant[strings.ToUpper(strings.TrimSpace(recordType))] = struct{}{}
	}
	return relevant
}

func loadCloudflareDNSCatalog(userUUID, entryID, zoneName string) (*DNSCatalog, error) {
	entry, err := loadGenericProviderEntry(userUUID, cloudflareProviderName, entryID)
	if err != nil {
		return nil, err
	}
	configValue, err := decodeGenericEntryConfig[cloudflareConfig](entry)
	if err != nil {
		return nil, fmt.Errorf("cloudflare config is invalid: %w", err)
	}
	if strings.TrimSpace(configValue.APIToken) == "" {
		return nil, errors.New("cloudflare api_token is required")
	}

	catalog := &DNSCatalog{
		Provider: cloudflareProviderName,
		Defaults: DNSCatalogDefaults{
			ZoneID:   strings.TrimSpace(configValue.ZoneID),
			ZoneName: firstNonEmpty(strings.TrimSpace(zoneName), strings.TrimSpace(configValue.ZoneName)),
			Proxied:  ptrBool(configValue.Proxied),
		},
		Zones:   []DNSOption{},
		Domains: []DNSOption{},
		Records: []DNSRecordOption{},
		Lines:   []DNSOption{},
		TTLs:    buildDNSTTLOptions(cloudflareProviderName, nil),
	}

	client := newCloudflareDNSClient(configValue.APIToken)
	zones, err := client.listZones(context.Background())
	if err != nil {
		return nil, err
	}
	for _, zone := range zones {
		label := strings.TrimSpace(zone.Name)
		if label == "" {
			label = strings.TrimSpace(zone.ID)
		}
		catalog.Zones = append(catalog.Zones, DNSOption{
			Value: strings.TrimSpace(zone.Name),
			Label: label,
		})
	}

	if strings.TrimSpace(catalog.Defaults.ZoneName) == "" && len(catalog.Zones) == 1 {
		catalog.Defaults.ZoneName = strings.TrimSpace(catalog.Zones[0].Value)
	}

	resolvedZoneID := strings.TrimSpace(catalog.Defaults.ZoneID)
	if resolvedZoneID == "" && strings.TrimSpace(catalog.Defaults.ZoneName) != "" {
		resolvedZoneID, err = client.resolveZoneID(context.Background(), catalog.Defaults.ZoneName)
		if err != nil {
			return nil, err
		}
		catalog.Defaults.ZoneID = resolvedZoneID
	}
	if resolvedZoneID == "" {
		return catalog, nil
	}

	records, err := client.listRecords(context.Background(), resolvedZoneID)
	if err != nil {
		return nil, err
	}
	for _, record := range records {
		proxied := record.Proxied
		catalog.Records = append(catalog.Records, DNSRecordOption{
			ID:       strings.TrimSpace(record.ID),
			Name:     strings.TrimSpace(record.Name),
			Type:     strings.TrimSpace(record.Type),
			Value:    strings.TrimSpace(record.Content),
			TTL:      record.TTL,
			ZoneID:   resolvedZoneID,
			ZoneName: strings.TrimSpace(catalog.Defaults.ZoneName),
			Proxied:  &proxied,
		})
	}
	catalog.TTLs = buildDNSTTLOptions(cloudflareProviderName, catalog.Records)
	return catalog, nil
}

func loadAliyunDNSCatalog(userUUID, entryID, domainName string) (*DNSCatalog, error) {
	entry, err := loadGenericProviderEntry(userUUID, aliyunProviderName, entryID)
	if err != nil {
		return nil, err
	}
	configValue, err := decodeGenericEntryConfig[aliyunDNSConfig](entry)
	if err != nil {
		return nil, fmt.Errorf("aliyun dns config is invalid: %w", err)
	}
	if strings.TrimSpace(configValue.AccessKeyID) == "" || strings.TrimSpace(configValue.AccessKeySecret) == "" {
		return nil, errors.New("aliyun access_key_id and access_key_secret are required")
	}

	resolvedDomainName := firstNonEmpty(strings.TrimSpace(domainName), strings.TrimSpace(configValue.DomainName))
	catalog := &DNSCatalog{
		Provider: aliyunProviderName,
		Defaults: DNSCatalogDefaults{
			DomainName: resolvedDomainName,
		},
		Zones:   []DNSOption{},
		Domains: []DNSOption{},
		Records: []DNSRecordOption{},
		Lines:   defaultAliyunDNSLines(),
		TTLs:    buildDNSTTLOptions(aliyunProviderName, nil),
	}

	client := newAliyunDNSClient(configValue.AccessKeyID, configValue.AccessKeySecret, configValue.RegionID)
	domains, err := client.listDomains(context.Background())
	if err != nil {
		return nil, err
	}
	for _, domain := range domains {
		value := strings.TrimSpace(domain)
		if value == "" {
			continue
		}
		catalog.Domains = append(catalog.Domains, DNSOption{
			Value: value,
			Label: value,
		})
	}
	if strings.TrimSpace(catalog.Defaults.DomainName) == "" && len(catalog.Domains) == 1 {
		catalog.Defaults.DomainName = strings.TrimSpace(catalog.Domains[0].Value)
		resolvedDomainName = catalog.Defaults.DomainName
	}

	if strings.TrimSpace(resolvedDomainName) == "" {
		return catalog, nil
	}

	if info, infoErr := client.describeDomainInfo(context.Background(), resolvedDomainName); infoErr == nil && info != nil {
		if ttlValues := parseAliyunTTLValues(info.AvailableTTLs.AvailableTTL); len(ttlValues) > 0 {
			catalog.TTLs = buildDNSTTLOptionsFromValues(ttlValues, nil)
		} else if info.MinTTL > 0 {
			catalog.TTLs = buildDNSTTLOptionsFromMinTTL(info.MinTTL)
		}
	}

	if lines, lineErr := client.listLines(context.Background(), resolvedDomainName); lineErr == nil && len(lines) > 0 {
		catalog.Lines = lines
	}

	records, err := client.listRecords(context.Background(), resolvedDomainName)
	if err != nil {
		return nil, err
	}
	for _, record := range records {
		catalog.Records = append(catalog.Records, DNSRecordOption{
			ID:         strings.TrimSpace(record.RecordID),
			Name:       joinAliyunRecordName(resolvedDomainName, record.RR),
			Type:       strings.TrimSpace(record.Type),
			Value:      strings.TrimSpace(record.Value),
			TTL:        record.TTL,
			DomainName: resolvedDomainName,
			RR:         strings.TrimSpace(record.RR),
			Line:       strings.TrimSpace(record.Line),
		})
	}
	if len(catalog.TTLs) == 0 {
		catalog.TTLs = buildDNSTTLOptions(aliyunProviderName, catalog.Records)
	}
	return catalog, nil
}

func applyCloudflareDNSRecord(ctx context.Context, userUUID, entryID, payloadJSON, ipv4, ipv6 string) (*dnsUpdateResult, error) {
	entry, err := loadGenericProviderEntry(userUUID, cloudflareProviderName, entryID)
	if err != nil {
		return nil, err
	}
	configValue, err := decodeGenericEntryConfig[cloudflareConfig](entry)
	if err != nil {
		return nil, fmt.Errorf("cloudflare config is invalid: %w", err)
	}
	if strings.TrimSpace(configValue.APIToken) == "" {
		return nil, errors.New("cloudflare api_token is required")
	}

	var payload cloudflareDNSPayload
	if strings.TrimSpace(payloadJSON) != "" {
		if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
			return nil, fmt.Errorf("cloudflare dns payload is invalid: %w", err)
		}
	}

	plan, err := buildDNSApplyPlan(payload.RecordType, payload.SyncIPv6, ipv4, ipv6)
	if err != nil {
		return nil, err
	}

	zoneID := strings.TrimSpace(payload.ZoneID)
	zoneName := strings.TrimSpace(payload.ZoneName)
	if zoneID == "" {
		zoneID = strings.TrimSpace(configValue.ZoneID)
	}
	if zoneName == "" {
		zoneName = strings.TrimSpace(configValue.ZoneName)
	}

	recordName := strings.TrimSpace(payload.RecordName)
	if recordName == "" {
		recordName = zoneName
	}
	recordName = normalizeCloudflareRecordName(recordName, zoneName)
	if recordName == "" {
		return nil, errors.New("cloudflare record_name is required")
	}

	proxied := configValue.Proxied
	if payload.Proxied != nil {
		proxied = *payload.Proxied
	}
	ttl := payload.TTL
	if ttl <= 0 {
		ttl = 120
	}

	client := newCloudflareDNSClient(configValue.APIToken)
	if zoneID == "" {
		if zoneName == "" {
			return nil, errors.New("cloudflare zone_id or zone_name is required")
		}
		zoneID, err = client.resolveZoneID(contextOrBackground(ctx), zoneName)
		if err != nil {
			return nil, normalizeExecutionStopError(err)
		}
	}

	existingRecords, err := client.listRecords(contextOrBackground(ctx), zoneID)
	if err != nil {
		return nil, normalizeExecutionStopError(err)
	}

	results := make([]dnsUpdateResult, 0, len(plan.RecordTypes))
	removed := make([]dnsUpdateResult, 0)
	removedRecordIDs := map[string]struct{}{}
	for _, currentRecordType := range plan.RecordTypes {
		recordValue, _ := selectRecordValue(currentRecordType, ipv4, ipv6)
		existingMatches := filterCloudflareRecords(existingRecords, recordName, currentRecordType)
		record, err := client.upsertRecord(contextOrBackground(ctx), zoneID, existingMatches, recordName, currentRecordType, recordValue, ttl, proxied)
		if err != nil {
			return nil, normalizeExecutionStopError(err)
		}
		results = append(results, dnsUpdateResult{
			Provider: cloudflareProviderName,
			ID:       record.ID,
			Name:     record.Name,
			Type:     record.Type,
			Value:    record.Content,
			ZoneID:   zoneID,
			ZoneName: zoneName,
		})
		for _, existingRecord := range existingMatches {
			if strings.TrimSpace(existingRecord.ID) == "" || strings.TrimSpace(existingRecord.ID) == strings.TrimSpace(record.ID) {
				continue
			}
			if _, ok := removedRecordIDs[strings.TrimSpace(existingRecord.ID)]; ok {
				continue
			}
			if err := client.deleteRecord(contextOrBackground(ctx), zoneID, existingRecord.ID); err != nil {
				return nil, normalizeExecutionStopError(err)
			}
			removedRecordIDs[strings.TrimSpace(existingRecord.ID)] = struct{}{}
			removed = append(removed, dnsUpdateResult{
				Provider: cloudflareProviderName,
				ID:       existingRecord.ID,
				Name:     existingRecord.Name,
				Type:     existingRecord.Type,
				Value:    existingRecord.Content,
				ZoneID:   zoneID,
				ZoneName: zoneName,
			})
		}
	}
	for _, pruneType := range plan.PrunedTypes {
		for _, existingRecord := range filterCloudflareRecords(existingRecords, recordName, pruneType) {
			if strings.TrimSpace(existingRecord.ID) == "" {
				continue
			}
			if _, ok := removedRecordIDs[strings.TrimSpace(existingRecord.ID)]; ok {
				continue
			}
			if err := client.deleteRecord(contextOrBackground(ctx), zoneID, existingRecord.ID); err != nil {
				return nil, normalizeExecutionStopError(err)
			}
			removedRecordIDs[strings.TrimSpace(existingRecord.ID)] = struct{}{}
			removed = append(removed, dnsUpdateResult{
				Provider: cloudflareProviderName,
				ID:       existingRecord.ID,
				Name:     existingRecord.Name,
				Type:     existingRecord.Type,
				Value:    existingRecord.Content,
				ZoneID:   zoneID,
				ZoneName: zoneName,
			})
		}
	}
	return summarizeDNSResults(results, plan.SkippedTypes, plan.PrunedTypes, removed), nil
}

func normalizeCloudflareRecordName(recordName, zoneName string) string {
	recordName = strings.TrimSpace(recordName)
	zoneName = strings.TrimSpace(zoneName)
	if recordName == "@" {
		return zoneName
	}
	if recordName != "" && zoneName != "" && !strings.Contains(recordName, ".") {
		return recordName + "." + zoneName
	}
	return recordName
}

func applyAliyunDNSRecord(ctx context.Context, userUUID, entryID, payloadJSON, ipv4, ipv6 string) (*dnsUpdateResult, error) {
	entry, err := loadGenericProviderEntry(userUUID, aliyunProviderName, entryID)
	if err != nil {
		return nil, err
	}
	configValue, err := decodeGenericEntryConfig[aliyunDNSConfig](entry)
	if err != nil {
		return nil, fmt.Errorf("aliyun dns config is invalid: %w", err)
	}
	if strings.TrimSpace(configValue.AccessKeyID) == "" || strings.TrimSpace(configValue.AccessKeySecret) == "" {
		return nil, errors.New("aliyun access_key_id and access_key_secret are required")
	}

	var payload aliyunRecordPayload
	if strings.TrimSpace(payloadJSON) != "" {
		if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
			return nil, fmt.Errorf("aliyun dns payload is invalid: %w", err)
		}
	}

	plan, err := buildDNSApplyPlan(payload.RecordType, payload.SyncIPv6, ipv4, ipv6)
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
	ttl := payload.TTL
	if ttl <= 0 {
		ttl = 600
	}
	client := newAliyunDNSClient(configValue.AccessKeyID, configValue.AccessKeySecret, configValue.RegionID)
	lines := normalizeAliyunLines(payload.Line, payload.Lines)
	existingRecords, err := client.listRecords(contextOrBackground(ctx), domainName)
	if err != nil {
		return nil, normalizeExecutionStopError(err)
	}
	results := make([]dnsUpdateResult, 0, len(plan.RecordTypes))
	removed := make([]dnsUpdateResult, 0)
	removedRecordIDs := map[string]struct{}{}
	desiredLines := make(map[string]struct{}, len(lines))
	for _, line := range lines {
		desiredLines[normalizeAliyunLineIdentity(line)] = struct{}{}
	}
	for _, currentRecordType := range plan.RecordTypes {
		recordValue, _ := selectRecordValue(currentRecordType, ipv4, ipv6)
		recordIDs := make([]string, 0, len(lines))
		for _, line := range lines {
			existingMatch := findAliyunDNSRecord(existingRecords, rr, currentRecordType, line)
			recordID, err := client.upsertRecord(contextOrBackground(ctx), strings.TrimSpace(existingMatch.RecordID), domainName, rr, currentRecordType, recordValue, ttl, line)
			if err != nil {
				return nil, normalizeExecutionStopError(err)
			}
			recordIDs = append(recordIDs, recordID)
			for _, existingRecord := range existingRecords {
				if !sameAliyunRecordLine(existingRecord.Line, line) || !sameAliyunRecordIdentity(existingRecord, rr, currentRecordType) {
					continue
				}
				if strings.TrimSpace(existingRecord.RecordID) == "" || strings.TrimSpace(existingRecord.RecordID) == strings.TrimSpace(recordID) {
					continue
				}
				if _, ok := removedRecordIDs[strings.TrimSpace(existingRecord.RecordID)]; ok {
					continue
				}
				if err := client.deleteRecord(contextOrBackground(ctx), existingRecord.RecordID); err != nil {
					return nil, normalizeExecutionStopError(err)
				}
				removedRecordIDs[strings.TrimSpace(existingRecord.RecordID)] = struct{}{}
				removed = append(removed, dnsUpdateResult{
					Provider: aliyunProviderName,
					ID:       existingRecord.RecordID,
					Name:     joinAliyunRecordName(domainName, existingRecord.RR),
					Type:     existingRecord.Type,
					Value:    existingRecord.Value,
					Domain:   domainName,
					RR:       existingRecord.RR,
				})
			}
		}
		results = append(results, dnsUpdateResult{
			Provider: aliyunProviderName,
			ID:       strings.Join(recordIDs, ","),
			Type:     currentRecordType,
			Value:    recordValue,
			Domain:   domainName,
			RR:       rr,
		})
	}
	desiredTypes := make(map[string]struct{}, len(plan.RecordTypes))
	for _, recordType := range plan.RecordTypes {
		desiredTypes[strings.ToUpper(strings.TrimSpace(recordType))] = struct{}{}
	}
	prunedTypes := make(map[string]struct{}, len(plan.PrunedTypes))
	for _, recordType := range plan.PrunedTypes {
		prunedTypes[strings.ToUpper(strings.TrimSpace(recordType))] = struct{}{}
	}
	for _, existingRecord := range existingRecords {
		if !sameAliyunRecordRR(existingRecord.RR, rr) || strings.TrimSpace(existingRecord.RecordID) == "" {
			continue
		}
		recordID := strings.TrimSpace(existingRecord.RecordID)
		if _, ok := removedRecordIDs[recordID]; ok {
			continue
		}
		recordType := strings.ToUpper(strings.TrimSpace(existingRecord.Type))
		if _, ok := prunedTypes[recordType]; ok {
			if err := client.deleteRecord(contextOrBackground(ctx), recordID); err != nil {
				return nil, normalizeExecutionStopError(err)
			}
			removedRecordIDs[recordID] = struct{}{}
			removed = append(removed, dnsUpdateResult{
				Provider: aliyunProviderName,
				ID:       recordID,
				Name:     joinAliyunRecordName(domainName, existingRecord.RR),
				Type:     existingRecord.Type,
				Value:    existingRecord.Value,
				Domain:   domainName,
				RR:       existingRecord.RR,
			})
			continue
		}
		if _, ok := desiredTypes[recordType]; ok {
			if _, ok := desiredLines[normalizeAliyunLineIdentity(existingRecord.Line)]; ok {
				continue
			}
			if err := client.deleteRecord(contextOrBackground(ctx), recordID); err != nil {
				return nil, normalizeExecutionStopError(err)
			}
			removedRecordIDs[recordID] = struct{}{}
			removed = append(removed, dnsUpdateResult{
				Provider: aliyunProviderName,
				ID:       recordID,
				Name:     joinAliyunRecordName(domainName, existingRecord.RR),
				Type:     existingRecord.Type,
				Value:    existingRecord.Value,
				Domain:   domainName,
				RR:       existingRecord.RR,
			})
		}
	}
	return summarizeDNSResults(results, plan.SkippedTypes, plan.PrunedTypes, removed), nil
}

func evaluateCloudflareDNSVerification(ctx context.Context, userUUID, entryID, payloadJSON, ipv4, ipv6 string) (*dnsVerificationResult, error) {
	entry, err := loadGenericProviderEntry(userUUID, cloudflareProviderName, entryID)
	if err != nil {
		return nil, err
	}
	configValue, err := decodeGenericEntryConfig[cloudflareConfig](entry)
	if err != nil {
		return nil, fmt.Errorf("cloudflare config is invalid: %w", err)
	}
	if strings.TrimSpace(configValue.APIToken) == "" {
		return nil, errors.New("cloudflare api_token is required")
	}

	var payload cloudflareDNSPayload
	if strings.TrimSpace(payloadJSON) != "" {
		if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
			return nil, fmt.Errorf("cloudflare dns payload is invalid: %w", err)
		}
	}

	plan, err := buildDNSApplyPlan(payload.RecordType, payload.SyncIPv6, ipv4, ipv6)
	if err != nil {
		return nil, err
	}

	zoneID := strings.TrimSpace(payload.ZoneID)
	zoneName := strings.TrimSpace(payload.ZoneName)
	if zoneID == "" {
		zoneID = strings.TrimSpace(configValue.ZoneID)
	}
	if zoneName == "" {
		zoneName = strings.TrimSpace(configValue.ZoneName)
	}

	recordName := normalizeCloudflareRecordName(firstNonEmpty(strings.TrimSpace(payload.RecordName), zoneName), zoneName)
	if recordName == "" {
		return nil, errors.New("cloudflare record_name is required")
	}

	proxied := configValue.Proxied
	if payload.Proxied != nil {
		proxied = *payload.Proxied
	}

	client := newCloudflareDNSClient(configValue.APIToken)
	if zoneID == "" {
		if zoneName == "" {
			return nil, errors.New("cloudflare zone_id or zone_name is required")
		}
		zoneID, err = client.resolveZoneID(contextOrBackground(ctx), zoneName)
		if err != nil {
			return nil, normalizeExecutionStopError(err)
		}
	}

	records, err := client.listRecords(contextOrBackground(ctx), zoneID)
	if err != nil {
		return nil, normalizeExecutionStopError(err)
	}

	relevantTypes := dnsRelevantRecordTypes(plan)
	expected := make([]dnsUpdateResult, 0, len(plan.RecordTypes))
	for _, recordType := range plan.RecordTypes {
		recordValue, _ := selectRecordValue(recordType, ipv4, ipv6)
		expected = append(expected, dnsUpdateResult{
			Provider: cloudflareProviderName,
			Name:     recordName,
			Type:     strings.ToUpper(strings.TrimSpace(recordType)),
			Value:    recordValue,
			ZoneID:   zoneID,
			ZoneName: zoneName,
			Proxied:  ptrBool(proxied),
		})
	}

	observed := make([]dnsUpdateResult, 0)
	for _, record := range records {
		recordType := strings.ToUpper(strings.TrimSpace(record.Type))
		if !strings.EqualFold(strings.TrimSpace(record.Name), recordName) {
			continue
		}
		if _, ok := relevantTypes[recordType]; !ok {
			continue
		}
		proxiedValue := record.Proxied
		observed = append(observed, dnsUpdateResult{
			Provider: cloudflareProviderName,
			ID:       strings.TrimSpace(record.ID),
			Name:     strings.TrimSpace(record.Name),
			Type:     recordType,
			Value:    strings.TrimSpace(record.Content),
			ZoneID:   zoneID,
			ZoneName: zoneName,
			Proxied:  &proxiedValue,
		})
	}

	return evaluateDNSVerificationRecords(cloudflareProviderName, expected, observed, cloudflareDNSRecordsMatch), nil
}

func evaluateAliyunDNSVerification(ctx context.Context, userUUID, entryID, payloadJSON, ipv4, ipv6 string) (*dnsVerificationResult, error) {
	entry, err := loadGenericProviderEntry(userUUID, aliyunProviderName, entryID)
	if err != nil {
		return nil, err
	}
	configValue, err := decodeGenericEntryConfig[aliyunDNSConfig](entry)
	if err != nil {
		return nil, fmt.Errorf("aliyun dns config is invalid: %w", err)
	}
	if strings.TrimSpace(configValue.AccessKeyID) == "" || strings.TrimSpace(configValue.AccessKeySecret) == "" {
		return nil, errors.New("aliyun access_key_id and access_key_secret are required")
	}

	var payload aliyunRecordPayload
	if strings.TrimSpace(payloadJSON) != "" {
		if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
			return nil, fmt.Errorf("aliyun dns payload is invalid: %w", err)
		}
	}

	plan, err := buildDNSApplyPlan(payload.RecordType, payload.SyncIPv6, ipv4, ipv6)
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
	lines := normalizeAliyunLines(payload.Line, payload.Lines)
	client := newAliyunDNSClient(configValue.AccessKeyID, configValue.AccessKeySecret, configValue.RegionID)
	records, err := client.listRecords(contextOrBackground(ctx), domainName)
	if err != nil {
		return nil, normalizeExecutionStopError(err)
	}

	relevantTypes := dnsRelevantRecordTypes(plan)
	expected := make([]dnsUpdateResult, 0, len(plan.RecordTypes)*len(lines))
	for _, recordType := range plan.RecordTypes {
		recordValue, _ := selectRecordValue(recordType, ipv4, ipv6)
		for _, line := range lines {
			expected = append(expected, dnsUpdateResult{
				Provider: aliyunProviderName,
				Domain:   domainName,
				RR:       rr,
				Line:     canonicalAliyunLineValue(line),
				Type:     strings.ToUpper(strings.TrimSpace(recordType)),
				Value:    recordValue,
			})
		}
	}

	observed := make([]dnsUpdateResult, 0)
	for _, record := range records {
		recordType := strings.ToUpper(strings.TrimSpace(record.Type))
		if !sameAliyunRecordRR(record.RR, rr) {
			continue
		}
		if _, ok := relevantTypes[recordType]; !ok {
			continue
		}
		observed = append(observed, dnsUpdateResult{
			Provider: aliyunProviderName,
			ID:       strings.TrimSpace(record.RecordID),
			Domain:   domainName,
			RR:       strings.TrimSpace(record.RR),
			Line:     strings.TrimSpace(record.Line),
			Type:     recordType,
			Value:    strings.TrimSpace(record.Value),
		})
	}

	return evaluateDNSVerificationRecords(aliyunProviderName, expected, observed, aliyunDNSRecordsMatch), nil
}

func normalizeAliyunLines(primary string, values []string) []string {
	normalized := make([]string, 0, len(values)+1)
	seen := map[string]struct{}{}
	appendValue := func(value string) {
		value = canonicalAliyunLineValue(value)
		if value == "" {
			return
		}
		identity := normalizeAliyunLineIdentity(value)
		if _, ok := seen[identity]; ok {
			return
		}
		seen[identity] = struct{}{}
		normalized = append(normalized, value)
	}

	appendValue(primary)
	for _, value := range values {
		appendValue(value)
	}
	if len(normalized) == 0 {
		normalized = append(normalized, "default")
	}
	return normalized
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

func findAliyunDNSRecord(records []aliyunDNSRecord, rr, recordType, line string) aliyunDNSRecord {
	for _, record := range records {
		if !sameAliyunRecordIdentity(record, rr, recordType) || !sameAliyunRecordLine(record.Line, line) {
			continue
		}
		return record
	}
	return aliyunDNSRecord{}
}

func filterCloudflareRecords(records []cloudflareDNSRecord, name, recordType string) []cloudflareDNSRecord {
	filtered := make([]cloudflareDNSRecord, 0)
	for _, record := range records {
		if !strings.EqualFold(strings.TrimSpace(record.Name), strings.TrimSpace(name)) || !strings.EqualFold(strings.TrimSpace(record.Type), strings.TrimSpace(recordType)) {
			continue
		}
		filtered = append(filtered, record)
	}
	return filtered
}

func selectRecordValue(recordType, ipv4, ipv6 string) (string, error) {
	switch strings.ToUpper(strings.TrimSpace(recordType)) {
	case "A":
		return normalizeDNSRecordIPValue("ipv4", "A", ipv4, true)
	case "AAAA":
		return normalizeDNSRecordIPValue("ipv6", "AAAA", ipv6, false)
	default:
		return "", fmt.Errorf("unsupported DNS record type: %s", recordType)
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

type cloudflareDNSClient struct {
	token      string
	httpClient *http.Client
}

type cloudflareAPIEnvelope[T any] struct {
	Success bool `json:"success"`
	Errors  []struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"errors"`
	Result T `json:"result"`
}

type cloudflareZone struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

func (c *cloudflareDNSClient) listZones(ctx context.Context) ([]cloudflareZone, error) {
	query := url.Values{}
	query.Set("status", "active")
	query.Set("per_page", "100")

	var response cloudflareAPIEnvelope[[]cloudflareZone]
	if err := c.do(ctx, http.MethodGet, "https://api.cloudflare.com/client/v4/zones?"+query.Encode(), nil, &response); err != nil {
		return nil, err
	}
	return response.Result, nil
}

type cloudflareDNSRecord struct {
	ID      string `json:"id"`
	Type    string `json:"type"`
	Name    string `json:"name"`
	Content string `json:"content"`
	TTL     int    `json:"ttl"`
	Proxied bool   `json:"proxied"`
}

func newCloudflareDNSClient(token string) *cloudflareDNSClient {
	return &cloudflareDNSClient{
		token:      strings.TrimSpace(token),
		httpClient: outboundproxy.NewDirectHTTPClient(20 * time.Second),
	}
}

func (c *cloudflareDNSClient) resolveZoneID(ctx context.Context, zoneName string) (string, error) {
	query := url.Values{}
	query.Set("name", strings.TrimSpace(zoneName))
	query.Set("status", "active")
	query.Set("per_page", "1")

	var response cloudflareAPIEnvelope[[]cloudflareZone]
	if err := c.do(ctx, http.MethodGet, "https://api.cloudflare.com/client/v4/zones?"+query.Encode(), nil, &response); err != nil {
		return "", err
	}
	if len(response.Result) == 0 {
		return "", fmt.Errorf("cloudflare zone not found: %s", zoneName)
	}
	return strings.TrimSpace(response.Result[0].ID), nil
}

func (c *cloudflareDNSClient) listRecords(ctx context.Context, zoneID string) ([]cloudflareDNSRecord, error) {
	query := url.Values{}
	query.Set("per_page", "100")

	var response cloudflareAPIEnvelope[[]cloudflareDNSRecord]
	if err := c.do(ctx, http.MethodGet, fmt.Sprintf("https://api.cloudflare.com/client/v4/zones/%s/dns_records?%s", strings.TrimSpace(zoneID), query.Encode()), nil, &response); err != nil {
		return nil, err
	}
	return response.Result, nil
}

func (c *cloudflareDNSClient) upsertRecord(ctx context.Context, zoneID string, existing []cloudflareDNSRecord, name, recordType, content string, ttl int, proxied bool) (*cloudflareDNSRecord, error) {
	requestBody := map[string]interface{}{
		"type":    recordType,
		"name":    name,
		"content": content,
		"ttl":     ttl,
		"proxied": proxied,
	}

	if len(existing) > 0 {
		record := existing[0]
		var updateResponse cloudflareAPIEnvelope[cloudflareDNSRecord]
		if err := c.do(ctx, http.MethodPut, fmt.Sprintf("https://api.cloudflare.com/client/v4/zones/%s/dns_records/%s", strings.TrimSpace(zoneID), record.ID), requestBody, &updateResponse); err != nil {
			return nil, err
		}
		return &updateResponse.Result, nil
	}

	var createResponse cloudflareAPIEnvelope[cloudflareDNSRecord]
	if err := c.do(ctx, http.MethodPost, fmt.Sprintf("https://api.cloudflare.com/client/v4/zones/%s/dns_records", strings.TrimSpace(zoneID)), requestBody, &createResponse); err != nil {
		return nil, err
	}
	return &createResponse.Result, nil
}

func (c *cloudflareDNSClient) deleteRecord(ctx context.Context, zoneID, recordID string) error {
	return c.do(ctx, http.MethodDelete, fmt.Sprintf("https://api.cloudflare.com/client/v4/zones/%s/dns_records/%s", strings.TrimSpace(zoneID), strings.TrimSpace(recordID)), nil, &cloudflareAPIEnvelope[map[string]interface{}]{})
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

	request, err := http.NewRequestWithContext(ctx, method, targetURL, body)
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

type aliyunDNSClient struct {
	accessKeyID     string
	accessKeySecret string
	endpoint        string
	httpClient      *http.Client
}

type aliyunDescribeRecordsResponse struct {
	DomainRecords struct {
		Record []aliyunDNSRecord `json:"Record"`
	} `json:"DomainRecords"`
}

type aliyunDNSRecord struct {
	RecordID string `json:"RecordId"`
	RR       string `json:"RR"`
	Type     string `json:"Type"`
	Value    string `json:"Value"`
	TTL      int    `json:"TTL"`
	Line     string `json:"Line"`
}

type aliyunDescribeDomainLinesResponse struct {
	DomainLines struct {
		Line []struct {
			LineCode string `json:"LineCode"`
			LineName string `json:"LineName"`
		} `json:"Line"`
	} `json:"DomainLines"`
}

type aliyunDescribeDomainsResponse struct {
	Domains struct {
		Domain []struct {
			DomainName string `json:"DomainName"`
		} `json:"Domain"`
	} `json:"Domains"`
}

type aliyunDescribeDomainInfoResponse struct {
	MinTTL        int `json:"MinTtl"`
	AvailableTTLs struct {
		AvailableTTL []string `json:"AvailableTtl"`
	} `json:"AvailableTtls"`
}

type aliyunRecordMutationResponse struct {
	RecordID string `json:"RecordId"`
}

type aliyunAPIErrorResponse struct {
	Code      string `json:"Code"`
	Message   string `json:"Message"`
	RequestID string `json:"RequestId"`
}

func newAliyunDNSClient(accessKeyID, accessKeySecret, regionID string) *aliyunDNSClient {
	endpoint := "https://alidns.aliyuncs.com/"
	regionID = strings.TrimSpace(regionID)
	if regionID != "" {
		endpoint = fmt.Sprintf("https://alidns.%s.aliyuncs.com/", regionID)
	}
	return &aliyunDNSClient{
		accessKeyID:     strings.TrimSpace(accessKeyID),
		accessKeySecret: strings.TrimSpace(accessKeySecret),
		endpoint:        endpoint,
		httpClient:      outboundproxy.NewDirectHTTPClient(20 * time.Second),
	}
}

func (c *aliyunDNSClient) upsertRecord(ctx context.Context, existingRecordID, domainName, rr, recordType, value string, ttl int, line string) (string, error) {
	values := url.Values{}
	values.Set("DomainName", domainName)
	values.Set("RR", rr)
	values.Set("Type", recordType)
	values.Set("Value", value)
	values.Set("TTL", strconv.Itoa(ttl))
	values.Set("Line", canonicalAliyunLineValue(line))

	if existingRecordID != "" {
		values.Set("RecordId", existingRecordID)
		var response aliyunRecordMutationResponse
		if err := c.doRPC(ctx, "UpdateDomainRecord", values, &response); err != nil {
			return "", err
		}
		if strings.TrimSpace(response.RecordID) != "" {
			return strings.TrimSpace(response.RecordID), nil
		}
		return existingRecordID, nil
	}

	var response aliyunRecordMutationResponse
	if err := c.doRPC(ctx, "AddDomainRecord", values, &response); err != nil {
		return "", err
	}
	return strings.TrimSpace(response.RecordID), nil
}

func (c *aliyunDNSClient) deleteRecord(ctx context.Context, recordID string) error {
	values := url.Values{}
	values.Set("RecordId", strings.TrimSpace(recordID))
	return c.doRPC(ctx, "DeleteDomainRecord", values, &aliyunRecordMutationResponse{})
}

func (c *aliyunDNSClient) listRecords(ctx context.Context, domainName string) ([]aliyunDNSRecord, error) {
	values := url.Values{}
	values.Set("DomainName", domainName)
	values.Set("PageSize", "100")

	var response aliyunDescribeRecordsResponse
	if err := c.doRPC(ctx, "DescribeDomainRecords", values, &response); err != nil {
		return nil, err
	}
	return response.DomainRecords.Record, nil
}

func (c *aliyunDNSClient) listLines(ctx context.Context, domainName string) ([]DNSOption, error) {
	values := url.Values{}
	if strings.TrimSpace(domainName) != "" {
		values.Set("DomainName", strings.TrimSpace(domainName))
	}

	var response aliyunDescribeDomainLinesResponse
	if err := c.doRPC(ctx, "DescribeDomainLines", values, &response); err != nil {
		return nil, err
	}

	options := make([]DNSOption, 0, len(response.DomainLines.Line))
	for _, line := range response.DomainLines.Line {
		value := strings.TrimSpace(line.LineCode)
		label := localizedAliyunLineLabel(value, strings.TrimSpace(line.LineName))
		if value == "" {
			continue
		}
		if label == "" {
			label = value
		}
		options = append(options, DNSOption{
			Value: value,
			Label: label,
		})
	}
	return options, nil
}

func (c *aliyunDNSClient) listDomains(ctx context.Context) ([]string, error) {
	values := url.Values{}
	values.Set("PageSize", "100")

	var response aliyunDescribeDomainsResponse
	if err := c.doRPC(ctx, "DescribeDomains", values, &response); err != nil {
		return nil, err
	}

	domains := make([]string, 0, len(response.Domains.Domain))
	for _, item := range response.Domains.Domain {
		value := strings.TrimSpace(item.DomainName)
		if value == "" {
			continue
		}
		domains = append(domains, value)
	}
	sort.Strings(domains)
	return domains, nil
}

func (c *aliyunDNSClient) describeDomainInfo(ctx context.Context, domainName string) (*aliyunDescribeDomainInfoResponse, error) {
	domainName = strings.TrimSpace(domainName)
	if domainName == "" {
		return nil, nil
	}

	values := url.Values{}
	values.Set("DomainName", domainName)
	values.Set("NeedDetailAttributes", "true")

	var response aliyunDescribeDomainInfoResponse
	if err := c.doRPC(ctx, "DescribeDomainInfo", values, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c *aliyunDNSClient) doRPC(ctx context.Context, action string, values url.Values, out any) error {
	if values == nil {
		values = url.Values{}
	}

	values.Set("Action", action)
	values.Set("Format", "JSON")
	values.Set("Version", "2015-01-09")
	values.Set("AccessKeyId", c.accessKeyID)
	values.Set("SignatureMethod", "HMAC-SHA1")
	values.Set("Timestamp", time.Now().UTC().Format("2006-01-02T15:04:05Z"))
	values.Set("SignatureVersion", "1.0")
	values.Set("SignatureNonce", uuid.NewString())

	signature := signAliyunRPC(values, c.accessKeySecret)
	values.Set("Signature", signature)

	requestURL := c.endpoint + "?" + values.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return err
	}
	request.Header.Set("Accept", "application/json")

	response, err := c.httpClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return formatAliyunRPCError(response.Status, body)
	}

	if err := json.Unmarshal(body, out); err != nil {
		return err
	}

	if apiErr, ok := parseAliyunAPIError(body); ok {
		message := firstNonEmpty(strings.TrimSpace(apiErr.Message), "aliyun dns request failed")
		if requestID := strings.TrimSpace(apiErr.RequestID); requestID != "" {
			return fmt.Errorf("%s: %s (request_id: %s)", strings.TrimSpace(apiErr.Code), message, requestID)
		}
		return fmt.Errorf("%s: %s", strings.TrimSpace(apiErr.Code), message)
	}
	return nil
}

func parseAliyunAPIError(body []byte) (*aliyunAPIErrorResponse, bool) {
	var apiErr aliyunAPIErrorResponse
	if err := json.Unmarshal(body, &apiErr); err != nil {
		return nil, false
	}
	if strings.TrimSpace(apiErr.Code) == "" && strings.TrimSpace(apiErr.Message) == "" {
		return nil, false
	}
	return &apiErr, true
}

func formatAliyunRPCError(status string, body []byte) error {
	if apiErr, ok := parseAliyunAPIError(body); ok {
		parts := make([]string, 0, 2)
		if code := strings.TrimSpace(apiErr.Code); code != "" {
			parts = append(parts, code)
		}
		if message := strings.TrimSpace(apiErr.Message); message != "" {
			parts = append(parts, message)
		}
		detail := strings.Join(parts, ": ")
		if detail == "" {
			detail = "aliyun dns request failed"
		}
		if requestID := strings.TrimSpace(apiErr.RequestID); requestID != "" {
			detail = fmt.Sprintf("%s (request_id: %s)", detail, requestID)
		}
		return fmt.Errorf("aliyun dns request failed: %s: %s", strings.TrimSpace(status), detail)
	}

	bodyText := strings.TrimSpace(string(body))
	if bodyText != "" {
		if len(bodyText) > 240 {
			bodyText = bodyText[:240] + "..."
		}
		return fmt.Errorf("aliyun dns request failed: %s: %s", strings.TrimSpace(status), bodyText)
	}
	return fmt.Errorf("aliyun dns request failed: %s", strings.TrimSpace(status))
}

func signAliyunRPC(values url.Values, secret string) string {
	pairs := make([]string, 0, len(values))
	for key, rawValues := range values {
		for _, value := range rawValues {
			pairs = append(pairs, percentEncode(key)+"="+percentEncode(value))
		}
	}
	sort.Strings(pairs)
	canonicalized := strings.Join(pairs, "&")
	stringToSign := "GET&%2F&" + percentEncode(canonicalized)

	mac := hmac.New(sha1.New, []byte(strings.TrimSpace(secret)+"&"))
	_, _ = mac.Write([]byte(stringToSign))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

func percentEncode(value string) string {
	encoded := url.QueryEscape(value)
	encoded = strings.ReplaceAll(encoded, "+", "%20")
	encoded = strings.ReplaceAll(encoded, "*", "%2A")
	encoded = strings.ReplaceAll(encoded, "%7E", "~")
	return encoded
}

func defaultAliyunDNSLines() []DNSOption {
	return []DNSOption{
		{Value: "default", Label: "默认"},
		{Value: "telecom", Label: "电信"},
		{Value: "unicom", Label: "联通"},
		{Value: "mobile", Label: "移动"},
		{Value: "edu", Label: "教育网"},
		{Value: "oversea", Label: "境外"},
		{Value: "search", Label: "搜索引擎"},
		{Value: "school", Label: "校园网"},
	}
}

func localizedAliyunLineLabel(value, fallback string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "default":
		return "默认"
	case "telecom":
		return "电信"
	case "unicom":
		return "联通"
	case "mobile":
		return "移动"
	case "edu":
		return "教育网"
	case "oversea":
		return "境外"
	case "search":
		return "搜索引擎"
	case "school":
		return "校园网"
	}

	if strings.TrimSpace(fallback) != "" {
		return strings.TrimSpace(fallback)
	}
	return strings.TrimSpace(value)
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

func ptrBool(value bool) *bool {
	result := value
	return &result
}
