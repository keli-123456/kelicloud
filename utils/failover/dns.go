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
	Provider string `json:"provider"`
	ID       string `json:"id,omitempty"`
	Name     string `json:"name,omitempty"`
	Type     string `json:"type,omitempty"`
	Value    string `json:"value,omitempty"`
	ZoneID   string `json:"zone_id,omitempty"`
	ZoneName string `json:"zone_name,omitempty"`
	Domain   string `json:"domain,omitempty"`
	RR       string `json:"rr,omitempty"`
	Records      []dnsUpdateResult `json:"records,omitempty"`
	SkippedTypes []string          `json:"skipped_types,omitempty"`
}

type DNSUpdateResult = dnsUpdateResult

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

func applyDNSRecord(userUUID, providerName, entryID, payloadJSON, ipv4, ipv6 string) (*dnsUpdateResult, error) {
	switch strings.ToLower(strings.TrimSpace(providerName)) {
	case cloudflareProviderName:
		return applyCloudflareDNSRecord(userUUID, entryID, payloadJSON, ipv4, ipv6)
	case aliyunProviderName:
		return applyAliyunDNSRecord(userUUID, entryID, payloadJSON, ipv4, ipv6)
	default:
		return nil, fmt.Errorf("unsupported dns provider: %s", providerName)
	}
}

func ApplyDNSRecord(userUUID, providerName, entryID, payloadJSON, ipv4, ipv6 string) (*DNSUpdateResult, error) {
	return applyDNSRecord(userUUID, providerName, entryID, payloadJSON, ipv4, ipv6)
}

type dnsApplyPlan struct {
	RecordTypes  []string
	SkippedTypes []string
}

func buildDNSApplyPlan(recordType string, syncIPv6 bool, ipv4, ipv6 string) (*dnsApplyPlan, error) {
	normalized := strings.ToUpper(strings.TrimSpace(recordType))
	if normalized == "" {
		normalized = "A"
	}

	if !syncIPv6 {
		if _, err := selectRecordValue(normalized, ipv4, ipv6); err != nil {
			return nil, err
		}
		return &dnsApplyPlan{RecordTypes: []string{normalized}}, nil
	}

	if _, err := selectRecordValue(normalized, ipv4, ipv6); err != nil {
		return nil, err
	}

	counterpart := "AAAA"
	if normalized == "AAAA" {
		counterpart = "A"
	}

	plan := &dnsApplyPlan{
		RecordTypes: []string{normalized},
	}
	if _, err := selectRecordValue(counterpart, ipv4, ipv6); err == nil {
		plan.RecordTypes = append(plan.RecordTypes, counterpart)
	} else {
		plan.SkippedTypes = append(plan.SkippedTypes, counterpart)
	}
	return plan, nil
}

func summarizeDNSResults(results []dnsUpdateResult, skippedTypes []string) *dnsUpdateResult {
	if len(results) == 0 {
		return nil
	}
	if len(results) == 1 && len(skippedTypes) == 0 {
		result := results[0]
		return &result
	}
	summary := results[0]
	summary.Records = append([]dnsUpdateResult(nil), results...)
	summary.SkippedTypes = append([]string(nil), skippedTypes...)
	return &summary
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

func applyCloudflareDNSRecord(userUUID, entryID, payloadJSON, ipv4, ipv6 string) (*dnsUpdateResult, error) {
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
		zoneID, err = client.resolveZoneID(context.Background(), zoneName)
		if err != nil {
			return nil, err
		}
	}

	results := make([]dnsUpdateResult, 0, len(plan.RecordTypes))
	for _, currentRecordType := range plan.RecordTypes {
		recordValue, _ := selectRecordValue(currentRecordType, ipv4, ipv6)
		record, err := client.upsertRecord(context.Background(), zoneID, recordName, currentRecordType, recordValue, ttl, proxied)
		if err != nil {
			return nil, err
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
	}
	return summarizeDNSResults(results, plan.SkippedTypes), nil
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

func applyAliyunDNSRecord(userUUID, entryID, payloadJSON, ipv4, ipv6 string) (*dnsUpdateResult, error) {
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

	rr := strings.TrimSpace(payload.RR)
	if rr == "" {
		rr = "@"
	}
	ttl := payload.TTL
	if ttl <= 0 {
		ttl = 600
	}
	client := newAliyunDNSClient(configValue.AccessKeyID, configValue.AccessKeySecret, configValue.RegionID)
	lines := normalizeAliyunLines(payload.Line, payload.Lines)
	results := make([]dnsUpdateResult, 0, len(plan.RecordTypes))
	for _, currentRecordType := range plan.RecordTypes {
		recordValue, _ := selectRecordValue(currentRecordType, ipv4, ipv6)
		recordIDs := make([]string, 0, len(lines))
		for _, line := range lines {
			recordID, err := client.upsertRecord(context.Background(), domainName, rr, currentRecordType, recordValue, ttl, line)
			if err != nil {
				return nil, err
			}
			recordIDs = append(recordIDs, recordID)
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
	return summarizeDNSResults(results, plan.SkippedTypes), nil
}

func normalizeAliyunLines(primary string, values []string) []string {
	normalized := make([]string, 0, len(values)+1)
	seen := map[string]struct{}{}
	appendValue := func(value string) {
		value = strings.TrimSpace(value)
		if value == "" {
			return
		}
		if _, ok := seen[value]; ok {
			return
		}
		seen[value] = struct{}{}
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

func selectRecordValue(recordType, ipv4, ipv6 string) (string, error) {
	switch strings.ToUpper(strings.TrimSpace(recordType)) {
	case "A":
		ipv4 = strings.TrimSpace(ipv4)
		if ipv4 == "" {
			return "", errors.New("ipv4 address is empty for A record")
		}
		return ipv4, nil
	case "AAAA":
		ipv6 = strings.TrimSpace(ipv6)
		if ipv6 == "" {
			return "", errors.New("ipv6 address is empty for AAAA record")
		}
		return ipv6, nil
	default:
		return "", fmt.Errorf("unsupported DNS record type: %s", recordType)
	}
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

func (c *cloudflareDNSClient) upsertRecord(ctx context.Context, zoneID, name, recordType, content string, ttl int, proxied bool) (*cloudflareDNSRecord, error) {
	query := url.Values{}
	query.Set("name", name)
	query.Set("type", recordType)
	query.Set("per_page", "100")

	var listResponse cloudflareAPIEnvelope[[]cloudflareDNSRecord]
	if err := c.do(ctx, http.MethodGet, fmt.Sprintf("https://api.cloudflare.com/client/v4/zones/%s/dns_records?%s", strings.TrimSpace(zoneID), query.Encode()), nil, &listResponse); err != nil {
		return nil, err
	}

	requestBody := map[string]interface{}{
		"type":    recordType,
		"name":    name,
		"content": content,
		"ttl":     ttl,
		"proxied": proxied,
	}

	if len(listResponse.Result) > 0 {
		record := listResponse.Result[0]
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

	if envelope, ok := out.(*cloudflareAPIEnvelope[[]cloudflareZone]); ok && !envelope.Success && len(envelope.Errors) > 0 {
		return errors.New(envelope.Errors[0].Message)
	}
	if envelope, ok := out.(*cloudflareAPIEnvelope[[]cloudflareDNSRecord]); ok && !envelope.Success && len(envelope.Errors) > 0 {
		return errors.New(envelope.Errors[0].Message)
	}
	if envelope, ok := out.(*cloudflareAPIEnvelope[cloudflareDNSRecord]); ok && !envelope.Success && len(envelope.Errors) > 0 {
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
		Record []struct {
			RecordID string `json:"RecordId"`
			RR       string `json:"RR"`
			Type     string `json:"Type"`
			Value    string `json:"Value"`
			TTL      int    `json:"TTL"`
			Line     string `json:"Line"`
		} `json:"Record"`
	} `json:"DomainRecords"`
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

func (c *aliyunDNSClient) upsertRecord(ctx context.Context, domainName, rr, recordType, value string, ttl int, line string) (string, error) {
	existingRecordID, err := c.findRecordID(ctx, domainName, rr, recordType)
	if err != nil {
		return "", err
	}

	values := url.Values{}
	values.Set("DomainName", domainName)
	values.Set("RR", rr)
	values.Set("Type", recordType)
	values.Set("Value", value)
	values.Set("TTL", strconv.Itoa(ttl))
	values.Set("Line", line)

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

func (c *aliyunDNSClient) findRecordID(ctx context.Context, domainName, rr, recordType string) (string, error) {
	values := url.Values{}
	values.Set("DomainName", domainName)
	values.Set("RRKeyWord", rr)
	values.Set("TypeKeyWord", recordType)
	values.Set("PageSize", "100")

	var response aliyunDescribeRecordsResponse
	if err := c.doRPC(ctx, "DescribeDomainRecords", values, &response); err != nil {
		return "", err
	}

	for _, record := range response.DomainRecords.Record {
		if strings.TrimSpace(record.RR) == rr && strings.EqualFold(strings.TrimSpace(record.Type), recordType) {
			return strings.TrimSpace(record.RecordID), nil
		}
	}
	return "", nil
}

func (c *aliyunDNSClient) listRecords(ctx context.Context, domainName string) ([]struct {
	RecordID string `json:"RecordId"`
	RR       string `json:"RR"`
	Type     string `json:"Type"`
	Value    string `json:"Value"`
	TTL      int    `json:"TTL"`
	Line     string `json:"Line"`
}, error) {
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
		return fmt.Errorf("aliyun dns request failed: %s", response.Status)
	}

	if err := json.Unmarshal(body, out); err != nil {
		return err
	}

	var apiErr struct {
		Code    string `json:"Code"`
		Message string `json:"Message"`
	}
	if err := json.Unmarshal(body, &apiErr); err == nil && strings.TrimSpace(apiErr.Code) != "" {
		return fmt.Errorf("%s: %s", apiErr.Code, apiErr.Message)
	}
	return nil
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
