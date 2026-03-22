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
}

type cloudflareDNSPayload struct {
	ZoneID     string `json:"zone_id,omitempty"`
	ZoneName   string `json:"zone_name,omitempty"`
	RecordName string `json:"record_name,omitempty"`
	RecordType string `json:"record_type,omitempty"`
	TTL        int    `json:"ttl,omitempty"`
	Proxied    *bool  `json:"proxied,omitempty"`
}

type aliyunRecordPayload struct {
	DomainName string `json:"domain_name,omitempty"`
	RR         string `json:"rr,omitempty"`
	RecordType string `json:"record_type,omitempty"`
	TTL        int    `json:"ttl,omitempty"`
	Line       string `json:"line,omitempty"`
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

	recordType := strings.ToUpper(strings.TrimSpace(payload.RecordType))
	if recordType == "" {
		recordType = "A"
	}
	recordValue, err := selectRecordValue(recordType, ipv4, ipv6)
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

	record, err := client.upsertRecord(context.Background(), zoneID, recordName, recordType, recordValue, ttl, proxied)
	if err != nil {
		return nil, err
	}
	return &dnsUpdateResult{
		Provider: cloudflareProviderName,
		ID:       record.ID,
		Name:     record.Name,
		Type:     record.Type,
		Value:    record.Content,
		ZoneID:   zoneID,
		ZoneName: zoneName,
	}, nil
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

	recordType := strings.ToUpper(strings.TrimSpace(payload.RecordType))
	if recordType == "" {
		recordType = "A"
	}
	recordValue, err := selectRecordValue(recordType, ipv4, ipv6)
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
	line := strings.TrimSpace(payload.Line)
	if line == "" {
		line = "default"
	}

	client := newAliyunDNSClient(configValue.AccessKeyID, configValue.AccessKeySecret, configValue.RegionID)
	recordID, err := client.upsertRecord(context.Background(), domainName, rr, recordType, recordValue, ttl, line)
	if err != nil {
		return nil, err
	}

	return &dnsUpdateResult{
		Provider: aliyunProviderName,
		ID:       recordID,
		Type:     recordType,
		Value:    recordValue,
		Domain:   domainName,
		RR:       rr,
	}, nil
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
		token: strings.TrimSpace(token),
		httpClient: &http.Client{
			Timeout: 20 * time.Second,
		},
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
		} `json:"Record"`
	} `json:"DomainRecords"`
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
		httpClient: &http.Client{
			Timeout: 20 * time.Second,
		},
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
