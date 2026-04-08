package failoverv2

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
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

type aliyunRecordClient interface {
	listRecords(ctx context.Context, domainName string) ([]aliyunDNSRecord, error)
	upsertRecord(ctx context.Context, existingRecordID, domainName, rr, recordType, value string, ttl int, line string) (string, error)
	deleteRecord(ctx context.Context, recordID string) error
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
	PageNumber int `json:"PageNumber"`
	PageSize   int `json:"PageSize"`
	TotalCount int `json:"TotalCount"`
}

type aliyunDNSRecord struct {
	RecordID string `json:"RecordId"`
	RR       string `json:"RR"`
	Type     string `json:"Type"`
	Value    string `json:"Value"`
	TTL      int    `json:"TTL"`
	Line     string `json:"Line"`
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
	values.Set("DomainName", strings.TrimSpace(domainName))
	values.Set("RR", strings.TrimSpace(rr))
	values.Set("Type", strings.ToUpper(strings.TrimSpace(recordType)))
	values.Set("Value", strings.TrimSpace(value))
	values.Set("TTL", strconv.Itoa(ttl))
	values.Set("Line", canonicalAliyunLineValue(line))

	if strings.TrimSpace(existingRecordID) != "" {
		values.Set("RecordId", strings.TrimSpace(existingRecordID))
		var response aliyunRecordMutationResponse
		if err := c.doRPC(ctx, "UpdateDomainRecord", values, &response); err != nil {
			return "", err
		}
		if strings.TrimSpace(response.RecordID) != "" {
			return strings.TrimSpace(response.RecordID), nil
		}
		return strings.TrimSpace(existingRecordID), nil
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
	const pageSize = 100
	pageNumber := 1
	records := []aliyunDNSRecord{}
	for {
		values := url.Values{}
		values.Set("DomainName", strings.TrimSpace(domainName))
		values.Set("PageNumber", strconv.Itoa(pageNumber))
		values.Set("PageSize", strconv.Itoa(pageSize))

		var response aliyunDescribeRecordsResponse
		if err := c.doRPC(ctx, "DescribeDomainRecords", values, &response); err != nil {
			return nil, err
		}
		pageRecords := response.DomainRecords.Record
		records = append(records, pageRecords...)
		if response.TotalCount > 0 && len(records) >= response.TotalCount {
			break
		}
		if len(pageRecords) < pageSize {
			break
		}
		pageNumber++
	}
	return records, nil
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
	request, err := http.NewRequestWithContext(contextOrBackground(ctx), http.MethodGet, requestURL, nil)
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

	if out != nil {
		if err := json.Unmarshal(body, out); err != nil {
			return err
		}
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

func cloneAliyunRecords(records []aliyunDNSRecord) []aliyunDNSRecord {
	if len(records) == 0 {
		return nil
	}
	cloned := make([]aliyunDNSRecord, len(records))
	copy(cloned, records)
	return cloned
}

func marshalJSON(value any) []byte {
	body, _ := json.Marshal(value)
	return bytes.TrimSpace(body)
}
