package failoverv2

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/komari-monitor/komari/database/models"
)

type testCloudflareAPI struct {
	zoneID  string
	records map[string]cloudflareDNSRecord
	nextID  int
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

func newTestCloudflareAPI(records []cloudflareDNSRecord) *testCloudflareAPI {
	store := make(map[string]cloudflareDNSRecord, len(records))
	for _, record := range records {
		store[strings.TrimSpace(record.ID)] = record
	}
	return &testCloudflareAPI{
		zoneID:  "zone-1",
		records: store,
		nextID:  len(records) + 1,
	}
}

func (a *testCloudflareAPI) list() []cloudflareDNSRecord {
	result := make([]cloudflareDNSRecord, 0, len(a.records))
	for _, record := range a.records {
		result = append(result, record)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].ID < result[j].ID
	})
	return result
}

func (a *testCloudflareAPI) serveHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	path := strings.TrimPrefix(r.URL.Path, "/client/v4")
	switch {
	case r.Method == http.MethodGet && path == "/zones":
		_ = json.NewEncoder(w).Encode(cloudflareAPIEnvelope[[]cloudflareZone]{
			Success: true,
			Result:  []cloudflareZone{{ID: a.zoneID, Name: "example.com"}},
		})
		return
	case path == "/zones/"+a.zoneID+"/dns_records" && r.Method == http.MethodGet:
		all := a.list()
		page, _ := strconv.Atoi(r.URL.Query().Get("page"))
		if page <= 0 {
			page = 1
		}
		perPage, _ := strconv.Atoi(r.URL.Query().Get("per_page"))
		if perPage <= 0 {
			perPage = len(all)
		}
		totalPages := 0
		if len(all) > 0 {
			totalPages = (len(all) + perPage - 1) / perPage
		}
		start := (page - 1) * perPage
		end := start + perPage
		if start > len(all) {
			start = len(all)
		}
		if end > len(all) {
			end = len(all)
		}
		_ = json.NewEncoder(w).Encode(cloudflareAPIEnvelope[[]cloudflareDNSRecord]{
			Success: true,
			Result:  all[start:end],
			ResultInfo: cloudflareResultInfo{
				Page:       page,
				PerPage:    perPage,
				Count:      end - start,
				TotalCount: len(all),
				TotalPages: totalPages,
			},
		})
		return
	case path == "/zones/"+a.zoneID+"/dns_records" && r.Method == http.MethodPost:
		var payload map[string]interface{}
		_ = json.NewDecoder(r.Body).Decode(&payload)
		record := cloudflareDNSRecord{
			ID:      "rec-new-" + string(rune('0'+a.nextID)),
			Type:    strings.TrimSpace(payload["type"].(string)),
			Name:    strings.TrimSpace(payload["name"].(string)),
			Content: strings.TrimSpace(payload["content"].(string)),
			TTL:     int(payload["ttl"].(float64)),
			Proxied: payload["proxied"].(bool),
		}
		a.nextID++
		a.records[record.ID] = record
		_ = json.NewEncoder(w).Encode(cloudflareAPIEnvelope[cloudflareDNSRecord]{
			Success: true,
			Result:  record,
		})
		return
	case strings.HasPrefix(path, "/zones/"+a.zoneID+"/dns_records/") && r.Method == http.MethodPut:
		recordID := strings.TrimPrefix(path, "/zones/"+a.zoneID+"/dns_records/")
		record := a.records[recordID]
		var payload map[string]interface{}
		_ = json.NewDecoder(r.Body).Decode(&payload)
		record.Type = strings.TrimSpace(payload["type"].(string))
		record.Name = strings.TrimSpace(payload["name"].(string))
		record.Content = strings.TrimSpace(payload["content"].(string))
		record.TTL = int(payload["ttl"].(float64))
		record.Proxied = payload["proxied"].(bool)
		a.records[recordID] = record
		_ = json.NewEncoder(w).Encode(cloudflareAPIEnvelope[cloudflareDNSRecord]{
			Success: true,
			Result:  record,
		})
		return
	case strings.HasPrefix(path, "/zones/"+a.zoneID+"/dns_records/") && r.Method == http.MethodDelete:
		recordID := strings.TrimPrefix(path, "/zones/"+a.zoneID+"/dns_records/")
		delete(a.records, recordID)
		_ = json.NewEncoder(w).Encode(cloudflareAPIEnvelope[map[string]interface{}]{
			Success: true,
			Result:  map[string]interface{}{},
		})
		return
	default:
		http.NotFound(w, r)
		return
	}
}

func useMockCloudflareDNSDependencies(t *testing.T, api *testCloudflareAPI) {
	t.Helper()

	previousLoadConfig := loadCloudflareDNSConfigFunc
	previousNewClient := newCloudflareDNSClientFunc
	loadCloudflareDNSConfigFunc = func(userUUID, entryID string) (*cloudflareDNSConfig, error) {
		return &cloudflareDNSConfig{
			APIToken: "token",
			ZoneID:   api.zoneID,
			ZoneName: "example.com",
			Proxied:  false,
		}, nil
	}
	newCloudflareDNSClientFunc = func(configValue *cloudflareDNSConfig) *cloudflareDNSClient {
		return newTestCloudflareClient(api)
	}
	t.Cleanup(func() {
		loadCloudflareDNSConfigFunc = previousLoadConfig
		newCloudflareDNSClientFunc = previousNewClient
	})
}

func newTestCloudflareClient(api *testCloudflareAPI) *cloudflareDNSClient {
	return &cloudflareDNSClient{
		token:   "token",
		baseURL: "http://cloudflare.test/client/v4",
		httpClient: &http.Client{
			Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
				recorder := httptest.NewRecorder()
				api.serveHTTP(recorder, request)
				return recorder.Result(), nil
			}),
		},
	}
}

func testCloudflareService() *models.FailoverV2Service {
	return &models.FailoverV2Service{
		DNSProvider: models.FailoverDNSProviderCloudflare,
		DNSEntryID:  "default",
		DNSPayload:  `{"zone_name":"example.com","record_name":"app.example.com","record_type":"A","ttl":120}`,
	}
}

func testCloudflareMember() *models.FailoverV2Member {
	return &models.FailoverV2Member{
		DNSLine:       "telecom",
		DNSRecordRefs: `{}`,
	}
}

func TestCloudflareDNSClientListRecordsPaginates(t *testing.T) {
	records := make([]cloudflareDNSRecord, 0, 101)
	for i := 0; i < 101; i++ {
		records = append(records, cloudflareDNSRecord{
			ID:      fmt.Sprintf("rec-%03d", i),
			Name:    "app.example.com",
			Type:    "A",
			Content: fmt.Sprintf("203.0.113.%d", i%255),
			TTL:     120,
		})
	}
	api := newTestCloudflareAPI(records)
	client := newTestCloudflareClient(api)

	listed, err := client.listRecords(context.Background(), api.zoneID)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(listed) != 101 {
		t.Fatalf("expected 101 records across pages, got %d", len(listed))
	}
}

func TestApplyCloudflareMemberDNSAttachPrunesNonMemberRecords(t *testing.T) {
	api := newTestCloudflareAPI([]cloudflareDNSRecord{
		{ID: "rec-other-a", Name: "app.example.com", Type: "A", Content: "198.51.100.20", TTL: 120},
		{ID: "rec-owned-aaaa", Name: "app.example.com", Type: "AAAA", Content: "2001:db8::20", TTL: 120},
	})
	useMockCloudflareDNSDependencies(t, api)

	member := testCloudflareMember()
	member.DNSRecordRefs = `{"AAAA":"rec-owned-aaaa"}`

	result, err := ApplyCloudflareMemberDNSAttach(context.Background(), "user-a", testCloudflareService(), member, "203.0.113.8", "")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if got := result.RecordRefs["A"]; got == "" {
		t.Fatalf("expected A record ref, got %#v", result.RecordRefs)
	}
	record, ok := api.records["rec-other-a"]
	if !ok {
		t.Fatalf("expected provider record to be reused, got %#v", api.records)
	}
	if record.Content != "203.0.113.8" {
		t.Fatalf("expected reused record to carry target value, got %#v", record)
	}
	if _, ok := api.records["rec-owned-aaaa"]; ok {
		t.Fatalf("expected owned stale AAAA record to be pruned, got %#v", api.records)
	}
}

func TestApplyCloudflareMemberDNSDetachFallsBackToCurrentAddress(t *testing.T) {
	api := newTestCloudflareAPI([]cloudflareDNSRecord{
		{ID: "rec-old-a", Name: "app.example.com", Type: "A", Content: "198.51.100.10", TTL: 120},
		{ID: "rec-other-a", Name: "app.example.com", Type: "A", Content: "198.51.100.11", TTL: 120},
	})
	useMockCloudflareDNSDependencies(t, api)

	member := testCloudflareMember()
	member.CurrentAddress = "198.51.100.10"

	result, err := ApplyCloudflareMemberDNSDetach(context.Background(), "user-a", testCloudflareService(), member)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(result.Removed) != 1 || result.Removed[0].ID != "rec-old-a" {
		t.Fatalf("expected current address record to be removed, got %#v", result.Removed)
	}
	if _, ok := api.records["rec-other-a"]; !ok {
		t.Fatalf("expected unrelated record to remain, got %#v", api.records)
	}
}

func TestApplyCloudflareMemberDNSDetachRemovesOnlyReferencedRecord(t *testing.T) {
	api := newTestCloudflareAPI([]cloudflareDNSRecord{
		{ID: "rec-member-1-a", Name: "app.example.com", Type: "A", Content: "198.51.100.10", TTL: 120},
		{ID: "rec-member-2-a", Name: "app.example.com", Type: "A", Content: "198.51.100.11", TTL: 120},
	})
	useMockCloudflareDNSDependencies(t, api)

	member := testCloudflareMember()
	member.DNSRecordRefs = `{"A":"rec-member-1-a"}`

	result, err := ApplyCloudflareMemberDNSDetach(context.Background(), "user-a", testCloudflareService(), member)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(result.Removed) != 1 || result.Removed[0].ID != "rec-member-1-a" {
		t.Fatalf("expected only referenced record to be removed, got %#v", result.Removed)
	}
	if _, ok := api.records["rec-member-2-a"]; !ok {
		t.Fatalf("expected other member record to remain, got %#v", api.records)
	}
}

func TestApplyCloudflareMemberDNSDetachRemovesStoredAAAAWhenCurrentAddressIsIPv4(t *testing.T) {
	service := testCloudflareService()
	service.DNSPayload = `{"zone_name":"example.com","record_name":"app.example.com","record_type":"A","sync_ipv6":true,"ttl":120}`

	api := newTestCloudflareAPI([]cloudflareDNSRecord{
		{ID: "rec-member-a", Name: "app.example.com", Type: "A", Content: "198.51.100.10", TTL: 120},
		{ID: "rec-member-aaaa", Name: "app.example.com", Type: "AAAA", Content: "2001:db8::10", TTL: 120},
	})
	useMockCloudflareDNSDependencies(t, api)

	member := testCloudflareMember()
	member.DNSRecordRefs = `{"A":"rec-member-a","AAAA":"rec-member-aaaa"}`
	member.CurrentAddress = "198.51.100.10"

	result, err := ApplyCloudflareMemberDNSDetach(context.Background(), "user-a", service, member)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(result.Removed) != 2 {
		t.Fatalf("expected both managed records to be removed, got %#v", result.Removed)
	}
	if _, ok := api.records["rec-member-a"]; ok {
		t.Fatalf("expected referenced A record to be deleted, got %#v", api.records)
	}
	if _, ok := api.records["rec-member-aaaa"]; ok {
		t.Fatalf("expected referenced AAAA record to be deleted, got %#v", api.records)
	}
}

func TestApplyCloudflareMemberDNSDetachSkipsStaleRefWhenCurrentAddressMismatches(t *testing.T) {
	api := newTestCloudflareAPI([]cloudflareDNSRecord{
		{ID: "rec-member-1-a", Name: "app.example.com", Type: "A", Content: "198.51.100.10", TTL: 120},
		{ID: "rec-member-2-a", Name: "app.example.com", Type: "A", Content: "198.51.100.11", TTL: 120},
	})
	useMockCloudflareDNSDependencies(t, api)

	member := testCloudflareMember()
	member.DNSRecordRefs = `{"A":"rec-member-2-a"}`
	member.CurrentAddress = "198.51.100.10"

	result, err := ApplyCloudflareMemberDNSDetach(context.Background(), "user-a", testCloudflareService(), member)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(result.Removed) != 1 || result.Removed[0].ID != "rec-member-1-a" {
		t.Fatalf("expected only current-address record to be removed, got %#v", result.Removed)
	}
	if _, ok := api.records["rec-member-2-a"]; !ok {
		t.Fatalf("expected stale-ref record to remain, got %#v", api.records)
	}
}

func TestVerifyCloudflareMemberDNSAttachedDetectsUnexpectedNonMemberRecord(t *testing.T) {
	api := newTestCloudflareAPI([]cloudflareDNSRecord{
		{ID: "rec-owned-a", Name: "app.example.com", Type: "A", Content: "203.0.113.8", TTL: 120},
		{ID: "rec-other-a", Name: "app.example.com", Type: "A", Content: "203.0.113.9", TTL: 120},
	})
	useMockCloudflareDNSDependencies(t, api)

	member := testCloudflareMember()
	member.DNSRecordRefs = `{"A":"rec-owned-a"}`

	verification, err := VerifyCloudflareMemberDNSAttached(context.Background(), "user-a", testCloudflareService(), member, "203.0.113.8", "")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if verification.Success {
		t.Fatalf("expected verification to fail, got %#v", verification)
	}
	if len(verification.Unexpected) != 1 || verification.Unexpected[0].ID != "rec-other-a" {
		t.Fatalf("expected non-member record to be unexpected, got %#v", verification.Unexpected)
	}
}

func TestVerifyCloudflareMemberDNSAttachedAcceptsServiceMemberSet(t *testing.T) {
	api := newTestCloudflareAPI([]cloudflareDNSRecord{
		{ID: "rec-member-1-a", Name: "app.example.com", Type: "A", Content: "203.0.113.8", TTL: 120},
		{ID: "rec-member-2-a", Name: "app.example.com", Type: "A", Content: "203.0.113.9", TTL: 120},
	})
	useMockCloudflareDNSDependencies(t, api)

	service := testCloudflareService()
	service.Members = []models.FailoverV2Member{
		{ID: 1, Enabled: true, DNSLine: "telecom", CurrentAddress: "203.0.113.8"},
		{ID: 2, Enabled: true, DNSLine: "unicom", CurrentAddress: "203.0.113.9"},
	}
	member := testCloudflareMember()
	member.ID = 1
	member.DNSRecordRefs = `{"A":"rec-member-1-a"}`

	verification, err := VerifyCloudflareMemberDNSAttached(context.Background(), "user-a", service, member, "203.0.113.8", "")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !verification.Success {
		t.Fatalf("expected verification to pass for service member set, got %#v", verification)
	}
}
