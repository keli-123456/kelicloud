package failoverv2

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/komari-monitor/komari/database/models"
)

type mockAliyunDNSClient struct {
	records        []aliyunDNSRecord
	listErr        error
	upsertErr      error
	deleteErr      error
	upsertCalls    []mockAliyunUpsertCall
	deleteCalls    []string
	upsertRecordID string
}

type mockAliyunUpsertCall struct {
	ExistingRecordID string
	DomainName       string
	RR               string
	RecordType       string
	Value            string
	TTL              int
	Line             string
}

func (m *mockAliyunDNSClient) listRecords(ctx context.Context, domainName string) ([]aliyunDNSRecord, error) {
	if m.listErr != nil {
		return nil, m.listErr
	}
	return cloneAliyunRecords(m.records), nil
}

func (m *mockAliyunDNSClient) upsertRecord(ctx context.Context, existingRecordID, domainName, rr, recordType, value string, ttl int, line string) (string, error) {
	m.upsertCalls = append(m.upsertCalls, mockAliyunUpsertCall{
		ExistingRecordID: existingRecordID,
		DomainName:       domainName,
		RR:               rr,
		RecordType:       recordType,
		Value:            value,
		TTL:              ttl,
		Line:             line,
	})
	if m.upsertErr != nil {
		return "", m.upsertErr
	}
	if m.upsertRecordID != "" {
		return m.upsertRecordID, nil
	}
	if existingRecordID != "" {
		return existingRecordID, nil
	}
	return "generated-record-id", nil
}

func (m *mockAliyunDNSClient) deleteRecord(ctx context.Context, recordID string) error {
	m.deleteCalls = append(m.deleteCalls, recordID)
	if m.deleteErr != nil {
		return m.deleteErr
	}
	return nil
}

func useMockAliyunDNSDependencies(t *testing.T, client aliyunRecordClient) {
	t.Helper()

	previousLoadConfig := loadAliyunDNSConfigFunc
	previousNewClient := newAliyunDNSClientFunc
	loadAliyunDNSConfigFunc = func(userUUID, entryID string) (*aliyunDNSConfig, error) {
		return &aliyunDNSConfig{
			AccessKeyID:     "ak",
			AccessKeySecret: "sk",
			DomainName:      "example.com",
		}, nil
	}
	newAliyunDNSClientFunc = func(configValue *aliyunDNSConfig) aliyunRecordClient {
		return client
	}
	t.Cleanup(func() {
		loadAliyunDNSConfigFunc = previousLoadConfig
		newAliyunDNSClientFunc = previousNewClient
	})
}

func TestAliyunDNSClientListRecordsPaginates(t *testing.T) {
	client := &aliyunDNSClient{
		accessKeyID:     "ak",
		accessKeySecret: "sk",
		endpoint:        "http://aliyun.test/",
		httpClient: &http.Client{
			Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
				recorder := httptest.NewRecorder()
				if action := request.URL.Query().Get("Action"); action != "DescribeDomainRecords" {
					t.Fatalf("unexpected aliyun action %q", action)
				}
				pageNumber := request.URL.Query().Get("PageNumber")
				records := make([]aliyunDNSRecord, 0, 100)
				start := 1
				end := 100
				if pageNumber == "2" {
					start = 101
					end = 101
				}
				for i := start; i <= end; i++ {
					records = append(records, aliyunDNSRecord{
						RecordID: fmt.Sprintf("record-%03d", i),
						RR:       "@",
						Type:     "A",
						Value:    fmt.Sprintf("203.0.113.%d", i%255),
						TTL:      60,
						Line:     "telecom",
					})
				}
				_ = json.NewEncoder(recorder).Encode(aliyunDescribeRecordsResponse{
					DomainRecords: struct {
						Record []aliyunDNSRecord `json:"Record"`
					}{Record: records},
					PageNumber: 1,
					PageSize:   100,
					TotalCount: 101,
				})
				return recorder.Result(), nil
			}),
		},
	}
	records, err := client.listRecords(context.Background(), "example.com")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(records) != 101 {
		t.Fatalf("expected 101 records across pages, got %d", len(records))
	}
	if records[100].RecordID != "record-101" {
		t.Fatalf("expected second page record to be included, got %#v", records[100])
	}
}

func testAliyunService() *models.FailoverV2Service {
	return &models.FailoverV2Service{
		DNSProvider: models.FailoverDNSProviderAliyun,
		DNSEntryID:  "default",
		DNSPayload:  `{"domain_name":"example.com","rr":"@","record_type":"A","ttl":60}`,
	}
}

func testAliyunMember() *models.FailoverV2Member {
	return &models.FailoverV2Member{
		DNSLine:       "telecom",
		DNSRecordRefs: `{"A":"record-telecom-a"}`,
	}
}

func TestApplyAliyunMemberDNSAttachUpdatesOnlyTargetLine(t *testing.T) {
	client := &mockAliyunDNSClient{
		records: []aliyunDNSRecord{
			{RecordID: "record-default-a", RR: "@", Type: "A", Value: "198.51.100.10", TTL: 60, Line: "default"},
			{RecordID: "record-telecom-a", RR: "@", Type: "A", Value: "198.51.100.11", TTL: 60, Line: "telecom"},
			{RecordID: "record-telecom-a-duplicate", RR: "@", Type: "A", Value: "198.51.100.12", TTL: 60, Line: "telecom"},
			{RecordID: "record-telecom-aaaa", RR: "@", Type: "AAAA", Value: "2001:db8::12", TTL: 60, Line: "telecom"},
		},
	}
	useMockAliyunDNSDependencies(t, client)

	service := testAliyunService()
	service.Members = []models.FailoverV2Member{
		{ID: 1, Enabled: true, DNSLine: "telecom", CurrentAddress: "198.51.100.11"},
		{ID: 2, Enabled: true, DNSLine: "telecom", CurrentAddress: "198.51.100.12"},
	}
	member := testAliyunMember()
	member.ID = 1

	result, err := ApplyAliyunMemberDNSAttach(context.Background(), "user-a", service, member, "203.0.113.8", "")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(client.upsertCalls) != 1 {
		t.Fatalf("expected 1 upsert call, got %#v", client.upsertCalls)
	}
	if client.upsertCalls[0].ExistingRecordID != "record-telecom-a" {
		t.Fatalf("expected existing telecom record to be updated, got %#v", client.upsertCalls[0])
	}
	if client.upsertCalls[0].Line != "telecom" || client.upsertCalls[0].Value != "203.0.113.8" {
		t.Fatalf("unexpected upsert call: %#v", client.upsertCalls[0])
	}
	if len(client.deleteCalls) != 1 || client.deleteCalls[0] != "record-telecom-aaaa" {
		t.Fatalf("expected stale AAAA record to be pruned, got %#v", client.deleteCalls)
	}
	if len(result.Records) != 1 || result.Records[0].Line != "telecom" || result.Records[0].Value != "203.0.113.8" {
		t.Fatalf("unexpected result records: %#v", result.Records)
	}
	if len(result.Removed) != 1 || result.Removed[0].ID != "record-telecom-aaaa" {
		t.Fatalf("expected removed stale AAAA record, got %#v", result.Removed)
	}
	if got := result.RecordRefs["A"]; got != "record-telecom-a" {
		t.Fatalf("expected updated record ref, got %#v", result.RecordRefs)
	}
	if len(result.PrunedTypes) != 1 || result.PrunedTypes[0] != "AAAA" {
		t.Fatalf("expected AAAA to be pruned, got %#v", result.PrunedTypes)
	}
}

func TestApplyAliyunMemberDNSAttachDoesNotOverwriteOtherMemberRecordWhenTargetRefIsStale(t *testing.T) {
	service := testAliyunService()
	service.DNSPayload = `{"domain_name":"example.com","rr":"@","record_type":"AAAA","ttl":600}`
	service.Members = []models.FailoverV2Member{
		{ID: 1, Enabled: true, DNSLine: "telecom", CurrentAddress: "2001:db8::8", DNSRecordRefs: `{"AAAA":"record-telecom-aaaa-member-2"}`},
		{ID: 2, Enabled: true, DNSLine: "telecom", CurrentAddress: "2600:3c15::2000:acff:fe65:9be6", DNSRecordRefs: `{"AAAA":"record-telecom-aaaa-member-2"}`},
	}

	client := &mockAliyunDNSClient{
		records: []aliyunDNSRecord{
			{
				RecordID: "record-telecom-aaaa-member-2",
				RR:       "@",
				Type:     "AAAA",
				Value:    "2600:3c15::2000:acff:fe65:9be6",
				TTL:      600,
				Line:     "telecom",
			},
		},
	}
	useMockAliyunDNSDependencies(t, client)

	member := testAliyunMember()
	member.ID = 1
	member.DNSLine = "telecom"
	member.DNSRecordRefs = `{"AAAA":"record-telecom-aaaa-member-2"}`

	result, err := ApplyAliyunMemberDNSAttach(context.Background(), "user-a", service, member, "", "2001:db8::8")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(client.upsertCalls) != 1 {
		t.Fatalf("expected one create upsert call, got %#v", client.upsertCalls)
	}
	if client.upsertCalls[0].ExistingRecordID != "" || client.upsertCalls[0].Value != "2001:db8::8" {
		t.Fatalf("expected target AAAA to be created instead of reusing member-2 record, got %#v", client.upsertCalls[0])
	}
	if len(client.deleteCalls) != 0 {
		t.Fatalf("expected other member record to remain untouched, got delete calls %#v", client.deleteCalls)
	}
	if got := result.RecordRefs["AAAA"]; got != "generated-record-id" {
		t.Fatalf("expected target AAAA record ref to point to newly created record, got %#v", result.RecordRefs)
	}
}

func TestApplyAliyunMemberDNSAttachPreservesOtherMemberOwnedRecordWhenAddressUnavailable(t *testing.T) {
	service := testAliyunService()
	service.DNSPayload = `{"domain_name":"example.com","rr":"@","record_type":"AAAA","ttl":600}`
	service.Members = []models.FailoverV2Member{
		{ID: 1, Enabled: true, DNSLine: "default", CurrentAddress: "2001:db8::8"},
		{ID: 2, Enabled: true, DNSLine: "default", CurrentAddress: "", DNSRecordRefs: `{"AAAA":"record-member-2-aaaa"}`},
	}

	client := &mockAliyunDNSClient{
		records: []aliyunDNSRecord{
			{
				RecordID: "record-member-2-aaaa",
				RR:       "@",
				Type:     "AAAA",
				Value:    "2600:3c15::2000:16ff:fec0:18a9",
				TTL:      600,
				Line:     "default",
			},
		},
	}
	useMockAliyunDNSDependencies(t, client)

	member := testAliyunMember()
	member.ID = 1
	member.DNSLine = "default"
	member.DNSRecordRefs = `{}`

	result, err := ApplyAliyunMemberDNSAttach(context.Background(), "user-a", service, member, "", "2001:db8::8")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(client.upsertCalls) != 1 || client.upsertCalls[0].ExistingRecordID != "" {
		t.Fatalf("expected target AAAA to be created, got %#v", client.upsertCalls)
	}
	if len(client.deleteCalls) != 0 {
		t.Fatalf("expected other member owned AAAA to be preserved, got delete calls %#v", client.deleteCalls)
	}
	if got := result.RecordRefs["AAAA"]; got != "generated-record-id" {
		t.Fatalf("expected target AAAA record ref to point to newly created record, got %#v", result.RecordRefs)
	}
}

func TestApplyAliyunMemberDNSDetachRemovesOnlyManagedTargetLine(t *testing.T) {
	service := testAliyunService()
	service.DNSPayload = `{"domain_name":"example.com","rr":"@","record_type":"A","sync_ipv6":true,"ttl":60}`

	client := &mockAliyunDNSClient{
		records: []aliyunDNSRecord{
			{RecordID: "record-default-a", RR: "@", Type: "A", Value: "198.51.100.10", TTL: 60, Line: "default"},
			{RecordID: "record-telecom-a", RR: "@", Type: "A", Value: "198.51.100.11", TTL: 60, Line: "telecom"},
			{RecordID: "record-telecom-a-other-member", RR: "@", Type: "A", Value: "198.51.100.12", TTL: 60, Line: "telecom"},
			{RecordID: "record-telecom-aaaa", RR: "@", Type: "AAAA", Value: "2001:db8::11", TTL: 60, Line: "telecom"},
			{RecordID: "record-telecom-txt", RR: "@", Type: "TXT", Value: "ignore-me", TTL: 60, Line: "telecom"},
		},
	}
	useMockAliyunDNSDependencies(t, client)

	member := testAliyunMember()
	member.DNSRecordRefs = `{"A":"record-telecom-a","AAAA":"record-telecom-aaaa"}`

	result, err := ApplyAliyunMemberDNSDetach(context.Background(), "user-a", service, member)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(client.deleteCalls) != 2 {
		t.Fatalf("expected 2 delete calls, got %#v", client.deleteCalls)
	}
	if client.deleteCalls[0] != "record-telecom-a" || client.deleteCalls[1] != "record-telecom-aaaa" {
		t.Fatalf("unexpected delete calls: %#v", client.deleteCalls)
	}
	if len(result.RecordRefs) != 0 {
		t.Fatalf("expected record refs to be cleared, got %#v", result.RecordRefs)
	}
	if len(result.Removed) != 2 {
		t.Fatalf("expected removed records, got %#v", result.Removed)
	}
}

func TestApplyAliyunMemberDNSDetachRemovesStoredAAAAWhenCurrentAddressIsIPv4(t *testing.T) {
	service := testAliyunService()
	service.DNSPayload = `{"domain_name":"example.com","rr":"@","record_type":"A","sync_ipv6":true,"ttl":60}`

	client := &mockAliyunDNSClient{
		records: []aliyunDNSRecord{
			{RecordID: "record-telecom-a", RR: "@", Type: "A", Value: "198.51.100.11", TTL: 60, Line: "telecom"},
			{RecordID: "record-telecom-aaaa", RR: "@", Type: "AAAA", Value: "2001:db8::11", TTL: 60, Line: "telecom"},
		},
	}
	useMockAliyunDNSDependencies(t, client)

	member := testAliyunMember()
	member.DNSRecordRefs = `{"A":"record-telecom-a","AAAA":"record-telecom-aaaa"}`
	member.CurrentAddress = "198.51.100.11"

	result, err := ApplyAliyunMemberDNSDetach(context.Background(), "user-a", service, member)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(client.deleteCalls) != 2 {
		t.Fatalf("expected both managed records to be deleted, got %#v", client.deleteCalls)
	}
	if client.deleteCalls[0] != "record-telecom-a" || client.deleteCalls[1] != "record-telecom-aaaa" {
		t.Fatalf("unexpected delete calls: %#v", client.deleteCalls)
	}
	if len(result.Removed) != 2 {
		t.Fatalf("expected two removed records, got %#v", result.Removed)
	}
}

func TestApplyAliyunMemberDNSDetachSkipsMissingStoredRefs(t *testing.T) {
	service := testAliyunService()
	service.DNSPayload = `{"domain_name":"example.com","rr":"@","record_type":"A","sync_ipv6":true,"ttl":60}`

	client := &mockAliyunDNSClient{
		records: []aliyunDNSRecord{
			{RecordID: "record-telecom-a", RR: "@", Type: "A", Value: "198.51.100.11", TTL: 60, Line: "telecom"},
		},
	}
	useMockAliyunDNSDependencies(t, client)

	member := testAliyunMember()
	member.DNSRecordRefs = `{"A":"record-telecom-a","AAAA":"record-telecom-aaaa-missing"}`
	member.CurrentAddress = "198.51.100.11"

	result, err := ApplyAliyunMemberDNSDetach(context.Background(), "user-a", service, member)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(client.deleteCalls) != 1 || client.deleteCalls[0] != "record-telecom-a" {
		t.Fatalf("expected only existing record to be deleted, got %#v", client.deleteCalls)
	}
	if len(result.Removed) != 1 || result.Removed[0].ID != "record-telecom-a" {
		t.Fatalf("expected only existing record to be marked removed, got %#v", result.Removed)
	}
	if len(result.RecordRefs) != 0 {
		t.Fatalf("expected record refs to be cleared, got %#v", result.RecordRefs)
	}
}

func TestApplyAliyunMemberDNSDetachFallsBackToCurrentAddressWhenRecordRefsMissing(t *testing.T) {
	client := &mockAliyunDNSClient{
		records: []aliyunDNSRecord{
			{RecordID: "record-telecom-a-old", RR: "@", Type: "A", Value: "198.51.100.10", TTL: 60, Line: "telecom"},
			{RecordID: "record-telecom-a-other", RR: "@", Type: "A", Value: "198.51.100.11", TTL: 60, Line: "telecom"},
		},
	}
	useMockAliyunDNSDependencies(t, client)

	member := testAliyunMember()
	member.DNSRecordRefs = `{}`
	member.CurrentAddress = "198.51.100.10"

	result, err := ApplyAliyunMemberDNSDetach(context.Background(), "user-a", testAliyunService(), member)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(client.deleteCalls) != 1 || client.deleteCalls[0] != "record-telecom-a-old" {
		t.Fatalf("expected only matching current address record to be deleted, got %#v", client.deleteCalls)
	}
	if len(result.Removed) != 1 || result.Removed[0].ID != "record-telecom-a-old" {
		t.Fatalf("expected only existing matching record to be marked removed, got %#v", result.Removed)
	}
}

func TestApplyAliyunMemberDNSDetachSkipsRecordWhenRefBelongsToAnotherMember(t *testing.T) {
	client := &mockAliyunDNSClient{
		records: []aliyunDNSRecord{
			{RecordID: "record-member-1-a", RR: "@", Type: "A", Value: "198.51.100.10", TTL: 60, Line: "telecom"},
			{RecordID: "record-member-2-a", RR: "@", Type: "A", Value: "198.51.100.11", TTL: 60, Line: "telecom"},
		},
	}
	useMockAliyunDNSDependencies(t, client)

	service := testAliyunService()
	service.Members = []models.FailoverV2Member{
		{ID: 1, Enabled: true, DNSLine: "telecom", CurrentAddress: "198.51.100.10", DNSRecordRefs: `{"A":"record-member-1-a"}`},
		{ID: 2, Enabled: true, DNSLine: "telecom", CurrentAddress: "198.51.100.11", DNSRecordRefs: `{"A":"record-member-2-a"}`},
	}

	member := testAliyunMember()
	member.ID = 1
	member.DNSRecordRefs = `{"A":"record-member-2-a"}`
	member.CurrentAddress = "198.51.100.10"

	result, err := ApplyAliyunMemberDNSDetach(context.Background(), "user-a", service, member)
	if err != nil {
		t.Fatalf("expected detach to continue when record belongs to another member, got %v", err)
	}
	if len(client.deleteCalls) != 0 {
		t.Fatalf("expected no deletes when record ref belongs to another member, got %#v", client.deleteCalls)
	}
	if len(result.Removed) != 0 {
		t.Fatalf("expected no removed records when ref belongs to another member, got %#v", result.Removed)
	}
}

func TestApplyAliyunMemberDNSDetachAllowsStaleAddressWhenNotAnotherMember(t *testing.T) {
	client := &mockAliyunDNSClient{
		records: []aliyunDNSRecord{
			{RecordID: "record-member-stale-a", RR: "@", Type: "A", Value: "198.51.100.99", TTL: 60, Line: "telecom"},
		},
	}
	useMockAliyunDNSDependencies(t, client)

	service := testAliyunService()
	service.Members = []models.FailoverV2Member{
		{ID: 1, Enabled: true, DNSLine: "telecom", CurrentAddress: "198.51.100.10"},
		{ID: 2, Enabled: true, DNSLine: "telecom", CurrentAddress: "198.51.100.11"},
	}

	member := testAliyunMember()
	member.ID = 1
	member.DNSRecordRefs = `{"A":"record-member-stale-a"}`
	member.CurrentAddress = "198.51.100.10"

	result, err := ApplyAliyunMemberDNSDetach(context.Background(), "user-a", service, member)
	if err != nil {
		t.Fatalf("expected stale-address detach to continue, got %v", err)
	}
	if len(client.deleteCalls) != 1 || client.deleteCalls[0] != "record-member-stale-a" {
		t.Fatalf("expected stale owned ref to be deleted, got %#v", client.deleteCalls)
	}
	if len(result.Removed) != 1 || result.Removed[0].ID != "record-member-stale-a" {
		t.Fatalf("expected stale owned record to be removed, got %#v", result.Removed)
	}
}

func TestApplyAliyunMemberDNSDetachFallsBackWhenStoredRefNoLongerMatchesIdentity(t *testing.T) {
	client := &mockAliyunDNSClient{
		records: []aliyunDNSRecord{
			{RecordID: "record-stale-ref", RR: "legacy", Type: "A", Value: "203.0.113.250", TTL: 60, Line: "telecom"},
			{RecordID: "record-target-a", RR: "@", Type: "A", Value: "198.51.100.10", TTL: 60, Line: "telecom"},
		},
	}
	useMockAliyunDNSDependencies(t, client)

	member := testAliyunMember()
	member.DNSRecordRefs = `{"A":"record-stale-ref"}`
	member.CurrentAddress = "198.51.100.10"

	result, err := ApplyAliyunMemberDNSDetach(context.Background(), "user-a", testAliyunService(), member)
	if err != nil {
		t.Fatalf("expected stale ref detach to continue, got %v", err)
	}
	if len(client.deleteCalls) != 1 || client.deleteCalls[0] != "record-target-a" {
		t.Fatalf("expected detach to remove only current-address target record, got %#v", client.deleteCalls)
	}
	if len(result.Removed) != 1 || result.Removed[0].ID != "record-target-a" {
		t.Fatalf("expected stale ref fallback to remove target record, got %#v", result.Removed)
	}
}

func TestApplyAliyunMemberDNSDetachSkipsAAAAWhenRefBelongsToAnotherMember(t *testing.T) {
	service := testAliyunService()
	service.DNSPayload = `{"domain_name":"example.com","rr":"@","record_type":"A","sync_ipv6":true,"ttl":60}`
	service.Members = []models.FailoverV2Member{
		{ID: 1, Enabled: true, DNSLine: "telecom", CurrentAddress: "198.51.100.10", DNSRecordRefs: `{"A":"record-member-1-a"}`},
		{ID: 2, Enabled: true, DNSLine: "telecom", CurrentAddress: "2600:3c15::2000:acff:fe65:9be6", DNSRecordRefs: `{"AAAA":"record-member-2-aaaa"}`},
	}

	client := &mockAliyunDNSClient{
		records: []aliyunDNSRecord{
			{RecordID: "record-member-2-aaaa", RR: "@", Type: "AAAA", Value: "2600:3c15::2000:acff:fe65:9be6", TTL: 60, Line: "telecom"},
		},
	}
	useMockAliyunDNSDependencies(t, client)

	member := testAliyunMember()
	member.ID = 1
	member.DNSRecordRefs = `{"AAAA":"record-member-2-aaaa"}`
	member.CurrentAddress = "198.51.100.10"

	result, err := ApplyAliyunMemberDNSDetach(context.Background(), "user-a", service, member)
	if err != nil {
		t.Fatalf("expected detach to continue when AAAA belongs to another member, got %v", err)
	}
	if len(client.deleteCalls) != 0 {
		t.Fatalf("expected no deletes when AAAA ref belongs to another member, got %#v", client.deleteCalls)
	}
	if len(result.Removed) != 0 {
		t.Fatalf("expected no removed records when AAAA belongs to another member, got %#v", result.Removed)
	}
}

func TestVerifyAliyunMemberDNSAttachedDetectsUnexpectedCounterpart(t *testing.T) {
	client := &mockAliyunDNSClient{
		records: []aliyunDNSRecord{
			{RecordID: "record-telecom-a", RR: "@", Type: "A", Value: "203.0.113.8", TTL: 60, Line: "电信"},
			{RecordID: "record-telecom-aaaa", RR: "@", Type: "AAAA", Value: "2001:db8::11", TTL: 60, Line: "telecom"},
		},
	}
	useMockAliyunDNSDependencies(t, client)

	member := testAliyunMember()
	member.DNSRecordRefs = `{"A":"record-telecom-a","AAAA":"record-telecom-aaaa"}`

	verification, err := VerifyAliyunMemberDNSAttached(context.Background(), "user-a", testAliyunService(), member, "203.0.113.8", "")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if verification.Success {
		t.Fatalf("expected verification to fail, got %#v", verification)
	}
	if len(verification.Unexpected) != 1 || verification.Unexpected[0].Type != "AAAA" {
		t.Fatalf("expected stale AAAA record to be unexpected, got %#v", verification.Unexpected)
	}
}

func TestVerifyAliyunMemberDNSAttachedDetectsUnexpectedNonMemberRecord(t *testing.T) {
	client := &mockAliyunDNSClient{
		records: []aliyunDNSRecord{
			{RecordID: "record-owned-a", RR: "@", Type: "A", Value: "203.0.113.8", TTL: 60, Line: "telecom"},
			{RecordID: "record-other-a", RR: "@", Type: "A", Value: "203.0.113.9", TTL: 60, Line: "telecom"},
		},
	}
	useMockAliyunDNSDependencies(t, client)

	member := testAliyunMember()
	member.DNSRecordRefs = `{"A":"record-owned-a"}`

	verification, err := VerifyAliyunMemberDNSAttached(context.Background(), "user-a", testAliyunService(), member, "203.0.113.8", "")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if verification.Success {
		t.Fatalf("expected verification to fail on non-member record, got %#v", verification)
	}
	if len(verification.Unexpected) != 1 || verification.Unexpected[0].ID != "record-other-a" {
		t.Fatalf("expected non-member record to be unexpected, got %#v", verification.Unexpected)
	}
}

func TestVerifyAliyunMemberDNSAttachedAcceptsServiceMemberSet(t *testing.T) {
	client := &mockAliyunDNSClient{
		records: []aliyunDNSRecord{
			{RecordID: "record-member-1-a", RR: "@", Type: "A", Value: "203.0.113.8", TTL: 60, Line: "telecom"},
			{RecordID: "record-member-2-a", RR: "@", Type: "A", Value: "203.0.113.9", TTL: 60, Line: "telecom"},
		},
	}
	useMockAliyunDNSDependencies(t, client)

	service := testAliyunService()
	service.Members = []models.FailoverV2Member{
		{ID: 1, Enabled: true, DNSLine: "telecom", CurrentAddress: "203.0.113.8"},
		{ID: 2, Enabled: true, DNSLine: "telecom", CurrentAddress: "203.0.113.9"},
	}
	member := testAliyunMember()
	member.ID = 1
	member.DNSRecordRefs = `{"A":"record-member-1-a"}`

	verification, err := VerifyAliyunMemberDNSAttached(context.Background(), "user-a", service, member, "203.0.113.8", "")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !verification.Success {
		t.Fatalf("expected verification to pass for service member set, got %#v", verification)
	}
}

func TestVerifyAliyunMemberDNSAttachedIgnoresUnexpectedRecordOwnedByOtherMember(t *testing.T) {
	client := &mockAliyunDNSClient{
		records: []aliyunDNSRecord{
			{RecordID: "record-member-1-aaaa", RR: "@", Type: "AAAA", Value: "2001:db8::8", TTL: 600, Line: "default"},
			{RecordID: "record-member-2-aaaa", RR: "@", Type: "AAAA", Value: "2600:3c15::2000:16ff:fec0:18a9", TTL: 600, Line: "default"},
		},
	}
	useMockAliyunDNSDependencies(t, client)

	service := testAliyunService()
	service.DNSPayload = `{"domain_name":"example.com","rr":"@","record_type":"AAAA","ttl":600}`
	service.Members = []models.FailoverV2Member{
		{ID: 1, Enabled: true, DNSLine: "default", CurrentAddress: "2001:db8::8", DNSRecordRefs: `{"AAAA":"record-member-1-aaaa"}`},
		{ID: 2, Enabled: true, DNSLine: "default", CurrentAddress: "", DNSRecordRefs: `{"AAAA":"record-member-2-aaaa"}`},
	}

	member := testAliyunMember()
	member.ID = 1
	member.DNSLine = "default"
	member.DNSRecordRefs = `{"AAAA":"record-member-1-aaaa"}`

	verification, err := VerifyAliyunMemberDNSAttached(context.Background(), "user-a", service, member, "", "2001:db8::8")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !verification.Success {
		t.Fatalf("expected verification to pass and ignore other member owned record, got %#v", verification)
	}
}

func TestVerifyAliyunMemberDNSDetachedIgnoresOtherLines(t *testing.T) {
	client := &mockAliyunDNSClient{
		records: []aliyunDNSRecord{
			{RecordID: "record-default-a", RR: "@", Type: "A", Value: "203.0.113.8", TTL: 60, Line: "default"},
			{RecordID: "record-default-aaaa", RR: "@", Type: "AAAA", Value: "2001:db8::8", TTL: 60, Line: "default"},
		},
	}
	useMockAliyunDNSDependencies(t, client)

	verification, err := VerifyAliyunMemberDNSDetached(context.Background(), "user-a", testAliyunService(), testAliyunMember())
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !verification.Success {
		t.Fatalf("expected verification to pass, got %#v", verification)
	}
}

func TestVerifyAliyunMemberDNSDetachedFlagsStoredAAAAWithIPv4CurrentAddress(t *testing.T) {
	service := testAliyunService()
	service.DNSPayload = `{"domain_name":"example.com","rr":"@","record_type":"A","sync_ipv6":true,"ttl":60}`

	client := &mockAliyunDNSClient{
		records: []aliyunDNSRecord{
			{RecordID: "record-telecom-aaaa", RR: "@", Type: "AAAA", Value: "2001:db8::11", TTL: 60, Line: "telecom"},
		},
	}
	useMockAliyunDNSDependencies(t, client)

	member := testAliyunMember()
	member.DNSRecordRefs = `{"AAAA":"record-telecom-aaaa"}`
	member.CurrentAddress = "198.51.100.11"

	verification, err := VerifyAliyunMemberDNSDetached(context.Background(), "user-a", service, member)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if verification.Success {
		t.Fatalf("expected verification to fail when stored AAAA still exists, got %#v", verification)
	}
	if len(verification.Observed) != 1 || verification.Observed[0].ID != "record-telecom-aaaa" {
		t.Fatalf("expected stored AAAA to be observed, got %#v", verification.Observed)
	}
}

func TestApplyAliyunMemberDNSAttachReturnsListError(t *testing.T) {
	client := &mockAliyunDNSClient{listErr: errors.New("boom")}
	useMockAliyunDNSDependencies(t, client)

	if _, err := ApplyAliyunMemberDNSAttach(context.Background(), "user-a", testAliyunService(), testAliyunMember(), "203.0.113.8", ""); err == nil {
		t.Fatal("expected list error")
	}
}
