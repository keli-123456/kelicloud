package failoverv2

import (
	"context"
	"testing"

	failoverv2db "github.com/komari-monitor/komari/database/failoverv2"
	"github.com/komari-monitor/komari/database/models"
)

func TestShouldRebuildServiceMemberDNSRecordRefs(t *testing.T) {
	before := &models.FailoverV2Service{
		DNSProvider: models.FailoverDNSProviderAliyun,
		DNSEntryID:  "entry-a",
		DNSPayload:  `{"rr":"@","domain_name":"example.com","record_type":"A"}`,
	}
	afterSameSemantic := &models.FailoverV2Service{
		DNSProvider: models.FailoverDNSProviderAliyun,
		DNSEntryID:  "entry-a",
		DNSPayload:  `{"record_type":"A","domain_name":"example.com","rr":"@"}`,
	}
	if ShouldRebuildServiceMemberDNSRecordRefs(before, afterSameSemantic) {
		t.Fatal("expected same semantic dns payload to skip rebuild")
	}

	afterChanged := &models.FailoverV2Service{
		DNSProvider: models.FailoverDNSProviderAliyun,
		DNSEntryID:  "entry-a",
		DNSPayload:  `{"rr":"@","domain_name":"new.example.com","record_type":"A"}`,
	}
	if !ShouldRebuildServiceMemberDNSRecordRefs(before, afterChanged) {
		t.Fatal("expected dns payload change to require rebuild")
	}
}

func TestRebuildServiceMemberDNSRecordRefsAliyunRebindsMatchingRecords(t *testing.T) {
	configureFailoverV2RunnerTestDB(t)
	useMockFailoverV2RunnerDeps(t)

	service, member := createTestRunnerServiceAndMember(t)
	updatedService := *service
	updatedService.DNSPayload = `{"domain_name":"new-example.com","rr":"@","record_type":"A","ttl":60}`
	if _, err := failoverv2db.UpdateServiceForUser("user-a", service.ID, &updatedService); err != nil {
		t.Fatalf("failed to update service dns payload: %v", err)
	}

	client := &mockAliyunDNSClient{
		records: []aliyunDNSRecord{
			{RecordID: "record-new-a", RR: "@", Type: "A", Value: member.CurrentAddress, TTL: 60, Line: member.DNSLine},
		},
	}
	useMockAliyunDNSDependencies(t, client)

	if err := RebuildServiceMemberDNSRecordRefs("user-a", service.ID); err != nil {
		t.Fatalf("failed to rebuild dns refs: %v", err)
	}

	reloadedService, err := failoverv2db.GetServiceByIDForUser("user-a", service.ID)
	if err != nil {
		t.Fatalf("failed to reload service: %v", err)
	}
	if len(reloadedService.Members) != 1 || len(reloadedService.Members[0].Lines) == 0 {
		t.Fatalf("expected reloaded member lines, got %#v", reloadedService.Members)
	}
	lineRefs := decodeMemberDNSRecordRefs(reloadedService.Members[0].Lines[0].DNSRecordRefs)
	if lineRefs["A"] != "record-new-a" {
		t.Fatalf("expected rebuilt line A ref record-new-a, got %#v", lineRefs)
	}
	legacyRefs := decodeMemberDNSRecordRefs(reloadedService.Members[0].DNSRecordRefs)
	if legacyRefs["A"] != "record-new-a" {
		t.Fatalf("expected legacy A ref record-new-a, got %#v", legacyRefs)
	}
}

func TestRebuildServiceMemberDNSRecordRefsClearsStaleRefsWhenNoMatch(t *testing.T) {
	configureFailoverV2RunnerTestDB(t)
	useMockFailoverV2RunnerDeps(t)

	service, member := createTestRunnerServiceAndMember(t)
	updatedService := *service
	updatedService.DNSPayload = `{"domain_name":"new-example.com","rr":"@","record_type":"A","ttl":60}`
	if _, err := failoverv2db.UpdateServiceForUser("user-a", service.ID, &updatedService); err != nil {
		t.Fatalf("failed to update service dns payload: %v", err)
	}

	// No records matching member current address under the new domain.
	client := &mockAliyunDNSClient{
		records: []aliyunDNSRecord{
			{RecordID: "record-other-a", RR: "@", Type: "A", Value: "198.51.100.250", TTL: 60, Line: member.DNSLine},
		},
	}
	useMockAliyunDNSDependencies(t, client)

	if err := RebuildServiceMemberDNSRecordRefs("user-a", service.ID); err != nil {
		t.Fatalf("failed to rebuild dns refs: %v", err)
	}

	reloadedService, err := failoverv2db.GetServiceByIDForUser("user-a", service.ID)
	if err != nil {
		t.Fatalf("failed to reload service: %v", err)
	}
	if len(reloadedService.Members) != 1 || len(reloadedService.Members[0].Lines) == 0 {
		t.Fatalf("expected reloaded member lines, got %#v", reloadedService.Members)
	}
	lineRefs := decodeMemberDNSRecordRefs(reloadedService.Members[0].Lines[0].DNSRecordRefs)
	if len(lineRefs) != 0 {
		t.Fatalf("expected stale refs to be cleared when no match exists, got %#v", lineRefs)
	}
	legacyRefs := decodeMemberDNSRecordRefs(reloadedService.Members[0].DNSRecordRefs)
	if len(legacyRefs) != 0 {
		t.Fatalf("expected legacy stale refs to be cleared when no match exists, got %#v", legacyRefs)
	}
}

func TestRebuildServiceMemberDNSRecordRefsSkipsWhenProviderLookupFails(t *testing.T) {
	configureFailoverV2RunnerTestDB(t)
	useMockFailoverV2RunnerDeps(t)

	service, _ := createTestRunnerServiceAndMember(t)

	previousLoadConfig := loadAliyunDNSConfigFunc
	loadAliyunDNSConfigFunc = func(userUUID, entryID string) (*aliyunDNSConfig, error) {
		return nil, context.Canceled
	}
	t.Cleanup(func() {
		loadAliyunDNSConfigFunc = previousLoadConfig
	})

	if err := RebuildServiceMemberDNSRecordRefs("user-a", service.ID); err != nil {
		t.Fatalf("expected rebuild to skip provider lookup failures after clearing refs, got %v", err)
	}
}
