package failoverv2

import (
	"strings"
	"testing"

	failoverdb "github.com/komari-monitor/komari/database/failover"
	failoverv2db "github.com/komari-monitor/komari/database/failoverv2"
	"github.com/komari-monitor/komari/database/models"
)

func useMockFailoverV2OwnershipConfig(t *testing.T) {
	t.Helper()

	previousLoadConfig := loadAliyunDNSConfigFunc
	previousLoadCloudflareConfig := loadCloudflareDNSConfigFunc
	loadAliyunDNSConfigFunc = func(userUUID, entryID string) (*aliyunDNSConfig, error) {
		return &aliyunDNSConfig{
			AccessKeyID:     "ak",
			AccessKeySecret: "sk",
			DomainName:      "example.com",
		}, nil
	}
	loadCloudflareDNSConfigFunc = func(userUUID, entryID string) (*cloudflareDNSConfig, error) {
		return &cloudflareDNSConfig{
			APIToken: "token",
			ZoneID:   "zone-1",
			ZoneName: "example.com",
			Proxied:  false,
		}, nil
	}
	t.Cleanup(func() {
		loadAliyunDNSConfigFunc = previousLoadConfig
		loadCloudflareDNSConfigFunc = previousLoadCloudflareConfig
	})
}

func createOwnershipTestService(t *testing.T, userUUID, name, rr string) *models.FailoverV2Service {
	t.Helper()

	service, err := failoverv2db.CreateServiceForUser(userUUID, &models.FailoverV2Service{
		Name:        name,
		Enabled:     true,
		DNSProvider: models.FailoverDNSProviderAliyun,
		DNSEntryID:  "default",
		DNSPayload:  `{"domain_name":"example.com","rr":"` + rr + `","record_type":"A","ttl":60}`,
	})
	if err != nil {
		t.Fatalf("failed to create test service %s: %v", name, err)
	}
	return service
}

func TestResolveServiceDNSOwnershipNormalizesAliyunTarget(t *testing.T) {
	useMockFailoverV2OwnershipConfig(t)

	ownership, err := ResolveServiceDNSOwnership("user-a", &models.FailoverV2Service{
		DNSProvider: models.FailoverDNSProviderAliyun,
		DNSEntryID:  "default",
		DNSPayload:  `{"domain_name":"Example.com.","rr":"Api.Example.com"}`,
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if ownership.DomainName != "example.com" {
		t.Fatalf("expected normalized domain example.com, got %q", ownership.DomainName)
	}
	if ownership.RR != "api" {
		t.Fatalf("expected normalized rr api, got %q", ownership.RR)
	}
	if !strings.Contains(ownership.Key, "example.com|api") {
		t.Fatalf("expected ownership key to contain normalized target, got %q", ownership.Key)
	}
}

func TestResolveServiceDNSOwnershipNormalizesCloudflareTarget(t *testing.T) {
	useMockFailoverV2OwnershipConfig(t)

	ownership, err := ResolveServiceDNSOwnership("user-a", &models.FailoverV2Service{
		DNSProvider: models.FailoverDNSProviderCloudflare,
		DNSEntryID:  "default",
		DNSPayload:  `{"zone_name":"Example.com.","record_name":"Api.Example.com","record_type":"A"}`,
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if ownership.DomainName != "example.com" {
		t.Fatalf("expected normalized domain example.com, got %q", ownership.DomainName)
	}
	if ownership.RR != "api" {
		t.Fatalf("expected normalized rr api, got %q", ownership.RR)
	}
	if !strings.Contains(ownership.Key, "cloudflare|example.com|api") {
		t.Fatalf("expected ownership key to contain normalized cloudflare target, got %q", ownership.Key)
	}
}

func TestEnsureServiceDNSOwnershipAvailableRejectsConflictingService(t *testing.T) {
	configureFailoverV2RunnerTestDB(t)
	useMockFailoverV2OwnershipConfig(t)

	createOwnershipTestService(t, "user-a", "service-a", "@")
	serviceB := createOwnershipTestService(t, "user-a", "service-b", "@")

	_, err := EnsureServiceDNSOwnershipAvailable("user-a", serviceB.ID, serviceB)
	if err == nil {
		t.Fatal("expected ownership conflict error")
	}
	if !strings.Contains(err.Error(), "ownership conflict") {
		t.Fatalf("expected ownership conflict in error, got %v", err)
	}
}

func TestEnsureServiceDNSOwnershipAvailableRejectsActiveV1Task(t *testing.T) {
	configureFailoverV2RunnerTestDB(t)
	useMockFailoverV2OwnershipConfig(t)

	if _, err := failoverdb.CreateTaskForUser("user-a", &models.FailoverTask{
		Name:            "v1-prod",
		Enabled:         true,
		WatchClientUUID: "client-a",
		DNSProvider:     models.FailoverDNSProviderAliyun,
		DNSEntryID:      "default",
		DNSPayload:      `{"domain_name":"example.com","rr":"api","record_type":"A","ttl":60}`,
	}, nil); err != nil {
		t.Fatalf("failed to create v1 failover task: %v", err)
	}

	_, err := EnsureServiceDNSOwnershipAvailable("user-a", 0, &models.FailoverV2Service{
		Name:        "v2-service",
		DNSProvider: models.FailoverDNSProviderAliyun,
		DNSEntryID:  "default",
		DNSPayload:  `{"domain_name":"Example.com.","rr":"Api.Example.com"}`,
	})
	if err == nil {
		t.Fatal("expected active v1 ownership conflict")
	}
	if !strings.Contains(err.Error(), "active v1 failover task") {
		t.Fatalf("expected active v1 failover task conflict, got %v", err)
	}
}

func TestRunMemberFailoverNowForUserRejectsActiveV1WatchClient(t *testing.T) {
	configureFailoverV2RunnerTestDB(t)
	useMockFailoverV2OwnershipConfig(t)

	if _, err := failoverdb.CreateTaskForUser("user-a", &models.FailoverTask{
		Name:            "v1-prod",
		Enabled:         true,
		WatchClientUUID: "client-shared",
		DNSProvider:     models.FailoverDNSProviderAliyun,
		DNSEntryID:      "default",
		DNSPayload:      `{"domain_name":"example.com","rr":"v1","record_type":"A","ttl":60}`,
	}, nil); err != nil {
		t.Fatalf("failed to create v1 failover task: %v", err)
	}

	service := createOwnershipTestService(t, "user-a", "v2-service", "v2")
	member, err := failoverv2db.CreateMemberForUser("user-a", service.ID, &models.FailoverV2Member{
		Name:            "shared-client",
		Enabled:         true,
		WatchClientUUID: "client-shared",
		DNSLine:         "telecom",
		Provider:        "digitalocean",
		ProviderEntryID: "token-1",
		PlanPayload:     `{"region":"nyc1","size":"s-1vcpu-1gb","image":"ubuntu-24-04-x64"}`,
	})
	if err != nil {
		t.Fatalf("failed to create v2 member: %v", err)
	}

	if _, err := RunMemberFailoverNowForUser("user-a", service.ID, member.ID); err == nil {
		t.Fatal("expected active v1 watch_client_uuid conflict")
	} else if !strings.Contains(err.Error(), "same watch_client_uuid") {
		t.Fatalf("expected watch_client_uuid conflict, got %v", err)
	}
}

func TestRunMemberFailoverNowForUserRejectsOwnershipConflict(t *testing.T) {
	configureFailoverV2RunnerTestDB(t)
	useMockFailoverV2RunnerDeps(t)
	useMockFailoverV2OwnershipConfig(t)

	serviceA := createOwnershipTestService(t, "user-a", "service-a", "@")
	if _, err := failoverv2db.CreateMemberForUser("user-a", serviceA.ID, &models.FailoverV2Member{
		Name:            "telecom-a",
		Enabled:         true,
		WatchClientUUID: "client-a",
		DNSLine:         "telecom",
		Provider:        "digitalocean",
		ProviderEntryID: "token-1",
		PlanPayload:     `{"region":"nyc1","size":"s-1vcpu-1gb","image":"ubuntu-24-04-x64"}`,
	}); err != nil {
		t.Fatalf("failed to create service-a member: %v", err)
	}

	serviceB := createOwnershipTestService(t, "user-a", "service-b", "@")
	memberB, err := failoverv2db.CreateMemberForUser("user-a", serviceB.ID, &models.FailoverV2Member{
		Name:            "telecom-b",
		Enabled:         true,
		WatchClientUUID: "client-b",
		DNSLine:         "unicom",
		Provider:        "digitalocean",
		ProviderEntryID: "token-1",
		PlanPayload:     `{"region":"nyc1","size":"s-1vcpu-1gb","image":"ubuntu-24-04-x64"}`,
	})
	if err != nil {
		t.Fatalf("failed to create service-b member: %v", err)
	}

	if _, err := RunMemberFailoverNowForUser("user-a", serviceB.ID, memberB.ID); err == nil {
		t.Fatal("expected ownership conflict to block failover")
	} else if !strings.Contains(err.Error(), "ownership conflict") {
		t.Fatalf("expected ownership conflict error, got %v", err)
	}
}

func TestRunMemberFailoverNowForUserRejectsActiveDNSLock(t *testing.T) {
	configureFailoverV2RunnerTestDB(t)
	useMockFailoverV2RunnerDeps(t)
	useMockFailoverV2OwnershipConfig(t)

	service, member := createTestRunnerServiceAndMember(t)
	ownership, err := ResolveServiceDNSOwnership("user-a", service)
	if err != nil {
		t.Fatalf("failed to resolve ownership: %v", err)
	}

	if _, claimed := claimDNSRun(ownership.Key, 999); !claimed {
		t.Fatal("expected to claim dns run lock for test")
	}
	t.Cleanup(func() {
		releaseDNSRun(ownership.Key, 999)
	})

	if _, err := RunMemberFailoverNowForUser("user-a", service.ID, member.ID); err == nil {
		t.Fatal("expected active dns lock to block failover start")
	} else if !strings.Contains(err.Error(), "already being modified") {
		t.Fatalf("expected active dns lock conflict error, got %v", err)
	}
}
