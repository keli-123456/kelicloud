package failover

import (
	"strings"
	"testing"

	"github.com/komari-monitor/komari/database/models"
)

func TestVerifyPreviewCatalogOption(t *testing.T) {
	var (
		issues   []string
		warnings []string
	)

	verifyPreviewCatalogOption(&issues, &warnings, "region", "us-east", []CatalogOption{
		{Value: "us-east", Label: "US East"},
	})
	if len(issues) != 0 || len(warnings) != 0 {
		t.Fatalf("expected exact catalog match to pass, got issues=%#v warnings=%#v", issues, warnings)
	}

	verifyPreviewCatalogOption(&issues, &warnings, "size", "s-1vcpu-1gb", nil)
	if len(warnings) != 1 || !strings.Contains(warnings[0], "could not be verified") {
		t.Fatalf("expected empty catalog to produce warning, got %#v", warnings)
	}

	verifyPreviewCatalogOption(&issues, &warnings, "image", "ubuntu-24", []CatalogOption{
		{Value: "debian-12", Label: "Debian 12"},
	})
	if len(issues) != 1 || !strings.Contains(issues[0], "was not found") {
		t.Fatalf("expected missing option to produce issue, got %#v", issues)
	}
}

func TestBuildPreviewDNSRecordsForCloudflare(t *testing.T) {
	task := models.FailoverTask{
		DNSProvider: cloudflareProviderName,
		DNSPayload:  `{"zone_name":"example.com","record_name":"www","record_type":"A","sync_ipv6":true}`,
	}

	records := buildPreviewDNSRecords(task, &DNSCatalog{
		Defaults: DNSCatalogDefaults{
			ZoneName: "example.com",
		},
	})
	if len(records) != 2 {
		t.Fatalf("expected dual-stack preview records, got %#v", records)
	}
	if records[0]["name"] != "www.example.com" {
		t.Fatalf("expected fqdn record name, got %#v", records[0]["name"])
	}
	if records[0]["type"] != "A" || records[1]["type"] != "AAAA" {
		t.Fatalf("unexpected record types %#v", records)
	}
}

func TestBuildPreviewDNSRecordsForAliyun(t *testing.T) {
	task := models.FailoverTask{
		DNSProvider: aliyunProviderName,
		DNSPayload:  `{"domain_name":"example.com","rr":"www.example.com","record_type":"A","sync_ipv6":false,"lines":["默认","电信"]}`,
	}

	records := buildPreviewDNSRecords(task, nil)
	if len(records) != 2 {
		t.Fatalf("expected two line-specific records, got %#v", records)
	}
	if records[0]["rr"] != "www" {
		t.Fatalf("expected rr to be normalized, got %#v", records[0]["rr"])
	}
	if records[0]["line"] != "default" || records[1]["line"] != "telecom" {
		t.Fatalf("expected aliyun lines to be normalized, got %#v", records)
	}
}
