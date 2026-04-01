package failover

import (
	"strings"
	"testing"
)

func TestBuildDNSApplyPlan(t *testing.T) {
	t.Run("strict ipv4 only", func(t *testing.T) {
		plan, err := buildDNSApplyPlan("A", false, "1.2.3.4", "")
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(plan.RecordTypes) != 1 || plan.RecordTypes[0] != "A" {
			t.Fatalf("unexpected record types: %#v", plan.RecordTypes)
		}
		if len(plan.SkippedTypes) != 0 {
			t.Fatalf("expected no skipped types, got %#v", plan.SkippedTypes)
		}
		if len(plan.PrunedTypes) != 1 || plan.PrunedTypes[0] != "AAAA" {
			t.Fatalf("unexpected pruned types: %#v", plan.PrunedTypes)
		}
	})

	t.Run("dual stack skips aaaa when ipv6 missing", func(t *testing.T) {
		plan, err := buildDNSApplyPlan("A", true, "1.2.3.4", "")
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(plan.RecordTypes) != 1 || plan.RecordTypes[0] != "A" {
			t.Fatalf("unexpected record types: %#v", plan.RecordTypes)
		}
		if len(plan.SkippedTypes) != 1 || plan.SkippedTypes[0] != "AAAA" {
			t.Fatalf("unexpected skipped types: %#v", plan.SkippedTypes)
		}
		if len(plan.PrunedTypes) != 1 || plan.PrunedTypes[0] != "AAAA" {
			t.Fatalf("unexpected pruned types: %#v", plan.PrunedTypes)
		}
	})

	t.Run("dual stack includes aaaa when ipv6 exists", func(t *testing.T) {
		plan, err := buildDNSApplyPlan("A", true, "1.2.3.4", "2001:db8::1")
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(plan.RecordTypes) != 2 || plan.RecordTypes[0] != "A" || plan.RecordTypes[1] != "AAAA" {
			t.Fatalf("unexpected record types: %#v", plan.RecordTypes)
		}
		if len(plan.SkippedTypes) != 0 {
			t.Fatalf("expected no skipped types, got %#v", plan.SkippedTypes)
		}
		if len(plan.PrunedTypes) != 0 {
			t.Fatalf("expected no pruned types, got %#v", plan.PrunedTypes)
		}
	})

	t.Run("ipv6 only still requires ipv6", func(t *testing.T) {
		if _, err := buildDNSApplyPlan("AAAA", false, "1.2.3.4", ""); err == nil {
			t.Fatalf("expected error when ipv6 is missing")
		}
	})

	t.Run("strict ipv6 only prunes a records", func(t *testing.T) {
		plan, err := buildDNSApplyPlan("AAAA", false, "", "2001:db8::1")
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(plan.RecordTypes) != 1 || plan.RecordTypes[0] != "AAAA" {
			t.Fatalf("unexpected record types: %#v", plan.RecordTypes)
		}
		if len(plan.PrunedTypes) != 1 || plan.PrunedTypes[0] != "A" {
			t.Fatalf("unexpected pruned types: %#v", plan.PrunedTypes)
		}
	})
}

func TestNormalizeAliyunLinesCanonicalizesKnownLabels(t *testing.T) {
	lines := normalizeAliyunLines("默认", []string{"telecom", "电信", "境外"})
	if len(lines) != 3 {
		t.Fatalf("expected 3 unique lines, got %#v", lines)
	}
	if lines[0] != "default" || lines[1] != "telecom" || lines[2] != "oversea" {
		t.Fatalf("unexpected normalized lines: %#v", lines)
	}
}

func TestEvaluateDNSVerificationRecordsDetectsUnexpectedPrunedRecord(t *testing.T) {
	proxied := false
	result := evaluateDNSVerificationRecords(cloudflareProviderName, []dnsUpdateResult{
		{
			Provider: cloudflareProviderName,
			Name:     "example.com",
			Type:     "A",
			Value:    "1.2.3.4",
			Proxied:  &proxied,
		},
	}, []dnsUpdateResult{
		{
			Provider: cloudflareProviderName,
			Name:     "example.com",
			Type:     "A",
			Value:    "1.2.3.4",
			Proxied:  &proxied,
		},
		{
			Provider: cloudflareProviderName,
			Name:     "example.com",
			Type:     "AAAA",
			Value:    "2001:db8::1",
			Proxied:  &proxied,
		},
	}, cloudflareDNSRecordsMatch)
	if result == nil {
		t.Fatal("expected verification result")
	}
	if result.Success {
		t.Fatal("expected verification to fail when stale AAAA record remains")
	}
	if len(result.Missing) != 0 {
		t.Fatalf("expected no missing records, got %#v", result.Missing)
	}
	if len(result.Unexpected) != 1 || result.Unexpected[0].Type != "AAAA" {
		t.Fatalf("unexpected unexpected records: %#v", result.Unexpected)
	}
}

func TestEvaluateDNSVerificationRecordsMatchesAliyunNormalizedLines(t *testing.T) {
	result := evaluateDNSVerificationRecords(aliyunProviderName, []dnsUpdateResult{
		{
			Provider: aliyunProviderName,
			Domain:   "example.com",
			RR:       "@",
			Line:     "default",
			Type:     "A",
			Value:    "1.2.3.4",
		},
		{
			Provider: aliyunProviderName,
			Domain:   "example.com",
			RR:       "@",
			Line:     "telecom",
			Type:     "A",
			Value:    "1.2.3.4",
		},
	}, []dnsUpdateResult{
		{
			Provider: aliyunProviderName,
			Domain:   "example.com",
			RR:       "@",
			Line:     "默认",
			Type:     "A",
			Value:    "1.2.3.4",
		},
		{
			Provider: aliyunProviderName,
			Domain:   "example.com",
			RR:       "@",
			Line:     "电信",
			Type:     "A",
			Value:    "1.2.3.4",
		},
	}, aliyunDNSRecordsMatch)
	if result == nil {
		t.Fatal("expected verification result")
	}
	if !result.Success {
		t.Fatalf("expected verification to pass, got missing=%#v unexpected=%#v", result.Missing, result.Unexpected)
	}
}

func TestFormatAliyunRPCErrorIncludesAPIErrorDetails(t *testing.T) {
	err := formatAliyunRPCError("400 Bad Request", []byte(`{"Code":"InvalidTTL","Message":"invalid ttl","RequestId":"req-123"}`))
	if err == nil {
		t.Fatal("expected error")
	}
	message := err.Error()
	for _, part := range []string{"400 Bad Request", "InvalidTTL", "invalid ttl", "req-123"} {
		if !strings.Contains(message, part) {
			t.Fatalf("expected %q in error message %q", part, message)
		}
	}
}

func TestFormatAliyunRPCErrorFallsBackToBodyText(t *testing.T) {
	err := formatAliyunRPCError("400 Bad Request", []byte("plain failure"))
	if err == nil {
		t.Fatal("expected error")
	}
	message := err.Error()
	for _, part := range []string{"400 Bad Request", "plain failure"} {
		if !strings.Contains(message, part) {
			t.Fatalf("expected %q in error message %q", part, message)
		}
	}
}
