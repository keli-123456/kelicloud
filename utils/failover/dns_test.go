package failover

import (
	"context"
	"errors"
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

func TestSelectRecordValueNormalizesCIDRValues(t *testing.T) {
	value, err := selectRecordValue("AAAA", "", "2001:db8::10/64")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if value != "2001:db8::10" {
		t.Fatalf("expected normalized ipv6 value, got %q", value)
	}

	value, err = selectRecordValue("A", "203.0.113.8/32", "")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if value != "203.0.113.8" {
		t.Fatalf("expected normalized ipv4 value, got %q", value)
	}
}

func TestSelectRecordValueRejectsWrongAddressFamily(t *testing.T) {
	if _, err := selectRecordValue("AAAA", "", "203.0.113.8"); err == nil {
		t.Fatal("expected ipv4 value to be rejected for AAAA record")
	}
	if _, err := selectRecordValue("A", "2001:db8::10", ""); err == nil {
		t.Fatal("expected ipv6 value to be rejected for A record")
	}
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

func TestNormalizeAliyunRR(t *testing.T) {
	if got := normalizeAliyunRR("example.com", ""); got != "@" {
		t.Fatalf("expected apex rr, got %q", got)
	}
	if got := normalizeAliyunRR("example.com", "example.com"); got != "@" {
		t.Fatalf("expected apex rr, got %q", got)
	}
	if got := normalizeAliyunRR("example.com", "www.example.com"); got != "www" {
		t.Fatalf("expected host rr, got %q", got)
	}
	if got := normalizeAliyunRR("example.com", "api.internal"); got != "api.internal" {
		t.Fatalf("expected relative rr to stay unchanged, got %q", got)
	}
}

func TestValidateAliyunRRRejectsURLs(t *testing.T) {
	if _, err := validateAliyunRR("example.com", "https://example.com"); err == nil {
		t.Fatal("expected url rr to be rejected")
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

func TestFindAliyunDNSRecordExactMatchPrefersDesiredValueWithinDuplicates(t *testing.T) {
	match := findAliyunDNSRecordExactMatch([]aliyunDNSRecord{
		{
			RecordID: "record-old",
			RR:       "@",
			Type:     "A",
			Value:    "198.51.100.10",
			TTL:      600,
			Line:     "默认",
		},
		{
			RecordID: "record-keep",
			RR:       "@",
			Type:     "A",
			Value:    "203.0.113.20",
			TTL:      600,
			Line:     "default",
		},
	}, "@", "A", "默认", "203.0.113.20")
	if match.RecordID != "record-keep" {
		t.Fatalf("expected exact duplicate match to keep record-keep, got %#v", match)
	}
}

func TestApplyDNSRecordRetriesTransientAliyunError(t *testing.T) {
	originalApply := dnsApplyAliyunFunc
	originalDelay := dnsApplyRetryDelay
	originalAttempts := dnsApplyMaxAttempts
	attempts := 0

	dnsApplyAliyunFunc = func(ctx context.Context, userUUID, entryID, payloadJSON, ipv4, ipv6 string) (*dnsUpdateResult, error) {
		attempts++
		if attempts < 3 {
			return nil, errors.New(`Get "https://alidns.aliyuncs.com/": read tcp 172.23.0.2:45970->47.241.205.161:443: read: connection reset by peer`)
		}
		return &dnsUpdateResult{
			Provider: aliyunProviderName,
			Type:     "A",
			Value:    ipv4,
		}, nil
	}
	dnsApplyRetryDelay = 0
	dnsApplyMaxAttempts = 3
	t.Cleanup(func() {
		dnsApplyAliyunFunc = originalApply
		dnsApplyRetryDelay = originalDelay
		dnsApplyMaxAttempts = originalAttempts
	})

	result, err := applyDNSRecord(context.Background(), "user-1", aliyunProviderName, "entry-1", `{"domain_name":"example.com","rr":"@"}`, "203.0.113.20", "")
	if err != nil {
		t.Fatalf("expected transient aliyun dns error to recover after retry, got %v", err)
	}
	if attempts != 3 {
		t.Fatalf("expected 3 apply attempts, got %d", attempts)
	}
	if result == nil || result.Value != "203.0.113.20" {
		t.Fatalf("expected successful dns update result, got %#v", result)
	}
}

func TestApplyDNSRecordDoesNotRetryNonRetryableAliyunError(t *testing.T) {
	originalApply := dnsApplyAliyunFunc
	originalDelay := dnsApplyRetryDelay
	originalAttempts := dnsApplyMaxAttempts
	attempts := 0

	dnsApplyAliyunFunc = func(ctx context.Context, userUUID, entryID, payloadJSON, ipv4, ipv6 string) (*dnsUpdateResult, error) {
		attempts++
		return nil, errors.New("aliyun dns request failed: 400 Bad Request: DomainRecordDuplicate")
	}
	dnsApplyRetryDelay = 0
	dnsApplyMaxAttempts = 3
	t.Cleanup(func() {
		dnsApplyAliyunFunc = originalApply
		dnsApplyRetryDelay = originalDelay
		dnsApplyMaxAttempts = originalAttempts
	})

	_, err := applyDNSRecord(context.Background(), "user-1", aliyunProviderName, "entry-1", `{"domain_name":"example.com","rr":"@"}`, "203.0.113.20", "")
	if err == nil {
		t.Fatal("expected non-retryable aliyun dns error")
	}
	if attempts != 1 {
		t.Fatalf("expected duplicate-record error not to retry, got %d attempts", attempts)
	}
}

func TestIsRetryableDNSApplyError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "connection reset",
			err:  errors.New(`Get "https://alidns.aliyuncs.com/": read: connection reset by peer`),
			want: true,
		},
		{
			name: "timeout",
			err:  context.DeadlineExceeded,
			want: true,
		},
		{
			name: "rate limited",
			err:  errors.New("cloudflare api request failed: 429 Too Many Requests"),
			want: true,
		},
		{
			name: "duplicate record",
			err:  errors.New("aliyun dns request failed: 400 Bad Request: DomainRecordDuplicate"),
			want: false,
		},
		{
			name: "invalid credential",
			err:  errors.New("aliyun dns request failed: 403 Forbidden: SignatureDoesNotMatch"),
			want: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isRetryableDNSApplyError(tc.err); got != tc.want {
				t.Fatalf("expected retryable=%v, got %v for %v", tc.want, got, tc.err)
			}
		})
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
