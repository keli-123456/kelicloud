package failover

import "testing"

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
	})

	t.Run("ipv6 only still requires ipv6", func(t *testing.T) {
		if _, err := buildDNSApplyPlan("AAAA", false, "1.2.3.4", ""); err == nil {
			t.Fatalf("expected error when ipv6 is missing")
		}
	})
}
