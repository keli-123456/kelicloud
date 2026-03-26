package client

import "testing"

func TestPopulateBasicInfoFallbackIPUsesStringForIPv4(t *testing.T) {
	cbi := map[string]interface{}{
		"ipv4": "",
		"ipv6": "",
	}

	populateBasicInfoFallbackIP(cbi, "203.0.113.10")

	got, ok := cbi["ipv4"].(string)
	if !ok {
		t.Fatalf("expected ipv4 fallback to be stored as string, got %T", cbi["ipv4"])
	}
	if got != "203.0.113.10" {
		t.Fatalf("expected ipv4 fallback %q, got %q", "203.0.113.10", got)
	}
}

func TestPopulateBasicInfoFallbackIPUsesStringForIPv6(t *testing.T) {
	cbi := map[string]interface{}{
		"ipv4": "",
		"ipv6": "",
	}

	populateBasicInfoFallbackIP(cbi, "2001:db8::10")

	got, ok := cbi["ipv6"].(string)
	if !ok {
		t.Fatalf("expected ipv6 fallback to be stored as string, got %T", cbi["ipv6"])
	}
	if got != "2001:db8::10" {
		t.Fatalf("expected ipv6 fallback %q, got %q", "2001:db8::10", got)
	}
}

func TestPopulateBasicInfoFallbackIPKeepsProvidedIP(t *testing.T) {
	cbi := map[string]interface{}{
		"ipv4": "198.51.100.20",
		"ipv6": "",
	}

	populateBasicInfoFallbackIP(cbi, "203.0.113.10")

	got, ok := cbi["ipv4"].(string)
	if !ok {
		t.Fatalf("expected ipv4 to stay as string, got %T", cbi["ipv4"])
	}
	if got != "198.51.100.20" {
		t.Fatalf("expected existing ipv4 %q, got %q", "198.51.100.20", got)
	}
}
