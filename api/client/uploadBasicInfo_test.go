package client

import (
	"testing"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/komari-monitor/komari/utils/geoip"
)

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

func TestSetRegionFromGeoInfoPrefersEmoji(t *testing.T) {
	cbi := map[string]interface{}{}

	got := setRegionFromGeoInfo(cbi, &geoip.GeoInfo{ISOCode: "de"})

	if got != "DE" {
		t.Fatalf("expected region code %q, got %q", "DE", got)
	}
	if region, ok := cbi["region"].(string); !ok || region != "🇩🇪" {
		t.Fatalf("expected emoji region %q, got %#v", "🇩🇪", cbi["region"])
	}
}

func TestNormalizeRegionCodeRejectsInvalidValues(t *testing.T) {
	if got := normalizeRegionCode("de"); got != "DE" {
		t.Fatalf("expected normalized code %q, got %q", "DE", got)
	}
	if got := normalizeRegionCode("eu-west"); got != "" {
		t.Fatalf("expected invalid code to normalize to empty string, got %q", got)
	}
}

func TestShouldFallbackRegionToCodeRecognizesMySQLRegionEncodingError(t *testing.T) {
	err := &mysqlDriver.MySQLError{
		Number:  1366,
		Message: "Incorrect string value: '\\xF0\\x9F...' for column 'region' at row 1",
	}

	if !shouldFallbackRegionToCode(err) {
		t.Fatal("expected MySQL region encoding error to trigger ASCII region fallback")
	}
}

func TestShouldFallbackRegionToCodeIgnoresOtherErrors(t *testing.T) {
	err := &mysqlDriver.MySQLError{
		Number:  1366,
		Message: "Incorrect string value: '\\xF0\\x9F...' for column 'name' at row 1",
	}

	if shouldFallbackRegionToCode(err) {
		t.Fatal("expected non-region MySQL encoding error to skip ASCII region fallback")
	}
}
