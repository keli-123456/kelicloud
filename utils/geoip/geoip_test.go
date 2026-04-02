package geoip_test

import (
	"net"
	"os"
	"testing"

	"github.com/komari-monitor/komari/utils/geoip"
)

const geoIPIntegrationEnv = "KOMARI_RUN_GEOIP_INTEGRATION"

type stubGeoIPService struct {
	name  string
	info  *geoip.GeoInfo
	err   error
	calls int
}

func (s *stubGeoIPService) Name() string {
	return s.name
}

func (s *stubGeoIPService) GetGeoInfo(net.IP) (*geoip.GeoInfo, error) {
	s.calls++
	return s.info, s.err
}

func (s *stubGeoIPService) UpdateDatabase() error {
	return nil
}

func (s *stubGeoIPService) Close() error {
	return nil
}

func TestGetGeoInfoCachesByProvider(t *testing.T) {
	previousProvider := geoip.CurrentProvider
	provider := &stubGeoIPService{
		name: t.Name(),
		info: &geoip.GeoInfo{
			ISOCode: "US",
			Name:    "United States",
		},
	}
	geoip.CurrentProvider = provider
	t.Cleanup(func() {
		geoip.CurrentProvider = previousProvider
	})

	record, err := geoip.GetGeoInfo(net.ParseIP("203.0.113.10"))
	if err != nil {
		t.Fatalf("GetGeoInfo() returned error: %v", err)
	}
	if record == nil {
		t.Fatal("GetGeoInfo() returned nil record")
	}

	record, err = geoip.GetGeoInfo(net.ParseIP("203.0.113.10"))
	if err != nil {
		t.Fatalf("GetGeoInfo() returned error on cached call: %v", err)
	}
	if record == nil {
		t.Fatal("GetGeoInfo() returned nil record on cached call")
	}
	if provider.calls != 1 {
		t.Fatalf("expected provider to be called once, got %d", provider.calls)
	}
}

func TestMMDBIntegration(t *testing.T) {
	runGeoIPIntegrationTest(t, func() (geoip.GeoIPService, error) {
		return geoip.NewMaxMindGeoIPService()
	})
}

func TestIPAPIIntegration(t *testing.T) {
	runGeoIPIntegrationTest(t, func() (geoip.GeoIPService, error) {
		return geoip.NewIPAPIService()
	})
}

func TestGeoJSIntegration(t *testing.T) {
	runGeoIPIntegrationTest(t, func() (geoip.GeoIPService, error) {
		return geoip.NewGeoJSService()
	})
}

func TestIPInfoIntegration(t *testing.T) {
	runGeoIPIntegrationTest(t, func() (geoip.GeoIPService, error) {
		return geoip.NewIPInfoService()
	})
}

func runGeoIPIntegrationTest(t *testing.T, newProvider func() (geoip.GeoIPService, error)) {
	t.Helper()

	if os.Getenv(geoIPIntegrationEnv) != "1" {
		t.Skipf("set %s=1 to run external GeoIP integration tests", geoIPIntegrationEnv)
	}

	previousProvider := geoip.CurrentProvider
	provider, err := newProvider()
	if err != nil {
		t.Fatalf("failed to initialize GeoIP provider: %v", err)
	}
	if provider == nil {
		t.Fatal("GeoIP provider factory returned nil provider")
	}

	geoip.CurrentProvider = provider
	t.Cleanup(func() {
		_ = provider.Close()
		geoip.CurrentProvider = previousProvider
	})

	for _, ipaddr := range []string{
		"8.8.8.8",
		"2001:4860:4860::8888",
	} {
		ip := net.ParseIP(ipaddr)
		if ip == nil {
			t.Fatalf("failed to parse IP %s", ipaddr)
		}

		record, err := geoip.GetGeoInfo(ip)
		if err != nil {
			t.Fatalf("failed to get GeoIP info for %s: %v", ipaddr, err)
		}
		if record == nil {
			t.Fatalf("GeoIP record is nil for %s", ipaddr)
		}
		if record.ISOCode == "" && record.Name == "" {
			t.Fatalf("country information is missing for %s", ipaddr)
		}
	}
}

func TestUnicodeEmoji(t *testing.T) {
	ISOCode := "CN"
	emoji := geoip.GetRegionUnicodeEmoji(ISOCode)
	if emoji != "🇨🇳" {
		t.Errorf("Expected emoji for %s, got %s", ISOCode, emoji)
	}
	t.Logf("Emoji for %s: %s", ISOCode, emoji)
}
