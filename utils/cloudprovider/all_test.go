package cloudprovider

import (
	"testing"

	"github.com/komari-monitor/komari/utils/cloudprovider/factory"
)

func TestDNSProvidersRegistered(t *testing.T) {
	for _, name := range []string{"cloudflare", "aliyun"} {
		if _, ok := factory.GetConstructor(name); !ok {
			t.Fatalf("expected cloud provider %s to be registered", name)
		}
	}
}
