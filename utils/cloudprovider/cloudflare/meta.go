package cloudflare

import "github.com/komari-monitor/komari/utils/cloudprovider/factory"

type providerConfig struct {
	APIToken string `json:"api_token" required:"true" help:"Cloudflare API token with Zone.DNS edit permission."`
	ZoneID   string `json:"zone_id" required:"false" help:"Optional fixed Cloudflare zone ID. Leave empty if you prefer resolving the zone by name later."`
	ZoneName string `json:"zone_name" required:"false" help:"Optional zone name, for example example.com."`
	Proxied  bool   `json:"proxied" required:"false" default:"false" help:"Whether new A/AAAA records should be proxied through Cloudflare by default."`
}

type Provider struct{}

func (p *Provider) GetName() string {
	return "cloudflare"
}

func (p *Provider) GetConfiguration() factory.Configuration {
	return &providerConfig{}
}

func init() {
	factory.RegisterCloudProvider(func() factory.ICloudProvider {
		return &Provider{}
	})
}
