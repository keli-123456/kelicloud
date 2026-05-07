package vultr

import "github.com/komari-monitor/komari/utils/cloudprovider/factory"

type providerConfig struct {
	Token string `json:"token" required:"false" help:"Legacy Vultr API token field. Use the token pool in the cloud panel for multi-token management."`
}

type Provider struct{}

func (p *Provider) GetName() string {
	return "vultr"
}

func (p *Provider) GetConfiguration() factory.Configuration {
	return &providerConfig{}
}

func init() {
	factory.RegisterCloudProvider(func() factory.ICloudProvider {
		return &Provider{}
	})
}
