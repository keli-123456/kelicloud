package digitalocean

import "github.com/komari-monitor/komari/utils/cloudprovider/factory"

type providerConfig struct {
	Token string `json:"token" required:"true" help:"DigitalOcean Personal Access Token. It should have Droplet read/write permissions."`
}

type Provider struct{}

func (p *Provider) GetName() string {
	return "digitalocean"
}

func (p *Provider) GetConfiguration() factory.Configuration {
	return &providerConfig{}
}

func init() {
	factory.RegisterCloudProvider(func() factory.ICloudProvider {
		return &Provider{}
	})
}
