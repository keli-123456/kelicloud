package azure

import "github.com/komari-monitor/komari/utils/cloudprovider/factory"

type providerConfig struct {
	TenantID        string `json:"tenant_id" required:"false" help:"Legacy Azure tenant ID field. Use the credential pool in the cloud panel for multi-subscription management."`
	ClientID        string `json:"client_id" required:"false" help:"Legacy Azure application (client) ID field."`
	ClientSecret    string `json:"client_secret" required:"false" help:"Legacy Azure client secret field."`
	SubscriptionID  string `json:"subscription_id" required:"false" help:"Legacy Azure subscription ID field."`
	DefaultLocation string `json:"default_location" required:"false" help:"Default Azure location used by the panel and future VM operations."`
}

type Provider struct{}

func (p *Provider) GetName() string {
	return "azure"
}

func (p *Provider) GetConfiguration() factory.Configuration {
	return &providerConfig{}
}

func init() {
	factory.RegisterCloudProvider(func() factory.ICloudProvider {
		return &Provider{}
	})
}
