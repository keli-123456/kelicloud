package aws

import "github.com/komari-monitor/komari/utils/cloudprovider/factory"

type providerConfig struct {
	AccessKeyID     string `json:"access_key_id" required:"false" help:"Legacy AWS access key field. Use the credential pool in the cloud panel for multi-account management."`
	SecretAccessKey string `json:"secret_access_key" required:"false" help:"Legacy AWS secret access key field."`
	SessionToken    string `json:"session_token" required:"false" help:"Optional AWS session token for temporary credentials."`
	DefaultRegion   string `json:"default_region" required:"false" help:"Default AWS region for EC2 operations. The panel also supports switching regions dynamically."`
}

type Provider struct{}

func (p *Provider) GetName() string {
	return "aws"
}

func (p *Provider) GetConfiguration() factory.Configuration {
	return &providerConfig{}
}

func init() {
	factory.RegisterCloudProvider(func() factory.ICloudProvider {
		return &Provider{}
	})
}
