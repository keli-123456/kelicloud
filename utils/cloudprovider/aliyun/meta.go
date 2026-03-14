package aliyun

import "github.com/komari-monitor/komari/utils/cloudprovider/factory"

type providerConfig struct {
	AccessKeyID     string `json:"access_key_id" required:"true" help:"Aliyun AccessKey ID with AliDNS permissions."`
	AccessKeySecret string `json:"access_key_secret" required:"true" help:"Aliyun AccessKey Secret paired with the AccessKey ID."`
	RegionID        string `json:"region_id" required:"false" default:"cn-hangzhou" help:"AliDNS API region. Leave the default unless your deployment requires another endpoint."`
	DomainName      string `json:"domain_name" required:"false" help:"Optional root domain, for example example.com."`
}

type Provider struct{}

func (p *Provider) GetName() string {
	return "aliyun"
}

func (p *Provider) GetConfiguration() factory.Configuration {
	return &providerConfig{}
}

func init() {
	factory.RegisterCloudProvider(func() factory.ICloudProvider {
		return &Provider{}
	})
}
