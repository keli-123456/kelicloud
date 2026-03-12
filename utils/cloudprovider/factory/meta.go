package factory

type Configuration interface{}

type ICloudProvider interface {
	GetName() string
	GetConfiguration() Configuration
}

type CloudProviderConstructor func() ICloudProvider
