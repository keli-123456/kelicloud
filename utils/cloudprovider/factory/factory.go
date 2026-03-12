package factory

import (
	"log"

	"github.com/komari-monitor/komari/utils/item"
)

var (
	providers           = make(map[string]ICloudProvider)
	providerConstructor = make(map[string]CloudProviderConstructor)
	providerItems       = make(map[string][]item.Item)
)

func RegisterCloudProvider(constructor CloudProviderConstructor) {
	provider := constructor()
	if provider == nil {
		panic("cloud provider constructor returned nil")
	}

	name := provider.GetName()
	providerConstructor[name] = constructor
	if _, exists := providers[name]; exists {
		log.Println("Cloud provider already registered: " + name)
	}
	providers[name] = provider
	providerItems[name] = item.Parse(provider.GetConfiguration())
}

func GetProviderConfigs() map[string][]item.Item {
	return providerItems
}

func GetConstructor(name string) (CloudProviderConstructor, bool) {
	constructor, exists := providerConstructor[name]
	return constructor, exists
}
