package azure

import "testing"

func TestBuildVirtualNetworkPropertiesIPv4Only(t *testing.T) {
	properties := buildVirtualNetworkProperties("default", false)

	addressSpace, ok := properties["addressSpace"].(map[string]any)
	if !ok {
		t.Fatalf("addressSpace missing or wrong type: %#v", properties["addressSpace"])
	}
	prefixes, ok := addressSpace["addressPrefixes"].([]string)
	if !ok {
		t.Fatalf("addressPrefixes missing or wrong type: %#v", addressSpace["addressPrefixes"])
	}
	if len(prefixes) != 1 || prefixes[0] != azureVNetIPv4CIDR {
		t.Fatalf("unexpected IPv4-only address prefixes: %#v", prefixes)
	}

	subnets, ok := properties["subnets"].([]map[string]any)
	if !ok || len(subnets) != 1 {
		t.Fatalf("unexpected subnets payload: %#v", properties["subnets"])
	}
	subnetProperties, ok := subnets[0]["properties"].(map[string]any)
	if !ok {
		t.Fatalf("subnet properties missing: %#v", subnets[0]["properties"])
	}
	if subnetProperties["addressPrefix"] != azureSubnetIPv4CIDR {
		t.Fatalf("unexpected IPv4 subnet prefix: %#v", subnetProperties["addressPrefix"])
	}
	if _, exists := subnetProperties["addressPrefixes"]; exists {
		t.Fatalf("unexpected dual-stack subnet prefixes in IPv4-only payload: %#v", subnetProperties["addressPrefixes"])
	}
}

func TestBuildVirtualNetworkPropertiesDualStack(t *testing.T) {
	properties := buildVirtualNetworkProperties("default", true)

	addressSpace, ok := properties["addressSpace"].(map[string]any)
	if !ok {
		t.Fatalf("addressSpace missing or wrong type: %#v", properties["addressSpace"])
	}
	prefixes, ok := addressSpace["addressPrefixes"].([]string)
	if !ok {
		t.Fatalf("addressPrefixes missing or wrong type: %#v", addressSpace["addressPrefixes"])
	}
	if len(prefixes) != 2 || prefixes[0] != azureVNetIPv4CIDR || prefixes[1] != azureVNetIPv6CIDR {
		t.Fatalf("unexpected dual-stack address prefixes: %#v", prefixes)
	}

	subnets, ok := properties["subnets"].([]map[string]any)
	if !ok || len(subnets) != 1 {
		t.Fatalf("unexpected subnets payload: %#v", properties["subnets"])
	}
	subnetProperties, ok := subnets[0]["properties"].(map[string]any)
	if !ok {
		t.Fatalf("subnet properties missing: %#v", subnets[0]["properties"])
	}
	subnetPrefixes, ok := subnetProperties["addressPrefixes"].([]string)
	if !ok {
		t.Fatalf("dual-stack subnet prefixes missing: %#v", subnetProperties["addressPrefixes"])
	}
	if len(subnetPrefixes) != 2 || subnetPrefixes[0] != azureSubnetIPv4CIDR || subnetPrefixes[1] != azureSubnetIPv6CIDR {
		t.Fatalf("unexpected dual-stack subnet prefixes: %#v", subnetPrefixes)
	}
}

func TestBuildNetworkInterfaceIPConfigurationsDualStack(t *testing.T) {
	configurations := buildNetworkInterfaceIPConfigurations("/subnet/id", "/public/ipv4", "/public/ipv6", true)
	if len(configurations) != 2 {
		t.Fatalf("expected dual-stack NIC configs, got %#v", configurations)
	}

	ipv4Properties, ok := configurations[0]["properties"].(map[string]any)
	if !ok {
		t.Fatalf("IPv4 config properties missing: %#v", configurations[0]["properties"])
	}
	if ipv4Properties["privateIPAddressVersion"] != "IPv4" {
		t.Fatalf("unexpected IPv4 config version: %#v", ipv4Properties["privateIPAddressVersion"])
	}
	if ipv4Properties["publicIPAddress"] == nil {
		t.Fatalf("expected public IPv4 attachment: %#v", ipv4Properties)
	}

	ipv6Properties, ok := configurations[1]["properties"].(map[string]any)
	if !ok {
		t.Fatalf("IPv6 config properties missing: %#v", configurations[1]["properties"])
	}
	if ipv6Properties["privateIPAddressVersion"] != "IPv6" {
		t.Fatalf("unexpected IPv6 config version: %#v", ipv6Properties["privateIPAddressVersion"])
	}
	if ipv6Properties["publicIPAddress"] == nil {
		t.Fatalf("expected public IPv6 attachment: %#v", ipv6Properties)
	}
}
