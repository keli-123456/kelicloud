package failoverv2

import "testing"

func TestParseAzureMemberPlanPayloadDefaultsNetworkingAndImageVersion(t *testing.T) {
	payload, err := parseAzureMemberPlanPayload(`{
		"location": "eastus",
		"size": "Standard_B1s",
		"image": {
			"publisher": "Canonical",
			"offer": "ubuntu-24_04-lts",
			"sku": "server"
		}
	}`)
	if err != nil {
		t.Fatalf("parseAzureMemberPlanPayload returned error: %v", err)
	}

	if !payload.PublicIP {
		t.Fatalf("expected public_ip to default to true")
	}
	if !payload.AssignIPv6 {
		t.Fatalf("expected assign_ipv6 to default to true")
	}
	if payload.RootPasswordMode != "random" {
		t.Fatalf("expected root_password_mode random, got %q", payload.RootPasswordMode)
	}
	if payload.Image.Version != "latest" {
		t.Fatalf("expected image version latest, got %q", payload.Image.Version)
	}
}

func TestParseAzureMemberPlanPayloadPreservesExplicitNetworking(t *testing.T) {
	payload, err := parseAzureMemberPlanPayload(`{
		"location": "eastus",
		"size": "Standard_B1s",
		"public_ip": false,
		"assign_ipv6": false,
		"image": {
			"publisher": "Canonical",
			"offer": "ubuntu-24_04-lts",
			"sku": "server",
			"version": "latest"
		}
	}`)
	if err != nil {
		t.Fatalf("parseAzureMemberPlanPayload returned error: %v", err)
	}

	if payload.PublicIP {
		t.Fatalf("expected explicit public_ip=false to be preserved")
	}
	if payload.AssignIPv6 {
		t.Fatalf("expected explicit assign_ipv6=false to be preserved")
	}
}

func TestParseAzureMemberPlanPayloadRequiresImageReference(t *testing.T) {
	_, err := parseAzureMemberPlanPayload(`{"size":"Standard_B1s"}`)
	if err == nil {
		t.Fatalf("expected missing image reference to fail")
	}
}

func TestFirstAzurePublicIPByVersion(t *testing.T) {
	addresses := []string{"10.0.0.4", "203.0.113.9", "2001:db8::9"}

	if got := firstAzurePublicIPByVersion(addresses, false); got != "10.0.0.4" {
		t.Fatalf("expected first IPv4, got %q", got)
	}
	if got := firstAzurePublicIPByVersion(addresses, true); got != "2001:db8::9" {
		t.Fatalf("expected first IPv6, got %q", got)
	}
}
