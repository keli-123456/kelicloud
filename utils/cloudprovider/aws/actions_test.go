package aws

import (
	"testing"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/require"
)

func TestResolveInstanceAddressTargetPrivateIP(t *testing.T) {
	instance := ec2types.Instance{
		PrivateIpAddress: awssdk.String("10.0.0.24"),
	}

	require.Equal(t, "10.0.0.8", resolveInstanceAddressTargetPrivateIP(instance, "10.0.0.8"))
	require.Equal(t, "10.0.0.24", resolveInstanceAddressTargetPrivateIP(instance, ""))
}

func TestFilterAssociatedInstanceAddresses(t *testing.T) {
	addresses := []Address{
		{AllocationID: "eipalloc-1", AssociationID: "eipassoc-1", PrivateIP: "10.0.0.10"},
		{AllocationID: "eipalloc-2", AssociationID: "eipassoc-2", PrivateIP: "10.0.0.11"},
		{AllocationID: "eipalloc-3", AssociationID: "", PrivateIP: "10.0.0.10"},
	}

	filtered := filterAssociatedInstanceAddresses(addresses, "10.0.0.10")
	require.Len(t, filtered, 1)
	require.Equal(t, "eipalloc-1", filtered[0].AllocationID)

	filtered = filterAssociatedInstanceAddresses(addresses, "")
	require.Len(t, filtered, 2)
}

func TestPrimaryInstanceNetworkInterfaceID(t *testing.T) {
	instance := ec2types.Instance{
		NetworkInterfaces: []ec2types.InstanceNetworkInterface{
			{
				NetworkInterfaceId: awssdk.String("eni-secondary"),
				Attachment: &ec2types.InstanceNetworkInterfaceAttachment{
					DeviceIndex: awssdk.Int32(1),
				},
			},
			{
				NetworkInterfaceId: awssdk.String("eni-primary"),
				Attachment: &ec2types.InstanceNetworkInterfaceAttachment{
					DeviceIndex: awssdk.Int32(0),
				},
			},
		},
	}

	require.Equal(t, "eni-primary", primaryInstanceNetworkInterfaceID(instance))
}

func TestFindAttachedLightsailStaticIPName(t *testing.T) {
	staticIPs := []LightsailStaticIP{
		{Name: "ip-a", AttachedTo: "node-a"},
		{Name: "ip-b", AttachedTo: "node-b"},
	}

	require.Equal(t, "ip-b", findAttachedLightsailStaticIPName(staticIPs, "node-b"))
	require.Empty(t, findAttachedLightsailStaticIPName(staticIPs, "node-c"))
}
