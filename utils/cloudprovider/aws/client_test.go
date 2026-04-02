package aws

import (
	"context"
	"errors"
	"testing"

	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/require"
)

func TestCountsTowardStandardOnDemandVCPUQuota(t *testing.T) {
	require.True(t, countsTowardStandardOnDemandVCPUQuota(ec2types.Instance{
		InstanceType: ec2types.InstanceType("c7a.large"),
	}))

	require.False(t, countsTowardStandardOnDemandVCPUQuota(ec2types.Instance{
		InstanceType:      ec2types.InstanceType("c7a.large"),
		InstanceLifecycle: ec2types.InstanceLifecycleTypeSpot,
	}))

	require.False(t, countsTowardStandardOnDemandVCPUQuota(ec2types.Instance{
		InstanceType:    ec2types.InstanceType("p5.48xlarge"),
		CapacityBlockId: stringPtr("cr-block-1"),
	}))

	require.False(t, countsTowardStandardOnDemandVCPUQuota(ec2types.Instance{
		InstanceType: ec2types.InstanceType("p5.48xlarge"),
	}))
}

func TestCapacityReservationCountsTowardStandardOnDemandVCPUQuota(t *testing.T) {
	require.True(t, capacityReservationCountsTowardStandardOnDemandVCPUQuota(ec2types.CapacityReservation{
		InstanceType:    stringPtr("m7i.large"),
		ReservationType: ec2types.CapacityReservationTypeDefault,
		State:           ec2types.CapacityReservationStateActive,
	}))

	require.True(t, capacityReservationCountsTowardStandardOnDemandVCPUQuota(ec2types.CapacityReservation{
		InstanceType: stringPtr("c7a.large"),
		State:        ec2types.CapacityReservationStateScheduled,
	}))

	require.False(t, capacityReservationCountsTowardStandardOnDemandVCPUQuota(ec2types.CapacityReservation{
		InstanceType:    stringPtr("m7i.large"),
		ReservationType: ec2types.CapacityReservationTypeCapacityBlock,
		State:           ec2types.CapacityReservationStateActive,
	}))

	require.False(t, capacityReservationCountsTowardStandardOnDemandVCPUQuota(ec2types.CapacityReservation{
		InstanceType: stringPtr("p5.48xlarge"),
		State:        ec2types.CapacityReservationStateActive,
	}))

	require.False(t, capacityReservationCountsTowardStandardOnDemandVCPUQuota(ec2types.CapacityReservation{
		InstanceType: stringPtr("m7i.large"),
		State:        ec2types.CapacityReservationStateExpired,
	}))
}

func TestCollectInstanceTypes(t *testing.T) {
	require.Equal(t, []string{"c7a.large", "m7i.large"}, collectInstanceTypes(
		map[string]int{
			"m7i.large": 1,
			"":          4,
		},
		map[string]int{
			"c7a.large": 2,
			"m7i.large": 3,
			"t4g.small": 0,
		},
	))
}

func TestShouldUseDefaultStandardOnDemandVCPUQuota(t *testing.T) {
	require.True(t, shouldUseDefaultStandardOnDemandVCPUQuota(context.DeadlineExceeded))
	require.True(t, shouldUseDefaultStandardOnDemandVCPUQuota(errors.New("request timed out")))
	require.True(t, shouldUseDefaultStandardOnDemandVCPUQuota(errors.New("timeout while calling service quotas")))
	require.False(t, shouldUseDefaultStandardOnDemandVCPUQuota(errors.New("access denied")))
	require.False(t, shouldUseDefaultStandardOnDemandVCPUQuota(nil))
}

func TestParseManagedDebianImageReference(t *testing.T) {
	preset, ok := parseManagedDebianImageReference("komari:debian-13-amd64")
	require.True(t, ok)
	require.Equal(t, "13", preset.Release)
	require.Equal(t, "amd64", preset.Architecture)
	require.Equal(t, "debian-13-amd64-*", preset.NamePattern)

	preset, ok = parseManagedDebianImageReference("komari:debian-12-arm64")
	require.True(t, ok)
	require.Equal(t, "debian-12-arm64-*", preset.NamePattern)

	_, ok = parseManagedDebianImageReference("komari:debian-12")
	require.False(t, ok)
	_, ok = parseManagedDebianImageReference("resolve:ssm:/aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id")
	require.False(t, ok)
}

func stringPtr(value string) *string {
	return &value
}
