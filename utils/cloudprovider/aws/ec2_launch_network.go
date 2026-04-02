package aws

import (
	"context"
	"fmt"
	"math/big"
	"net/netip"
	"sort"
	"strings"
	"time"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
)

const (
	defaultLaunchNetworkPollAttempts = 8
	defaultLaunchNetworkPollDelay    = 2 * time.Second
)

type createInstanceNetworkPlan struct {
	SubnetID   string
	AssignIPv6 bool
	Warnings   []string
}

func prepareCreateInstanceNetwork(ctx context.Context, client *ec2.Client, wantIPv6 bool) (createInstanceNetworkPlan, error) {
	vpc, err := getOrCreateDefaultVPC(ctx, client)
	if err != nil {
		return createInstanceNetworkPlan{}, fmt.Errorf("failed to prepare a default VPC automatically: %w", err)
	}

	vpcID := strings.TrimSpace(awssdk.ToString(vpc.VpcId))
	subnets, err := waitForVPCSubnets(ctx, client, vpcID)
	if err != nil {
		return createInstanceNetworkPlan{}, fmt.Errorf("failed to load subnets for the default VPC: %w", err)
	}

	defaultSubnets := filterDefaultSubnets(subnets)
	if len(defaultSubnets) == 0 {
		createdSubnet, createErr := createFallbackDefaultSubnet(ctx, client, subnets)
		if createErr != nil {
			return createInstanceNetworkPlan{}, fmt.Errorf("failed to create a default subnet automatically: %w", createErr)
		}
		subnets = append(subnets, createdSubnet)
		defaultSubnets = append(defaultSubnets, createdSubnet)
	}

	subnet, ok := selectPreferredLaunchSubnet(defaultSubnets, wantIPv6)
	if !ok {
		return createInstanceNetworkPlan{}, fmt.Errorf("the default VPC has no usable default subnet for instance launch")
	}

	plan := createInstanceNetworkPlan{
		SubnetID:   strings.TrimSpace(awssdk.ToString(subnet.SubnetId)),
		AssignIPv6: wantIPv6,
	}
	if plan.SubnetID == "" {
		return createInstanceNetworkPlan{}, fmt.Errorf("the selected launch subnet is missing an ID")
	}

	if wantIPv6 {
		subnet, err = ensureSubnetSupportsIPv6(ctx, client, vpc, subnet, subnets)
		if err != nil {
			plan.AssignIPv6 = false
			plan.Warnings = append(plan.Warnings, fmt.Sprintf(
				"Komari could not prepare IPv6 automatically on the default AWS subnet, so this instance was launched with IPv4 only: %v",
				err,
			))
		} else {
			plan.SubnetID = strings.TrimSpace(awssdk.ToString(subnet.SubnetId))
		}
	}

	return plan, nil
}

func getOrCreateDefaultVPC(ctx context.Context, client *ec2.Client) (ec2types.Vpc, error) {
	vpc, ok, err := findDefaultVPC(ctx, client)
	if err != nil {
		return ec2types.Vpc{}, err
	}
	if ok {
		return vpc, nil
	}

	output, err := client.CreateDefaultVpc(ctx, &ec2.CreateDefaultVpcInput{})
	if err != nil {
		return ec2types.Vpc{}, err
	}
	if output == nil || output.Vpc == nil {
		return ec2types.Vpc{}, fmt.Errorf("aws create default vpc returned no VPC")
	}
	return *output.Vpc, nil
}

func findDefaultVPC(ctx context.Context, client *ec2.Client) (ec2types.Vpc, bool, error) {
	output, err := client.DescribeVpcs(ctx, &ec2.DescribeVpcsInput{
		Filters: []ec2types.Filter{
			{
				Name:   awssdk.String("isDefault"),
				Values: []string{"true"},
			},
		},
	})
	if err != nil {
		return ec2types.Vpc{}, false, err
	}

	vpcs := make([]ec2types.Vpc, 0, len(output.Vpcs))
	for _, vpc := range output.Vpcs {
		if !awssdk.ToBool(vpc.IsDefault) {
			continue
		}
		if vpc.State != "" && vpc.State != ec2types.VpcStateAvailable {
			continue
		}
		vpcs = append(vpcs, vpc)
	}
	if len(vpcs) == 0 {
		return ec2types.Vpc{}, false, nil
	}

	sort.Slice(vpcs, func(i, j int) bool {
		return strings.TrimSpace(awssdk.ToString(vpcs[i].VpcId)) < strings.TrimSpace(awssdk.ToString(vpcs[j].VpcId))
	})
	return vpcs[0], true, nil
}

func waitForVPCSubnets(ctx context.Context, client *ec2.Client, vpcID string) ([]ec2types.Subnet, error) {
	for attempt := 0; attempt < defaultLaunchNetworkPollAttempts; attempt++ {
		subnets, err := describeSubnetsByVPC(ctx, client, vpcID)
		if err != nil {
			return nil, err
		}
		if len(subnets) > 0 {
			return subnets, nil
		}
		if err := sleepWithContext(ctx, defaultLaunchNetworkPollDelay); err != nil {
			return nil, err
		}
	}

	return describeSubnetsByVPC(ctx, client, vpcID)
}

func describeSubnetsByVPC(ctx context.Context, client *ec2.Client, vpcID string) ([]ec2types.Subnet, error) {
	paginator := ec2.NewDescribeSubnetsPaginator(client, &ec2.DescribeSubnetsInput{
		Filters: []ec2types.Filter{
			{
				Name:   awssdk.String("vpc-id"),
				Values: []string{strings.TrimSpace(vpcID)},
			},
		},
		MaxResults: awssdk.Int32(200),
	})

	subnets := make([]ec2types.Subnet, 0)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		subnets = append(subnets, page.Subnets...)
	}
	return subnets, nil
}

func filterDefaultSubnets(subnets []ec2types.Subnet) []ec2types.Subnet {
	filtered := make([]ec2types.Subnet, 0, len(subnets))
	for _, subnet := range subnets {
		if !awssdk.ToBool(subnet.DefaultForAz) {
			continue
		}
		filtered = append(filtered, subnet)
	}
	return filtered
}

func selectPreferredLaunchSubnet(subnets []ec2types.Subnet, wantIPv6 bool) (ec2types.Subnet, bool) {
	candidates := make([]ec2types.Subnet, 0, len(subnets))
	for _, subnet := range subnets {
		if strings.TrimSpace(awssdk.ToString(subnet.SubnetId)) == "" {
			continue
		}
		if subnet.State != "" && subnet.State != ec2types.SubnetStateAvailable {
			continue
		}
		candidates = append(candidates, subnet)
	}
	if len(candidates) == 0 {
		return ec2types.Subnet{}, false
	}

	sort.Slice(candidates, func(i, j int) bool {
		leftScore := subnetLaunchPriority(candidates[i], wantIPv6)
		rightScore := subnetLaunchPriority(candidates[j], wantIPv6)
		if leftScore != rightScore {
			return leftScore > rightScore
		}

		leftIPs := awssdk.ToInt32(candidates[i].AvailableIpAddressCount)
		rightIPs := awssdk.ToInt32(candidates[j].AvailableIpAddressCount)
		if leftIPs != rightIPs {
			return leftIPs > rightIPs
		}

		leftZone := strings.TrimSpace(awssdk.ToString(candidates[i].AvailabilityZone))
		rightZone := strings.TrimSpace(awssdk.ToString(candidates[j].AvailabilityZone))
		if leftZone != rightZone {
			return leftZone < rightZone
		}

		return strings.TrimSpace(awssdk.ToString(candidates[i].SubnetId)) < strings.TrimSpace(awssdk.ToString(candidates[j].SubnetId))
	})

	return candidates[0], true
}

func subnetLaunchPriority(subnet ec2types.Subnet, wantIPv6 bool) int {
	score := 0
	if awssdk.ToBool(subnet.DefaultForAz) {
		score += 100
	}
	if subnetHasAssociatedIPv6CIDR(subnet) {
		score += 40
	} else if wantIPv6 {
		score -= 15
	}
	if awssdk.ToBool(subnet.MapPublicIpOnLaunch) {
		score += 20
	}
	if awssdk.ToInt32(subnet.AvailableIpAddressCount) > 0 {
		score += 10
	}
	return score
}

func createFallbackDefaultSubnet(ctx context.Context, client *ec2.Client, existingSubnets []ec2types.Subnet) (ec2types.Subnet, error) {
	availabilityZones, err := listAvailableAvailabilityZones(ctx, client)
	if err != nil {
		return ec2types.Subnet{}, err
	}

	usedDefaultZones := make(map[string]struct{}, len(existingSubnets))
	for _, subnet := range existingSubnets {
		if !awssdk.ToBool(subnet.DefaultForAz) {
			continue
		}
		zone := strings.TrimSpace(awssdk.ToString(subnet.AvailabilityZone))
		if zone != "" {
			usedDefaultZones[zone] = struct{}{}
		}
	}

	selectedZone := ""
	for _, zone := range availabilityZones {
		if _, exists := usedDefaultZones[zone]; exists {
			continue
		}
		selectedZone = zone
		break
	}
	if selectedZone == "" && len(availabilityZones) > 0 {
		selectedZone = availabilityZones[0]
	}
	if selectedZone == "" {
		return ec2types.Subnet{}, fmt.Errorf("aws returned no available availability zones for default subnet creation")
	}

	output, err := client.CreateDefaultSubnet(ctx, &ec2.CreateDefaultSubnetInput{
		AvailabilityZone: awssdk.String(selectedZone),
	})
	if err != nil {
		return ec2types.Subnet{}, err
	}
	if output == nil || output.Subnet == nil {
		return ec2types.Subnet{}, fmt.Errorf("aws create default subnet returned no subnet")
	}
	return *output.Subnet, nil
}

func listAvailableAvailabilityZones(ctx context.Context, client *ec2.Client) ([]string, error) {
	output, err := client.DescribeAvailabilityZones(ctx, &ec2.DescribeAvailabilityZonesInput{
		Filters: []ec2types.Filter{
			{
				Name:   awssdk.String("state"),
				Values: []string{"available"},
			},
			{
				Name:   awssdk.String("zone-type"),
				Values: []string{"availability-zone"},
			},
		},
	})
	if err != nil {
		return nil, err
	}

	zones := make([]string, 0, len(output.AvailabilityZones))
	for _, zone := range output.AvailabilityZones {
		name := strings.TrimSpace(awssdk.ToString(zone.ZoneName))
		if name == "" {
			continue
		}
		zones = append(zones, name)
	}
	sort.Strings(zones)
	return zones, nil
}

func ensureSubnetSupportsIPv6(ctx context.Context, client *ec2.Client, vpc ec2types.Vpc, subnet ec2types.Subnet, allSubnets []ec2types.Subnet) (ec2types.Subnet, error) {
	if subnetHasAssociatedIPv6CIDR(subnet) {
		return ensureSubnetIPv6AutoAssignment(ctx, client, subnet)
	}

	vpcIPv6CIDR, err := ensureVPCIPv6CIDR(ctx, client, vpc)
	if err != nil {
		return ec2types.Subnet{}, err
	}

	subnetID := strings.TrimSpace(awssdk.ToString(subnet.SubnetId))
	if subnetHasPendingIPv6Association(subnet) {
		subnet, err = waitForSubnetIPv6Association(ctx, client, subnetID)
		if err != nil {
			return ec2types.Subnet{}, err
		}
		return ensureSubnetIPv6AutoAssignment(ctx, client, subnet)
	}

	subnetCIDR, err := pickUnusedSubnetIPv6CIDR(vpcIPv6CIDR, allSubnets)
	if err != nil {
		return ec2types.Subnet{}, err
	}

	if _, err := client.AssociateSubnetCidrBlock(ctx, &ec2.AssociateSubnetCidrBlockInput{
		SubnetId:      awssdk.String(subnetID),
		Ipv6CidrBlock: awssdk.String(subnetCIDR),
	}); err != nil {
		return ec2types.Subnet{}, err
	}

	subnet, err = waitForSubnetIPv6Association(ctx, client, subnetID)
	if err != nil {
		return ec2types.Subnet{}, err
	}
	return ensureSubnetIPv6AutoAssignment(ctx, client, subnet)
}

func ensureVPCIPv6CIDR(ctx context.Context, client *ec2.Client, vpc ec2types.Vpc) (string, error) {
	if cidr := vpcAssociatedIPv6CIDR(vpc); cidr != "" {
		return cidr, nil
	}

	vpcID := strings.TrimSpace(awssdk.ToString(vpc.VpcId))
	if vpcHasPendingIPv6Association(vpc) {
		return waitForVPCIPv6Association(ctx, client, vpcID)
	}

	if _, err := client.AssociateVpcCidrBlock(ctx, &ec2.AssociateVpcCidrBlockInput{
		VpcId:                       awssdk.String(vpcID),
		AmazonProvidedIpv6CidrBlock: awssdk.Bool(true),
	}); err != nil {
		return "", err
	}

	return waitForVPCIPv6Association(ctx, client, vpcID)
}

func ensureSubnetIPv6AutoAssignment(ctx context.Context, client *ec2.Client, subnet ec2types.Subnet) (ec2types.Subnet, error) {
	if awssdk.ToBool(subnet.AssignIpv6AddressOnCreation) {
		return subnet, nil
	}

	subnetID := strings.TrimSpace(awssdk.ToString(subnet.SubnetId))
	if _, err := client.ModifySubnetAttribute(ctx, &ec2.ModifySubnetAttributeInput{
		SubnetId: awssdk.String(subnetID),
		AssignIpv6AddressOnCreation: &ec2types.AttributeBooleanValue{
			Value: awssdk.Bool(true),
		},
	}); err != nil {
		return ec2types.Subnet{}, err
	}

	return describeSubnetByID(ctx, client, subnetID)
}

func waitForVPCIPv6Association(ctx context.Context, client *ec2.Client, vpcID string) (string, error) {
	for attempt := 0; attempt < defaultLaunchNetworkPollAttempts; attempt++ {
		vpc, err := describeVPCByID(ctx, client, vpcID)
		if err != nil {
			return "", err
		}
		if cidr := vpcAssociatedIPv6CIDR(vpc); cidr != "" {
			return cidr, nil
		}
		if err := sleepWithContext(ctx, defaultLaunchNetworkPollDelay); err != nil {
			return "", err
		}
	}

	vpc, err := describeVPCByID(ctx, client, vpcID)
	if err != nil {
		return "", err
	}
	if cidr := vpcAssociatedIPv6CIDR(vpc); cidr != "" {
		return cidr, nil
	}
	return "", fmt.Errorf("timed out while AWS was associating an IPv6 CIDR block to the default VPC")
}

func waitForSubnetIPv6Association(ctx context.Context, client *ec2.Client, subnetID string) (ec2types.Subnet, error) {
	for attempt := 0; attempt < defaultLaunchNetworkPollAttempts; attempt++ {
		subnet, err := describeSubnetByID(ctx, client, subnetID)
		if err != nil {
			return ec2types.Subnet{}, err
		}
		if subnetHasAssociatedIPv6CIDR(subnet) {
			return subnet, nil
		}
		if err := sleepWithContext(ctx, defaultLaunchNetworkPollDelay); err != nil {
			return ec2types.Subnet{}, err
		}
	}

	subnet, err := describeSubnetByID(ctx, client, subnetID)
	if err != nil {
		return ec2types.Subnet{}, err
	}
	if subnetHasAssociatedIPv6CIDR(subnet) {
		return subnet, nil
	}
	return ec2types.Subnet{}, fmt.Errorf("timed out while AWS was associating an IPv6 CIDR block to subnet %s", subnetID)
}

func describeVPCByID(ctx context.Context, client *ec2.Client, vpcID string) (ec2types.Vpc, error) {
	output, err := client.DescribeVpcs(ctx, &ec2.DescribeVpcsInput{
		VpcIds: []string{strings.TrimSpace(vpcID)},
	})
	if err != nil {
		return ec2types.Vpc{}, err
	}
	if len(output.Vpcs) == 0 {
		return ec2types.Vpc{}, fmt.Errorf("aws returned no VPC for %s", strings.TrimSpace(vpcID))
	}
	return output.Vpcs[0], nil
}

func describeSubnetByID(ctx context.Context, client *ec2.Client, subnetID string) (ec2types.Subnet, error) {
	output, err := client.DescribeSubnets(ctx, &ec2.DescribeSubnetsInput{
		SubnetIds: []string{strings.TrimSpace(subnetID)},
	})
	if err != nil {
		return ec2types.Subnet{}, err
	}
	if len(output.Subnets) == 0 {
		return ec2types.Subnet{}, fmt.Errorf("aws returned no subnet for %s", strings.TrimSpace(subnetID))
	}
	return output.Subnets[0], nil
}

func vpcAssociatedIPv6CIDR(vpc ec2types.Vpc) string {
	for _, association := range vpc.Ipv6CidrBlockAssociationSet {
		cidr := strings.TrimSpace(awssdk.ToString(association.Ipv6CidrBlock))
		if cidr == "" {
			continue
		}
		if association.Ipv6CidrBlockState != nil && string(association.Ipv6CidrBlockState.State) != "associated" {
			continue
		}
		return cidr
	}
	return ""
}

func vpcHasPendingIPv6Association(vpc ec2types.Vpc) bool {
	for _, association := range vpc.Ipv6CidrBlockAssociationSet {
		cidr := strings.TrimSpace(awssdk.ToString(association.Ipv6CidrBlock))
		if cidr == "" {
			continue
		}
		return true
	}
	return false
}

func subnetHasAssociatedIPv6CIDR(subnet ec2types.Subnet) bool {
	for _, association := range subnet.Ipv6CidrBlockAssociationSet {
		cidr := strings.TrimSpace(awssdk.ToString(association.Ipv6CidrBlock))
		if cidr == "" {
			continue
		}
		if association.Ipv6CidrBlockState != nil && string(association.Ipv6CidrBlockState.State) != "associated" {
			continue
		}
		return true
	}
	return false
}

func subnetHasPendingIPv6Association(subnet ec2types.Subnet) bool {
	for _, association := range subnet.Ipv6CidrBlockAssociationSet {
		cidr := strings.TrimSpace(awssdk.ToString(association.Ipv6CidrBlock))
		if cidr == "" {
			continue
		}
		return true
	}
	return false
}

func pickUnusedSubnetIPv6CIDR(vpcIPv6CIDR string, subnets []ec2types.Subnet) (string, error) {
	prefix, err := netip.ParsePrefix(strings.TrimSpace(vpcIPv6CIDR))
	if err != nil {
		return "", fmt.Errorf("invalid VPC IPv6 CIDR %q: %w", vpcIPv6CIDR, err)
	}
	prefix = prefix.Masked()
	if !prefix.Addr().Is6() {
		return "", fmt.Errorf("VPC IPv6 CIDR %q is not an IPv6 prefix", vpcIPv6CIDR)
	}
	if prefix.Bits() > 64 {
		return "", fmt.Errorf("VPC IPv6 CIDR %q is too narrow for automatic /64 subnet allocation", vpcIPv6CIDR)
	}

	subnetBits := 64 - prefix.Bits()
	if subnetBits > 16 {
		return "", fmt.Errorf("VPC IPv6 CIDR %q is too wide for deterministic /64 subnet allocation", vpcIPv6CIDR)
	}

	usedCIDRs := make(map[string]struct{}, len(subnets))
	for _, subnet := range subnets {
		for _, association := range subnet.Ipv6CidrBlockAssociationSet {
			cidr := strings.ToLower(strings.TrimSpace(awssdk.ToString(association.Ipv6CidrBlock)))
			if cidr == "" {
				continue
			}
			usedCIDRs[cidr] = struct{}{}
		}
	}

	baseBytes := prefix.Addr().As16()
	baseInt := new(big.Int).SetBytes(baseBytes[:])
	step := new(big.Int).Lsh(big.NewInt(1), 64)
	candidateCount := 1 << subnetBits

	for index := 0; index < candidateCount; index++ {
		offset := new(big.Int).Mul(big.NewInt(int64(index)), step)
		value := new(big.Int).Add(baseInt, offset)

		addr, err := bigIntToIPv6Addr(value)
		if err != nil {
			return "", err
		}

		candidate := netip.PrefixFrom(addr, 64).Masked().String()
		if _, exists := usedCIDRs[strings.ToLower(candidate)]; exists {
			continue
		}
		return candidate, nil
	}

	return "", fmt.Errorf("no free /64 IPv6 subnet remains under %s", prefix.String())
}

func bigIntToIPv6Addr(value *big.Int) (netip.Addr, error) {
	bytes := value.Bytes()
	if len(bytes) > 16 {
		return netip.Addr{}, fmt.Errorf("IPv6 value overflow while generating subnet CIDR")
	}

	var raw [16]byte
	copy(raw[16-len(bytes):], bytes)
	return netip.AddrFrom16(raw), nil
}

func sleepWithContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
