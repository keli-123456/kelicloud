package aws

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awscredentials "github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/servicequotas"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/komari-monitor/komari/utils/outboundproxy"
)

const (
	ec2ServiceCode                   = "ec2"
	ec2StandardOnDemandVCPUQuotaCode = "L-1216C47A"
	defaultEC2StandardOnDemandVCPUs  = 5
	describeInstanceTypesBatchSize   = 100
	debianEC2ImageOwnerID            = "136693071363"
	managedDebianImagePrefix         = "komari:debian-"
)

type Identity struct {
	AccountID string `json:"account_id"`
	ARN       string `json:"arn"`
	UserID    string `json:"user_id"`
}

type EC2QuotaSummary struct {
	Region                           string `json:"region,omitempty"`
	MaxStandardVCPUs                 int    `json:"max_standard_vcpus,omitempty"`
	MaxInstances                     int    `json:"max_instances,omitempty"`
	MaxElasticIPs                    int    `json:"max_elastic_ips,omitempty"`
	VPCMaxElasticIPs                 int    `json:"vpc_max_elastic_ips,omitempty"`
	VPCMaxSecurityGroupsPerInterface int    `json:"vpc_max_security_groups_per_interface,omitempty"`
	InstanceStandardVCPUs            int    `json:"instance_standard_vcpus,omitempty"`
	ReservedStandardVCPUs            int    `json:"reserved_standard_vcpus,omitempty"`
	RunningStandardVCPUs             int    `json:"running_standard_vcpus,omitempty"`
	RunningInstances                 int    `json:"running_instances,omitempty"`
	TotalInstances                   int    `json:"total_instances,omitempty"`
	AllocatedElasticIPs              int    `json:"allocated_elastic_ips,omitempty"`
	AssociatedElasticIPs             int    `json:"associated_elastic_ips,omitempty"`
}

type Region struct {
	Name     string `json:"name"`
	Endpoint string `json:"endpoint"`
}

type InstanceType struct {
	Name                string   `json:"name"`
	VCpus               int32    `json:"vcpus"`
	MemoryMiB           int64    `json:"memory_mib"`
	Hypervisor          string   `json:"hypervisor"`
	BareMetal           bool     `json:"bare_metal"`
	CurrentGeneration   bool     `json:"current_generation"`
	FreeTierEligible    bool     `json:"free_tier_eligible"`
	NetworkPerformance  string   `json:"network_performance"`
	SupportedUsageClass []string `json:"supported_usage_class"`
}

type Image struct {
	ImageID         string `json:"image_id"`
	Name            string `json:"name"`
	Description     string `json:"description"`
	Architecture    string `json:"architecture"`
	CreationDate    string `json:"creation_date"`
	OwnerID         string `json:"owner_id"`
	PlatformDetails string `json:"platform_details"`
}

type KeyPair struct {
	KeyName     string `json:"key_name"`
	KeyPairID   string `json:"key_pair_id"`
	Fingerprint string `json:"fingerprint"`
	KeyType     string `json:"key_type"`
}

type Subnet struct {
	SubnetID            string `json:"subnet_id"`
	VpcID               string `json:"vpc_id"`
	AvailabilityZone    string `json:"availability_zone"`
	CidrBlock           string `json:"cidr_block"`
	AvailableIPCount    int32  `json:"available_ip_count"`
	DefaultForAZ        bool   `json:"default_for_az"`
	MapPublicIPOnLaunch bool   `json:"map_public_ip_on_launch"`
}

type SecurityGroup struct {
	GroupID     string `json:"group_id"`
	GroupName   string `json:"group_name"`
	Description string `json:"description"`
	VpcID       string `json:"vpc_id"`
}

type InstanceTypeOffering struct {
	InstanceType      string   `json:"instance_type"`
	AvailabilityZones []string `json:"availability_zones"`
}

type Instance struct {
	InstanceID       string            `json:"instance_id"`
	Name             string            `json:"name"`
	State            string            `json:"state"`
	InstanceType     string            `json:"instance_type"`
	ImageID          string            `json:"image_id"`
	KeyName          string            `json:"key_name"`
	PublicIP         string            `json:"public_ip"`
	PrivateIP        string            `json:"private_ip"`
	IPv6Addresses    []string          `json:"ipv6_addresses"`
	AvailabilityZone string            `json:"availability_zone"`
	LaunchTime       string            `json:"launch_time"`
	Tags             map[string]string `json:"tags"`
}

type Tag struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type CreateInstanceRequest struct {
	Name             string   `json:"name"`
	ImageID          string   `json:"image_id" binding:"required"`
	InstanceType     string   `json:"instance_type" binding:"required"`
	KeyName          string   `json:"key_name,omitempty"`
	SubnetID         string   `json:"subnet_id,omitempty"`
	SecurityGroupIDs []string `json:"security_group_ids,omitempty"`
	UserData         string   `json:"user_data,omitempty"`
	AssignPublicIP   bool     `json:"assign_public_ip"`
	AssignIPv6       bool     `json:"assign_ipv6"`
	Tags             []Tag    `json:"tags,omitempty"`
}

type CreateInstanceResult struct {
	Instance   *Instance
	Warnings   []string
	AssignIPv6 bool
}

func buildConfig(ctx context.Context, credential *CredentialRecord, region string) (awssdk.Config, error) {
	if credential == nil {
		return awssdk.Config{}, errors.New("aws credential is missing")
	}
	if strings.TrimSpace(credential.AccessKeyID) == "" || strings.TrimSpace(credential.SecretAccessKey) == "" {
		return awssdk.Config{}, errors.New("aws credential is incomplete")
	}

	resolvedRegion := normalizeRegion(firstNonEmpty(strings.TrimSpace(region), credential.DefaultRegion))
	return awsconfig.LoadDefaultConfig(
		ctx,
		awsconfig.WithRegion(resolvedRegion),
		awsconfig.WithHTTPClient(outboundproxy.NewHTTPClient(20*time.Second)),
		awsconfig.WithCredentialsProvider(
			awscredentials.NewStaticCredentialsProvider(
				strings.TrimSpace(credential.AccessKeyID),
				strings.TrimSpace(credential.SecretAccessKey),
				strings.TrimSpace(credential.SessionToken),
			),
		),
	)
}

func GetIdentity(ctx context.Context, credential *CredentialRecord, region string) (*Identity, error) {
	cfg, err := buildConfig(ctx, credential, region)
	if err != nil {
		return nil, err
	}

	client := sts.NewFromConfig(cfg)
	output, err := client.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return nil, err
	}

	return &Identity{
		AccountID: awssdk.ToString(output.Account),
		ARN:       awssdk.ToString(output.Arn),
		UserID:    awssdk.ToString(output.UserId),
	}, nil
}

func GetEC2QuotaSummary(ctx context.Context, credential *CredentialRecord, region string) (*EC2QuotaSummary, error) {
	cfg, err := buildConfig(ctx, credential, region)
	if err != nil {
		return nil, err
	}

	client := ec2.NewFromConfig(cfg)
	output, err := client.DescribeAccountAttributes(ctx, &ec2.DescribeAccountAttributesInput{
		AttributeNames: []ec2types.AccountAttributeName{
			ec2types.AccountAttributeName("max-instances"),
			ec2types.AccountAttributeName("vpc-max-security-groups-per-interface"),
		},
	})
	if err != nil {
		return nil, err
	}

	summary := &EC2QuotaSummary{
		Region: normalizeRegion(firstNonEmpty(strings.TrimSpace(region), credential.DefaultRegion)),
	}
	for _, attribute := range output.AccountAttributes {
		name := strings.TrimSpace(awssdk.ToString(attribute.AttributeName))
		value, ok := parseAccountAttributeInt(attribute.AttributeValues)
		if !ok {
			continue
		}

		switch name {
		case "max-instances":
			summary.MaxInstances = value
		case "vpc-max-security-groups-per-interface":
			summary.VPCMaxSecurityGroupsPerInterface = value
		}
	}

	warnings := make([]string, 0, 2)

	if maxStandardVCPUs, quotaErr := getStandardOnDemandVCPUQuota(ctx, cfg); quotaErr == nil {
		summary.MaxStandardVCPUs = maxStandardVCPUs
	} else if shouldUseDefaultStandardOnDemandVCPUQuota(quotaErr) {
		summary.MaxStandardVCPUs = defaultEC2StandardOnDemandVCPUs
	} else {
		warnings = append(warnings, "standard vCPU quota: "+quotaErr.Error())
	}

	if usage, usageErr := getInstanceUsageSummary(ctx, client); usageErr == nil {
		summary.RunningInstances = usage.RunningInstances
		summary.TotalInstances = usage.TotalInstances
		summary.InstanceStandardVCPUs = usage.InstanceStandardVCPUs
		summary.ReservedStandardVCPUs = usage.ReservedStandardVCPUs
		summary.RunningStandardVCPUs = usage.RunningStandardVCPUs
	} else {
		warnings = append(warnings, "instance usage: "+usageErr.Error())
	}

	if summary.MaxStandardVCPUs == 0 &&
		summary.MaxInstances == 0 &&
		summary.VPCMaxSecurityGroupsPerInterface == 0 {
		// Keep returning a summary when usage can still be observed even if
		// account attributes are not populated for this credential.
	}

	if summary.MaxStandardVCPUs == 0 &&
		summary.MaxInstances == 0 &&
		summary.VPCMaxSecurityGroupsPerInterface == 0 &&
		summary.RunningStandardVCPUs == 0 &&
		summary.RunningInstances == 0 &&
		summary.TotalInstances == 0 {
		return nil, joinLookupWarnings(warnings)
	}

	return summary, joinLookupWarnings(warnings)
}

func ListRegions(ctx context.Context, credential *CredentialRecord) ([]Region, error) {
	cfg, err := buildConfig(ctx, credential, DefaultRegion)
	if err != nil {
		return nil, err
	}

	client := ec2.NewFromConfig(cfg)
	output, err := client.DescribeRegions(ctx, &ec2.DescribeRegionsInput{
		AllRegions: awssdk.Bool(false),
	})
	if err != nil {
		return nil, err
	}

	regions := make([]Region, 0, len(output.Regions))
	for _, region := range output.Regions {
		regions = append(regions, Region{
			Name:     awssdk.ToString(region.RegionName),
			Endpoint: awssdk.ToString(region.Endpoint),
		})
	}
	sort.Slice(regions, func(i, j int) bool {
		return regions[i].Name < regions[j].Name
	})
	return regions, nil
}

func ListInstanceTypes(ctx context.Context, credential *CredentialRecord, region string) ([]InstanceType, error) {
	cfg, err := buildConfig(ctx, credential, region)
	if err != nil {
		return nil, err
	}

	client := ec2.NewFromConfig(cfg)
	paginator := ec2.NewDescribeInstanceTypesPaginator(client, &ec2.DescribeInstanceTypesInput{
		MaxResults: awssdk.Int32(100),
	})

	instanceTypes := make([]InstanceType, 0)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, item := range page.InstanceTypes {
			instanceTypes = append(instanceTypes, InstanceType{
				Name:               string(item.InstanceType),
				VCpus:              awssdk.ToInt32(item.VCpuInfo.DefaultVCpus),
				MemoryMiB:          awssdk.ToInt64(item.MemoryInfo.SizeInMiB),
				Hypervisor:         string(item.Hypervisor),
				BareMetal:          awssdk.ToBool(item.BareMetal),
				CurrentGeneration:  awssdk.ToBool(item.CurrentGeneration),
				FreeTierEligible:   awssdk.ToBool(item.FreeTierEligible),
				NetworkPerformance: awssdk.ToString(item.NetworkInfo.NetworkPerformance),
				SupportedUsageClass: func(values []ec2types.UsageClassType) []string {
					result := make([]string, 0, len(values))
					for _, value := range values {
						result = append(result, string(value))
					}
					return result
				}(item.SupportedUsageClasses),
			})
		}
	}

	sort.Slice(instanceTypes, func(i, j int) bool {
		return instanceTypes[i].Name < instanceTypes[j].Name
	})
	return instanceTypes, nil
}

func ListInstanceTypeOfferings(ctx context.Context, credential *CredentialRecord, region string) ([]InstanceTypeOffering, error) {
	cfg, err := buildConfig(ctx, credential, region)
	if err != nil {
		return nil, err
	}

	client := ec2.NewFromConfig(cfg)
	paginator := ec2.NewDescribeInstanceTypeOfferingsPaginator(client, &ec2.DescribeInstanceTypeOfferingsInput{
		LocationType: ec2types.LocationTypeAvailabilityZone,
		MaxResults:   awssdk.Int32(100),
	})

	zoneSetsByType := make(map[string]map[string]struct{})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, item := range page.InstanceTypeOfferings {
			instanceType := strings.TrimSpace(string(item.InstanceType))
			location := strings.TrimSpace(awssdk.ToString(item.Location))
			if instanceType == "" || location == "" {
				continue
			}

			if _, exists := zoneSetsByType[instanceType]; !exists {
				zoneSetsByType[instanceType] = make(map[string]struct{})
			}
			zoneSetsByType[instanceType][location] = struct{}{}
		}
	}

	offerings := make([]InstanceTypeOffering, 0, len(zoneSetsByType))
	for instanceType, zoneSet := range zoneSetsByType {
		zones := make([]string, 0, len(zoneSet))
		for zone := range zoneSet {
			zones = append(zones, zone)
		}
		sort.Strings(zones)
		offerings = append(offerings, InstanceTypeOffering{
			InstanceType:      instanceType,
			AvailabilityZones: zones,
		})
	}

	sort.Slice(offerings, func(i, j int) bool {
		return offerings[i].InstanceType < offerings[j].InstanceType
	})
	return offerings, nil
}

func ListSuggestedImages(ctx context.Context, credential *CredentialRecord, region string) ([]Image, error) {
	cfg, err := buildConfig(ctx, credential, region)
	if err != nil {
		return nil, err
	}

	client := ec2.NewFromConfig(cfg)
	paginator := ec2.NewDescribeImagesPaginator(client, &ec2.DescribeImagesInput{
		Owners: []string{"amazon"},
		Filters: []ec2types.Filter{
			{Name: awssdk.String("state"), Values: []string{"available"}},
			{Name: awssdk.String("root-device-type"), Values: []string{"ebs"}},
			{Name: awssdk.String("virtualization-type"), Values: []string{"hvm"}},
			{Name: awssdk.String("architecture"), Values: []string{"x86_64"}},
		},
		MaxResults: awssdk.Int32(200),
	})

	images := make([]Image, 0)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, item := range page.Images {
			images = append(images, Image{
				ImageID:         awssdk.ToString(item.ImageId),
				Name:            awssdk.ToString(item.Name),
				Description:     awssdk.ToString(item.Description),
				Architecture:    string(item.Architecture),
				CreationDate:    awssdk.ToString(item.CreationDate),
				OwnerID:         awssdk.ToString(item.OwnerId),
				PlatformDetails: awssdk.ToString(item.PlatformDetails),
			})
		}
		if len(images) >= 160 {
			break
		}
	}

	sort.Slice(images, func(i, j int) bool {
		return images[i].CreationDate > images[j].CreationDate
	})
	if len(images) > 80 {
		images = images[:80]
	}
	return images, nil
}

func ListKeyPairs(ctx context.Context, credential *CredentialRecord, region string) ([]KeyPair, error) {
	cfg, err := buildConfig(ctx, credential, region)
	if err != nil {
		return nil, err
	}

	client := ec2.NewFromConfig(cfg)
	keyPairs := make([]KeyPair, 0)
	page, err := client.DescribeKeyPairs(ctx, &ec2.DescribeKeyPairsInput{})
	if err != nil {
		return nil, err
	}
	for _, item := range page.KeyPairs {
		keyPairs = append(keyPairs, KeyPair{
			KeyName:     awssdk.ToString(item.KeyName),
			KeyPairID:   awssdk.ToString(item.KeyPairId),
			Fingerprint: awssdk.ToString(item.KeyFingerprint),
			KeyType:     string(item.KeyType),
		})
	}

	sort.Slice(keyPairs, func(i, j int) bool {
		return keyPairs[i].KeyName < keyPairs[j].KeyName
	})
	return keyPairs, nil
}

func ListSubnets(ctx context.Context, credential *CredentialRecord, region string) ([]Subnet, error) {
	cfg, err := buildConfig(ctx, credential, region)
	if err != nil {
		return nil, err
	}

	client := ec2.NewFromConfig(cfg)
	paginator := ec2.NewDescribeSubnetsPaginator(client, &ec2.DescribeSubnetsInput{
		MaxResults: awssdk.Int32(200),
	})

	subnets := make([]Subnet, 0)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, item := range page.Subnets {
			subnets = append(subnets, Subnet{
				SubnetID:            awssdk.ToString(item.SubnetId),
				VpcID:               awssdk.ToString(item.VpcId),
				AvailabilityZone:    awssdk.ToString(item.AvailabilityZone),
				CidrBlock:           awssdk.ToString(item.CidrBlock),
				AvailableIPCount:    awssdk.ToInt32(item.AvailableIpAddressCount),
				DefaultForAZ:        awssdk.ToBool(item.DefaultForAz),
				MapPublicIPOnLaunch: awssdk.ToBool(item.MapPublicIpOnLaunch),
			})
		}
	}

	sort.Slice(subnets, func(i, j int) bool {
		if subnets[i].VpcID == subnets[j].VpcID {
			return subnets[i].AvailabilityZone < subnets[j].AvailabilityZone
		}
		return subnets[i].VpcID < subnets[j].VpcID
	})
	return subnets, nil
}

func ListSecurityGroups(ctx context.Context, credential *CredentialRecord, region string) ([]SecurityGroup, error) {
	cfg, err := buildConfig(ctx, credential, region)
	if err != nil {
		return nil, err
	}

	client := ec2.NewFromConfig(cfg)
	paginator := ec2.NewDescribeSecurityGroupsPaginator(client, &ec2.DescribeSecurityGroupsInput{
		MaxResults: awssdk.Int32(200),
	})

	groups := make([]SecurityGroup, 0)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, item := range page.SecurityGroups {
			groups = append(groups, SecurityGroup{
				GroupID:     awssdk.ToString(item.GroupId),
				GroupName:   awssdk.ToString(item.GroupName),
				Description: awssdk.ToString(item.Description),
				VpcID:       awssdk.ToString(item.VpcId),
			})
		}
	}

	sort.Slice(groups, func(i, j int) bool {
		if groups[i].VpcID == groups[j].VpcID {
			return groups[i].GroupName < groups[j].GroupName
		}
		return groups[i].VpcID < groups[j].VpcID
	})
	return groups, nil
}

func ListInstances(ctx context.Context, credential *CredentialRecord, region string) ([]Instance, error) {
	cfg, err := buildConfig(ctx, credential, region)
	if err != nil {
		return nil, err
	}

	client := ec2.NewFromConfig(cfg)
	paginator := ec2.NewDescribeInstancesPaginator(client, &ec2.DescribeInstancesInput{
		MaxResults: awssdk.Int32(100),
	})

	instances := make([]Instance, 0)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, reservation := range page.Reservations {
			for _, item := range reservation.Instances {
				instances = append(instances, mapInstance(item))
			}
		}
	}

	sort.Slice(instances, func(i, j int) bool {
		return instances[i].LaunchTime > instances[j].LaunchTime
	})
	return instances, nil
}

func CreateInstance(ctx context.Context, credential *CredentialRecord, region string, request CreateInstanceRequest) (*CreateInstanceResult, error) {
	cfg, err := buildConfig(ctx, credential, region)
	if err != nil {
		return nil, err
	}

	client := ec2.NewFromConfig(cfg)
	imageID, err := resolveRunInstancesImageID(ctx, client, strings.TrimSpace(request.ImageID))
	if err != nil {
		return nil, err
	}
	networkPlan := createInstanceNetworkPlan{
		SubnetID:   strings.TrimSpace(request.SubnetID),
		AssignIPv6: request.AssignIPv6,
	}
	if networkPlan.SubnetID == "" && (request.AssignPublicIP || request.AssignIPv6 || len(request.SecurityGroupIDs) > 0) {
		networkPlan, err = prepareCreateInstanceNetwork(ctx, client, request.AssignIPv6)
		if err != nil {
			return nil, err
		}
	}

	input := &ec2.RunInstancesInput{
		ImageId:      awssdk.String(imageID),
		InstanceType: ec2types.InstanceType(strings.TrimSpace(request.InstanceType)),
		MinCount:     awssdk.Int32(1),
		MaxCount:     awssdk.Int32(1),
	}

	if keyName := strings.TrimSpace(request.KeyName); keyName != "" {
		input.KeyName = awssdk.String(keyName)
	}
	if userData := strings.TrimSpace(request.UserData); userData != "" {
		input.UserData = awssdk.String(base64.StdEncoding.EncodeToString([]byte(userData)))
	}
	if tags := buildTagSpecifications(request.Name, request.Tags); len(tags) > 0 {
		input.TagSpecifications = tags
	}

	securityGroupIDs := normalizeStringSlice(request.SecurityGroupIDs)
	if subnetID := networkPlan.SubnetID; subnetID != "" {
		networkInterface := ec2types.InstanceNetworkInterfaceSpecification{
			DeviceIndex: awssdk.Int32(0),
			SubnetId:    awssdk.String(subnetID),
			Groups:      securityGroupIDs,
		}
		if request.AssignPublicIP {
			networkInterface.AssociatePublicIpAddress = awssdk.Bool(true)
		}
		if networkPlan.AssignIPv6 {
			networkInterface.Ipv6AddressCount = awssdk.Int32(1)
		}
		input.NetworkInterfaces = []ec2types.InstanceNetworkInterfaceSpecification{networkInterface}
	} else if len(securityGroupIDs) > 0 {
		input.SecurityGroupIds = securityGroupIDs
	}

	output, err := client.RunInstances(ctx, input)
	if err != nil {
		return nil, err
	}
	if len(output.Instances) == 0 {
		return nil, errors.New("aws run instances returned no instances")
	}

	instanceID := awssdk.ToString(output.Instances[0].InstanceId)
	instance, err := GetInstance(ctx, credential, region, instanceID)
	if err != nil {
		return nil, err
	}

	return &CreateInstanceResult{
		Instance:   instance,
		Warnings:   append([]string(nil), networkPlan.Warnings...),
		AssignIPv6: networkPlan.AssignIPv6,
	}, nil
}

func resolveRunInstancesImageID(ctx context.Context, client *ec2.Client, imageID string) (string, error) {
	imageID = strings.TrimSpace(imageID)
	if imageID == "" {
		return "", errors.New("image id is required")
	}

	preset, ok := parseManagedDebianImageReference(imageID)
	if !ok {
		return imageID, nil
	}

	output, err := client.DescribeImages(ctx, &ec2.DescribeImagesInput{
		Owners: []string{debianEC2ImageOwnerID},
		Filters: []ec2types.Filter{
			{
				Name:   awssdk.String("name"),
				Values: []string{preset.NamePattern},
			},
			{
				Name:   awssdk.String("state"),
				Values: []string{"available"},
			},
		},
	})
	if err != nil {
		return "", err
	}
	if len(output.Images) == 0 {
		return "", fmt.Errorf("no official Debian AMI found for %s", preset.NamePattern)
	}

	sort.Slice(output.Images, func(i, j int) bool {
		return awssdk.ToString(output.Images[i].CreationDate) > awssdk.ToString(output.Images[j].CreationDate)
	})

	resolvedImageID := strings.TrimSpace(awssdk.ToString(output.Images[0].ImageId))
	if resolvedImageID == "" {
		return "", fmt.Errorf("official Debian AMI lookup returned an empty image id for %s", preset.NamePattern)
	}
	return resolvedImageID, nil
}

type managedDebianImagePreset struct {
	Release      string
	Architecture string
	NamePattern  string
}

func parseManagedDebianImageReference(imageID string) (managedDebianImagePreset, bool) {
	normalized := strings.ToLower(strings.TrimSpace(imageID))
	if !strings.HasPrefix(normalized, managedDebianImagePrefix) {
		return managedDebianImagePreset{}, false
	}

	parts := strings.Split(strings.TrimPrefix(normalized, managedDebianImagePrefix), "-")
	if len(parts) != 2 {
		return managedDebianImagePreset{}, false
	}
	release := strings.TrimSpace(parts[0])
	architecture := strings.TrimSpace(parts[1])
	if release == "" || architecture == "" {
		return managedDebianImagePreset{}, false
	}

	switch architecture {
	case "amd64", "arm64":
	default:
		return managedDebianImagePreset{}, false
	}

	return managedDebianImagePreset{
		Release:      release,
		Architecture: architecture,
		NamePattern:  fmt.Sprintf("debian-%s-%s-*", release, architecture),
	}, true
}

func GetInstance(ctx context.Context, credential *CredentialRecord, region, instanceID string) (*Instance, error) {
	cfg, err := buildConfig(ctx, credential, region)
	if err != nil {
		return nil, err
	}

	client := ec2.NewFromConfig(cfg)
	output, err := client.DescribeInstances(ctx, &ec2.DescribeInstancesInput{
		InstanceIds: []string{strings.TrimSpace(instanceID)},
	})
	if err != nil {
		return nil, err
	}
	for _, reservation := range output.Reservations {
		for _, item := range reservation.Instances {
			instance := mapInstance(item)
			return &instance, nil
		}
	}
	return nil, fmt.Errorf("instance not found: %s", instanceID)
}

func StartInstance(ctx context.Context, credential *CredentialRecord, region, instanceID string) error {
	cfg, err := buildConfig(ctx, credential, region)
	if err != nil {
		return err
	}
	client := ec2.NewFromConfig(cfg)
	_, err = client.StartInstances(ctx, &ec2.StartInstancesInput{InstanceIds: []string{strings.TrimSpace(instanceID)}})
	return err
}

func StopInstance(ctx context.Context, credential *CredentialRecord, region, instanceID string) error {
	cfg, err := buildConfig(ctx, credential, region)
	if err != nil {
		return err
	}
	client := ec2.NewFromConfig(cfg)
	_, err = client.StopInstances(ctx, &ec2.StopInstancesInput{InstanceIds: []string{strings.TrimSpace(instanceID)}})
	return err
}

func RebootInstance(ctx context.Context, credential *CredentialRecord, region, instanceID string) error {
	cfg, err := buildConfig(ctx, credential, region)
	if err != nil {
		return err
	}
	client := ec2.NewFromConfig(cfg)
	_, err = client.RebootInstances(ctx, &ec2.RebootInstancesInput{InstanceIds: []string{strings.TrimSpace(instanceID)}})
	return err
}

func TerminateInstance(ctx context.Context, credential *CredentialRecord, region, instanceID string) error {
	cfg, err := buildConfig(ctx, credential, region)
	if err != nil {
		return err
	}
	client := ec2.NewFromConfig(cfg)
	_, err = client.TerminateInstances(ctx, &ec2.TerminateInstancesInput{InstanceIds: []string{strings.TrimSpace(instanceID)}})
	return err
}

func mapInstance(item ec2types.Instance) Instance {
	tags := make(map[string]string, len(item.Tags))
	name := ""
	for _, tag := range item.Tags {
		key := awssdk.ToString(tag.Key)
		value := awssdk.ToString(tag.Value)
		if key == "" {
			continue
		}
		tags[key] = value
		if key == "Name" && name == "" {
			name = value
		}
	}

	launchTime := ""
	if item.LaunchTime != nil {
		launchTime = item.LaunchTime.UTC().Format(time.RFC3339)
	}

	return Instance{
		InstanceID:       awssdk.ToString(item.InstanceId),
		Name:             name,
		State:            string(item.State.Name),
		InstanceType:     string(item.InstanceType),
		ImageID:          awssdk.ToString(item.ImageId),
		KeyName:          awssdk.ToString(item.KeyName),
		PublicIP:         awssdk.ToString(item.PublicIpAddress),
		PrivateIP:        awssdk.ToString(item.PrivateIpAddress),
		IPv6Addresses:    listInstanceIPv6Addresses(item),
		AvailabilityZone: awssdk.ToString(item.Placement.AvailabilityZone),
		LaunchTime:       launchTime,
		Tags:             tags,
	}
}

func listInstanceIPv6Addresses(item ec2types.Instance) []string {
	seen := map[string]struct{}{}
	addresses := make([]string, 0)
	for _, networkInterface := range item.NetworkInterfaces {
		for _, address := range networkInterface.Ipv6Addresses {
			value := strings.TrimSpace(awssdk.ToString(address.Ipv6Address))
			if value == "" {
				continue
			}
			if _, exists := seen[value]; exists {
				continue
			}
			seen[value] = struct{}{}
			addresses = append(addresses, value)
		}
	}
	sort.Strings(addresses)
	return addresses
}

type instanceUsageSummary struct {
	RunningInstances      int
	TotalInstances        int
	InstanceStandardVCPUs int
	ReservedStandardVCPUs int
	RunningStandardVCPUs  int
}

func getInstanceUsageSummary(ctx context.Context, client *ec2.Client) (instanceUsageSummary, error) {
	paginator := ec2.NewDescribeInstancesPaginator(client, &ec2.DescribeInstancesInput{
		MaxResults: awssdk.Int32(100),
	})

	summary := instanceUsageSummary{}
	nonReservedRunningByType := make(map[string]int)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return instanceUsageSummary{}, err
		}
		for _, reservation := range page.Reservations {
			for _, item := range reservation.Instances {
				state := item.State.Name
				if state == ec2types.InstanceStateNameTerminated {
					continue
				}
				summary.TotalInstances++
				if state == ec2types.InstanceStateNameRunning {
					summary.RunningInstances++
					if countsTowardStandardOnDemandVCPUQuota(item) {
						instanceType := strings.TrimSpace(string(item.InstanceType))
						if instanceType != "" && strings.TrimSpace(awssdk.ToString(item.CapacityReservationId)) == "" {
							nonReservedRunningByType[instanceType]++
						}
					}
				}
			}
		}
	}

	reservedByType, err := getStandardOnDemandCapacityReservationCounts(ctx, client)
	if err != nil {
		return summary, err
	}

	instanceTypes := collectInstanceTypes(nonReservedRunningByType, reservedByType)
	if len(instanceTypes) == 0 {
		return summary, nil
	}

	vcpusByType, err := describeInstanceTypeDefaultVCPUs(ctx, client, instanceTypes)
	if err != nil {
		return summary, err
	}
	for instanceType, count := range nonReservedRunningByType {
		summary.InstanceStandardVCPUs += vcpusByType[instanceType] * count
	}
	for instanceType, count := range reservedByType {
		summary.ReservedStandardVCPUs += vcpusByType[instanceType] * count
	}
	summary.RunningStandardVCPUs = summary.InstanceStandardVCPUs + summary.ReservedStandardVCPUs

	return summary, nil
}

func getElasticAddressUsageCounts(ctx context.Context, client *ec2.Client) (int, int, error) {
	output, err := client.DescribeAddresses(ctx, &ec2.DescribeAddressesInput{})
	if err != nil {
		return 0, 0, err
	}

	allocatedCount := len(output.Addresses)
	associatedCount := 0
	for _, item := range output.Addresses {
		if strings.TrimSpace(awssdk.ToString(item.AssociationId)) != "" {
			associatedCount++
		}
	}

	return allocatedCount, associatedCount, nil
}

func getStandardOnDemandVCPUQuota(ctx context.Context, cfg awssdk.Config) (int, error) {
	client := servicequotas.NewFromConfig(cfg)
	output, err := client.GetServiceQuota(ctx, &servicequotas.GetServiceQuotaInput{
		ServiceCode: awssdk.String(ec2ServiceCode),
		QuotaCode:   awssdk.String(ec2StandardOnDemandVCPUQuotaCode),
	})
	if err != nil {
		return 0, err
	}
	if output == nil || output.Quota == nil {
		return 0, errors.New("aws service quotas returned no EC2 standard vCPU quota")
	}
	return int(math.Round(awssdk.ToFloat64(output.Quota.Value))), nil
}

func describeInstanceTypeDefaultVCPUs(ctx context.Context, client *ec2.Client, instanceTypes []string) (map[string]int, error) {
	result := make(map[string]int, len(instanceTypes))
	for start := 0; start < len(instanceTypes); start += describeInstanceTypesBatchSize {
		end := start + describeInstanceTypesBatchSize
		if end > len(instanceTypes) {
			end = len(instanceTypes)
		}

		batch := make([]ec2types.InstanceType, 0, end-start)
		for _, instanceType := range instanceTypes[start:end] {
			batch = append(batch, ec2types.InstanceType(instanceType))
		}

		output, err := client.DescribeInstanceTypes(ctx, &ec2.DescribeInstanceTypesInput{
			InstanceTypes: batch,
		})
		if err != nil {
			return nil, err
		}
		for _, item := range output.InstanceTypes {
			result[strings.TrimSpace(string(item.InstanceType))] = int(awssdk.ToInt32(item.VCpuInfo.DefaultVCpus))
		}
	}
	return result, nil
}

func getStandardOnDemandCapacityReservationCounts(ctx context.Context, client *ec2.Client) (map[string]int, error) {
	paginator := ec2.NewDescribeCapacityReservationsPaginator(client, &ec2.DescribeCapacityReservationsInput{
		MaxResults: awssdk.Int32(100),
	})

	countsByType := make(map[string]int)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, reservation := range page.CapacityReservations {
			if !capacityReservationCountsTowardStandardOnDemandVCPUQuota(reservation) {
				continue
			}

			instanceType := strings.TrimSpace(awssdk.ToString(reservation.InstanceType))
			totalInstances := int(awssdk.ToInt32(reservation.TotalInstanceCount))
			if instanceType == "" || totalInstances <= 0 {
				continue
			}
			countsByType[instanceType] += totalInstances
		}
	}

	return countsByType, nil
}

func countsTowardStandardOnDemandVCPUQuota(instance ec2types.Instance) bool {
	if instance.InstanceLifecycle != "" || strings.TrimSpace(awssdk.ToString(instance.CapacityBlockId)) != "" {
		return false
	}
	return isStandardOnDemandInstanceType(string(instance.InstanceType))
}

func capacityReservationCountsTowardStandardOnDemandVCPUQuota(reservation ec2types.CapacityReservation) bool {
	if reservation.ReservationType == ec2types.CapacityReservationTypeCapacityBlock {
		return false
	}
	switch reservation.State {
	case ec2types.CapacityReservationStateAssessing,
		ec2types.CapacityReservationStateScheduled,
		ec2types.CapacityReservationStatePending,
		ec2types.CapacityReservationStateActive,
		ec2types.CapacityReservationStateDelayed:
		return isStandardOnDemandInstanceType(awssdk.ToString(reservation.InstanceType))
	default:
		return false
	}
}

func isStandardOnDemandInstanceType(instanceType string) bool {
	prefix := instanceFamilyPrefix(instanceType)
	switch prefix {
	case "", "dl", "f", "g", "hpc", "inf", "mac", "p", "trn", "u", "vt", "x":
		return false
	}

	switch prefix[0] {
	case 'a', 'c', 'd', 'h', 'i', 'm', 'r', 't', 'z':
		return true
	default:
		return false
	}
}

func instanceFamilyPrefix(instanceType string) string {
	family := strings.ToLower(strings.TrimSpace(instanceType))
	if dot := strings.Index(family, "."); dot >= 0 {
		family = family[:dot]
	}
	for index, value := range family {
		if value == '-' || (value >= '0' && value <= '9') {
			return family[:index]
		}
	}
	return family
}

func collectInstanceTypes(groupedCounts ...map[string]int) []string {
	set := make(map[string]struct{})
	for _, counts := range groupedCounts {
		for instanceType, count := range counts {
			if strings.TrimSpace(instanceType) == "" || count <= 0 {
				continue
			}
			set[instanceType] = struct{}{}
		}
	}

	result := make([]string, 0, len(set))
	for instanceType := range set {
		result = append(result, instanceType)
	}
	sort.Strings(result)
	return result
}

func joinLookupWarnings(parts []string) error {
	filtered := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			filtered = append(filtered, part)
		}
	}
	if len(filtered) == 0 {
		return nil
	}
	return errors.New(strings.Join(filtered, "; "))
}

func shouldUseDefaultStandardOnDemandVCPUQuota(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	message := strings.ToLower(strings.TrimSpace(err.Error()))
	return strings.Contains(message, "context deadline exceeded") ||
		strings.Contains(message, "request timed out") ||
		strings.Contains(message, "timeout") ||
		strings.Contains(message, "timed out")
}

func buildTagSpecifications(name string, tags []Tag) []ec2types.TagSpecification {
	tagMap := make(map[string]string, len(tags)+1)
	for _, tag := range tags {
		key := strings.TrimSpace(tag.Key)
		value := strings.TrimSpace(tag.Value)
		if key == "" || value == "" {
			continue
		}
		tagMap[key] = value
	}

	name = strings.TrimSpace(name)
	if name != "" {
		tagMap["Name"] = name
	}
	if len(tagMap) == 0 {
		return nil
	}

	items := make([]ec2types.Tag, 0, len(tagMap))
	for key, value := range tagMap {
		items = append(items, ec2types.Tag{
			Key:   awssdk.String(key),
			Value: awssdk.String(value),
		})
	}
	sort.Slice(items, func(i, j int) bool {
		return awssdk.ToString(items[i].Key) < awssdk.ToString(items[j].Key)
	})

	return []ec2types.TagSpecification{
		{
			ResourceType: ec2types.ResourceTypeInstance,
			Tags:         items,
		},
	}
}

func normalizeStringSlice(values []string) []string {
	result := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			return value
		}
	}
	return ""
}

func parseAccountAttributeInt(values []ec2types.AccountAttributeValue) (int, bool) {
	for _, value := range values {
		raw := strings.TrimSpace(awssdk.ToString(value.AttributeValue))
		if raw == "" {
			continue
		}
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			continue
		}
		return parsed, true
	}
	return 0, false
}
