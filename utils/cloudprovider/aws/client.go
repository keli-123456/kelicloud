package aws

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awscredentials "github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

type Identity struct {
	AccountID string `json:"account_id"`
	ARN       string `json:"arn"`
	UserID    string `json:"user_id"`
}

type EC2QuotaSummary struct {
	Region                           string `json:"region,omitempty"`
	MaxInstances                     int    `json:"max_instances,omitempty"`
	MaxElasticIPs                    int    `json:"max_elastic_ips,omitempty"`
	VPCMaxElasticIPs                 int    `json:"vpc_max_elastic_ips,omitempty"`
	VPCMaxSecurityGroupsPerInterface int    `json:"vpc_max_security_groups_per_interface,omitempty"`
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

type Instance struct {
	InstanceID       string            `json:"instance_id"`
	Name             string            `json:"name"`
	State            string            `json:"state"`
	InstanceType     string            `json:"instance_type"`
	ImageID          string            `json:"image_id"`
	KeyName          string            `json:"key_name"`
	PublicIP         string            `json:"public_ip"`
	PrivateIP        string            `json:"private_ip"`
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
	Tags             []Tag    `json:"tags,omitempty"`
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
			ec2types.AccountAttributeName("max-elastic-ips"),
			ec2types.AccountAttributeName("vpc-max-elastic-ips"),
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
		case "max-elastic-ips":
			summary.MaxElasticIPs = value
		case "vpc-max-elastic-ips":
			summary.VPCMaxElasticIPs = value
		case "vpc-max-security-groups-per-interface":
			summary.VPCMaxSecurityGroupsPerInterface = value
		}
	}

	if summary.MaxInstances == 0 &&
		summary.MaxElasticIPs == 0 &&
		summary.VPCMaxElasticIPs == 0 &&
		summary.VPCMaxSecurityGroupsPerInterface == 0 {
		return nil, nil
	}

	return summary, nil
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

func CreateInstance(ctx context.Context, credential *CredentialRecord, region string, request CreateInstanceRequest) (*Instance, error) {
	cfg, err := buildConfig(ctx, credential, region)
	if err != nil {
		return nil, err
	}

	client := ec2.NewFromConfig(cfg)
	input := &ec2.RunInstancesInput{
		ImageId:      awssdk.String(strings.TrimSpace(request.ImageID)),
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
	if subnetID := strings.TrimSpace(request.SubnetID); subnetID != "" {
		networkInterface := ec2types.InstanceNetworkInterfaceSpecification{
			DeviceIndex: awssdk.Int32(0),
			SubnetId:    awssdk.String(subnetID),
			Groups:      securityGroupIDs,
		}
		if request.AssignPublicIP {
			networkInterface.AssociatePublicIpAddress = awssdk.Bool(true)
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
	return GetInstance(ctx, credential, region, instanceID)
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
		AvailabilityZone: awssdk.ToString(item.Placement.AvailabilityZone),
		LaunchTime:       launchTime,
		Tags:             tags,
	}
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
