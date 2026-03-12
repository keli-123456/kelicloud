package aws

import (
	"context"
	"encoding/base64"
	"fmt"
	"sort"
	"strings"
	"time"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
)

type Volume struct {
	VolumeID            string `json:"volume_id"`
	DeviceName          string `json:"device_name"`
	SizeGiB             int32  `json:"size_gib"`
	VolumeType          string `json:"volume_type"`
	State               string `json:"state"`
	DeleteOnTermination bool   `json:"delete_on_termination"`
	Encrypted           bool   `json:"encrypted"`
	IOPS                int32  `json:"iops"`
	Throughput          int32  `json:"throughput"`
	SnapshotID          string `json:"snapshot_id"`
	CreatedAt           string `json:"created_at"`
}

type Address struct {
	AllocationID       string `json:"allocation_id"`
	AssociationID      string `json:"association_id"`
	PublicIP           string `json:"public_ip"`
	PrivateIP          string `json:"private_ip"`
	Domain             string `json:"domain"`
	NetworkInterfaceID string `json:"network_interface_id"`
}

type InstanceDetail struct {
	Instance           Instance        `json:"instance"`
	VpcID              string          `json:"vpc_id"`
	SubnetID           string          `json:"subnet_id"`
	Architecture       string          `json:"architecture"`
	PlatformDetails    string          `json:"platform_details"`
	VirtualizationType string          `json:"virtualization_type"`
	RootDeviceName     string          `json:"root_device_name"`
	MonitoringState    string          `json:"monitoring_state"`
	StateReason        string          `json:"state_reason"`
	PublicDNSName      string          `json:"public_dns_name"`
	PrivateDNSName     string          `json:"private_dns_name"`
	SecurityGroups     []SecurityGroup `json:"security_groups"`
	Volumes            []Volume        `json:"volumes"`
	Addresses          []Address       `json:"addresses"`
	ConsoleOutput      string          `json:"console_output"`
}

func ListElasticAddresses(ctx context.Context, credential *CredentialRecord, region string) ([]Address, error) {
	cfg, err := buildConfig(ctx, credential, region)
	if err != nil {
		return nil, err
	}

	client := ec2.NewFromConfig(cfg)
	output, err := client.DescribeAddresses(ctx, &ec2.DescribeAddressesInput{})
	if err != nil {
		return nil, err
	}

	addresses := make([]Address, 0, len(output.Addresses))
	for _, item := range output.Addresses {
		addresses = append(addresses, mapAddress(item))
	}
	sort.Slice(addresses, func(i, j int) bool {
		return addresses[i].PublicIP < addresses[j].PublicIP
	})
	return addresses, nil
}

func GetInstanceDetail(ctx context.Context, credential *CredentialRecord, region, instanceID string) (*InstanceDetail, error) {
	cfg, err := buildConfig(ctx, credential, region)
	if err != nil {
		return nil, err
	}

	client := ec2.NewFromConfig(cfg)
	instance, err := describeInstance(ctx, client, instanceID)
	if err != nil {
		return nil, err
	}

	detail := &InstanceDetail{
		Instance:           mapInstance(instance),
		VpcID:              awssdk.ToString(instance.VpcId),
		SubnetID:           awssdk.ToString(instance.SubnetId),
		Architecture:       string(instance.Architecture),
		PlatformDetails:    awssdk.ToString(instance.PlatformDetails),
		VirtualizationType: string(instance.VirtualizationType),
		RootDeviceName:     awssdk.ToString(instance.RootDeviceName),
		MonitoringState:    string(instance.Monitoring.State),
		StateReason:        awssdk.ToString(instance.StateTransitionReason),
		PublicDNSName:      awssdk.ToString(instance.PublicDnsName),
		PrivateDNSName:     awssdk.ToString(instance.PrivateDnsName),
		SecurityGroups:     mapSecurityGroups(instance.SecurityGroups),
		Volumes:            make([]Volume, 0),
		Addresses:          make([]Address, 0),
	}

	volumeIDs := make([]string, 0, len(instance.BlockDeviceMappings))
	blockDeviceByVolumeID := make(map[string]ec2types.InstanceBlockDeviceMapping, len(instance.BlockDeviceMappings))
	for _, mapping := range instance.BlockDeviceMappings {
		if mapping.Ebs == nil || strings.TrimSpace(awssdk.ToString(mapping.Ebs.VolumeId)) == "" {
			continue
		}
		volumeID := strings.TrimSpace(awssdk.ToString(mapping.Ebs.VolumeId))
		volumeIDs = append(volumeIDs, volumeID)
		blockDeviceByVolumeID[volumeID] = mapping
	}
	if len(volumeIDs) > 0 {
		volumes, err := describeVolumes(ctx, client, volumeIDs, blockDeviceByVolumeID)
		if err != nil {
			return nil, err
		}
		detail.Volumes = volumes
	}

	addresses, err := listInstanceAddresses(ctx, client, instanceID)
	if err != nil {
		return nil, err
	}
	detail.Addresses = addresses
	detail.ConsoleOutput = getInstanceConsoleOutput(ctx, client, instanceID)

	return detail, nil
}

func CreateImage(ctx context.Context, credential *CredentialRecord, region, instanceID, name, description string, noReboot bool) (string, error) {
	cfg, err := buildConfig(ctx, credential, region)
	if err != nil {
		return "", err
	}

	client := ec2.NewFromConfig(cfg)
	output, err := client.CreateImage(ctx, &ec2.CreateImageInput{
		InstanceId:  awssdk.String(strings.TrimSpace(instanceID)),
		Name:        awssdk.String(strings.TrimSpace(name)),
		Description: awssdk.String(strings.TrimSpace(description)),
		NoReboot:    awssdk.Bool(noReboot),
	})
	if err != nil {
		return "", err
	}

	return strings.TrimSpace(awssdk.ToString(output.ImageId)), nil
}

func ModifyInstanceType(ctx context.Context, credential *CredentialRecord, region, instanceID, instanceType string) error {
	cfg, err := buildConfig(ctx, credential, region)
	if err != nil {
		return err
	}

	client := ec2.NewFromConfig(cfg)
	_, err = client.ModifyInstanceAttribute(ctx, &ec2.ModifyInstanceAttributeInput{
		InstanceId: awssdk.String(strings.TrimSpace(instanceID)),
		InstanceType: &ec2types.AttributeValue{
			Value: awssdk.String(strings.TrimSpace(instanceType)),
		},
	})
	return err
}

func SetDetailedMonitoring(ctx context.Context, credential *CredentialRecord, region, instanceID string, enabled bool) error {
	cfg, err := buildConfig(ctx, credential, region)
	if err != nil {
		return err
	}

	client := ec2.NewFromConfig(cfg)
	if enabled {
		_, err = client.MonitorInstances(ctx, &ec2.MonitorInstancesInput{
			InstanceIds: []string{strings.TrimSpace(instanceID)},
		})
		return err
	}

	_, err = client.UnmonitorInstances(ctx, &ec2.UnmonitorInstancesInput{
		InstanceIds: []string{strings.TrimSpace(instanceID)},
	})
	return err
}

func ReplaceInstanceTags(ctx context.Context, credential *CredentialRecord, region, instanceID string, tags []Tag) error {
	cfg, err := buildConfig(ctx, credential, region)
	if err != nil {
		return err
	}

	client := ec2.NewFromConfig(cfg)
	instance, err := describeInstance(ctx, client, instanceID)
	if err != nil {
		return err
	}

	current := make(map[string]string, len(instance.Tags))
	for _, tag := range instance.Tags {
		key := strings.TrimSpace(awssdk.ToString(tag.Key))
		value := strings.TrimSpace(awssdk.ToString(tag.Value))
		if key == "" || strings.HasPrefix(key, "aws:") {
			continue
		}
		current[key] = value
	}

	next := normalizeTagMap(tags)

	deleteTags := make([]ec2types.Tag, 0)
	for key, value := range current {
		if _, exists := next[key]; exists {
			continue
		}
		deleteTags = append(deleteTags, ec2types.Tag{
			Key:   awssdk.String(key),
			Value: awssdk.String(value),
		})
	}
	if len(deleteTags) > 0 {
		_, err = client.DeleteTags(ctx, &ec2.DeleteTagsInput{
			Resources: []string{strings.TrimSpace(instanceID)},
			Tags:      deleteTags,
		})
		if err != nil {
			return err
		}
	}

	createTags := make([]ec2types.Tag, 0, len(next))
	for key, value := range next {
		createTags = append(createTags, ec2types.Tag{
			Key:   awssdk.String(key),
			Value: awssdk.String(value),
		})
	}
	if len(createTags) == 0 {
		return nil
	}

	_, err = client.CreateTags(ctx, &ec2.CreateTagsInput{
		Resources: []string{strings.TrimSpace(instanceID)},
		Tags:      createTags,
	})
	return err
}

func CreateVolumeSnapshots(ctx context.Context, credential *CredentialRecord, region, instanceID, descriptionPrefix string) ([]string, error) {
	cfg, err := buildConfig(ctx, credential, region)
	if err != nil {
		return nil, err
	}

	client := ec2.NewFromConfig(cfg)
	instance, err := describeInstance(ctx, client, instanceID)
	if err != nil {
		return nil, err
	}

	snapshotIDs := make([]string, 0)
	prefix := strings.TrimSpace(descriptionPrefix)
	for _, mapping := range instance.BlockDeviceMappings {
		if mapping.Ebs == nil || strings.TrimSpace(awssdk.ToString(mapping.Ebs.VolumeId)) == "" {
			continue
		}
		volumeID := strings.TrimSpace(awssdk.ToString(mapping.Ebs.VolumeId))
		description := prefix
		if description == "" {
			description = fmt.Sprintf("Snapshot for %s %s", strings.TrimSpace(instanceID), strings.TrimSpace(awssdk.ToString(mapping.DeviceName)))
		}
		output, err := client.CreateSnapshot(ctx, &ec2.CreateSnapshotInput{
			VolumeId:    awssdk.String(volumeID),
			Description: awssdk.String(description),
		})
		if err != nil {
			return nil, err
		}
		if snapshotID := strings.TrimSpace(awssdk.ToString(output.SnapshotId)); snapshotID != "" {
			snapshotIDs = append(snapshotIDs, snapshotID)
		}
	}

	return snapshotIDs, nil
}

func AllocateAndAssociateAddress(ctx context.Context, credential *CredentialRecord, region, instanceID, privateIP string) (*Address, error) {
	cfg, err := buildConfig(ctx, credential, region)
	if err != nil {
		return nil, err
	}

	client := ec2.NewFromConfig(cfg)
	allocation, err := client.AllocateAddress(ctx, &ec2.AllocateAddressInput{
		Domain: ec2types.DomainTypeVpc,
	})
	if err != nil {
		return nil, err
	}

	allocationID := strings.TrimSpace(awssdk.ToString(allocation.AllocationId))
	if _, err := client.AssociateAddress(ctx, &ec2.AssociateAddressInput{
		AllocationId: awssdk.String(allocationID),
		InstanceId:   awssdk.String(strings.TrimSpace(instanceID)),
		PrivateIpAddress: func() *string {
			privateIP = strings.TrimSpace(privateIP)
			if privateIP == "" {
				return nil
			}
			return awssdk.String(privateIP)
		}(),
	}); err != nil {
		_, _ = client.ReleaseAddress(ctx, &ec2.ReleaseAddressInput{
			AllocationId: awssdk.String(allocationID),
		})
		return nil, err
	}

	return describeAddressByAllocationID(ctx, client, allocationID)
}

func AssociateAddress(ctx context.Context, credential *CredentialRecord, region, allocationID, instanceID, privateIP string) (*Address, error) {
	cfg, err := buildConfig(ctx, credential, region)
	if err != nil {
		return nil, err
	}

	client := ec2.NewFromConfig(cfg)
	_, err = client.AssociateAddress(ctx, &ec2.AssociateAddressInput{
		AllocationId: awssdk.String(strings.TrimSpace(allocationID)),
		InstanceId:   awssdk.String(strings.TrimSpace(instanceID)),
		PrivateIpAddress: func() *string {
			privateIP = strings.TrimSpace(privateIP)
			if privateIP == "" {
				return nil
			}
			return awssdk.String(privateIP)
		}(),
	})
	if err != nil {
		return nil, err
	}

	return describeAddressByAllocationID(ctx, client, allocationID)
}

func DisassociateAddress(ctx context.Context, credential *CredentialRecord, region, associationID string) error {
	cfg, err := buildConfig(ctx, credential, region)
	if err != nil {
		return err
	}

	client := ec2.NewFromConfig(cfg)
	_, err = client.DisassociateAddress(ctx, &ec2.DisassociateAddressInput{
		AssociationId: awssdk.String(strings.TrimSpace(associationID)),
	})
	return err
}

func ReleaseAddress(ctx context.Context, credential *CredentialRecord, region, allocationID string) error {
	cfg, err := buildConfig(ctx, credential, region)
	if err != nil {
		return err
	}

	client := ec2.NewFromConfig(cfg)
	_, err = client.ReleaseAddress(ctx, &ec2.ReleaseAddressInput{
		AllocationId: awssdk.String(strings.TrimSpace(allocationID)),
	})
	return err
}

func describeInstance(ctx context.Context, client *ec2.Client, instanceID string) (ec2types.Instance, error) {
	output, err := client.DescribeInstances(ctx, &ec2.DescribeInstancesInput{
		InstanceIds: []string{strings.TrimSpace(instanceID)},
	})
	if err != nil {
		return ec2types.Instance{}, err
	}
	for _, reservation := range output.Reservations {
		for _, item := range reservation.Instances {
			return item, nil
		}
	}
	return ec2types.Instance{}, fmt.Errorf("instance not found: %s", instanceID)
}

func describeVolumes(ctx context.Context, client *ec2.Client, volumeIDs []string, mappings map[string]ec2types.InstanceBlockDeviceMapping) ([]Volume, error) {
	output, err := client.DescribeVolumes(ctx, &ec2.DescribeVolumesInput{
		VolumeIds: volumeIDs,
	})
	if err != nil {
		return nil, err
	}

	volumes := make([]Volume, 0, len(output.Volumes))
	for _, item := range output.Volumes {
		volumeID := strings.TrimSpace(awssdk.ToString(item.VolumeId))
		mapping := mappings[volumeID]
		createdAt := ""
		if item.CreateTime != nil {
			createdAt = item.CreateTime.UTC().Format(time.RFC3339)
		}
		volumes = append(volumes, Volume{
			VolumeID:            volumeID,
			DeviceName:          strings.TrimSpace(awssdk.ToString(mapping.DeviceName)),
			SizeGiB:             awssdk.ToInt32(item.Size),
			VolumeType:          string(item.VolumeType),
			State:               string(item.State),
			DeleteOnTermination: mapping.Ebs != nil && awssdk.ToBool(mapping.Ebs.DeleteOnTermination),
			Encrypted:           awssdk.ToBool(item.Encrypted),
			IOPS:                awssdk.ToInt32(item.Iops),
			Throughput:          awssdk.ToInt32(item.Throughput),
			SnapshotID:          strings.TrimSpace(awssdk.ToString(item.SnapshotId)),
			CreatedAt:           createdAt,
		})
	}
	sort.Slice(volumes, func(i, j int) bool {
		return volumes[i].DeviceName < volumes[j].DeviceName
	})
	return volumes, nil
}

func listInstanceAddresses(ctx context.Context, client *ec2.Client, instanceID string) ([]Address, error) {
	output, err := client.DescribeAddresses(ctx, &ec2.DescribeAddressesInput{
		Filters: []ec2types.Filter{
			{
				Name:   awssdk.String("instance-id"),
				Values: []string{strings.TrimSpace(instanceID)},
			},
		},
	})
	if err != nil {
		return nil, err
	}

	addresses := make([]Address, 0, len(output.Addresses))
	for _, item := range output.Addresses {
		addresses = append(addresses, mapAddress(item))
	}
	sort.Slice(addresses, func(i, j int) bool {
		return addresses[i].PublicIP < addresses[j].PublicIP
	})
	return addresses, nil
}

func describeAddressByAllocationID(ctx context.Context, client *ec2.Client, allocationID string) (*Address, error) {
	output, err := client.DescribeAddresses(ctx, &ec2.DescribeAddressesInput{
		AllocationIds: []string{strings.TrimSpace(allocationID)},
	})
	if err != nil {
		return nil, err
	}
	if len(output.Addresses) == 0 {
		return nil, fmt.Errorf("address not found: %s", allocationID)
	}
	address := mapAddress(output.Addresses[0])
	return &address, nil
}

func getInstanceConsoleOutput(ctx context.Context, client *ec2.Client, instanceID string) string {
	output, err := client.GetConsoleOutput(ctx, &ec2.GetConsoleOutputInput{
		InstanceId: awssdk.String(strings.TrimSpace(instanceID)),
		Latest:     awssdk.Bool(true),
	})
	if err != nil {
		return ""
	}

	value := strings.TrimSpace(awssdk.ToString(output.Output))
	if value == "" {
		return ""
	}

	decoded, err := base64.StdEncoding.DecodeString(value)
	if err == nil && len(decoded) > 0 {
		return strings.TrimSpace(string(decoded))
	}
	return value
}

func mapSecurityGroups(items []ec2types.GroupIdentifier) []SecurityGroup {
	groups := make([]SecurityGroup, 0, len(items))
	for _, item := range items {
		groups = append(groups, SecurityGroup{
			GroupID:   strings.TrimSpace(awssdk.ToString(item.GroupId)),
			GroupName: strings.TrimSpace(awssdk.ToString(item.GroupName)),
		})
	}
	sort.Slice(groups, func(i, j int) bool {
		return groups[i].GroupName < groups[j].GroupName
	})
	return groups
}

func mapAddress(item ec2types.Address) Address {
	return Address{
		AllocationID:       strings.TrimSpace(awssdk.ToString(item.AllocationId)),
		AssociationID:      strings.TrimSpace(awssdk.ToString(item.AssociationId)),
		PublicIP:           strings.TrimSpace(awssdk.ToString(item.PublicIp)),
		PrivateIP:          strings.TrimSpace(awssdk.ToString(item.PrivateIpAddress)),
		Domain:             string(item.Domain),
		NetworkInterfaceID: strings.TrimSpace(awssdk.ToString(item.NetworkInterfaceId)),
	}
}

func normalizeTagMap(tags []Tag) map[string]string {
	normalized := make(map[string]string, len(tags))
	for _, tag := range tags {
		key := strings.TrimSpace(tag.Key)
		value := strings.TrimSpace(tag.Value)
		if key == "" || strings.HasPrefix(key, "aws:") {
			continue
		}
		normalized[key] = value
	}
	return normalized
}
