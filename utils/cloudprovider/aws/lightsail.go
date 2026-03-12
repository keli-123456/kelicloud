package aws

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lightsail"
	lightsailtypes "github.com/aws/aws-sdk-go-v2/service/lightsail/types"
)

type LightsailAvailabilityZone struct {
	Name  string `json:"name"`
	State string `json:"state"`
}

type LightsailRegion struct {
	Name              string                      `json:"name"`
	DisplayName       string                      `json:"display_name"`
	Description       string                      `json:"description"`
	AvailabilityZones []LightsailAvailabilityZone `json:"availability_zones"`
}

type LightsailBundle struct {
	BundleID             string  `json:"bundle_id"`
	Name                 string  `json:"name"`
	Price                float32 `json:"price"`
	RAMSizeInGB          float32 `json:"ram_size_in_gb"`
	DiskSizeInGB         int32   `json:"disk_size_in_gb"`
	TransferPerMonthInGB int32   `json:"transfer_per_month_in_gb"`
	CPUCount             int32   `json:"cpu_count"`
	IsActive             bool    `json:"is_active"`
}

type LightsailBlueprint struct {
	BlueprintID string `json:"blueprint_id"`
	Name        string `json:"name"`
	Description string `json:"description"`
	Group       string `json:"group"`
	Platform    string `json:"platform"`
	IsActive    bool   `json:"is_active"`
}

type LightsailKeyPair struct {
	Name        string `json:"name"`
	Fingerprint string `json:"fingerprint"`
	CreatedAt   string `json:"created_at"`
}

type LightsailStaticIP struct {
	Name       string `json:"name"`
	IPAddress  string `json:"ip_address"`
	AttachedTo string `json:"attached_to"`
	IsAttached bool   `json:"is_attached"`
	CreatedAt  string `json:"created_at"`
}

type LightsailInstance struct {
	Name             string            `json:"name"`
	State            string            `json:"state"`
	BlueprintID      string            `json:"blueprint_id"`
	BlueprintName    string            `json:"blueprint_name"`
	BundleID         string            `json:"bundle_id"`
	PublicIP         string            `json:"public_ip"`
	PrivateIP        string            `json:"private_ip"`
	IPv6Addresses    []string          `json:"ipv6_addresses"`
	Username         string            `json:"username"`
	SSHKeyName       string            `json:"ssh_key_name"`
	AvailabilityZone string            `json:"availability_zone"`
	Region           string            `json:"region"`
	IsStaticIP       bool              `json:"is_static_ip"`
	CreatedAt        string            `json:"created_at"`
	CPUCount         int32             `json:"cpu_count"`
	RAMSizeInGB      float32           `json:"ram_size_in_gb"`
	Disks            []LightsailDisk   `json:"disks"`
	Tags             map[string]string `json:"tags"`
}

type LightsailDisk struct {
	Name       string `json:"name"`
	Path       string `json:"path"`
	SizeInGB   int32  `json:"size_in_gb"`
	IsSystem   bool   `json:"is_system"`
	AttachedTo string `json:"attached_to,omitempty"`
	State      string `json:"state,omitempty"`
}

type LightsailPort struct {
	FromPort        int32    `json:"from_port"`
	ToPort          int32    `json:"to_port"`
	Protocol        string   `json:"protocol"`
	AccessFrom      string   `json:"access_from"`
	AccessType      string   `json:"access_type"`
	CIDRs           []string `json:"cidrs"`
	IPv6CIDRs       []string `json:"ipv6_cidrs"`
	CIDRAliases     []string `json:"cidr_aliases"`
	CommonName      string   `json:"common_name"`
	AccessDirection string   `json:"access_direction"`
}

type LightsailSnapshot struct {
	Name             string `json:"name"`
	FromInstanceName string `json:"from_instance_name"`
	FromBlueprintID  string `json:"from_blueprint_id"`
	FromBundleID     string `json:"from_bundle_id"`
	State            string `json:"state"`
	SizeInGB         int32  `json:"size_in_gb"`
	IsAuto           bool   `json:"is_auto"`
	CreatedAt        string `json:"created_at"`
}

type LightsailInstanceDetail struct {
	Instance  LightsailInstance   `json:"instance"`
	Ports     []LightsailPort     `json:"ports"`
	StaticIPs []LightsailStaticIP `json:"static_ips"`
	Snapshots []LightsailSnapshot `json:"snapshots"`
}

type CreateLightsailInstanceRequest struct {
	Name             string `json:"name" binding:"required"`
	AvailabilityZone string `json:"availability_zone" binding:"required"`
	BlueprintID      string `json:"blueprint_id" binding:"required"`
	BundleID         string `json:"bundle_id" binding:"required"`
	KeyPairName      string `json:"key_pair_name,omitempty"`
	UserData         string `json:"user_data,omitempty"`
	Tags             []Tag  `json:"tags,omitempty"`
	IPAddressType    string `json:"ip_address_type,omitempty"`
}

func ListLightsailRegions(ctx context.Context, credential *CredentialRecord, region string) ([]LightsailRegion, error) {
	client, err := newLightsailClient(ctx, credential, region)
	if err != nil {
		return nil, err
	}

	output, err := client.GetRegions(ctx, &lightsail.GetRegionsInput{
		IncludeAvailabilityZones: awssdk.Bool(true),
	})
	if err != nil {
		return nil, err
	}

	regions := make([]LightsailRegion, 0, len(output.Regions))
	for _, item := range output.Regions {
		regions = append(regions, mapLightsailRegion(item))
	}
	sort.Slice(regions, func(i, j int) bool {
		return regions[i].Name < regions[j].Name
	})
	return regions, nil
}

func ListLightsailBundles(ctx context.Context, credential *CredentialRecord, region string) ([]LightsailBundle, error) {
	client, err := newLightsailClient(ctx, credential, region)
	if err != nil {
		return nil, err
	}

	bundles := make([]LightsailBundle, 0)
	var pageToken *string
	for {
		output, err := client.GetBundles(ctx, &lightsail.GetBundlesInput{
			IncludeInactive: awssdk.Bool(false),
			PageToken:       pageToken,
		})
		if err != nil {
			return nil, err
		}
		for _, item := range output.Bundles {
			bundles = append(bundles, mapLightsailBundle(item))
		}
		if output.NextPageToken == nil || strings.TrimSpace(awssdk.ToString(output.NextPageToken)) == "" {
			break
		}
		pageToken = output.NextPageToken
	}

	sort.Slice(bundles, func(i, j int) bool {
		return bundles[i].Price < bundles[j].Price
	})
	return bundles, nil
}

func ListLightsailBlueprints(ctx context.Context, credential *CredentialRecord, region string) ([]LightsailBlueprint, error) {
	client, err := newLightsailClient(ctx, credential, region)
	if err != nil {
		return nil, err
	}

	blueprints := make([]LightsailBlueprint, 0)
	var pageToken *string
	for {
		output, err := client.GetBlueprints(ctx, &lightsail.GetBlueprintsInput{
			IncludeInactive: awssdk.Bool(false),
			PageToken:       pageToken,
		})
		if err != nil {
			return nil, err
		}
		for _, item := range output.Blueprints {
			blueprints = append(blueprints, mapLightsailBlueprint(item))
		}
		if output.NextPageToken == nil || strings.TrimSpace(awssdk.ToString(output.NextPageToken)) == "" {
			break
		}
		pageToken = output.NextPageToken
	}

	sort.Slice(blueprints, func(i, j int) bool {
		left := strings.ToLower(strings.TrimSpace(blueprints[i].Platform + " " + blueprints[i].Name))
		right := strings.ToLower(strings.TrimSpace(blueprints[j].Platform + " " + blueprints[j].Name))
		return left < right
	})
	return blueprints, nil
}

func ListLightsailKeyPairs(ctx context.Context, credential *CredentialRecord, region string) ([]LightsailKeyPair, error) {
	client, err := newLightsailClient(ctx, credential, region)
	if err != nil {
		return nil, err
	}

	output, err := client.GetKeyPairs(ctx, &lightsail.GetKeyPairsInput{})
	if err != nil {
		return nil, err
	}

	keyPairs := make([]LightsailKeyPair, 0, len(output.KeyPairs))
	for _, item := range output.KeyPairs {
		keyPairs = append(keyPairs, mapLightsailKeyPair(item))
	}
	sort.Slice(keyPairs, func(i, j int) bool {
		return keyPairs[i].Name < keyPairs[j].Name
	})
	return keyPairs, nil
}

func ListLightsailStaticIPs(ctx context.Context, credential *CredentialRecord, region string) ([]LightsailStaticIP, error) {
	client, err := newLightsailClient(ctx, credential, region)
	if err != nil {
		return nil, err
	}

	staticIPs := make([]LightsailStaticIP, 0)
	var pageToken *string
	for {
		output, err := client.GetStaticIps(ctx, &lightsail.GetStaticIpsInput{
			PageToken: pageToken,
		})
		if err != nil {
			return nil, err
		}
		for _, item := range output.StaticIps {
			staticIPs = append(staticIPs, mapLightsailStaticIP(item))
		}
		if output.NextPageToken == nil || strings.TrimSpace(awssdk.ToString(output.NextPageToken)) == "" {
			break
		}
		pageToken = output.NextPageToken
	}

	sort.Slice(staticIPs, func(i, j int) bool {
		return staticIPs[i].Name < staticIPs[j].Name
	})
	return staticIPs, nil
}

func ListLightsailInstances(ctx context.Context, credential *CredentialRecord, region string) ([]LightsailInstance, error) {
	client, err := newLightsailClient(ctx, credential, region)
	if err != nil {
		return nil, err
	}

	instances := make([]LightsailInstance, 0)
	var pageToken *string
	for {
		output, err := client.GetInstances(ctx, &lightsail.GetInstancesInput{
			PageToken: pageToken,
		})
		if err != nil {
			return nil, err
		}
		for _, item := range output.Instances {
			instances = append(instances, mapLightsailInstance(item))
		}
		if output.NextPageToken == nil || strings.TrimSpace(awssdk.ToString(output.NextPageToken)) == "" {
			break
		}
		pageToken = output.NextPageToken
	}

	sort.Slice(instances, func(i, j int) bool {
		return instances[i].CreatedAt > instances[j].CreatedAt
	})
	return instances, nil
}

func GetLightsailInstanceDetail(ctx context.Context, credential *CredentialRecord, region, instanceName string) (*LightsailInstanceDetail, error) {
	client, err := newLightsailClient(ctx, credential, region)
	if err != nil {
		return nil, err
	}

	output, err := client.GetInstance(ctx, &lightsail.GetInstanceInput{
		InstanceName: awssdk.String(strings.TrimSpace(instanceName)),
	})
	if err != nil {
		return nil, err
	}
	if output.Instance == nil {
		return nil, fmt.Errorf("lightsail instance not found: %s", strings.TrimSpace(instanceName))
	}

	snapshots, err := ListLightsailSnapshots(ctx, credential, region, instanceName)
	if err != nil {
		return nil, err
	}
	staticIPs, err := ListLightsailStaticIPs(ctx, credential, region)
	if err != nil {
		return nil, err
	}

	ports := make([]LightsailPort, 0)
	if output.Instance.Networking != nil {
		for _, item := range output.Instance.Networking.Ports {
			ports = append(ports, mapLightsailPort(item))
		}
	}
	sort.Slice(ports, func(i, j int) bool {
		if ports[i].FromPort == ports[j].FromPort {
			return ports[i].Protocol < ports[j].Protocol
		}
		return ports[i].FromPort < ports[j].FromPort
	})

	return &LightsailInstanceDetail{
		Instance:  mapLightsailInstance(*output.Instance),
		Ports:     ports,
		StaticIPs: staticIPs,
		Snapshots: snapshots,
	}, nil
}

func CreateLightsailInstance(ctx context.Context, credential *CredentialRecord, region string, request CreateLightsailInstanceRequest) error {
	client, err := newLightsailClient(ctx, credential, region)
	if err != nil {
		return err
	}

	input := &lightsail.CreateInstancesInput{
		AvailabilityZone: awssdk.String(strings.TrimSpace(request.AvailabilityZone)),
		BlueprintId:      awssdk.String(strings.TrimSpace(request.BlueprintID)),
		BundleId:         awssdk.String(strings.TrimSpace(request.BundleID)),
		InstanceNames:    []string{strings.TrimSpace(request.Name)},
	}
	if keyPairName := strings.TrimSpace(request.KeyPairName); keyPairName != "" {
		input.KeyPairName = awssdk.String(keyPairName)
	}
	if userData := strings.TrimSpace(request.UserData); userData != "" {
		input.UserData = awssdk.String(userData)
	}
	if ipAddressType := strings.TrimSpace(request.IPAddressType); ipAddressType != "" {
		input.IpAddressType = lightsailtypes.IpAddressType(ipAddressType)
	}
	if len(request.Tags) > 0 {
		input.Tags = make([]lightsailtypes.Tag, 0, len(request.Tags))
		for _, tag := range request.Tags {
			key := strings.TrimSpace(tag.Key)
			value := strings.TrimSpace(tag.Value)
			if key == "" {
				continue
			}
			input.Tags = append(input.Tags, lightsailtypes.Tag{
				Key:   awssdk.String(key),
				Value: awssdk.String(value),
			})
		}
	}

	_, err = client.CreateInstances(ctx, input)
	return err
}

func StartLightsailInstance(ctx context.Context, credential *CredentialRecord, region, instanceName string) error {
	client, err := newLightsailClient(ctx, credential, region)
	if err != nil {
		return err
	}
	_, err = client.StartInstance(ctx, &lightsail.StartInstanceInput{
		InstanceName: awssdk.String(strings.TrimSpace(instanceName)),
	})
	return err
}

func StopLightsailInstance(ctx context.Context, credential *CredentialRecord, region, instanceName string) error {
	client, err := newLightsailClient(ctx, credential, region)
	if err != nil {
		return err
	}
	_, err = client.StopInstance(ctx, &lightsail.StopInstanceInput{
		InstanceName: awssdk.String(strings.TrimSpace(instanceName)),
	})
	return err
}

func RebootLightsailInstance(ctx context.Context, credential *CredentialRecord, region, instanceName string) error {
	client, err := newLightsailClient(ctx, credential, region)
	if err != nil {
		return err
	}
	_, err = client.RebootInstance(ctx, &lightsail.RebootInstanceInput{
		InstanceName: awssdk.String(strings.TrimSpace(instanceName)),
	})
	return err
}

func DeleteLightsailInstance(ctx context.Context, credential *CredentialRecord, region, instanceName string) error {
	client, err := newLightsailClient(ctx, credential, region)
	if err != nil {
		return err
	}
	_, err = client.DeleteInstance(ctx, &lightsail.DeleteInstanceInput{
		InstanceName: awssdk.String(strings.TrimSpace(instanceName)),
	})
	return err
}

func ListLightsailSnapshots(ctx context.Context, credential *CredentialRecord, region, instanceName string) ([]LightsailSnapshot, error) {
	client, err := newLightsailClient(ctx, credential, region)
	if err != nil {
		return nil, err
	}

	snapshots := make([]LightsailSnapshot, 0)
	var pageToken *string
	for {
		output, err := client.GetInstanceSnapshots(ctx, &lightsail.GetInstanceSnapshotsInput{
			PageToken: pageToken,
		})
		if err != nil {
			return nil, err
		}
		for _, item := range output.InstanceSnapshots {
			if strings.TrimSpace(awssdk.ToString(item.FromInstanceName)) != strings.TrimSpace(instanceName) {
				continue
			}
			snapshots = append(snapshots, mapLightsailSnapshot(item))
		}
		if output.NextPageToken == nil || strings.TrimSpace(awssdk.ToString(output.NextPageToken)) == "" {
			break
		}
		pageToken = output.NextPageToken
	}

	sort.Slice(snapshots, func(i, j int) bool {
		return snapshots[i].CreatedAt > snapshots[j].CreatedAt
	})
	return snapshots, nil
}

func CreateLightsailSnapshot(ctx context.Context, credential *CredentialRecord, region, instanceName, snapshotName string, tags []Tag) error {
	client, err := newLightsailClient(ctx, credential, region)
	if err != nil {
		return err
	}

	input := &lightsail.CreateInstanceSnapshotInput{
		InstanceName:         awssdk.String(strings.TrimSpace(instanceName)),
		InstanceSnapshotName: awssdk.String(strings.TrimSpace(snapshotName)),
	}
	if len(tags) > 0 {
		input.Tags = make([]lightsailtypes.Tag, 0, len(tags))
		for _, tag := range tags {
			key := strings.TrimSpace(tag.Key)
			value := strings.TrimSpace(tag.Value)
			if key == "" {
				continue
			}
			input.Tags = append(input.Tags, lightsailtypes.Tag{
				Key:   awssdk.String(key),
				Value: awssdk.String(value),
			})
		}
	}

	_, err = client.CreateInstanceSnapshot(ctx, input)
	return err
}

func AllocateLightsailStaticIP(ctx context.Context, credential *CredentialRecord, region, staticIPName string) error {
	client, err := newLightsailClient(ctx, credential, region)
	if err != nil {
		return err
	}
	_, err = client.AllocateStaticIp(ctx, &lightsail.AllocateStaticIpInput{
		StaticIpName: awssdk.String(strings.TrimSpace(staticIPName)),
	})
	return err
}

func AttachLightsailStaticIP(ctx context.Context, credential *CredentialRecord, region, staticIPName, instanceName string) error {
	client, err := newLightsailClient(ctx, credential, region)
	if err != nil {
		return err
	}
	_, err = client.AttachStaticIp(ctx, &lightsail.AttachStaticIpInput{
		StaticIpName: awssdk.String(strings.TrimSpace(staticIPName)),
		InstanceName: awssdk.String(strings.TrimSpace(instanceName)),
	})
	return err
}

func DetachLightsailStaticIP(ctx context.Context, credential *CredentialRecord, region, staticIPName string) error {
	client, err := newLightsailClient(ctx, credential, region)
	if err != nil {
		return err
	}
	_, err = client.DetachStaticIp(ctx, &lightsail.DetachStaticIpInput{
		StaticIpName: awssdk.String(strings.TrimSpace(staticIPName)),
	})
	return err
}

func ReleaseLightsailStaticIP(ctx context.Context, credential *CredentialRecord, region, staticIPName string) error {
	client, err := newLightsailClient(ctx, credential, region)
	if err != nil {
		return err
	}
	_, err = client.ReleaseStaticIp(ctx, &lightsail.ReleaseStaticIpInput{
		StaticIpName: awssdk.String(strings.TrimSpace(staticIPName)),
	})
	return err
}

func newLightsailClient(ctx context.Context, credential *CredentialRecord, region string) (*lightsail.Client, error) {
	cfg, err := buildConfig(ctx, credential, region)
	if err != nil {
		return nil, err
	}
	return lightsail.NewFromConfig(cfg), nil
}

func mapLightsailRegion(item lightsailtypes.Region) LightsailRegion {
	zones := make([]LightsailAvailabilityZone, 0, len(item.AvailabilityZones))
	for _, zone := range item.AvailabilityZones {
		zones = append(zones, LightsailAvailabilityZone{
			Name:  strings.TrimSpace(awssdk.ToString(zone.ZoneName)),
			State: strings.TrimSpace(awssdk.ToString(zone.State)),
		})
	}
	sort.Slice(zones, func(i, j int) bool {
		return zones[i].Name < zones[j].Name
	})

	return LightsailRegion{
		Name:              string(item.Name),
		DisplayName:       strings.TrimSpace(awssdk.ToString(item.DisplayName)),
		Description:       strings.TrimSpace(awssdk.ToString(item.Description)),
		AvailabilityZones: zones,
	}
}

func mapLightsailBundle(item lightsailtypes.Bundle) LightsailBundle {
	return LightsailBundle{
		BundleID:             strings.TrimSpace(awssdk.ToString(item.BundleId)),
		Name:                 strings.TrimSpace(awssdk.ToString(item.Name)),
		Price:                awssdk.ToFloat32(item.Price),
		RAMSizeInGB:          awssdk.ToFloat32(item.RamSizeInGb),
		DiskSizeInGB:         awssdk.ToInt32(item.DiskSizeInGb),
		TransferPerMonthInGB: awssdk.ToInt32(item.TransferPerMonthInGb),
		CPUCount:             awssdk.ToInt32(item.CpuCount),
		IsActive:             awssdk.ToBool(item.IsActive),
	}
}

func mapLightsailBlueprint(item lightsailtypes.Blueprint) LightsailBlueprint {
	return LightsailBlueprint{
		BlueprintID: strings.TrimSpace(awssdk.ToString(item.BlueprintId)),
		Name:        strings.TrimSpace(awssdk.ToString(item.Name)),
		Description: strings.TrimSpace(awssdk.ToString(item.Description)),
		Group:       strings.TrimSpace(awssdk.ToString(item.Group)),
		Platform:    string(item.Platform),
		IsActive:    awssdk.ToBool(item.IsActive),
	}
}

func mapLightsailKeyPair(item lightsailtypes.KeyPair) LightsailKeyPair {
	createdAt := ""
	if item.CreatedAt != nil {
		createdAt = item.CreatedAt.UTC().Format(time.RFC3339)
	}
	return LightsailKeyPair{
		Name:        strings.TrimSpace(awssdk.ToString(item.Name)),
		Fingerprint: strings.TrimSpace(awssdk.ToString(item.Fingerprint)),
		CreatedAt:   createdAt,
	}
}

func mapLightsailStaticIP(item lightsailtypes.StaticIp) LightsailStaticIP {
	createdAt := ""
	if item.CreatedAt != nil {
		createdAt = item.CreatedAt.UTC().Format(time.RFC3339)
	}
	return LightsailStaticIP{
		Name:       strings.TrimSpace(awssdk.ToString(item.Name)),
		IPAddress:  strings.TrimSpace(awssdk.ToString(item.IpAddress)),
		AttachedTo: strings.TrimSpace(awssdk.ToString(item.AttachedTo)),
		IsAttached: awssdk.ToBool(item.IsAttached),
		CreatedAt:  createdAt,
	}
}

func mapLightsailInstance(item lightsailtypes.Instance) LightsailInstance {
	createdAt := ""
	if item.CreatedAt != nil {
		createdAt = item.CreatedAt.UTC().Format(time.RFC3339)
	}

	disks := make([]LightsailDisk, 0)
	cpuCount := int32(0)
	ramSize := float32(0)
	if item.Hardware != nil {
		cpuCount = awssdk.ToInt32(item.Hardware.CpuCount)
		ramSize = awssdk.ToFloat32(item.Hardware.RamSizeInGb)
		disks = make([]LightsailDisk, 0, len(item.Hardware.Disks))
		for _, disk := range item.Hardware.Disks {
			disks = append(disks, LightsailDisk{
				Name:     strings.TrimSpace(awssdk.ToString(disk.Name)),
				Path:     strings.TrimSpace(awssdk.ToString(disk.Path)),
				SizeInGB: awssdk.ToInt32(disk.SizeInGb),
				IsSystem: awssdk.ToBool(disk.IsSystemDisk),
			})
		}
	}

	tags := make(map[string]string, len(item.Tags))
	for _, tag := range item.Tags {
		key := strings.TrimSpace(awssdk.ToString(tag.Key))
		if key == "" {
			continue
		}
		tags[key] = strings.TrimSpace(awssdk.ToString(tag.Value))
	}

	state := ""
	if item.State != nil {
		state = strings.TrimSpace(awssdk.ToString(item.State.Name))
	}

	availabilityZone := ""
	region := ""
	if item.Location != nil {
		availabilityZone = strings.TrimSpace(awssdk.ToString(item.Location.AvailabilityZone))
		region = string(item.Location.RegionName)
	}

	return LightsailInstance{
		Name:             strings.TrimSpace(awssdk.ToString(item.Name)),
		State:            state,
		BlueprintID:      strings.TrimSpace(awssdk.ToString(item.BlueprintId)),
		BlueprintName:    strings.TrimSpace(awssdk.ToString(item.BlueprintName)),
		BundleID:         strings.TrimSpace(awssdk.ToString(item.BundleId)),
		PublicIP:         strings.TrimSpace(awssdk.ToString(item.PublicIpAddress)),
		PrivateIP:        strings.TrimSpace(awssdk.ToString(item.PrivateIpAddress)),
		IPv6Addresses:    append([]string(nil), item.Ipv6Addresses...),
		Username:         strings.TrimSpace(awssdk.ToString(item.Username)),
		SSHKeyName:       strings.TrimSpace(awssdk.ToString(item.SshKeyName)),
		AvailabilityZone: availabilityZone,
		Region:           region,
		IsStaticIP:       awssdk.ToBool(item.IsStaticIp),
		CreatedAt:        createdAt,
		CPUCount:         cpuCount,
		RAMSizeInGB:      ramSize,
		Disks:            disks,
		Tags:             tags,
	}
}

func mapLightsailPort(item lightsailtypes.InstancePortInfo) LightsailPort {
	return LightsailPort{
		FromPort:        item.FromPort,
		ToPort:          item.ToPort,
		Protocol:        string(item.Protocol),
		AccessFrom:      strings.TrimSpace(awssdk.ToString(item.AccessFrom)),
		AccessType:      string(item.AccessType),
		CIDRs:           append([]string(nil), item.Cidrs...),
		IPv6CIDRs:       append([]string(nil), item.Ipv6Cidrs...),
		CIDRAliases:     append([]string(nil), item.CidrListAliases...),
		CommonName:      strings.TrimSpace(awssdk.ToString(item.CommonName)),
		AccessDirection: string(item.AccessDirection),
	}
}

func mapLightsailSnapshot(item lightsailtypes.InstanceSnapshot) LightsailSnapshot {
	createdAt := ""
	if item.CreatedAt != nil {
		createdAt = item.CreatedAt.UTC().Format(time.RFC3339)
	}
	return LightsailSnapshot{
		Name:             strings.TrimSpace(awssdk.ToString(item.Name)),
		FromInstanceName: strings.TrimSpace(awssdk.ToString(item.FromInstanceName)),
		FromBlueprintID:  strings.TrimSpace(awssdk.ToString(item.FromBlueprintId)),
		FromBundleID:     strings.TrimSpace(awssdk.ToString(item.FromBundleId)),
		State:            string(item.State),
		SizeInGB:         awssdk.ToInt32(item.SizeInGb),
		IsAuto:           awssdk.ToBool(item.IsFromAutoSnapshot),
		CreatedAt:        createdAt,
	}
}
