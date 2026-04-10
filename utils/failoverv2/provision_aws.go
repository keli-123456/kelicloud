package failoverv2

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/komari-monitor/komari/database/models"
	awscloud "github.com/komari-monitor/komari/utils/cloudprovider/aws"
)

type awsMemberPlanPayload struct {
	Service          string         `json:"service,omitempty"`
	Region           string         `json:"region,omitempty"`
	Name             string         `json:"name,omitempty"`
	ImageID          string         `json:"image_id,omitempty"`
	InstanceType     string         `json:"instance_type,omitempty"`
	KeyName          string         `json:"key_name,omitempty"`
	SubnetID         string         `json:"subnet_id,omitempty"`
	SecurityGroupIDs []string       `json:"security_group_ids,omitempty"`
	UserData         string         `json:"user_data,omitempty"`
	AssignPublicIP   bool           `json:"assign_public_ip"`
	AssignIPv6       bool           `json:"assign_ipv6"`
	AllowAllTraffic  bool           `json:"allow_all_traffic"`
	Tags             []awscloud.Tag `json:"tags,omitempty"`
	RootPasswordMode string         `json:"root_password_mode,omitempty"`
	RootPassword     string         `json:"root_password,omitempty"`
	AvailabilityZone string         `json:"availability_zone,omitempty"`
	BlueprintID      string         `json:"blueprint_id,omitempty"`
	BundleID         string         `json:"bundle_id,omitempty"`
	KeyPairName      string         `json:"key_pair_name,omitempty"`
	IPAddressType    string         `json:"ip_address_type,omitempty"`
	AutoConnectGroup string         `json:"auto_connect_group,omitempty"`
}

func normalizeAWSFailoverService(service string) string {
	switch strings.ToLower(strings.TrimSpace(service)) {
	case "lightsail":
		return "lightsail"
	default:
		return "ec2"
	}
}

func provisionAWSMember(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member) (*memberProvisionOutcome, error) {
	if service == nil {
		return nil, errors.New("service is required")
	}
	if member == nil {
		return nil, errors.New("member is required")
	}
	if strings.ToLower(strings.TrimSpace(member.Provider)) != "aws" {
		return nil, fmt.Errorf("unsupported V2 member provider: %s", member.Provider)
	}

	payload, err := parseAWSMemberPlanPayload(member.PlanPayload)
	if err != nil {
		return nil, err
	}

	addition, credential, err := loadAWSCredentialSelection(userUUID, member.ProviderEntryID, member.ProviderEntryGroup)
	if err != nil {
		return nil, err
	}
	region := resolveAWSFailoverRegion(payload.Region, addition, credential)
	serviceType := normalizeAWSFailoverService(payload.Service)

	switch serviceType {
	case "ec2":
		name := strings.TrimSpace(payload.Name)
		if name == "" {
			name = fmt.Sprintf("failover-v2-ec2-%d-%d-%d", service.ID, member.ID, time.Now().Unix())
		}

		userData := strings.TrimSpace(payload.UserData)
		userData, generatedRootPassword, err := resolveAWSRootPasswordUserData(payload.RootPasswordMode, payload.RootPassword, userData)
		if err != nil {
			return nil, err
		}
		if payload.AssignIPv6 {
			userData, err = buildAWSIPv6RefreshUserData(userData)
			if err != nil {
				return nil, err
			}
		}

		autoConnectGroup := firstNonEmpty(strings.TrimSpace(payload.AutoConnectGroup), defaultV2AutoConnectGroup(service.ID, member.ID))
		userData, autoConnectGroup, err = buildAutoConnectUserData(autoConnectOptions{
			UserUUID:          userUUID,
			UserData:          userData,
			Group:             autoConnectGroup,
			WrapInShellScript: true,
		})
		if err != nil {
			return nil, err
		}

		createResult, err := awscloud.CreateInstance(ctx, credential, region, awscloud.CreateInstanceRequest{
			Name:             name,
			ImageID:          strings.TrimSpace(payload.ImageID),
			InstanceType:     strings.TrimSpace(payload.InstanceType),
			KeyName:          strings.TrimSpace(payload.KeyName),
			SubnetID:         strings.TrimSpace(payload.SubnetID),
			SecurityGroupIDs: trimStringSlice(payload.SecurityGroupIDs),
			UserData:         userData,
			AssignPublicIP:   payload.AssignPublicIP,
			AssignIPv6:       payload.AssignIPv6,
			Tags:             payload.Tags,
		})
		if err != nil {
			return nil, normalizeExecutionStopError(err)
		}

		cleanupInstanceID := strings.TrimSpace(createResult.Instance.InstanceID)
		rollbackLabel := "terminate failed aws ec2 instance " + cleanupInstanceID
		rollback := func(cleanupCtx context.Context) error {
			if cleanupInstanceID == "" {
				return nil
			}
			if err := awscloud.TerminateInstance(contextOrBackground(cleanupCtx), credential, region, cleanupInstanceID); err != nil {
				if isAWSResourceNotFoundError("ec2", err) {
					removeSavedAWSRootPassword(userUUID, addition, credential, "ec2", region, cleanupInstanceID)
					return nil
				}
				return normalizeExecutionStopError(err)
			}
			removeSavedAWSRootPassword(userUUID, addition, credential, "ec2", region, cleanupInstanceID)
			return nil
		}

		if payload.AssignIPv6 && !createResult.AssignIPv6 {
			warnings := strings.Join(trimStringSlice(createResult.Warnings), "; ")
			if warnings == "" {
				warnings = "Komari could not enable IPv6 automatically on the selected AWS network."
			}
			return nil, cleanupOnProvisionError(rollback, rollbackLabel, errors.New(warnings))
		}

		instance := createResult.Instance
		instance, detail, err := waitForAWSEC2Instance(ctx, region, credential, strings.TrimSpace(instance.InstanceID))
		if err != nil {
			return nil, cleanupOnProvisionError(rollback, rollbackLabel, err)
		}
		if payload.AssignIPv6 {
			if err := runAWSProvisionFollowUp(ctx, func(runCtx context.Context) error {
				_, followUpErr := awscloud.EnsureInstanceIPv6Address(runCtx, credential, region, strings.TrimSpace(instance.InstanceID))
				return followUpErr
			}); err != nil {
				return nil, cleanupOnProvisionError(rollback, rollbackLabel, normalizeExecutionStopError(err))
			}
			instance, detail, err = waitForAWSEC2Instance(ctx, region, credential, strings.TrimSpace(instance.InstanceID))
			if err != nil {
				return nil, cleanupOnProvisionError(rollback, rollbackLabel, err)
			}
		}
		if payload.AllowAllTraffic {
			if err := runAWSProvisionFollowUp(ctx, func(runCtx context.Context) error {
				_, followUpErr := awscloud.AllowAllSecurityGroupTraffic(runCtx, credential, region, strings.TrimSpace(instance.InstanceID))
				return followUpErr
			}); err != nil {
				return nil, cleanupOnProvisionError(rollback, rollbackLabel, normalizeExecutionStopError(err))
			}
			if refreshedDetail, detailErr := awscloud.GetInstanceDetail(contextOrBackground(ctx), credential, region, strings.TrimSpace(instance.InstanceID)); detailErr == nil {
				detail = refreshedDetail
			}
		}

		provisionedIPv6 := firstUsablePublicIPv6(instance.IPv6Addresses)
		if payload.AssignIPv6 {
			provisionedIPv6, err = requireUsableAWSIPv6(instance.IPv6Addresses)
			if err != nil {
				return nil, cleanupOnProvisionError(rollback, rollbackLabel, err)
			}
		}

		passwordMode := normalizeAWSRootPasswordMode(payload.RootPasswordMode)
		rootPassword := ""
		switch passwordMode {
		case "custom":
			rootPassword = strings.TrimSpace(payload.RootPassword)
		case "random":
			rootPassword = generatedRootPassword
		}
		passwordSaveErr := persistAWSRootPassword(
			userUUID,
			addition,
			credential,
			"ec2",
			region,
			instance.InstanceID,
			firstNonEmpty(instance.Name, instance.InstanceID),
			passwordMode,
			rootPassword,
		)
		if rootPassword != "" && passwordSaveErr != nil {
			return nil, cleanupOnProvisionError(rollback, rollbackLabel, fmt.Errorf("failed to save AWS root password: %w", passwordSaveErr))
		}

		detailAddresses := make([]awscloud.Address, 0)
		if detail != nil {
			detailAddresses = detail.Addresses
		}
		newInstanceRef := map[string]interface{}{
			"provider":            "aws",
			"service":             "ec2",
			"provider_entry_id":   credential.ID,
			"provider_entry_name": credential.Name,
			"region":              region,
			"instance_id":         instance.InstanceID,
			"name":                instance.Name,
		}
		if rootPassword != "" {
			newInstanceRef["root_password_mode"] = passwordMode
			newInstanceRef["root_password_saved"] = passwordSaveErr == nil
		}

		return &memberProvisionOutcome{
			IPv4:             strings.TrimSpace(instance.PublicIP),
			IPv6:             provisionedIPv6,
			AutoConnectGroup: autoConnectGroup,
			NewInstanceRef:   newInstanceRef,
			NewAddresses: map[string]interface{}{
				"public_ip":      instance.PublicIP,
				"private_ip":     instance.PrivateIP,
				"ipv6_addresses": instance.IPv6Addresses,
				"addresses":      detailAddresses,
			},
			RollbackLabel: rollbackLabel,
			Rollback:      rollback,
		}, nil
	case "lightsail":
		name := strings.TrimSpace(payload.Name)
		if name == "" {
			name = fmt.Sprintf("failover-v2-ls-%d-%d-%d", service.ID, member.ID, time.Now().Unix())
		}

		userData := strings.TrimSpace(payload.UserData)
		userData, generatedRootPassword, err := resolveAWSRootPasswordUserData(payload.RootPasswordMode, payload.RootPassword, userData)
		if err != nil {
			return nil, err
		}
		if lightsailProvisionWantsIPv6(payload.IPAddressType) {
			userData, err = buildAWSIPv6RefreshUserData(userData)
			if err != nil {
				return nil, err
			}
		}

		autoConnectGroup := firstNonEmpty(strings.TrimSpace(payload.AutoConnectGroup), defaultV2AutoConnectGroup(service.ID, member.ID))
		userData, autoConnectGroup, err = buildAutoConnectUserData(autoConnectOptions{
			UserUUID:          userUUID,
			UserData:          userData,
			Group:             autoConnectGroup,
			WrapInShellScript: true,
		})
		if err != nil {
			return nil, err
		}

		if err := awscloud.CreateLightsailInstance(ctx, credential, region, awscloud.CreateLightsailInstanceRequest{
			Name:             name,
			AvailabilityZone: strings.TrimSpace(payload.AvailabilityZone),
			BlueprintID:      strings.TrimSpace(payload.BlueprintID),
			BundleID:         strings.TrimSpace(payload.BundleID),
			KeyPairName:      strings.TrimSpace(payload.KeyPairName),
			UserData:         userData,
			Tags:             payload.Tags,
			IPAddressType:    strings.TrimSpace(payload.IPAddressType),
		}); err != nil {
			return nil, normalizeExecutionStopError(err)
		}

		rollbackLabel := "delete failed aws lightsail instance " + name
		rollback := func(cleanupCtx context.Context) error {
			if err := awscloud.DeleteLightsailInstance(contextOrBackground(cleanupCtx), credential, region, name); err != nil {
				if isAWSResourceNotFoundError("lightsail", err) {
					removeSavedAWSRootPassword(userUUID, addition, credential, "lightsail", region, name)
					return nil
				}
				return normalizeExecutionStopError(err)
			}
			removeSavedAWSRootPassword(userUUID, addition, credential, "lightsail", region, name)
			return nil
		}

		detail, err := waitForLightsailInstance(ctx, region, credential, name)
		if err != nil {
			return nil, cleanupOnProvisionError(rollback, rollbackLabel, err)
		}
		if payload.AllowAllTraffic {
			if err := runAWSProvisionFollowUp(ctx, func(runCtx context.Context) error {
				return awscloud.OpenLightsailAllPublicPorts(runCtx, credential, region, name)
			}); err != nil {
				return nil, cleanupOnProvisionError(rollback, rollbackLabel, normalizeExecutionStopError(err))
			}
			detail, err = waitForLightsailInstance(ctx, region, credential, name)
			if err != nil {
				return nil, cleanupOnProvisionError(rollback, rollbackLabel, err)
			}
		}

		provisionedIPv6 := firstUsablePublicIPv6(detail.Instance.IPv6Addresses)
		if lightsailProvisionWantsIPv6(payload.IPAddressType) {
			provisionedIPv6, err = requireUsableAWSIPv6(detail.Instance.IPv6Addresses)
			if err != nil {
				return nil, cleanupOnProvisionError(rollback, rollbackLabel, err)
			}
		}

		passwordMode := normalizeAWSRootPasswordMode(payload.RootPasswordMode)
		rootPassword := ""
		switch passwordMode {
		case "custom":
			rootPassword = strings.TrimSpace(payload.RootPassword)
		case "random":
			rootPassword = generatedRootPassword
		}
		passwordSaveErr := persistAWSRootPassword(
			userUUID,
			addition,
			credential,
			"lightsail",
			region,
			name,
			name,
			passwordMode,
			rootPassword,
		)
		if rootPassword != "" && passwordSaveErr != nil {
			return nil, cleanupOnProvisionError(rollback, rollbackLabel, fmt.Errorf("failed to save AWS root password: %w", passwordSaveErr))
		}

		newInstanceRef := map[string]interface{}{
			"provider":            "aws",
			"service":             "lightsail",
			"provider_entry_id":   credential.ID,
			"provider_entry_name": credential.Name,
			"region":              region,
			"instance_name":       name,
		}
		if rootPassword != "" {
			newInstanceRef["root_password_mode"] = passwordMode
			newInstanceRef["root_password_saved"] = passwordSaveErr == nil
		}

		return &memberProvisionOutcome{
			IPv4:             strings.TrimSpace(detail.Instance.PublicIP),
			IPv6:             provisionedIPv6,
			AutoConnectGroup: autoConnectGroup,
			NewInstanceRef:   newInstanceRef,
			NewAddresses: map[string]interface{}{
				"public_ip":      detail.Instance.PublicIP,
				"private_ip":     detail.Instance.PrivateIP,
				"ipv6_addresses": detail.Instance.IPv6Addresses,
			},
			RollbackLabel: rollbackLabel,
			Rollback:      rollback,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported aws provision service: %s", payload.Service)
	}
}

func parseAWSMemberPlanPayload(raw string) (*awsMemberPlanPayload, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		raw = "{}"
	}

	rawPayload := []byte(raw)
	var payload awsMemberPlanPayload
	if err := json.Unmarshal(rawPayload, &payload); err != nil {
		return nil, fmt.Errorf("invalid aws member plan_payload: %w", err)
	}
	var payloadFields map[string]json.RawMessage
	if err := json.Unmarshal(rawPayload, &payloadFields); err == nil {
		if _, ok := payloadFields["allow_all_traffic"]; !ok {
			payload.AllowAllTraffic = true
		}
	}
	if err := validateAWSMemberPlanPayload(payload); err != nil {
		return nil, err
	}
	return &payload, nil
}

func ParseAWSMemberPlanPayload(raw string) (interface{}, error) {
	return parseAWSMemberPlanPayload(raw)
}

func validateAWSMemberPlanPayload(payload awsMemberPlanPayload) error {
	service := normalizeAWSFailoverService(payload.Service)
	region := strings.TrimSpace(payload.Region)
	if region == "" {
		return errors.New("aws member plan_payload.region is required")
	}

	if service == "lightsail" {
		if strings.TrimSpace(payload.AvailabilityZone) == "" {
			return errors.New("aws member plan_payload.availability_zone is required for lightsail")
		}
		if strings.TrimSpace(payload.BlueprintID) == "" {
			return errors.New("aws member plan_payload.blueprint_id is required for lightsail")
		}
		if strings.TrimSpace(payload.BundleID) == "" {
			return errors.New("aws member plan_payload.bundle_id is required for lightsail")
		}
		return nil
	}

	if strings.TrimSpace(payload.ImageID) == "" {
		return errors.New("aws member plan_payload.image_id is required")
	}
	if strings.TrimSpace(payload.InstanceType) == "" {
		return errors.New("aws member plan_payload.instance_type is required")
	}
	return nil
}
