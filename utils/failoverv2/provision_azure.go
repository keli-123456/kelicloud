package failoverv2

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/komari-monitor/komari/database/models"
	azurecloud "github.com/komari-monitor/komari/utils/cloudprovider/azure"
	linodecloud "github.com/komari-monitor/komari/utils/cloudprovider/linode"
)

type azureMemberPlanPayload struct {
	Name             string                    `json:"name,omitempty"`
	NamePrefix       string                    `json:"name_prefix,omitempty"`
	ResourceGroup    string                    `json:"resource_group,omitempty"`
	Region           string                    `json:"region,omitempty"`
	Location         string                    `json:"location,omitempty"`
	Size             string                    `json:"size,omitempty"`
	AdminUsername    string                    `json:"admin_username,omitempty"`
	AdminPassword    string                    `json:"admin_password,omitempty"`
	SSHPublicKey     string                    `json:"ssh_public_key,omitempty"`
	UserData         string                    `json:"user_data,omitempty"`
	PublicIP         bool                      `json:"public_ip"`
	AssignIPv6       bool                      `json:"assign_ipv6"`
	Image            azurecloud.ImageReference `json:"image"`
	RootPasswordMode string                    `json:"root_password_mode,omitempty"`
	RootPassword     string                    `json:"root_password,omitempty"`
	AutoConnectGroup string                    `json:"auto_connect_group,omitempty"`
}

func provisionAzureMember(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member) (*memberProvisionOutcome, error) {
	if service == nil {
		return nil, errors.New("service is required")
	}
	if member == nil {
		return nil, errors.New("member is required")
	}
	if strings.ToLower(strings.TrimSpace(member.Provider)) != "azure" {
		return nil, fmt.Errorf("unsupported V2 member provider: %s", member.Provider)
	}

	payload, err := parseAzureMemberPlanPayload(member.PlanPayload)
	if err != nil {
		return nil, err
	}

	addition, credential, err := loadAzureCredentialSelection(userUUID, member.ProviderEntryID, member.ProviderEntryGroup)
	if err != nil {
		return nil, err
	}
	client, err := azurecloud.NewClientFromCredential(credential)
	if err != nil {
		return nil, err
	}

	location := firstNonEmpty(payload.Location, payload.Region, credential.DefaultLocation, azurecloud.DefaultLocation)
	name := strings.TrimSpace(payload.Name)
	if name == "" {
		prefix := strings.TrimSpace(payload.NamePrefix)
		if prefix == "" {
			prefix = fmt.Sprintf("failover-v2-azure-%d-%d", service.ID, member.ID)
		}
		name = prefix + "-" + strconv.FormatInt(time.Now().Unix(), 10)
	}
	username := strings.TrimSpace(payload.AdminUsername)
	if username == "" {
		username = "azureuser"
	}

	userData := strings.TrimSpace(payload.UserData)
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

	rootPassword := firstNonEmpty(payload.RootPassword, payload.AdminPassword)
	passwordMode := strings.ToLower(strings.TrimSpace(payload.RootPasswordMode))
	switch passwordMode {
	case "", "random":
		passwordMode = "random"
		rootPassword, err = linodecloud.GenerateRandomPassword(20)
		if err != nil {
			return nil, err
		}
	case "custom":
		if strings.TrimSpace(rootPassword) == "" {
			return nil, errors.New("azure root_password cannot be empty when root_password_mode=custom")
		}
	case "none":
		rootPassword = ""
		if strings.TrimSpace(payload.SSHPublicKey) == "" {
			return nil, errors.New("azure ssh_public_key is required when root_password_mode=none")
		}
	default:
		return nil, fmt.Errorf("unsupported azure root_password_mode: %s", payload.RootPasswordMode)
	}

	detail, err := client.CreateVirtualMachine(ctx, azurecloud.CreateVirtualMachineRequest{
		Name:          name,
		ResourceGroup: strings.TrimSpace(payload.ResourceGroup),
		Location:      location,
		Size:          strings.TrimSpace(payload.Size),
		AdminUsername: username,
		AdminPassword: rootPassword,
		SSHPublicKey:  strings.TrimSpace(payload.SSHPublicKey),
		UserData:      userData,
		PublicIP:      payload.PublicIP,
		AssignIPv6:    payload.AssignIPv6,
		Image:         payload.Image,
		Tags: map[string]string{
			"managed-by": "komari",
			"provider":   "failover-v2",
		},
	})
	if err != nil {
		return nil, normalizeExecutionStopError(err)
	}

	instance := detail.Instance
	cleanupLabel := "delete failed azure vm " + instance.Name
	cleanupProvisionedInstance := func(cleanupCtx context.Context) error {
		if _, err := client.DeleteVirtualMachine(contextOrBackground(cleanupCtx), instance.ResourceGroup, instance.Name); err != nil {
			if isAzureNotFoundError(err) {
				removeSavedAzureRootPassword(userUUID, addition, credential, instance.InstanceID)
				return nil
			}
			return normalizeExecutionStopError(err)
		}
		removeSavedAzureRootPassword(userUUID, addition, credential, instance.InstanceID)
		return nil
	}

	passwordSaveErr := persistAzureRootPassword(
		userUUID,
		addition,
		credential,
		instance.InstanceID,
		instance.Name,
		username,
		passwordMode,
		rootPassword,
	)
	if rootPassword != "" && passwordSaveErr != nil {
		return nil, cleanupOnProvisionError(cleanupProvisionedInstance, cleanupLabel, fmt.Errorf("failed to save Azure root password: %w", passwordSaveErr))
	}

	newInstanceRef := map[string]interface{}{
		"provider":            "azure",
		"provider_entry_id":   credential.ID,
		"provider_entry_name": credential.Name,
		"region":              instance.Location,
		"location":            instance.Location,
		"resource_group":      instance.ResourceGroup,
		"instance_id":         instance.InstanceID,
		"name":                instance.Name,
	}
	if rootPassword != "" {
		newInstanceRef["root_password_mode"] = passwordMode
		newInstanceRef["root_password_saved"] = passwordSaveErr == nil
	}

	return &memberProvisionOutcome{
		IPv4:             firstAzurePublicIPByVersion(instance.PublicIPs, false),
		IPv6:             firstAzurePublicIPByVersion(instance.PublicIPs, true),
		AutoConnectGroup: autoConnectGroup,
		NewInstanceRef:   newInstanceRef,
		NewAddresses: map[string]interface{}{
			"public_ips":  instance.PublicIPs,
			"private_ips": instance.PrivateIPs,
		},
		RollbackLabel: cleanupLabel,
		Rollback:      cleanupProvisionedInstance,
	}, nil
}

func parseAzureMemberPlanPayload(raw string) (*azureMemberPlanPayload, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		raw = "{}"
	}

	rawPayload := []byte(raw)
	var payload azureMemberPlanPayload
	if err := json.Unmarshal(rawPayload, &payload); err != nil {
		return nil, fmt.Errorf("invalid azure member plan_payload: %w", err)
	}
	var payloadFields map[string]json.RawMessage
	if err := json.Unmarshal(rawPayload, &payloadFields); err == nil {
		if _, ok := payloadFields["public_ip"]; !ok {
			payload.PublicIP = true
		}
		if _, ok := payloadFields["assign_ipv6"]; !ok {
			payload.AssignIPv6 = true
		}
	}
	if strings.TrimSpace(payload.RootPasswordMode) == "" {
		payload.RootPasswordMode = "random"
	}
	if strings.TrimSpace(payload.Image.Version) == "" {
		payload.Image.Version = "latest"
	}
	if err := validateAzureMemberPlanPayload(payload); err != nil {
		return nil, err
	}
	return &payload, nil
}

func ParseAzureMemberPlanPayload(raw string) (interface{}, error) {
	return parseAzureMemberPlanPayload(raw)
}

func validateAzureMemberPlanPayload(payload azureMemberPlanPayload) error {
	if strings.TrimSpace(payload.Size) == "" {
		return errors.New("azure member plan_payload.size is required")
	}
	if strings.TrimSpace(payload.Image.Publisher) == "" {
		return errors.New("azure member plan_payload.image.publisher is required")
	}
	if strings.TrimSpace(payload.Image.Offer) == "" {
		return errors.New("azure member plan_payload.image.offer is required")
	}
	if strings.TrimSpace(payload.Image.SKU) == "" {
		return errors.New("azure member plan_payload.image.sku is required")
	}
	return nil
}

func firstAzurePublicIPByVersion(values []string, ipv6 bool) string {
	for _, value := range values {
		normalized := normalizeIPAddress(value)
		if normalized == "" {
			continue
		}
		parsed := net.ParseIP(normalized)
		if parsed == nil {
			continue
		}
		if ipv6 {
			if parsed.To4() == nil {
				return normalized
			}
			continue
		}
		if parsed.To4() != nil {
			return normalized
		}
	}
	return ""
}
