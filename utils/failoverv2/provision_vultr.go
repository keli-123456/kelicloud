package failoverv2

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/komari-monitor/komari/database/models"
	vultrcloud "github.com/komari-monitor/komari/utils/cloudprovider/vultr"
)

type vultrMemberPlanPayload struct {
	Label             string   `json:"label,omitempty"`
	LabelPrefix       string   `json:"label_prefix,omitempty"`
	Hostname          string   `json:"hostname,omitempty"`
	Region            string   `json:"region,omitempty"`
	Plan              string   `json:"plan,omitempty"`
	OSID              int      `json:"os_id,omitempty"`
	SSHKeyIDs         []string `json:"sshkey_id,omitempty"`
	EnableIPv6        bool     `json:"enable_ipv6"`
	DisablePublicIPv4 bool     `json:"disable_public_ipv4"`
	BackupsEnabled    bool     `json:"backups_enabled"`
	DDOSProtection    bool     `json:"ddos_protection"`
	ActivationEmail   bool     `json:"activation_email"`
	Tags              []string `json:"tags,omitempty"`
	UserData          string   `json:"user_data,omitempty"`
	RootPasswordMode  string   `json:"root_password_mode,omitempty"`
	RootPassword      string   `json:"root_password,omitempty"`
	AutoConnectGroup  string   `json:"auto_connect_group,omitempty"`
}

func provisionVultrMember(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member) (*memberProvisionOutcome, error) {
	if service == nil {
		return nil, errors.New("service is required")
	}
	if member == nil {
		return nil, errors.New("member is required")
	}
	if strings.ToLower(strings.TrimSpace(member.Provider)) != "vultr" {
		return nil, fmt.Errorf("unsupported V2 member provider: %s", member.Provider)
	}

	payload, err := parseVultrMemberPlanPayload(member.PlanPayload)
	if err != nil {
		return nil, err
	}

	addition, token, err := loadVultrTokenSelection(userUUID, member.ProviderEntryID, member.ProviderEntryGroup)
	if err != nil {
		return nil, err
	}
	client, err := vultrcloud.NewClientFromToken(token.Token)
	if err != nil {
		return nil, err
	}

	label := strings.TrimSpace(payload.Label)
	if label == "" {
		prefix := strings.TrimSpace(payload.LabelPrefix)
		if prefix == "" {
			prefix = fmt.Sprintf("failover-v2-%d-%d", service.ID, member.ID)
		}
		label = prefix + "-" + strconv.FormatInt(time.Now().Unix(), 10)
	}
	hostname := strings.TrimSpace(payload.Hostname)
	if hostname == "" {
		hostname = label
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

	passwordMode := strings.ToLower(strings.TrimSpace(payload.RootPasswordMode))
	rootPassword := strings.TrimSpace(payload.RootPassword)
	switch passwordMode {
	case "", "provider_default":
		passwordMode = "provider_default"
		rootPassword = ""
	case "none":
		rootPassword = ""
	case "random":
		rootPassword, err = vultrcloud.GenerateRandomPassword(20)
		if err != nil {
			return nil, err
		}
		userData, err = vultrcloud.BuildRootPasswordUserData(rootPassword, userData)
		if err != nil {
			return nil, err
		}
	case "custom":
		if rootPassword == "" {
			return nil, errors.New("vultr root_password cannot be empty when root_password_mode=custom")
		}
		userData, err = vultrcloud.BuildRootPasswordUserData(rootPassword, userData)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported vultr root_password_mode: %s", payload.RootPasswordMode)
	}

	backups := ""
	if payload.BackupsEnabled {
		backups = "enabled"
	}
	request := vultrcloud.CreateInstanceRequest{
		Label:             label,
		Hostname:          hostname,
		Region:            strings.TrimSpace(payload.Region),
		Plan:              strings.TrimSpace(payload.Plan),
		OSID:              payload.OSID,
		SSHKeyIDs:         trimStringSlice(payload.SSHKeyIDs),
		EnableIPv6:        payload.EnableIPv6,
		DisablePublicIPv4: payload.DisablePublicIPv4,
		Backups:           backups,
		DDOSProtection:    payload.DDOSProtection,
		ActivationEmail:   payload.ActivationEmail,
		Tags:              trimStringSlice(payload.Tags),
	}
	if userData != "" {
		request.UserData = vultrcloud.EncodeUserData(userData)
	}

	instance, err := client.CreateInstance(ctx, request)
	if err != nil {
		return nil, normalizeExecutionStopError(err)
	}

	rollbackLabel := "delete failed vultr instance " + instance.ID
	rollback := func(cleanupCtx context.Context) error {
		if err := client.DeleteInstance(contextOrBackground(cleanupCtx), instance.ID); err != nil && !isVultrNotFoundError(err) {
			return normalizeExecutionStopError(err)
		}
		removeSavedVultrRootPassword(userUUID, addition, token, instance.ID)
		return nil
	}

	instance, err = waitForVultrInstance(ctx, client, instance.ID)
	if err != nil {
		return nil, cleanupOnProvisionError(rollback, rollbackLabel, normalizeExecutionStopError(err))
	}

	passwordToSave := rootPassword
	if passwordToSave == "" && passwordMode == "provider_default" && instance != nil {
		passwordToSave = strings.TrimSpace(instance.DefaultPassword)
	}
	passwordSaveErr := persistVultrRootPassword(userUUID, addition, token, instance.ID, instance.Label, passwordMode, passwordToSave)
	if passwordToSave != "" && passwordSaveErr != nil {
		return nil, cleanupOnProvisionError(rollback, rollbackLabel, fmt.Errorf("failed to save Vultr root password: %w", passwordSaveErr))
	}

	newInstanceRef := map[string]interface{}{
		"provider":            "vultr",
		"provider_entry_id":   token.ID,
		"provider_entry_name": token.Name,
		"region":              instance.Region,
		"instance_id":         instance.ID,
		"label":               instance.Label,
	}
	if passwordToSave != "" {
		newInstanceRef["root_password_mode"] = passwordMode
		newInstanceRef["root_password_saved"] = passwordSaveErr == nil
	}

	return &memberProvisionOutcome{
		IPv4:             vultrPublicIPv4(instance),
		IPv6:             vultrPublicIPv6(instance),
		AutoConnectGroup: autoConnectGroup,
		NewInstanceRef:   newInstanceRef,
		NewAddresses: map[string]interface{}{
			"main_ip":       instance.MainIP,
			"v6_main_ip":    instance.V6MainIP,
			"internal_ip":   instance.InternalIP,
			"status":        instance.Status,
			"server_status": instance.ServerStatus,
		},
		RollbackLabel: rollbackLabel,
		Rollback:      rollback,
	}, nil
}

func parseVultrMemberPlanPayload(raw string) (*vultrMemberPlanPayload, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		raw = "{}"
	}

	var payload vultrMemberPlanPayload
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return nil, fmt.Errorf("invalid vultr member plan_payload: %w", err)
	}
	if strings.TrimSpace(payload.RootPasswordMode) == "" {
		payload.RootPasswordMode = "provider_default"
	}
	if strings.TrimSpace(payload.Region) == "" {
		return nil, errors.New("vultr member plan_payload.region is required")
	}
	if strings.TrimSpace(payload.Plan) == "" {
		return nil, errors.New("vultr member plan_payload.plan is required")
	}
	if payload.OSID <= 0 {
		return nil, errors.New("vultr member plan_payload.os_id is required")
	}
	return &payload, nil
}

func ParseVultrMemberPlanPayload(raw string) (interface{}, error) {
	return parseVultrMemberPlanPayload(raw)
}
