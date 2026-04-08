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
	linodecloud "github.com/komari-monitor/komari/utils/cloudprovider/linode"
)

type linodeMemberPlanPayload struct {
	Label             string   `json:"label,omitempty"`
	LabelPrefix       string   `json:"label_prefix,omitempty"`
	Region            string   `json:"region,omitempty"`
	Type              string   `json:"type,omitempty"`
	Image             string   `json:"image,omitempty"`
	AuthorizedKeys    []string `json:"authorized_keys,omitempty"`
	BackupsEnabled    bool     `json:"backups_enabled"`
	Booted            bool     `json:"booted"`
	Tags              []string `json:"tags,omitempty"`
	UserData          string   `json:"user_data,omitempty"`
	RootPasswordMode  string   `json:"root_password_mode,omitempty"`
	RootPassword      string   `json:"root_password,omitempty"`
	AutoConnectGroup  string   `json:"auto_connect_group,omitempty"`
}

func provisionLinodeMember(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member) (*memberProvisionOutcome, error) {
	if service == nil {
		return nil, errors.New("service is required")
	}
	if member == nil {
		return nil, errors.New("member is required")
	}
	if strings.ToLower(strings.TrimSpace(member.Provider)) != "linode" {
		return nil, fmt.Errorf("unsupported V2 member provider: %s", member.Provider)
	}

	payload, err := parseLinodeMemberPlanPayload(member.PlanPayload)
	if err != nil {
		return nil, err
	}

	addition, token, err := loadLinodeToken(userUUID, member.ProviderEntryID)
	if err != nil {
		return nil, err
	}
	client, err := linodecloud.NewClientFromToken(token.Token)
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

	rootPassword := strings.TrimSpace(payload.RootPassword)
	passwordMode := strings.ToLower(strings.TrimSpace(payload.RootPasswordMode))
	switch passwordMode {
	case "", "random":
		passwordMode = "random"
		rootPassword, err = linodecloud.GenerateRandomPassword(20)
		if err != nil {
			return nil, err
		}
	case "custom":
		if rootPassword == "" {
			return nil, errors.New("linode root_password cannot be empty when root_password_mode=custom")
		}
	default:
		return nil, fmt.Errorf("unsupported linode root_password_mode: %s", payload.RootPasswordMode)
	}

	request := linodecloud.CreateInstanceRequest{
		Label:          label,
		Region:         strings.TrimSpace(payload.Region),
		Type:           strings.TrimSpace(payload.Type),
		Image:          strings.TrimSpace(payload.Image),
		RootPass:       rootPassword,
		AuthorizedKeys: trimStringSlice(payload.AuthorizedKeys),
		BackupsEnabled: payload.BackupsEnabled,
		Booted:         true,
		Tags:           trimStringSlice(payload.Tags),
	}
	if userData != "" {
		request.Metadata = &struct {
			UserData string `json:"user_data,omitempty"`
		}{
			UserData: linodecloud.EncodeUserData(userData),
		}
	}
	if autoConnectGroup != "" {
		if err := linodecloud.ValidateAutoConnectSupport(ctx, client, request.Region, request.Image); err != nil {
			return nil, err
		}
	}

	instance, err := client.CreateInstance(ctx, request)
	if err != nil {
		return nil, normalizeExecutionStopError(err)
	}

	rollbackLabel := fmt.Sprintf("delete failed linode instance %d", instance.ID)
	rollback := func(cleanupCtx context.Context) error {
		if err := client.DeleteInstance(contextOrBackground(cleanupCtx), instance.ID); err != nil && !isLinodeNotFoundError(err) {
			return normalizeExecutionStopError(err)
		}
		removeSavedLinodeRootPassword(userUUID, addition, token, instance.ID)
		return nil
	}

	instance, err = waitForLinodeInstance(ctx, client, instance.ID)
	if err != nil {
		return nil, cleanupOnProvisionError(rollback, rollbackLabel, normalizeExecutionStopError(err))
	}

	passwordSaveErr := persistLinodeRootPassword(
		userUUID,
		addition,
		token,
		instance.ID,
		instance.Label,
		passwordMode,
		rootPassword,
	)
	if rootPassword != "" && passwordSaveErr != nil {
		return nil, cleanupOnProvisionError(rollback, rollbackLabel, fmt.Errorf("failed to save Linode root password: %w", passwordSaveErr))
	}

	newInstanceRef := map[string]interface{}{
		"provider":            "linode",
		"provider_entry_id":   token.ID,
		"provider_entry_name": token.Name,
		"region":              instance.Region,
		"instance_id":         instance.ID,
		"label":               instance.Label,
	}
	if rootPassword != "" {
		newInstanceRef["root_password_mode"] = passwordMode
		newInstanceRef["root_password_saved"] = passwordSaveErr == nil
	}

	return &memberProvisionOutcome{
		IPv4:             normalizeIPAddress(firstString(instance.IPv4)),
		IPv6:             normalizeIPAddress(instance.IPv6),
		AutoConnectGroup: autoConnectGroup,
		NewInstanceRef:   newInstanceRef,
		NewAddresses: map[string]interface{}{
			"ipv4": instance.IPv4,
			"ipv6": instance.IPv6,
		},
		RollbackLabel: rollbackLabel,
		Rollback:      rollback,
	}, nil
}

func parseLinodeMemberPlanPayload(raw string) (*linodeMemberPlanPayload, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		raw = "{}"
	}

	var payload linodeMemberPlanPayload
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return nil, fmt.Errorf("invalid linode member plan_payload: %w", err)
	}
	if strings.TrimSpace(payload.Region) == "" {
		return nil, errors.New("linode member plan_payload.region is required")
	}
	if strings.TrimSpace(payload.Type) == "" {
		return nil, errors.New("linode member plan_payload.type is required")
	}
	if strings.TrimSpace(payload.Image) == "" {
		return nil, errors.New("linode member plan_payload.image is required")
	}
	return &payload, nil
}

func ParseLinodeMemberPlanPayload(raw string) (interface{}, error) {
	return parseLinodeMemberPlanPayload(raw)
}
