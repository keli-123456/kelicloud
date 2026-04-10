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
	"github.com/komari-monitor/komari/utils/cloudprovider/digitalocean"
)

type digitalOceanMemberPlanPayload struct {
	Name             string   `json:"name,omitempty"`
	NamePrefix       string   `json:"name_prefix,omitempty"`
	Region           string   `json:"region,omitempty"`
	Size             string   `json:"size,omitempty"`
	Image            string   `json:"image,omitempty"`
	Backups          bool     `json:"backups"`
	IPv6             bool     `json:"ipv6"`
	Monitoring       bool     `json:"monitoring"`
	Tags             []string `json:"tags,omitempty"`
	UserData         string   `json:"user_data,omitempty"`
	VPCUUID          string   `json:"vpc_uuid,omitempty"`
	RootPasswordMode string   `json:"root_password_mode,omitempty"`
	RootPassword     string   `json:"root_password,omitempty"`
	AutoConnectGroup string   `json:"auto_connect_group,omitempty"`
}

type memberProvisionOutcome struct {
	IPv4             string
	IPv6             string
	AutoConnectGroup string
	TargetClientUUID string
	NewInstanceRef   map[string]interface{}
	NewAddresses     map[string]interface{}
	SkipScripts      bool
	SkipPostCleanup  bool
	CleanupStatus    string
	CleanupResult    map[string]interface{}
	CleanupMessage   string
	RollbackLabel    string
	Rollback         func(context.Context) error
}

func (o *memberProvisionOutcome) primaryAddress() string {
	if o == nil {
		return ""
	}
	return firstNonEmpty(o.IPv4, o.IPv6)
}

func defaultV2AutoConnectGroup(serviceID, memberID uint) string {
	return normalizeAutoConnectGroup(fmt.Sprintf("failover-v2/%d/%d", serviceID, memberID))
}

func provisionDigitalOceanMember(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member) (*memberProvisionOutcome, error) {
	if service == nil {
		return nil, errors.New("service is required")
	}
	if member == nil {
		return nil, errors.New("member is required")
	}
	if strings.ToLower(strings.TrimSpace(member.Provider)) != "digitalocean" {
		return nil, fmt.Errorf("unsupported V2 member provider: %s", member.Provider)
	}

	payload, err := parseDigitalOceanMemberPlanPayload(member.PlanPayload)
	if err != nil {
		return nil, err
	}

	addition, token, err := loadDigitalOceanTokenSelection(userUUID, member.ProviderEntryID, member.ProviderEntryGroup)
	if err != nil {
		return nil, err
	}
	client, err := digitalocean.NewClientFromToken(token.Token)
	if err != nil {
		return nil, err
	}

	name := strings.TrimSpace(payload.Name)
	if name == "" {
		prefix := strings.TrimSpace(payload.NamePrefix)
		if prefix == "" {
			prefix = fmt.Sprintf("failover-v2-%d-%d", service.ID, member.ID)
		}
		name = prefix + "-" + strconv.FormatInt(time.Now().Unix(), 10)
	}

	userData := strings.TrimSpace(payload.UserData)
	autoConnectGroup := firstNonEmpty(strings.TrimSpace(payload.AutoConnectGroup), defaultV2AutoConnectGroup(service.ID, member.ID))
	userData, autoConnectGroup, err = buildAutoConnectUserData(autoConnectOptions{
		UserUUID:          userUUID,
		UserData:          userData,
		Group:             autoConnectGroup,
		WrapInShellScript: false,
	})
	if err != nil {
		return nil, err
	}

	passwordMode := strings.ToLower(strings.TrimSpace(payload.RootPasswordMode))
	rootPassword := ""
	switch passwordMode {
	case "", "none":
		passwordMode = "none"
	case "random":
		rootPassword, err = digitalocean.GenerateRandomPassword(20)
		if err != nil {
			return nil, err
		}
		userData, err = digitalocean.BuildRootPasswordUserData(rootPassword, userData)
		if err != nil {
			return nil, err
		}
	case "custom":
		if strings.TrimSpace(payload.RootPassword) == "" {
			return nil, errors.New("digitalocean root_password cannot be empty when root_password_mode=custom")
		}
		rootPassword = strings.TrimSpace(payload.RootPassword)
		userData, err = digitalocean.BuildRootPasswordUserData(rootPassword, userData)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported digitalocean root_password_mode: %s", payload.RootPasswordMode)
	}

	droplet, err := client.CreateDroplet(ctx, digitalocean.CreateDropletRequest{
		Name:       name,
		Region:     strings.TrimSpace(payload.Region),
		Size:       strings.TrimSpace(payload.Size),
		Image:      strings.TrimSpace(payload.Image),
		Backups:    payload.Backups,
		IPv6:       payload.IPv6,
		Monitoring: payload.Monitoring,
		Tags:       trimStringSlice(payload.Tags),
		UserData:   userData,
		VPCUUID:    strings.TrimSpace(payload.VPCUUID),
	})
	if err != nil {
		return nil, err
	}

	droplet, err = waitForDigitalOceanDroplet(ctx, client, droplet.ID)
	if err != nil {
		cleanupErr := client.DeleteDroplet(context.Background(), droplet.ID)
		if cleanupErr != nil && !isDigitalOceanNotFoundError(cleanupErr) {
			return nil, fmt.Errorf("failed waiting for digitalocean droplet %d: %w; automatic cleanup also failed: %v", droplet.ID, err, cleanupErr)
		}
		return nil, err
	}

	passwordSaveErr := persistDigitalOceanRootPassword(userUUID, addition, token, droplet.ID, droplet.Name, passwordMode, rootPassword)
	if rootPassword != "" && passwordSaveErr != nil {
		cleanupErr := client.DeleteDroplet(contextOrBackground(ctx), droplet.ID)
		if cleanupErr != nil && !isDigitalOceanNotFoundError(cleanupErr) {
			return nil, fmt.Errorf("failed to save droplet root password: %w; automatic cleanup also failed: %v", passwordSaveErr, cleanupErr)
		}
		return nil, fmt.Errorf("failed to save droplet root password: %w", passwordSaveErr)
	}

	newInstanceRef := map[string]interface{}{
		"provider":            "digitalocean",
		"provider_entry_id":   token.ID,
		"provider_entry_name": token.Name,
		"region":              strings.TrimSpace(payload.Region),
		"droplet_id":          droplet.ID,
		"name":                droplet.Name,
	}
	if rootPassword != "" {
		newInstanceRef["root_password_mode"] = passwordMode
		newInstanceRef["root_password_saved"] = passwordSaveErr == nil
	}

	return &memberProvisionOutcome{
		IPv4:             digitalOceanPublicIPv4(droplet),
		IPv6:             digitalOceanPublicIPv6(droplet),
		AutoConnectGroup: autoConnectGroup,
		NewInstanceRef:   newInstanceRef,
		NewAddresses: map[string]interface{}{
			"ipv4": droplet.Networks.V4,
			"ipv6": droplet.Networks.V6,
		},
		RollbackLabel: fmt.Sprintf("delete failed digitalocean droplet %d", droplet.ID),
		Rollback: func(ctx context.Context) error {
			if err := client.DeleteDroplet(contextOrBackground(ctx), droplet.ID); err != nil && !isDigitalOceanNotFoundError(err) {
				return err
			}
			removeSavedDigitalOceanRootPassword(userUUID, addition, token, droplet.ID)
			return nil
		},
	}, nil
}

func parseDigitalOceanMemberPlanPayload(raw string) (*digitalOceanMemberPlanPayload, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		raw = "{}"
	}

	var payload digitalOceanMemberPlanPayload
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return nil, fmt.Errorf("invalid digitalocean member plan_payload: %w", err)
	}
	if strings.TrimSpace(payload.RootPasswordMode) == "" {
		payload.RootPasswordMode = "random"
	}
	if strings.TrimSpace(payload.Region) == "" {
		return nil, errors.New("digitalocean member plan_payload.region is required")
	}
	if strings.TrimSpace(payload.Size) == "" {
		return nil, errors.New("digitalocean member plan_payload.size is required")
	}
	if strings.TrimSpace(payload.Image) == "" {
		return nil, errors.New("digitalocean member plan_payload.image is required")
	}
	return &payload, nil
}

func ParseDigitalOceanMemberPlanPayload(raw string) (interface{}, error) {
	return parseDigitalOceanMemberPlanPayload(raw)
}

func trimStringSlice(values []string) []string {
	normalized := make([]string, 0, len(values))
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			normalized = append(normalized, trimmed)
		}
	}
	return normalized
}

func waitForDigitalOceanDroplet(ctx context.Context, client *digitalocean.Client, dropletID int) (*digitalocean.Droplet, error) {
	deadline := time.Now().Add(5 * time.Minute)
	for time.Now().Before(deadline) {
		if err := waitContextOrDelay(ctx, 0); err != nil {
			return nil, err
		}

		droplets, err := client.ListDroplets(ctx)
		if err != nil {
			return nil, err
		}
		for _, droplet := range droplets {
			if droplet.ID == dropletID && digitalOceanPublicIPv4(&droplet) != "" {
				return &droplet, nil
			}
		}

		if err := waitContextOrDelay(ctx, 5*time.Second); err != nil {
			return nil, err
		}
	}

	droplets, err := client.ListDroplets(ctx)
	if err != nil {
		return nil, err
	}
	for _, droplet := range droplets {
		if droplet.ID == dropletID {
			return &droplet, nil
		}
	}
	return nil, fmt.Errorf("digitalocean droplet not found: %d", dropletID)
}

func digitalOceanPublicIPv4(droplet *digitalocean.Droplet) string {
	if droplet == nil {
		return ""
	}
	for _, network := range droplet.Networks.V4 {
		if strings.EqualFold(strings.TrimSpace(network.Type), "public") {
			return strings.TrimSpace(network.IPAddress)
		}
	}
	return ""
}

func digitalOceanPublicIPv6(droplet *digitalocean.Droplet) string {
	if droplet == nil {
		return ""
	}
	for _, network := range droplet.Networks.V6 {
		if strings.EqualFold(strings.TrimSpace(network.Type), "public") {
			return strings.TrimSpace(network.IPAddress)
		}
	}
	return ""
}

func waitContextOrDelay(ctx context.Context, delay time.Duration) error {
	if ctx == nil {
		if delay > 0 {
			time.Sleep(delay)
		}
		return nil
	}

	if delay <= 0 {
		select {
		case <-ctx.Done():
			return normalizeExecutionStopError(ctx.Err())
		default:
			return nil
		}
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return normalizeExecutionStopError(ctx.Err())
	case <-timer.C:
		return nil
	}
}
