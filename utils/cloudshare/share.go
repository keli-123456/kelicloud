package cloudshare

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/komari-monitor/komari/database"
	"github.com/komari-monitor/komari/database/models"
	awscloud "github.com/komari-monitor/komari/utils/cloudprovider/aws"
	"github.com/komari-monitor/komari/utils/cloudprovider/digitalocean"
	linodecloud "github.com/komari-monitor/komari/utils/cloudprovider/linode"
)

const (
	ProviderDigitalOcean = "digitalocean"
	ProviderLinode       = "linode"
	ProviderAWS          = "aws"

	ResourceTypeDroplet   = "droplet"
	ResourceTypeInstance  = "instance"
	ResourceTypeEC2       = "ec2"
	ResourceTypeLightsail = "lightsail"
)

var (
	ErrInvalidReference      = errors.New("invalid cloud share reference")
	ErrUnsupportedCapability = errors.New("requested share capability is not available for this instance")
	ErrInstanceNotFound      = errors.New("cloud instance not found")
	ErrCredentialNotFound    = errors.New("cloud credential not found")
	ErrProviderNotConfigured = errors.New("cloud provider is not configured")
)

type AdminResourceState struct {
	Provider              string `json:"provider"`
	ResourceType          string `json:"resource_type"`
	ResourceID            string `json:"resource_id"`
	ResourceName          string `json:"resource_name"`
	CredentialID          string `json:"credential_id,omitempty"`
	CredentialName        string `json:"credential_name,omitempty"`
	Region                string `json:"region,omitempty"`
	CanSharePassword      bool   `json:"can_share_password"`
	CanShareManagedSSHKey bool   `json:"can_share_managed_ssh_key"`
}

type AdminShareView struct {
	Token                 string `json:"token,omitempty"`
	Provider              string `json:"provider"`
	ResourceType          string `json:"resource_type"`
	ResourceID            string `json:"resource_id"`
	ResourceName          string `json:"resource_name"`
	CredentialName        string `json:"credential_name,omitempty"`
	Region                string `json:"region,omitempty"`
	Title                 string `json:"title,omitempty"`
	Note                  string `json:"note,omitempty"`
	SharePassword         bool   `json:"share_password"`
	ShareManagedSSHKey    bool   `json:"share_managed_ssh_key"`
	CanSharePassword      bool   `json:"can_share_password"`
	CanShareManagedSSHKey bool   `json:"can_share_managed_ssh_key"`
	CreatedAt             string `json:"created_at,omitempty"`
	UpdatedAt             string `json:"updated_at,omitempty"`
}

type SharedRootPasswordView struct {
	Username     string `json:"username"`
	PasswordMode string `json:"password_mode,omitempty"`
	RootPassword string `json:"root_password"`
	UpdatedAt    string `json:"updated_at,omitempty"`
}

type LinodeInstanceShareDetail struct {
	Instance *linodecloud.Instance `json:"instance"`
	Disks    []linodecloud.Disk    `json:"disks"`
	Configs  []linodecloud.Config  `json:"configs"`
	Backups  *linodecloud.Backups  `json:"backups,omitempty"`
}

type PublicShareView struct {
	Token              string                                  `json:"token"`
	Provider           string                                  `json:"provider"`
	ResourceType       string                                  `json:"resource_type"`
	ResourceID         string                                  `json:"resource_id"`
	ResourceName       string                                  `json:"resource_name"`
	CredentialName     string                                  `json:"credential_name,omitempty"`
	Region             string                                  `json:"region,omitempty"`
	Title              string                                  `json:"title,omitempty"`
	Note               string                                  `json:"note,omitempty"`
	SharePassword      bool                                    `json:"share_password"`
	ShareManagedSSHKey bool                                    `json:"share_managed_ssh_key"`
	CreatedAt          string                                  `json:"created_at,omitempty"`
	UpdatedAt          string                                  `json:"updated_at,omitempty"`
	Detail             interface{}                             `json:"detail"`
	RootPassword       *SharedRootPasswordView                 `json:"root_password,omitempty"`
	ManagedSSHKey      *digitalocean.ManagedSSHKeyMaterialView `json:"managed_ssh_key,omitempty"`
}

func NormalizeReference(provider, resourceType, resourceID string) (string, string, string, error) {
	provider = strings.ToLower(strings.TrimSpace(provider))
	resourceType = strings.ToLower(strings.TrimSpace(resourceType))
	resourceID = strings.TrimSpace(resourceID)

	if provider == "" || resourceType == "" || resourceID == "" {
		return "", "", "", ErrInvalidReference
	}

	switch provider {
	case ProviderDigitalOcean:
		if resourceType != ResourceTypeDroplet {
			return "", "", "", ErrInvalidReference
		}
		id, err := strconv.Atoi(resourceID)
		if err != nil || id <= 0 {
			return "", "", "", ErrInvalidReference
		}
		resourceID = strconv.Itoa(id)
	case ProviderLinode:
		if resourceType != ResourceTypeInstance {
			return "", "", "", ErrInvalidReference
		}
		id, err := strconv.Atoi(resourceID)
		if err != nil || id <= 0 {
			return "", "", "", ErrInvalidReference
		}
		resourceID = strconv.Itoa(id)
	case ProviderAWS:
		if resourceType != ResourceTypeEC2 && resourceType != ResourceTypeLightsail {
			return "", "", "", ErrInvalidReference
		}
	default:
		return "", "", "", ErrInvalidReference
	}

	return provider, resourceType, resourceID, nil
}

func BuildAdminShareView(share *models.CloudInstanceShare, state *AdminResourceState) *AdminShareView {
	view := &AdminShareView{}
	if state != nil {
		view.Provider = state.Provider
		view.ResourceType = state.ResourceType
		view.ResourceID = state.ResourceID
		view.ResourceName = state.ResourceName
		view.CredentialName = state.CredentialName
		view.Region = state.Region
		view.CanSharePassword = state.CanSharePassword
		view.CanShareManagedSSHKey = state.CanShareManagedSSHKey
	}
	if share == nil {
		return view
	}

	view.Token = strings.TrimSpace(share.ShareToken)
	if view.Provider == "" {
		view.Provider = strings.TrimSpace(share.Provider)
	}
	if view.ResourceType == "" {
		view.ResourceType = strings.TrimSpace(share.ResourceType)
	}
	if view.ResourceID == "" {
		view.ResourceID = strings.TrimSpace(share.ResourceID)
	}
	if view.ResourceName == "" {
		view.ResourceName = strings.TrimSpace(share.ResourceName)
	}
	if view.Region == "" {
		view.Region = strings.TrimSpace(share.Region)
	}
	view.Title = strings.TrimSpace(share.Title)
	view.Note = strings.TrimSpace(share.Note)
	view.SharePassword = share.SharePassword
	view.ShareManagedSSHKey = share.ShareManagedSSHKey
	view.CreatedAt = formatTime(share.CreatedAt)
	view.UpdatedAt = formatTime(share.UpdatedAt)
	return view
}

func ResolveActiveResource(provider, resourceType, resourceID string) (*AdminResourceState, error) {
	return ResolveActiveResourceForTenant("", provider, resourceType, resourceID)
}

func ResolveActiveResourceForTenant(tenantID, provider, resourceType, resourceID string) (*AdminResourceState, error) {
	provider, resourceType, resourceID, err := NormalizeReference(provider, resourceType, resourceID)
	if err != nil {
		return nil, err
	}

	switch provider {
	case ProviderDigitalOcean:
		return resolveDigitalOceanActiveResource(tenantID, resourceID)
	case ProviderLinode:
		return resolveLinodeActiveResource(tenantID, resourceID)
	case ProviderAWS:
		if resourceType == ResourceTypeEC2 {
			return resolveAWSEC2ActiveResource(tenantID, resourceID)
		}
		return resolveAWSLightsailActiveResource(tenantID, resourceID)
	default:
		return nil, ErrInvalidReference
	}
}

func ResolvePublicShare(share *models.CloudInstanceShare) (*PublicShareView, error) {
	if share == nil {
		return nil, ErrInstanceNotFound
	}

	provider, resourceType, resourceID, err := NormalizeReference(share.Provider, share.ResourceType, share.ResourceID)
	if err != nil {
		return nil, err
	}

	switch provider {
	case ProviderDigitalOcean:
		return resolvePublicDigitalOceanShare(share, resourceID)
	case ProviderLinode:
		return resolvePublicLinodeShare(share, resourceID)
	case ProviderAWS:
		if resourceType == ResourceTypeEC2 {
			return resolvePublicAWSEC2Share(share, resourceID)
		}
		return resolvePublicAWSLightsailShare(share, resourceID)
	default:
		return nil, ErrInvalidReference
	}
}

func resolveDigitalOceanActiveResource(tenantID, resourceID string) (*AdminResourceState, error) {
	addition, err := loadDigitalOceanAddition(tenantID)
	if err != nil {
		return nil, err
	}

	token := addition.ActiveToken()
	if token == nil {
		return nil, ErrCredentialNotFound
	}

	client, err := digitalocean.NewClientFromToken(token.Token)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	droplet, err := findDigitalOceanDroplet(ctx, client, resourceID)
	if err != nil {
		return nil, err
	}

	dropletID, _ := strconv.Atoi(resourceID)
	return &AdminResourceState{
		Provider:              ProviderDigitalOcean,
		ResourceType:          ResourceTypeDroplet,
		ResourceID:            resourceID,
		ResourceName:          strings.TrimSpace(droplet.Name),
		CredentialID:          token.ID,
		CredentialName:        token.Name,
		CanSharePassword:      token.HasSavedDropletPassword(dropletID),
		CanShareManagedSSHKey: addition.HasManagedSSHKeyMaterial(),
	}, nil
}

func resolveLinodeActiveResource(tenantID, resourceID string) (*AdminResourceState, error) {
	addition, err := loadLinodeAddition(tenantID)
	if err != nil {
		return nil, err
	}

	token := addition.ActiveToken()
	if token == nil {
		return nil, ErrCredentialNotFound
	}

	client, err := linodecloud.NewClientFromToken(token.Token)
	if err != nil {
		return nil, err
	}

	instanceID, _ := strconv.Atoi(resourceID)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	instance, err := client.GetInstance(ctx, instanceID)
	if err != nil {
		return nil, err
	}

	return &AdminResourceState{
		Provider:              ProviderLinode,
		ResourceType:          ResourceTypeInstance,
		ResourceID:            resourceID,
		ResourceName:          strings.TrimSpace(instance.Label),
		CredentialID:          token.ID,
		CredentialName:        token.Name,
		CanSharePassword:      token.HasSavedInstancePassword(instanceID),
		CanShareManagedSSHKey: false,
	}, nil
}

func resolveAWSEC2ActiveResource(tenantID, resourceID string) (*AdminResourceState, error) {
	addition, err := loadAWSAddition(tenantID)
	if err != nil {
		return nil, err
	}

	credential := addition.ActiveCredential()
	if credential == nil {
		return nil, ErrCredentialNotFound
	}

	region := strings.TrimSpace(addition.ActiveRegion)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	instance, err := awscloud.GetInstance(ctx, credential, region, resourceID)
	if err != nil {
		return nil, err
	}

	name := strings.TrimSpace(instance.Name)
	if name == "" {
		name = strings.TrimSpace(instance.InstanceID)
	}

	return &AdminResourceState{
		Provider:              ProviderAWS,
		ResourceType:          ResourceTypeEC2,
		ResourceID:            resourceID,
		ResourceName:          name,
		CredentialID:          credential.ID,
		CredentialName:        credential.Name,
		Region:                region,
		CanSharePassword:      false,
		CanShareManagedSSHKey: false,
	}, nil
}

func resolveAWSLightsailActiveResource(tenantID, resourceID string) (*AdminResourceState, error) {
	addition, err := loadAWSAddition(tenantID)
	if err != nil {
		return nil, err
	}

	credential := addition.ActiveCredential()
	if credential == nil {
		return nil, ErrCredentialNotFound
	}

	region := strings.TrimSpace(addition.ActiveRegion)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	detail, err := awscloud.GetLightsailInstanceDetail(ctx, credential, region, resourceID)
	if err != nil {
		return nil, err
	}

	return &AdminResourceState{
		Provider:              ProviderAWS,
		ResourceType:          ResourceTypeLightsail,
		ResourceID:            resourceID,
		ResourceName:          strings.TrimSpace(detail.Instance.Name),
		CredentialID:          credential.ID,
		CredentialName:        credential.Name,
		Region:                region,
		CanSharePassword:      false,
		CanShareManagedSSHKey: false,
	}, nil
}

func resolvePublicDigitalOceanShare(share *models.CloudInstanceShare, resourceID string) (*PublicShareView, error) {
	tenantID, err := resolveShareTenantID(share)
	if err != nil {
		return nil, err
	}

	addition, err := loadDigitalOceanAddition(tenantID)
	if err != nil {
		return nil, err
	}

	token := addition.FindToken(share.CredentialID)
	if token == nil {
		return nil, ErrCredentialNotFound
	}

	client, err := digitalocean.NewClientFromToken(token.Token)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	droplet, err := findDigitalOceanDroplet(ctx, client, resourceID)
	if err != nil {
		return nil, err
	}

	view := &PublicShareView{
		Token:              strings.TrimSpace(share.ShareToken),
		Provider:           ProviderDigitalOcean,
		ResourceType:       ResourceTypeDroplet,
		ResourceID:         resourceID,
		ResourceName:       firstNonEmpty(strings.TrimSpace(droplet.Name), strings.TrimSpace(share.ResourceName), resourceID),
		CredentialName:     token.Name,
		Title:              strings.TrimSpace(share.Title),
		Note:               strings.TrimSpace(share.Note),
		SharePassword:      share.SharePassword,
		ShareManagedSSHKey: share.ShareManagedSSHKey,
		CreatedAt:          formatTime(share.CreatedAt),
		UpdatedAt:          formatTime(share.UpdatedAt),
		Detail:             droplet,
	}

	if share.SharePassword {
		if dropletID, err := strconv.Atoi(resourceID); err == nil {
			if passwordView, err := token.RevealDropletPassword(dropletID); err == nil && passwordView != nil {
				view.RootPassword = &SharedRootPasswordView{
					Username:     firstNonEmpty(passwordView.Username, "root"),
					PasswordMode: passwordView.PasswordMode,
					RootPassword: passwordView.RootPassword,
					UpdatedAt:    passwordView.UpdatedAt,
				}
			}
		}
	}
	if share.ShareManagedSSHKey {
		view.ManagedSSHKey = addition.ManagedSSHKeyMaterialViewForToken(token)
	}

	return view, nil
}

func resolvePublicLinodeShare(share *models.CloudInstanceShare, resourceID string) (*PublicShareView, error) {
	tenantID, err := resolveShareTenantID(share)
	if err != nil {
		return nil, err
	}

	addition, err := loadLinodeAddition(tenantID)
	if err != nil {
		return nil, err
	}

	token := addition.FindToken(share.CredentialID)
	if token == nil {
		return nil, ErrCredentialNotFound
	}

	client, err := linodecloud.NewClientFromToken(token.Token)
	if err != nil {
		return nil, err
	}

	instanceID, _ := strconv.Atoi(resourceID)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	instance, err := client.GetInstance(ctx, instanceID)
	if err != nil {
		return nil, err
	}
	disks, err := client.ListInstanceDisks(ctx, instanceID)
	if err != nil {
		return nil, err
	}
	configs, err := client.ListInstanceConfigs(ctx, instanceID)
	if err != nil {
		return nil, err
	}
	backups, err := client.GetInstanceBackups(ctx, instanceID)
	if err != nil {
		return nil, err
	}
	if disks == nil {
		disks = make([]linodecloud.Disk, 0)
	}
	if configs == nil {
		configs = make([]linodecloud.Config, 0)
	}

	view := &PublicShareView{
		Token:              strings.TrimSpace(share.ShareToken),
		Provider:           ProviderLinode,
		ResourceType:       ResourceTypeInstance,
		ResourceID:         resourceID,
		ResourceName:       firstNonEmpty(strings.TrimSpace(instance.Label), strings.TrimSpace(share.ResourceName), resourceID),
		CredentialName:     token.Name,
		Title:              strings.TrimSpace(share.Title),
		Note:               strings.TrimSpace(share.Note),
		SharePassword:      share.SharePassword,
		ShareManagedSSHKey: false,
		CreatedAt:          formatTime(share.CreatedAt),
		UpdatedAt:          formatTime(share.UpdatedAt),
		Detail: LinodeInstanceShareDetail{
			Instance: instance,
			Disks:    disks,
			Configs:  configs,
			Backups:  backups,
		},
	}

	if share.SharePassword {
		if passwordView, err := token.RevealInstancePassword(instanceID); err == nil && passwordView != nil {
			view.RootPassword = &SharedRootPasswordView{
				Username:     firstNonEmpty(passwordView.Username, "root"),
				PasswordMode: passwordView.PasswordMode,
				RootPassword: passwordView.RootPassword,
				UpdatedAt:    passwordView.UpdatedAt,
			}
		}
	}

	return view, nil
}

func resolvePublicAWSEC2Share(share *models.CloudInstanceShare, resourceID string) (*PublicShareView, error) {
	tenantID, err := resolveShareTenantID(share)
	if err != nil {
		return nil, err
	}

	addition, err := loadAWSAddition(tenantID)
	if err != nil {
		return nil, err
	}

	credential := addition.FindCredential(share.CredentialID)
	if credential == nil {
		return nil, ErrCredentialNotFound
	}

	region := firstNonEmpty(strings.TrimSpace(share.Region), strings.TrimSpace(credential.DefaultRegion), strings.TrimSpace(addition.ActiveRegion))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	detail, err := awscloud.GetInstanceDetail(ctx, credential, region, resourceID)
	if err != nil {
		return nil, err
	}

	name := firstNonEmpty(strings.TrimSpace(detail.Instance.Name), strings.TrimSpace(detail.Instance.InstanceID), strings.TrimSpace(share.ResourceName), resourceID)
	return &PublicShareView{
		Token:              strings.TrimSpace(share.ShareToken),
		Provider:           ProviderAWS,
		ResourceType:       ResourceTypeEC2,
		ResourceID:         resourceID,
		ResourceName:       name,
		CredentialName:     credential.Name,
		Region:             region,
		Title:              strings.TrimSpace(share.Title),
		Note:               strings.TrimSpace(share.Note),
		SharePassword:      false,
		ShareManagedSSHKey: false,
		CreatedAt:          formatTime(share.CreatedAt),
		UpdatedAt:          formatTime(share.UpdatedAt),
		Detail:             detail,
	}, nil
}

func resolvePublicAWSLightsailShare(share *models.CloudInstanceShare, resourceID string) (*PublicShareView, error) {
	tenantID, err := resolveShareTenantID(share)
	if err != nil {
		return nil, err
	}

	addition, err := loadAWSAddition(tenantID)
	if err != nil {
		return nil, err
	}

	credential := addition.FindCredential(share.CredentialID)
	if credential == nil {
		return nil, ErrCredentialNotFound
	}

	region := firstNonEmpty(strings.TrimSpace(share.Region), strings.TrimSpace(credential.DefaultRegion), strings.TrimSpace(addition.ActiveRegion))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	detail, err := awscloud.GetLightsailInstanceDetail(ctx, credential, region, resourceID)
	if err != nil {
		return nil, err
	}

	name := firstNonEmpty(strings.TrimSpace(detail.Instance.Name), strings.TrimSpace(share.ResourceName), resourceID)
	return &PublicShareView{
		Token:              strings.TrimSpace(share.ShareToken),
		Provider:           ProviderAWS,
		ResourceType:       ResourceTypeLightsail,
		ResourceID:         resourceID,
		ResourceName:       name,
		CredentialName:     credential.Name,
		Region:             region,
		Title:              strings.TrimSpace(share.Title),
		Note:               strings.TrimSpace(share.Note),
		SharePassword:      false,
		ShareManagedSSHKey: false,
		CreatedAt:          formatTime(share.CreatedAt),
		UpdatedAt:          formatTime(share.UpdatedAt),
		Detail:             detail,
	}, nil
}

func loadDigitalOceanAddition(tenantID string) (*digitalocean.Addition, error) {
	config, err := database.GetCloudProviderConfigByTenantAndName(tenantID, ProviderDigitalOcean)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrProviderNotConfigured, ProviderDigitalOcean)
	}

	addition := &digitalocean.Addition{}
	raw := strings.TrimSpace(config.Addition)
	if raw == "" {
		raw = "{}"
	}
	if err := json.Unmarshal([]byte(raw), addition); err != nil {
		return nil, err
	}
	addition.Normalize()
	return addition, nil
}

func loadLinodeAddition(tenantID string) (*linodecloud.Addition, error) {
	config, err := database.GetCloudProviderConfigByTenantAndName(tenantID, ProviderLinode)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrProviderNotConfigured, ProviderLinode)
	}

	addition := &linodecloud.Addition{}
	raw := strings.TrimSpace(config.Addition)
	if raw == "" {
		raw = "{}"
	}
	if err := json.Unmarshal([]byte(raw), addition); err != nil {
		return nil, err
	}
	addition.Normalize()
	return addition, nil
}

func loadAWSAddition(tenantID string) (*awscloud.Addition, error) {
	config, err := database.GetCloudProviderConfigByTenantAndName(tenantID, ProviderAWS)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrProviderNotConfigured, ProviderAWS)
	}

	addition := &awscloud.Addition{}
	raw := strings.TrimSpace(config.Addition)
	if raw == "" {
		raw = "{}"
	}
	if err := json.Unmarshal([]byte(raw), addition); err != nil {
		return nil, err
	}
	addition.Normalize()
	return addition, nil
}

func findDigitalOceanDroplet(ctx context.Context, client *digitalocean.Client, resourceID string) (*digitalocean.Droplet, error) {
	dropletID, err := strconv.Atoi(resourceID)
	if err != nil || dropletID <= 0 {
		return nil, ErrInvalidReference
	}

	droplets, err := client.ListDroplets(ctx)
	if err != nil {
		return nil, err
	}

	for index := range droplets {
		if droplets[index].ID == dropletID {
			return &droplets[index], nil
		}
	}
	return nil, ErrInstanceNotFound
}

func resolveShareTenantID(share *models.CloudInstanceShare) (string, error) {
	if share == nil {
		return "", ErrInstanceNotFound
	}
	tenantID := strings.TrimSpace(share.TenantID)
	if tenantID != "" {
		return tenantID, nil
	}
	return database.GetDefaultTenantID()
}

func formatTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339)
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
