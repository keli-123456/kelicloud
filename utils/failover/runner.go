package failover

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/gorilla/websocket"
	"github.com/komari-monitor/komari/common"
	"github.com/komari-monitor/komari/config"
	clientdb "github.com/komari-monitor/komari/database/clients"
	clipboarddb "github.com/komari-monitor/komari/database/clipboard"
	failoverdb "github.com/komari-monitor/komari/database/failover"
	"github.com/komari-monitor/komari/database/models"
	"github.com/komari-monitor/komari/database/tasks"
	"github.com/komari-monitor/komari/utils"
	awscloud "github.com/komari-monitor/komari/utils/cloudprovider/aws"
	azurecloud "github.com/komari-monitor/komari/utils/cloudprovider/azure"
	"github.com/komari-monitor/komari/utils/cloudprovider/digitalocean"
	linodecloud "github.com/komari-monitor/komari/utils/cloudprovider/linode"
	vultrcloud "github.com/komari-monitor/komari/utils/cloudprovider/vultr"
	"github.com/komari-monitor/komari/ws"
	"gorm.io/gorm"
)

var (
	runningTasksMu   sync.Mutex
	runningTasks     = map[uint]struct{}{}
	runningTargetMu  sync.Mutex
	runningTargets   = map[string]uint{}
	executionStopMu  sync.Mutex
	executionCancels = map[uint]context.CancelFunc{}

	awsFailoverLoadCredential                  = loadAWSCredential
	awsFailoverGetEC2InstanceDetail            = awscloud.GetInstanceDetail
	awsFailoverGetLightsailDetail              = awscloud.GetLightsailInstanceDetail
	awsFailoverResolveCurrentInstanceByAddress = resolveAWSCurrentInstanceByAddress
	failoverDNSApplyFunc                       = applyDNSRecord
	failoverDNSVerifyFunc                      = verifyDNSRecord
)

const interruptedExecutionMessage = "failover execution was interrupted before completion"

const (
	scheduledAutomaticExecutionGlobalConcurrency  = 32
	scheduledAutomaticExecutionPerUserConcurrency = 4
)

var errExecutionStopped = errors.New("failover execution stopped by user")

var scheduledAutomaticExecutionLimiter = newScheduledAutomaticExecutionLimiter(
	scheduledAutomaticExecutionGlobalConcurrency,
	scheduledAutomaticExecutionPerUserConcurrency,
)

type scheduledAutomaticExecutionLimiterState struct {
	globalSlots chan struct{}
	perUser     int
	mu          sync.Mutex
	userSlots   map[string]chan struct{}
}

func newScheduledAutomaticExecutionLimiter(global, perUser int) *scheduledAutomaticExecutionLimiterState {
	if global <= 0 {
		global = scheduledAutomaticExecutionGlobalConcurrency
	}
	if perUser <= 0 {
		perUser = scheduledAutomaticExecutionPerUserConcurrency
	}
	return &scheduledAutomaticExecutionLimiterState{
		globalSlots: make(chan struct{}, global),
		perUser:     perUser,
		userSlots:   map[string]chan struct{}{},
	}
}

func (limiter *scheduledAutomaticExecutionLimiterState) slotForUser(userUUID string) chan struct{} {
	userUUID = strings.TrimSpace(userUUID)
	if userUUID == "" {
		userUUID = "unknown"
	}

	limiter.mu.Lock()
	defer limiter.mu.Unlock()
	if slot := limiter.userSlots[userUUID]; slot != nil {
		return slot
	}
	slot := make(chan struct{}, limiter.perUser)
	limiter.userSlots[userUUID] = slot
	return slot
}

func (limiter *scheduledAutomaticExecutionLimiterState) tryAcquire(userUUID string) (func(), bool) {
	if limiter == nil {
		return func() {}, true
	}

	select {
	case limiter.globalSlots <- struct{}{}:
	default:
		return nil, false
	}

	userSlot := limiter.slotForUser(userUUID)
	select {
	case userSlot <- struct{}{}:
		var once sync.Once
		return func() {
			once.Do(func() {
				<-userSlot
				<-limiter.globalSlots
			})
		}, true
	default:
		<-limiter.globalSlots
		return nil, false
	}
}

func tryAcquireScheduledAutomaticExecutionSlot(userUUID string) (func(), bool) {
	return scheduledAutomaticExecutionLimiter.tryAcquire(userUUID)
}

type executionRunner struct {
	task      models.FailoverTask
	execution *models.FailoverExecution
	ctx       context.Context
	startedAt time.Time
	stepSort  int
	attempts  []map[string]interface{}
	succeeded bool
}

type actionOutcome struct {
	IPv4              string
	IPv6              string
	TargetClientUUID  string
	NewClientUUID     string
	AutoConnectGroup  string
	NewInstanceRef    map[string]interface{}
	NewAddresses      map[string]interface{}
	OldInstanceRef    map[string]interface{}
	CleanupLabel      string
	Cleanup           func(context.Context) error
	CleanupAssessment *cleanupAssessment
	RollbackLabel     string
	Rollback          func(context.Context) error
}

type currentInstanceCleanup struct {
	Ref        map[string]interface{}
	Addresses  map[string]interface{}
	Label      string
	Cleanup    func(context.Context) error
	Missing    bool
	Assessment *cleanupAssessment
}

type cleanupAssessment struct {
	Status      string
	StepStatus  string
	StepMessage string
	Result      map[string]interface{}
}

type provisionCleanupError struct {
	Provider     string
	ResourceType string
	ResourceID   string
	CleanupLabel string
	CleanupError error
	Cause        error
}

func (e *provisionCleanupError) Error() string {
	if e == nil {
		return ""
	}

	resourceLabel := strings.TrimSpace(e.ResourceType)
	if resourceLabel == "" {
		resourceLabel = "instance"
	}
	resourceID := strings.TrimSpace(e.ResourceID)
	resourceRef := resourceLabel
	if resourceID != "" {
		resourceRef = resourceLabel + " " + resourceID
	}

	if e.CleanupError != nil {
		return fmt.Sprintf(
			"failed to save the root password for new %s; automatic cleanup failed and the resource may still exist: %v; cleanup error: %v",
			resourceRef,
			e.Cause,
			e.CleanupError,
		)
	}

	return fmt.Sprintf(
		"failed to save the root password for new %s; deleted the resource automatically to avoid leaving an unmanaged instance: %v",
		resourceRef,
		e.Cause,
	)
}

func (e *provisionCleanupError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func (e *provisionCleanupError) cleanupStatus() string {
	if e == nil {
		return ""
	}
	if e.CleanupError != nil {
		return "delete_failed"
	}
	return "deleted"
}

func (e *provisionCleanupError) stepMessage() string {
	if e == nil {
		return ""
	}
	if e.CleanupError != nil {
		return "new instance root password could not be saved and automatic cleanup failed"
	}
	return "new instance root password could not be saved; deleted the new instance automatically"
}

func (e *provisionCleanupError) detail() map[string]interface{} {
	if e == nil {
		return nil
	}

	detail := map[string]interface{}{
		"provider":       strings.TrimSpace(e.Provider),
		"resource_type":  strings.TrimSpace(e.ResourceType),
		"resource_id":    strings.TrimSpace(e.ResourceID),
		"failure_class":  "post_provision_error",
		"cleanup_status": e.cleanupStatus(),
		"label":          strings.TrimSpace(e.CleanupLabel),
	}
	if e.Cause != nil {
		detail["error"] = e.Cause.Error()
	}
	if e.CleanupError != nil {
		detail["cleanup_error"] = e.CleanupError.Error()
	}
	return detail
}

type blockedOutletError struct {
	ClientUUID string
	Status     string
	Message    string
}

func isDigitalOceanNotFoundError(err error) bool {
	var apiErr *digitalocean.APIError
	return errors.As(err, &apiErr) && apiErr != nil && apiErr.StatusCode == 404
}

func isLinodeNotFoundError(err error) bool {
	var apiErr *linodecloud.APIError
	return errors.As(err, &apiErr) && apiErr != nil && apiErr.StatusCode == 404
}

func isVultrNotFoundError(err error) bool {
	var apiErr *vultrcloud.APIError
	return errors.As(err, &apiErr) && apiErr != nil && apiErr.StatusCode == 404
}

func isAzureNotFoundError(err error) bool {
	var apiErr *azurecloud.APIError
	if !errors.As(err, &apiErr) || apiErr == nil {
		return false
	}
	if apiErr.StatusCode == 404 {
		return true
	}
	code := strings.ToLower(strings.TrimSpace(apiErr.Code))
	return code == "resourcenotfound" || code == "notfound" || code == "resourcegroupnotfound"
}

func isAWSResourceNotFoundError(service string, err error) bool {
	return isAWSRebindTargetMissingError(service, err)
}

func (e *blockedOutletError) Error() string {
	if e == nil {
		return "new outlet connectivity validation failed"
	}
	parts := make([]string, 0, 3)
	if strings.TrimSpace(e.ClientUUID) != "" {
		parts = append(parts, "client "+strings.TrimSpace(e.ClientUUID))
	}
	if strings.TrimSpace(e.Status) != "" {
		parts = append(parts, "status="+strings.TrimSpace(e.Status))
	}
	if strings.TrimSpace(e.Message) != "" {
		parts = append(parts, strings.TrimSpace(e.Message))
	}
	if len(parts) == 0 {
		return "new outlet connectivity validation failed"
	}
	return "new outlet connectivity validation failed: " + strings.Join(parts, ", ")
}

type planExecutionFailureDecision struct {
	Class          string
	RetrySameEntry bool
	Cooldown       time.Duration
}

type noPlanFallbackError struct {
	err error
}

type duplicateTargetRunError struct {
	TaskID       uint
	ActiveTaskID uint
	TargetKey    string
}

func (e *noPlanFallbackError) Error() string {
	if e == nil || e.err == nil {
		return ""
	}
	return e.err.Error()
}

func (e *noPlanFallbackError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.err
}

func (e *duplicateTargetRunError) Error() string {
	if e == nil {
		return "another failover task is already handling the same outlet"
	}
	if e.ActiveTaskID > 0 {
		return fmt.Sprintf(
			"failover task %d is already handling the same outlet target for task %d",
			e.ActiveTaskID,
			e.TaskID,
		)
	}
	if strings.TrimSpace(e.TargetKey) != "" {
		return "another failover task is already handling outlet target " + strings.TrimSpace(e.TargetKey)
	}
	return "another failover task is already handling the same outlet"
}

const (
	blockedOutletRetryBaseBackoff = 15 * time.Second
)

const (
	cleanupClassificationNotRequested                = "not_requested"
	cleanupClassificationInstanceDeleted             = "instance_deleted"
	cleanupClassificationInstanceMissing             = "instance_missing"
	cleanupClassificationProviderEntryMissing        = "provider_entry_missing"
	cleanupClassificationProviderEntryUnhealthy      = "provider_entry_unhealthy"
	cleanupClassificationCleanupStatusUnknown        = "cleanup_status_unknown"
	cleanupClassificationInstanceConfirmedDeleteFail = "instance_confirmed_delete_failed"

	cleanupStepMessageInstanceMissing        = "old instance already missing; no cleanup required"
	cleanupStepMessageProviderEntryMissing   = "original cloud credential was deleted; manual cleanup review required"
	cleanupStepMessageProviderEntryUnhealthy = "original cloud credential is unavailable; manual cleanup review required"
	cleanupStepMessageCleanupStatusUnknown   = "old instance cleanup could not be verified; manual review required"
)

type awsProvisionPayload struct {
	Service             string         `json:"service,omitempty"`
	Region              string         `json:"region,omitempty"`
	Name                string         `json:"name,omitempty"`
	ImageID             string         `json:"image_id,omitempty"`
	InstanceType        string         `json:"instance_type,omitempty"`
	KeyName             string         `json:"key_name,omitempty"`
	SubnetID            string         `json:"subnet_id,omitempty"`
	SecurityGroupIDs    []string       `json:"security_group_ids,omitempty"`
	UserData            string         `json:"user_data,omitempty"`
	AssignPublicIP      bool           `json:"assign_public_ip"`
	AssignIPv6          bool           `json:"assign_ipv6"`
	AllowAllTraffic     bool           `json:"allow_all_traffic"`
	Tags                []awscloud.Tag `json:"tags,omitempty"`
	RootPasswordMode    string         `json:"root_password_mode,omitempty"`
	RootPassword        string         `json:"root_password,omitempty"`
	AvailabilityZone    string         `json:"availability_zone,omitempty"`
	BlueprintID         string         `json:"blueprint_id,omitempty"`
	BundleID            string         `json:"bundle_id,omitempty"`
	KeyPairName         string         `json:"key_pair_name,omitempty"`
	IPAddressType       string         `json:"ip_address_type,omitempty"`
	CleanupInstanceID   string         `json:"cleanup_instance_id,omitempty"`
	CleanupInstanceName string         `json:"cleanup_instance_name,omitempty"`
}

type digitalOceanProvisionPayload struct {
	Name             string   `json:"name,omitempty"`
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
	CleanupDropletID int      `json:"cleanup_droplet_id,omitempty"`
}

type azureProvisionPayload struct {
	Name                 string                    `json:"name,omitempty"`
	ResourceGroup        string                    `json:"resource_group,omitempty"`
	Region               string                    `json:"region,omitempty"`
	Location             string                    `json:"location,omitempty"`
	Size                 string                    `json:"size,omitempty"`
	AdminUsername        string                    `json:"admin_username,omitempty"`
	AdminPassword        string                    `json:"admin_password,omitempty"`
	SSHPublicKey         string                    `json:"ssh_public_key,omitempty"`
	UserData             string                    `json:"user_data,omitempty"`
	PublicIP             bool                      `json:"public_ip"`
	AssignIPv6           bool                      `json:"assign_ipv6"`
	Image                azurecloud.ImageReference `json:"image"`
	RootPasswordMode     string                    `json:"root_password_mode,omitempty"`
	RootPassword         string                    `json:"root_password,omitempty"`
	CleanupInstanceID    string                    `json:"cleanup_instance_id,omitempty"`
	CleanupResourceGroup string                    `json:"cleanup_resource_group,omitempty"`
	CleanupName          string                    `json:"cleanup_name,omitempty"`
}

type linodeProvisionPayload struct {
	Label             string   `json:"label,omitempty"`
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
	CleanupInstanceID int      `json:"cleanup_instance_id,omitempty"`
}

type vultrProvisionPayload struct {
	Label             string   `json:"label,omitempty"`
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
	CleanupInstanceID string   `json:"cleanup_instance_id,omitempty"`
}

type awsRebindPayload struct {
	Service          string         `json:"service,omitempty"`
	Region           string         `json:"region,omitempty"`
	InstanceID       string         `json:"instance_id,omitempty"`
	PrivateIP        string         `json:"private_ip,omitempty"`
	InstanceName     string         `json:"instance_name,omitempty"`
	StaticIPName     string         `json:"static_ip_name,omitempty"`
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
}

func normalizeAWSFailoverService(service string) string {
	switch strings.ToLower(strings.TrimSpace(service)) {
	case "lightsail":
		return "lightsail"
	default:
		return "ec2"
	}
}

func normalizeAWSRebindPayload(payload awsRebindPayload) awsRebindPayload {
	payload.Service = normalizeAWSFailoverService(payload.Service)
	payload.Region = strings.TrimSpace(payload.Region)
	payload.InstanceID = strings.TrimSpace(payload.InstanceID)
	payload.PrivateIP = strings.TrimSpace(payload.PrivateIP)
	payload.InstanceName = strings.TrimSpace(payload.InstanceName)
	payload.StaticIPName = strings.TrimSpace(payload.StaticIPName)
	payload.Name = strings.TrimSpace(payload.Name)
	payload.ImageID = strings.TrimSpace(payload.ImageID)
	payload.InstanceType = strings.TrimSpace(payload.InstanceType)
	payload.KeyName = strings.TrimSpace(payload.KeyName)
	payload.SubnetID = strings.TrimSpace(payload.SubnetID)
	payload.SecurityGroupIDs = trimStrings(payload.SecurityGroupIDs)
	payload.UserData = strings.TrimSpace(payload.UserData)
	payload.RootPasswordMode = strings.TrimSpace(payload.RootPasswordMode)
	payload.RootPassword = strings.TrimSpace(payload.RootPassword)
	payload.AvailabilityZone = strings.TrimSpace(payload.AvailabilityZone)
	payload.BlueprintID = strings.TrimSpace(payload.BlueprintID)
	payload.BundleID = strings.TrimSpace(payload.BundleID)
	payload.KeyPairName = strings.TrimSpace(payload.KeyPairName)
	payload.IPAddressType = strings.TrimSpace(payload.IPAddressType)
	return payload
}

func resolveAWSRebindPayload(task models.FailoverTask, payload awsRebindPayload, entryID string) (awsRebindPayload, bool) {
	payload = normalizeAWSRebindPayload(payload)
	if payload.Service == "lightsail" {
		if payload.InstanceName != "" {
			return payload, true
		}
	} else if payload.InstanceID != "" {
		return payload, true
	}

	currentRef := parseJSONMap(task.CurrentInstanceRef)
	if len(currentRef) == 0 {
		return payload, false
	}
	if !strings.EqualFold(strings.TrimSpace(stringMapValue(currentRef, "provider")), "aws") {
		return payload, false
	}

	refService := normalizeAWSFailoverService(firstNonEmpty(stringMapValue(currentRef, "service"), "ec2"))
	if refService != payload.Service {
		return payload, false
	}

	if trackedEntryID := strings.TrimSpace(providerEntryIDFromRef(currentRef)); trackedEntryID != "" && strings.TrimSpace(entryID) != "" && trackedEntryID != strings.TrimSpace(entryID) {
		return payload, false
	}

	if payload.Region == "" {
		payload.Region = strings.TrimSpace(stringMapValue(currentRef, "region"))
	}

	if payload.Service == "lightsail" {
		payload.InstanceName = strings.TrimSpace(stringMapValue(currentRef, "instance_name"))
		if payload.InstanceName == "" {
			return payload, false
		}
		return payload, true
	}

	payload.InstanceID = strings.TrimSpace(stringMapValue(currentRef, "instance_id"))
	if payload.InstanceID == "" {
		return payload, false
	}
	return payload, true
}

func resolveAWSFailoverRegion(region string, addition *awscloud.Addition, credential *awscloud.CredentialRecord) string {
	region = strings.TrimSpace(region)
	if region != "" {
		return region
	}
	if addition != nil {
		if activeRegion := strings.TrimSpace(addition.ActiveRegion); activeRegion != "" {
			return activeRegion
		}
	}
	if credential != nil {
		if defaultRegion := strings.TrimSpace(credential.DefaultRegion); defaultRegion != "" {
			return defaultRegion
		}
	}
	return awscloud.DefaultRegion
}

func awsRebindProvisionPayload(payload awsRebindPayload) awsProvisionPayload {
	payload = normalizeAWSRebindPayload(payload)
	return awsProvisionPayload{
		Service:          payload.Service,
		Region:           payload.Region,
		Name:             payload.Name,
		ImageID:          payload.ImageID,
		InstanceType:     payload.InstanceType,
		KeyName:          payload.KeyName,
		SubnetID:         payload.SubnetID,
		SecurityGroupIDs: trimStrings(payload.SecurityGroupIDs),
		UserData:         payload.UserData,
		AssignPublicIP:   payload.AssignPublicIP,
		AssignIPv6:       payload.AssignIPv6,
		AllowAllTraffic:  payload.AllowAllTraffic,
		Tags:             payload.Tags,
		RootPasswordMode: payload.RootPasswordMode,
		RootPassword:     payload.RootPassword,
		AvailabilityZone: payload.AvailabilityZone,
		BlueprintID:      payload.BlueprintID,
		BundleID:         payload.BundleID,
		KeyPairName:      payload.KeyPairName,
		IPAddressType:    payload.IPAddressType,
	}
}

func validateAWSProvisionPayload(payload awsProvisionPayload) error {
	service := normalizeAWSFailoverService(payload.Service)
	region := strings.TrimSpace(payload.Region)
	if region == "" {
		return errors.New("region is required")
	}

	if service == "lightsail" {
		if strings.TrimSpace(payload.AvailabilityZone) == "" {
			return errors.New("availability_zone is required")
		}
		if strings.TrimSpace(payload.BlueprintID) == "" {
			return errors.New("blueprint_id is required")
		}
		if strings.TrimSpace(payload.BundleID) == "" {
			return errors.New("bundle_id is required")
		}
		return nil
	}

	if strings.TrimSpace(payload.ImageID) == "" {
		return errors.New("image_id is required")
	}
	if strings.TrimSpace(payload.InstanceType) == "" {
		return errors.New("instance_type is required")
	}
	return nil
}

func buildAWSProvisionFallbackPlan(plan models.FailoverPlan, payload awsRebindPayload, reason string) (models.FailoverPlan, string, string, error) {
	provisionPayload := awsRebindProvisionPayload(payload)
	if err := validateAWSProvisionPayload(provisionPayload); err != nil {
		return models.FailoverPlan{}, "", "", &noPlanFallbackError{
			err: fmt.Errorf("aws failover plan could not provision a replacement under the same credential: %w", err),
		}
	}

	encodedPayload, err := json.Marshal(provisionPayload)
	if err != nil {
		return models.FailoverPlan{}, "", "", fmt.Errorf("failed to encode aws fallback provision payload: %w", err)
	}

	nextPlan := plan
	nextPlan.ActionType = models.FailoverActionProvisionInstance
	nextPlan.Payload = string(encodedPayload)
	return nextPlan, "provision_new_instance", strings.TrimSpace(reason), nil
}

func isAWSRebindTargetMissingError(service string, err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(strings.TrimSpace(err.Error()))
	if service == "lightsail" {
		return strings.Contains(message, "lightsail instance not found")
	}
	return strings.Contains(message, "instance not found") || strings.Contains(message, "invalidinstanceid.notfound")
}

func awsRebindTargetAllowsReuse(service, state string) bool {
	state = strings.ToLower(strings.TrimSpace(state))
	if state == "" {
		return true
	}
	switch normalizeAWSFailoverService(service) {
	case "lightsail":
		return state == "running" || state == "pending" || state == "starting"
	default:
		return state == "running" || state == "pending"
	}
}

func resolveTrackedAWSRebindPayload(task models.FailoverTask, payload awsRebindPayload, entryID string) (awsRebindPayload, bool, bool) {
	payload = normalizeAWSRebindPayload(payload)
	if payload.Service == "lightsail" {
		if payload.InstanceName != "" {
			return payload, true, false
		}
	} else if payload.InstanceID != "" {
		return payload, true, false
	}

	currentRef := parseJSONMap(task.CurrentInstanceRef)
	if len(currentRef) == 0 {
		return payload, false, false
	}
	if !strings.EqualFold(strings.TrimSpace(stringMapValue(currentRef, "provider")), "aws") {
		return payload, false, false
	}

	refService := normalizeAWSFailoverService(firstNonEmpty(stringMapValue(currentRef, "service"), "ec2"))
	if refService != payload.Service {
		return payload, false, false
	}

	if trackedEntryID := strings.TrimSpace(providerEntryIDFromRef(currentRef)); trackedEntryID != "" && strings.TrimSpace(entryID) != "" && trackedEntryID != strings.TrimSpace(entryID) {
		return payload, false, false
	}

	if region := strings.TrimSpace(stringMapValue(currentRef, "region")); region != "" {
		payload.Region = region
	}

	if payload.Service == "lightsail" {
		payload.InstanceName = strings.TrimSpace(stringMapValue(currentRef, "instance_name"))
		if payload.InstanceName == "" {
			return payload, false, false
		}
		return payload, true, true
	}

	payload.InstanceID = strings.TrimSpace(stringMapValue(currentRef, "instance_id"))
	if payload.InstanceID == "" {
		return payload, false, false
	}
	return payload, true, true
}

func buildAWSRebindExecutionPlan(plan models.FailoverPlan, payload awsRebindPayload) models.FailoverPlan {
	rebindPlan := plan
	rebindPlan.ActionType = models.FailoverActionRebindPublicIP
	rebindPlan.Payload = marshalJSON(normalizeAWSRebindPayload(payload))
	return rebindPlan
}

func resolveTaskCurrentAddress(task models.FailoverTask) (string, error) {
	address := strings.TrimSpace(task.CurrentAddress)
	if address != "" {
		return address, nil
	}

	clientUUID := strings.TrimSpace(task.WatchClientUUID)
	userUUID := strings.TrimSpace(task.UserID)
	if clientUUID == "" || userUUID == "" {
		return "", nil
	}

	client, err := clientdb.GetClientByUUIDForUser(clientUUID, userUUID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return "", nil
		}
		return "", err
	}

	return strings.TrimSpace(firstNonEmpty(client.IPv4, client.IPv6)), nil
}

func prepareTrackedAWSRebindExecutionPlan(
	ctx context.Context,
	task models.FailoverTask,
	plan models.FailoverPlan,
	payload awsRebindPayload,
	addition *awscloud.Addition,
	credential *awscloud.CredentialRecord,
) (models.FailoverPlan, string, string, bool, error) {
	resolvedPayload, hasTarget, usedTrackedRef := resolveTrackedAWSRebindPayload(task, payload, plan.ProviderEntryID)
	if !hasTarget {
		return models.FailoverPlan{}, "", "", false, nil
	}

	service := normalizeAWSFailoverService(resolvedPayload.Service)
	region := resolveAWSFailoverRegion(resolvedPayload.Region, addition, credential)
	resolvedPayload.Region = region

	switch service {
	case "lightsail":
		detail, err := awsFailoverGetLightsailDetail(contextOrBackground(ctx), credential, region, strings.TrimSpace(resolvedPayload.InstanceName))
		if err != nil {
			if isAWSRebindTargetMissingError(service, err) {
				return models.FailoverPlan{}, "", "", false, nil
			}
			return models.FailoverPlan{}, "", "", false, err
		}
		if !awsRebindTargetAllowsReuse(service, detail.Instance.State) {
			reason := fmt.Sprintf(
				"the tracked AWS Lightsail instance %s is %s, so failover will create a replacement instance",
				strings.TrimSpace(resolvedPayload.InstanceName),
				firstNonEmpty(strings.TrimSpace(detail.Instance.State), "not reusable"),
			)
			fallbackPlan, mode, fallbackReason, err := buildAWSProvisionFallbackPlan(plan, payload, reason)
			return fallbackPlan, mode, fallbackReason, true, err
		}

		reason := "the task already tracks an AWS Lightsail instance under the selected credential, so failover will only replace its public IP"
		if !usedTrackedRef {
			reason = "the selected AWS credential already targets a Lightsail instance, so failover will only replace its public IP"
		}
		return buildAWSRebindExecutionPlan(plan, resolvedPayload), "rebind_existing_instance", reason, true, nil
	default:
		detail, err := awsFailoverGetEC2InstanceDetail(contextOrBackground(ctx), credential, region, strings.TrimSpace(resolvedPayload.InstanceID))
		if err != nil {
			if isAWSRebindTargetMissingError(service, err) {
				return models.FailoverPlan{}, "", "", false, nil
			}
			return models.FailoverPlan{}, "", "", false, err
		}
		if !awsRebindTargetAllowsReuse(service, detail.Instance.State) {
			reason := fmt.Sprintf(
				"the tracked AWS EC2 instance %s is %s, so failover will create a replacement instance",
				strings.TrimSpace(resolvedPayload.InstanceID),
				firstNonEmpty(strings.TrimSpace(detail.Instance.State), "not reusable"),
			)
			fallbackPlan, mode, fallbackReason, err := buildAWSProvisionFallbackPlan(plan, payload, reason)
			return fallbackPlan, mode, fallbackReason, true, err
		}

		resolvedPayload.PrivateIP = firstNonEmpty(resolvedPayload.PrivateIP, strings.TrimSpace(detail.Instance.PrivateIP))
		reason := "the task already tracks an AWS EC2 instance under the selected credential, so failover will only replace its public IP"
		if !usedTrackedRef {
			reason = "the selected AWS credential already targets an EC2 instance, so failover will only replace its public IP"
		}
		return buildAWSRebindExecutionPlan(plan, resolvedPayload), "rebind_existing_instance", reason, true, nil
	}
}

func prepareAWSRebindExecutionPlan(ctx context.Context, task models.FailoverTask, plan models.FailoverPlan) (models.FailoverPlan, string, string, error) {
	payload, err := parseAWSExecutionPayload(plan)
	if err != nil {
		return models.FailoverPlan{}, "", "", err
	}

	addition, credential, err := awsFailoverLoadCredential(task.UserID, plan.ProviderEntryID)
	if err != nil {
		return models.FailoverPlan{}, "", "", err
	}
	payload.Region = resolveAWSFailoverRegion(payload.Region, addition, credential)

	if currentAddress, addressErr := resolveTaskCurrentAddress(task); addressErr != nil {
		return models.FailoverPlan{}, "", "", addressErr
	} else if currentAddress != "" {
		task.CurrentAddress = currentAddress
	}

	if trackedPlan, mode, reason, resolved, trackedErr := prepareTrackedAWSRebindExecutionPlan(ctx, task, plan, payload, addition, credential); trackedErr != nil {
		return models.FailoverPlan{}, "", "", trackedErr
	} else if resolved {
		return trackedPlan, mode, reason, nil
	}

	cleanup, err := awsFailoverResolveCurrentInstanceByAddress(ctx, task, plan, payload)
	if err != nil {
		return models.FailoverPlan{}, "", "", err
	}
	if cleanup == nil {
		return buildAWSProvisionFallbackPlan(
			plan,
			payload,
			"the selected AWS credential does not have an instance with the task's current IP, so failover will create a replacement instance",
		)
	}

	resolvedPayload := payload
	resolvedPayload.Region = firstNonEmpty(stringMapValue(cleanup.Ref, "region"), resolvedPayload.Region)
	resolvedPayload.Service = normalizeAWSFailoverService(firstNonEmpty(stringMapValue(cleanup.Ref, "service"), resolvedPayload.Service))
	if resolvedPayload.Service == "lightsail" {
		resolvedPayload.InstanceName = strings.TrimSpace(stringMapValue(cleanup.Ref, "instance_name"))
	} else {
		resolvedPayload.InstanceID = strings.TrimSpace(stringMapValue(cleanup.Ref, "instance_id"))
		resolvedPayload.PrivateIP = firstNonEmpty(
			strings.TrimSpace(stringMapValue(cleanup.Addresses, "private_ip")),
			resolvedPayload.PrivateIP,
		)
	}

	return buildAWSRebindExecutionPlan(plan, resolvedPayload), "rebind_existing_instance", "the selected AWS credential already has an instance with the task's current IP, so failover will only replace its public IP", nil
}

func parseAWSExecutionPayload(plan models.FailoverPlan) (awsRebindPayload, error) {
	switch strings.TrimSpace(plan.ActionType) {
	case models.FailoverActionProvisionInstance:
		var payload awsProvisionPayload
		if err := json.Unmarshal([]byte(plan.Payload), &payload); err != nil {
			return awsRebindPayload{}, fmt.Errorf("invalid aws provision payload: %w", err)
		}
		return normalizeAWSRebindPayload(awsRebindPayload{
			Service:          payload.Service,
			Region:           payload.Region,
			Name:             payload.Name,
			ImageID:          payload.ImageID,
			InstanceType:     payload.InstanceType,
			KeyName:          payload.KeyName,
			SubnetID:         payload.SubnetID,
			SecurityGroupIDs: trimStrings(payload.SecurityGroupIDs),
			UserData:         payload.UserData,
			AssignPublicIP:   payload.AssignPublicIP,
			AssignIPv6:       payload.AssignIPv6,
			AllowAllTraffic:  payload.AllowAllTraffic,
			Tags:             payload.Tags,
			RootPasswordMode: payload.RootPasswordMode,
			RootPassword:     payload.RootPassword,
			AvailabilityZone: payload.AvailabilityZone,
			BlueprintID:      payload.BlueprintID,
			BundleID:         payload.BundleID,
			KeyPairName:      payload.KeyPairName,
			IPAddressType:    payload.IPAddressType,
		}), nil
	case models.FailoverActionRebindPublicIP:
		var payload awsRebindPayload
		if err := json.Unmarshal([]byte(plan.Payload), &payload); err != nil {
			return awsRebindPayload{}, fmt.Errorf("invalid aws rebind payload: %w", err)
		}
		return normalizeAWSRebindPayload(payload), nil
	default:
		return awsRebindPayload{}, fmt.Errorf("unsupported aws plan action: %s", plan.ActionType)
	}
}

func resolveAWSCurrentInstanceByAddress(ctx context.Context, task models.FailoverTask, plan models.FailoverPlan, payload awsRebindPayload) (*currentInstanceCleanup, error) {
	if strings.TrimSpace(task.CurrentAddress) == "" {
		return nil, nil
	}

	lookupPlan := plan
	lookupPlan.ActionType = models.FailoverActionRebindPublicIP
	lookupPlan.Payload = marshalJSON(awsRebindPayload{
		Service: payload.Service,
		Region:  payload.Region,
	})

	candidate := providerPoolCandidate{
		EntryID:   strings.TrimSpace(plan.ProviderEntryID),
		EntryName: strings.TrimSpace(plan.ProviderEntryID),
	}
	cleanup, err := (&executionRunner{task: task, ctx: ctx}).resolveCurrentInstanceCleanupByAddress(ctx, lookupPlan, candidate)
	if err != nil {
		return nil, err
	}
	if cleanup == nil || !strings.EqualFold(strings.TrimSpace(stringMapValue(cleanup.Ref, "provider")), "aws") {
		return nil, nil
	}
	return cleanup, nil
}

func planMayProvision(plan models.FailoverPlan) bool {
	if strings.ToLower(strings.TrimSpace(plan.Provider)) == "aws" {
		return true
	}
	return strings.TrimSpace(plan.ActionType) == models.FailoverActionProvisionInstance
}

func (r *executionRunner) preparePlanForCandidate(plan models.FailoverPlan) (models.FailoverPlan, string, string, error) {
	if strings.ToLower(strings.TrimSpace(plan.Provider)) == "aws" {
		return prepareAWSRebindExecutionPlan(r.ctx, r.task, plan)
	}
	return plan, "", "", nil
}

func RunScheduledWork() error {
	if err := runPendingRollbackCleanupRetries(); err != nil {
		log.Printf("failover: pending rollback cleanup retry failed: %v", err)
	}

	taskList, err := failoverdb.ListEnabledTasks()
	if err != nil {
		return err
	}

	latestReports := ws.GetLatestReport()
	now := time.Now()
	for _, task := range taskList {
		taskCopy := task
		report := latestReports[strings.TrimSpace(taskCopy.WatchClientUUID)]
		shouldTrigger, statusFields, reason := evaluateTaskHealth(&taskCopy, report, now)
		changedStatusFields := scheduledChangedTaskStatusFields(&taskCopy, statusFields)
		if len(changedStatusFields) > 0 {
			if err := failoverdb.UpdateTaskFields(taskCopy.ID, changedStatusFields); err != nil {
				log.Printf("failover: failed to update task %d status: %v", taskCopy.ID, err)
			}
		}
		if !shouldTrigger {
			continue
		}
		releaseExecutionSlot, acquired := tryAcquireScheduledAutomaticExecutionSlot(taskCopy.UserID)
		if !acquired {
			log.Printf(
				"failover: delayed automatic action for task %d because automatic execution concurrency limit is reached for user %s",
				taskCopy.ID,
				strings.TrimSpace(taskCopy.UserID),
			)
			continue
		}
		if _, err := queueExecution(&taskCopy, report, reason, releaseExecutionSlot); err != nil {
			var duplicateErr *duplicateTargetRunError
			if errors.As(err, &duplicateErr) {
				log.Printf(
					"failover: skipped task %d because task %d is already handling the same outlet",
					taskCopy.ID,
					duplicateErr.ActiveTaskID,
				)
				continue
			}
			log.Printf("failover: failed to queue task %d: %v", taskCopy.ID, err)
		}
	}

	return nil
}

func RecoverInterruptedExecutions() error {
	recovered, err := failoverdb.RecoverInterruptedExecutions(interruptedExecutionMessage)
	if err != nil {
		return err
	}
	if recovered > 0 {
		log.Printf("failover: recovered %d interrupted execution(s)", recovered)
	}
	if err := runPendingRollbackCleanupRetries(); err != nil {
		log.Printf("failover: pending rollback cleanup retry failed during recovery: %v", err)
	}
	return nil
}

func StopExecutionForUser(userUUID string, executionID uint) (*models.FailoverExecution, error) {
	execution, err := failoverdb.StopExecutionForUser(userUUID, executionID, errExecutionStopped.Error())
	if err != nil {
		return nil, err
	}
	cancelExecution(executionID)
	return execution, nil
}

func RunTaskNowForUser(userUUID string, taskID uint) (*models.FailoverExecution, error) {
	task, err := failoverdb.GetTaskByIDForUser(userUUID, taskID)
	if err != nil {
		return nil, err
	}
	latestReports := ws.GetLatestReport()
	return queueExecution(task, latestReports[strings.TrimSpace(task.WatchClientUUID)], "manual run")
}

func evaluateTaskHealth(task *models.FailoverTask, report *common.Report, now time.Time) (bool, map[string]interface{}, string) {
	fields := map[string]interface{}{}
	if task == nil {
		return false, fields, ""
	}

	if task.CooldownSeconds > 0 && task.LastTriggeredAt != nil {
		nextRun := task.LastTriggeredAt.ToTime().Add(time.Duration(task.CooldownSeconds) * time.Second)
		if nextRun.After(now) {
			fields["last_status"] = models.FailoverTaskStatusCooldown
			fields["last_message"] = "cooldown until " + nextRun.UTC().Format(time.RFC3339)
			return false, fields, ""
		}
	}

	if strings.TrimSpace(task.WatchClientUUID) == "" {
		fields["last_status"] = models.FailoverTaskStatusUnknown
		fields["last_message"] = "task is not initialized"
		fields["trigger_failure_count"] = 0
		return false, fields, ""
	}

	if report == nil || report.CNConnectivity == nil {
		return evaluateMissingReportHealth(task, fields, "cn_connectivity report is unavailable")
	}

	reportTime := report.UpdatedAt
	if report.CNConnectivity.CheckedAt.After(reportTime) {
		reportTime = report.CNConnectivity.CheckedAt
	}
	if reportTime.IsZero() || now.Sub(reportTime) > time.Duration(task.StaleAfterSeconds)*time.Second {
		return evaluateMissingReportHealth(task, fields, "latest report is stale")
	}

	fields["trigger_failure_count"] = 0

	if report.CNConnectivity.Status == "blocked_suspected" && report.CNConnectivity.ConsecutiveFailures >= task.FailureThreshold {
		fields["last_status"] = models.FailoverTaskStatusTriggered
		fields["last_message"] = fmt.Sprintf("cn_connectivity blocked_suspected (%d failures)", report.CNConnectivity.ConsecutiveFailures)
		return true, fields, fields["last_message"].(string)
	}

	fields["last_status"] = models.FailoverTaskStatusHealthy
	fields["last_message"] = report.CNConnectivity.Status
	return false, fields, ""
}

func scheduledChangedTaskStatusFields(task *models.FailoverTask, fields map[string]interface{}) map[string]interface{} {
	if task == nil || len(fields) == 0 {
		return fields
	}
	changed := map[string]interface{}{}
	for key, value := range fields {
		switch key {
		case "last_status":
			if strings.TrimSpace(stringMapValue(fields, key)) != strings.TrimSpace(task.LastStatus) {
				changed[key] = value
			}
		case "last_message":
			if strings.TrimSpace(stringMapValue(fields, key)) != strings.TrimSpace(task.LastMessage) {
				changed[key] = value
			}
		case "trigger_failure_count":
			if intMapValue(fields, key) != task.TriggerFailureCount {
				changed[key] = value
			}
		default:
			changed[key] = value
		}
	}
	return changed
}

func evaluateMissingReportHealth(task *models.FailoverTask, fields map[string]interface{}, baseMessage string) (bool, map[string]interface{}, string) {
	if fields == nil {
		fields = map[string]interface{}{}
	}
	threshold := task.FailureThreshold
	if threshold <= 0 {
		threshold = 2
	}
	failures := task.TriggerFailureCount + 1
	fields["trigger_failure_count"] = failures
	if failures >= threshold {
		fields["last_status"] = models.FailoverTaskStatusTriggered
		fields["last_message"] = fmt.Sprintf("%s (%d/%d)", strings.TrimSpace(baseMessage), failures, threshold)
		return true, fields, fields["last_message"].(string)
	}
	fields["last_status"] = models.FailoverTaskStatusUnknown
	fields["last_message"] = fmt.Sprintf("%s (%d/%d)", strings.TrimSpace(baseMessage), failures, threshold)
	return false, fields, ""
}

func queueExecution(task *models.FailoverTask, report *common.Report, reason string, onDone ...func()) (*models.FailoverExecution, error) {
	var done func()
	if len(onDone) > 0 {
		done = onDone[0]
	}
	doneHeld := done != nil
	defer func() {
		if doneHeld && done != nil {
			done()
		}
	}()

	if task == nil {
		return nil, errors.New("task is required")
	}
	if !task.Enabled {
		return nil, fmt.Errorf("failover task %d is disabled", task.ID)
	}

	if !claimTaskRun(task.ID) {
		return nil, fmt.Errorf("failover task %d is already running", task.ID)
	}

	active, err := failoverdb.HasActiveExecution(task.ID)
	if err != nil {
		releaseTaskRun(task.ID)
		return nil, err
	}
	if active {
		recovered, recoverErr := failoverdb.RecoverInterruptedExecutionsForTask(task.ID, interruptedExecutionMessage)
		if recoverErr != nil {
			releaseTaskRun(task.ID)
			return nil, recoverErr
		}
		if recovered == 0 {
			releaseTaskRun(task.ID)
			return nil, fmt.Errorf("failover task %d already has an active execution", task.ID)
		}
		log.Printf("failover: recovered %d interrupted execution(s) for task %d while queueing", recovered, task.ID)
	}

	targetRunKey, targetKeyErr := failoverTargetRunKey(*task)
	if targetKeyErr != nil {
		releaseTaskRun(task.ID)
		return nil, targetKeyErr
	}
	activeTaskID, claimedTarget := claimTargetRun(targetRunKey, task.ID)
	if !claimedTarget {
		releaseTaskRun(task.ID)
		return nil, &duplicateTargetRunError{
			TaskID:       task.ID,
			ActiveTaskID: activeTaskID,
			TargetKey:    targetRunKey,
		}
	}

	now := time.Now()
	snapshot := buildTriggerSnapshot(report)
	execution, err := failoverdb.CreateExecution(&models.FailoverExecution{
		TaskID:          task.ID,
		Status:          models.FailoverExecutionStatusQueued,
		TriggerReason:   strings.TrimSpace(reason),
		WatchClientUUID: task.WatchClientUUID,
		TriggerSnapshot: snapshot,
		DNSProvider:     task.DNSProvider,
		OldClientUUID:   task.WatchClientUUID,
		OldInstanceRef:  strings.TrimSpace(task.CurrentInstanceRef),
		OldAddresses:    marshalJSON(map[string]interface{}{"current_address": strings.TrimSpace(task.CurrentAddress)}),
		StartedAt:       models.FromTime(now),
	})
	if err != nil {
		releaseTargetRun(targetRunKey, task.ID)
		releaseTaskRun(task.ID)
		return nil, err
	}

	if err := failoverdb.UpdateTaskFields(task.ID, map[string]interface{}{
		"last_execution_id": execution.ID,
		"last_status":       models.FailoverTaskStatusRunning,
		"last_message":      strings.TrimSpace(reason),
		"last_triggered_at": models.FromTime(now),
	}); err != nil {
		markQueuedExecutionFailed(execution.ID, fmt.Sprintf("failed to mark task running: %v", err))
		releaseTargetRun(targetRunKey, task.ID)
		releaseTaskRun(task.ID)
		return nil, err
	}

	go func(taskCopy models.FailoverTask, execCopy models.FailoverExecution, reportCopy *common.Report, runKey string, doneCopy func()) {
		defer releaseTargetRun(runKey, taskCopy.ID)
		defer releaseTaskRun(taskCopy.ID)
		defer func() {
			if doneCopy != nil {
				doneCopy()
			}
		}()
		ctx, cancel := context.WithCancel(context.Background())
		registerExecutionCancel(execCopy.ID, cancel)
		defer unregisterExecutionCancel(execCopy.ID)
		runner := &executionRunner{
			task:      taskCopy,
			execution: &execCopy,
			ctx:       ctx,
			startedAt: now,
		}
		runner.run(reportCopy)
	}(*task, *execution, cloneReport(report), targetRunKey, done)
	doneHeld = false

	return execution, nil
}

func markQueuedExecutionFailed(executionID uint, message string) {
	if executionID == 0 {
		return
	}
	if err := failoverdb.UpdateExecutionFields(executionID, map[string]interface{}{
		"status":        models.FailoverExecutionStatusFailed,
		"error_message": strings.TrimSpace(message),
		"finished_at":   models.FromTime(time.Now()),
	}); err != nil {
		log.Printf("failover: failed to mark queued execution %d failed after queue error: %v", executionID, err)
	}
}

func (r *executionRunner) run(report *common.Report) {
	defer func() {
		if recovered := recover(); recovered != nil {
			message := fmt.Sprintf("failover execution panicked: %v", recovered)
			log.Printf("failover: execution %d panicked: %v\n%s", r.execution.ID, recovered, debug.Stack())
			r.failExecution(message)
		}
	}()

	if err := r.checkStopped(); err != nil {
		r.failExecution(err.Error())
		return
	}

	if err := failoverdb.UpdateExecutionFields(r.execution.ID, map[string]interface{}{
		"status": models.FailoverExecutionStatusDetecting,
	}); err != nil {
		log.Printf("failover: failed to mark execution %d detecting: %v", r.execution.ID, err)
	}

	detectStep := r.startStep("detect", "Detect Trigger", map[string]interface{}{
		"reason": strings.TrimSpace(r.execution.TriggerReason),
	})
	if report != nil && report.CNConnectivity != nil {
		r.finishStep(detectStep, models.FailoverStepStatusSuccess, "trigger snapshot recorded", map[string]interface{}{
			"status":               report.CNConnectivity.Status,
			"consecutive_failures": report.CNConnectivity.ConsecutiveFailures,
		})
	} else {
		r.finishStep(detectStep, models.FailoverStepStatusSuccess, "manual trigger without live cn_connectivity snapshot", nil)
	}

	plans := make([]models.FailoverPlan, 0, len(r.task.Plans))
	for _, plan := range r.task.Plans {
		if plan.Enabled {
			plans = append(plans, plan)
		}
	}
	if len(plans) == 0 {
		r.failExecution("no enabled failover plans are configured")
		return
	}

	for _, plan := range plans {
		if err := r.checkStopped(); err != nil {
			r.failExecution(err.Error())
			return
		}
		attemptStep := r.startStep(fmt.Sprintf("plan:%d", plan.ID), "Plan Attempt", map[string]interface{}{
			"plan_id":      plan.ID,
			"provider":     plan.Provider,
			"action_type":  plan.ActionType,
			"priority":     plan.Priority,
			"plan_name":    plan.Name,
			"entry_id":     plan.ProviderEntryID,
			"entry_group":  plan.ProviderEntryGroup,
			"auto_connect": plan.AutoConnectGroup,
		})
		_ = failoverdb.UpdateExecutionFields(r.execution.ID, map[string]interface{}{
			"selected_plan_id": plan.ID,
		})

		outcome, selectedEntryID, entryAttempts, err := r.executePlan(plan)
		attempt := map[string]interface{}{
			"plan_id":               plan.ID,
			"plan_name":             strings.TrimSpace(plan.Name),
			"priority":              plan.Priority,
			"provider":              plan.Provider,
			"action_type":           plan.ActionType,
			"preferred_entry_id":    plan.ProviderEntryID,
			"preferred_entry_group": plan.ProviderEntryGroup,
		}
		if selectedEntryID != "" {
			attempt["provider_entry_id"] = selectedEntryID
		}
		if len(entryAttempts) > 0 {
			attempt["provider_entry_attempts"] = entryAttempts
		}
		if err != nil {
			attempt["status"] = "failed"
			attempt["error"] = err.Error()
			r.attempts = append(r.attempts, attempt)
			_ = failoverdb.UpdateExecutionFields(r.execution.ID, map[string]interface{}{
				"attempted_plans": marshalJSON(r.attempts),
			})
			r.finishStep(attemptStep, models.FailoverStepStatusFailed, err.Error(), attempt)
			if errors.Is(err, errExecutionStopped) {
				r.failExecution(err.Error())
				return
			}
			var noFallbackErr *noPlanFallbackError
			if errors.As(err, &noFallbackErr) {
				r.failExecution(err.Error())
				return
			}
			continue
		}

		attempt["status"] = "success"
		r.attempts = append(r.attempts, attempt)
		_ = failoverdb.UpdateExecutionFields(r.execution.ID, map[string]interface{}{
			"attempted_plans":  marshalJSON(r.attempts),
			"new_client_uuid":  emptyToNilString(outcome.NewClientUUID),
			"new_instance_ref": marshalJSON(outcome.NewInstanceRef),
			"new_addresses":    marshalJSON(outcome.NewAddresses),
			"old_instance_ref": marshalJSON(outcome.OldInstanceRef),
		})
		r.finishStep(attemptStep, models.FailoverStepStatusSuccess, "plan completed", attempt)
		r.succeedExecution(outcome)
		return
	}

	r.failExecution("all failover plans failed")
}

func (r *executionRunner) executePlan(plan models.FailoverPlan) (*actionOutcome, string, []map[string]interface{}, error) {
	if err := r.checkStopped(); err != nil {
		return nil, "", nil, err
	}
	return r.executePlanActionWithProviderPool(plan)
}

func (r *executionRunner) executePlanActionWithProviderPool(plan models.FailoverPlan) (*actionOutcome, string, []map[string]interface{}, error) {
	candidates, err := listProviderPoolCandidates(r.task.UserID, plan)
	if err != nil {
		return nil, "", nil, err
	}

	postProvisionFailureSeen := false
	var postProvisionFailureErr error
	provisionFailureCount := 0
	entryAttempts := make([]map[string]interface{}, 0, len(candidates))
	for _, candidate := range candidates {
		if err := r.checkStopped(); err != nil {
			return nil, "", entryAttempts, err
		}
		candidateDetail := map[string]interface{}{
			"entry_id":   candidate.EntryID,
			"entry_name": candidate.EntryName,
		}
		if candidate.EntryGroup != "" {
			candidateDetail["entry_group"] = candidate.EntryGroup
		}
		if candidate.Preferred {
			candidateDetail["preferred"] = true
		}
		if candidate.Active {
			candidateDetail["active"] = true
		}

		selectedPlan := plan
		selectedPlan.ProviderEntryID = candidate.EntryID
		selectedPlan.ProviderEntryGroup = candidate.EntryGroup

		executionPlan, executionMode, executionReason, prepErr := r.preparePlanForCandidate(selectedPlan)
		if prepErr != nil {
			if errors.Is(prepErr, errExecutionStopped) {
				return nil, "", entryAttempts, prepErr
			}
			var noFallbackErr *noPlanFallbackError
			if errors.As(prepErr, &noFallbackErr) {
				candidateDetail["status"] = "failed"
				candidateDetail["error"] = prepErr.Error()
				entryAttempts = append(entryAttempts, candidateDetail)
				return nil, "", entryAttempts, prepErr
			}
			decision := classifyProviderFailure(plan.Provider, prepErr)
			applyProviderEntryFailure(r.task.UserID, plan.Provider, candidate.EntryID, decision, prepErr)
			candidateDetail["status"] = "failed"
			candidateDetail["error"] = prepErr.Error()
			candidateDetail["failure_class"] = decision.Class
			entryAttempts = append(entryAttempts, candidateDetail)
			continue
		}
		if executionMode != "" {
			candidateDetail["execution_mode"] = executionMode
		}
		if executionReason != "" {
			candidateDetail["execution_reason"] = executionReason
		}
		if strings.TrimSpace(executionPlan.ActionType) != strings.TrimSpace(selectedPlan.ActionType) {
			candidateDetail["resolved_action_type"] = executionPlan.ActionType
		}

		isProvisionPlan := strings.TrimSpace(executionPlan.ActionType) == models.FailoverActionProvisionInstance
		provisionFailureLimit := provisionPlanFailureFallbackLimit(r.task, executionPlan)

		if isProvisionPlan {
			recycledDetail, recycleErr := r.recycleCurrentOutletForCandidate(executionPlan, candidate)
			if recycleErr != nil {
				if errors.Is(recycleErr, errExecutionStopped) {
					return nil, "", entryAttempts, recycleErr
				}
				candidateDetail["status"] = "failed"
				candidateDetail["error"] = recycleErr.Error()
				candidateDetail["failure_class"] = "pre_reclaim_error"
				entryAttempts = append(entryAttempts, candidateDetail)
				continue
			}
			if len(recycledDetail) > 0 {
				candidateDetail["recycled_current_instance"] = recycledDetail
				if availabilityAfterRecycle := waitForProviderEntryCapacityAfterRecycle(r.ctx, r.task.UserID, executionPlan, candidate); len(availabilityAfterRecycle) > 0 {
					candidateDetail["availability_after_recycle"] = availabilityAfterRecycle
				}
			}
		}

		maxAttempts := providerEntryMaxAttempts(r.task, executionPlan)
		for attemptNumber := 1; attemptNumber <= maxAttempts; attemptNumber++ {
			if err := r.checkStopped(); err != nil {
				return nil, "", entryAttempts, err
			}
			attemptDetail := make(map[string]interface{}, len(candidateDetail)+1)
			for key, value := range candidateDetail {
				attemptDetail[key] = value
			}
			attemptDetail["attempt"] = attemptNumber

			lease, availability, reserveErr := acquireProviderEntryLease(r.ctx, r.task.UserID, executionPlan, candidate)
			if len(availability) > 0 {
				attemptDetail["availability"] = availability
			}
			if reserveErr != nil && isProvisionPlan && strings.TrimSpace(stringMapValue(availability, "status")) == "full" {
				recycledDetail, recycleErr := r.recycleCurrentOutletForCandidate(executionPlan, candidate)
				if recycleErr != nil {
					if errors.Is(recycleErr, errExecutionStopped) {
						return nil, "", entryAttempts, recycleErr
					}
					attemptDetail["recycle_error"] = recycleErr.Error()
				} else if len(recycledDetail) > 0 {
					attemptDetail["recycled_current_instance"] = recycledDetail
					if availabilityAfterRecycle := waitForProviderEntryCapacityAfterRecycle(r.ctx, r.task.UserID, executionPlan, candidate); len(availabilityAfterRecycle) > 0 {
						attemptDetail["availability_after_recycle"] = availabilityAfterRecycle
					}
					lease, availability, reserveErr = acquireProviderEntryLease(r.ctx, r.task.UserID, executionPlan, candidate)
					if len(availability) > 0 {
						attemptDetail["availability"] = availability
					}
				}
			}
			if reserveErr != nil {
				if errors.Is(reserveErr, errExecutionStopped) {
					return nil, "", entryAttempts, reserveErr
				}
				attemptDetail["status"] = "skipped"
				attemptDetail["error"] = reserveErr.Error()
				entryAttempts = append(entryAttempts, attemptDetail)
				goto nextCandidate
			}

			var finishOperation func()
			releaseProvisioningWindow := func(provisioned bool) {
				if lease != nil {
					lease.Release(provisioned)
					lease = nil
				}
				if finishOperation != nil {
					finishOperation()
					finishOperation = nil
				}
			}
			defer func() {
				if recovered := recover(); recovered != nil {
					releaseProvisioningWindow(false)
					panic(recovered)
				}
			}()
			if shouldSerializeProviderOperation(executionPlan) {
				serialDone, serialErr := lease.BeginSerializedOperation(r.ctx, providerEntryOperationSpacing(executionPlan))
				if serialErr != nil {
					releaseProvisioningWindow(false)
					if errors.Is(serialErr, errExecutionStopped) {
						attemptDetail["status"] = "failed"
						attemptDetail["error"] = serialErr.Error()
						entryAttempts = append(entryAttempts, attemptDetail)
						return nil, "", entryAttempts, serialErr
					}
					attemptDetail["status"] = "skipped"
					attemptDetail["error"] = serialErr.Error()
					entryAttempts = append(entryAttempts, attemptDetail)
					goto nextCandidate
				}
				finishOperation = serialDone
			}

			outcome, actionErr := r.executePlanAction(executionPlan)
			if actionErr != nil {
				releaseProvisioningWindow(false)
				if errors.Is(actionErr, errExecutionStopped) {
					attemptDetail["status"] = "failed"
					attemptDetail["error"] = actionErr.Error()
					entryAttempts = append(entryAttempts, attemptDetail)
					return nil, "", entryAttempts, actionErr
				}
				var provisionErr *provisionCleanupError
				if errors.As(actionErr, &provisionErr) {
					for key, value := range provisionErr.detail() {
						attemptDetail[key] = value
					}
					attemptDetail["status"] = "failed"
					attemptDetail["error"] = actionErr.Error()
					entryAttempts = append(entryAttempts, attemptDetail)

					rollbackStep := r.startStep("rollback_new", "Cleanup Failed New Instance", provisionErr.detail())
					rollbackStatus := models.FailoverStepStatusSuccess
					if provisionErr.CleanupError != nil {
						rollbackStatus = models.FailoverStepStatusFailed
					}
					r.finishStep(rollbackStep, rollbackStatus, provisionErr.stepMessage(), provisionErr.detail())

					postProvisionFailureSeen = true
					postProvisionFailureErr = actionErr
					goto nextCandidate
				}
				decision := classifyProviderFailure(executionPlan.Provider, actionErr)
				applyProviderEntryFailure(r.task.UserID, executionPlan.Provider, candidate.EntryID, decision, actionErr)
				attemptDetail["status"] = "failed"
				attemptDetail["error"] = actionErr.Error()
				attemptDetail["failure_class"] = decision.Class
				if isProvisionPlan {
					provisionFailureCount++
					attemptDetail["plan_provision_failures"] = provisionFailureCount
					attemptDetail["plan_provision_failure_limit"] = provisionFailureLimit
				}
				entryAttempts = append(entryAttempts, attemptDetail)
				if isProvisionPlan && provisionFailureCount >= provisionFailureLimit {
					return nil, "", entryAttempts, fmt.Errorf(
						"plan provisioning failed %d times, switching to next failover plan: %w",
						provisionFailureLimit,
						actionErr,
					)
				}
				goto nextCandidate
			}
			if isProvisionPlan {
				if cleanupErr := r.attachCurrentOutletCleanup(outcome, executionPlan, candidate); cleanupErr != nil {
					releaseProvisioningWindow(false)
					if errors.Is(cleanupErr, errExecutionStopped) {
						attemptDetail["status"] = "failed"
						attemptDetail["error"] = cleanupErr.Error()
						entryAttempts = append(entryAttempts, attemptDetail)
						return nil, "", entryAttempts, cleanupErr
					}
					attemptDetail["status"] = "failed"
					attemptDetail["error"] = cleanupErr.Error()
					attemptDetail["failure_class"] = "post_provision_error"
					entryAttempts = append(entryAttempts, attemptDetail)
					postProvisionFailureSeen = true
					postProvisionFailureErr = cleanupErr
					goto nextCandidate
				}

				// Only the create-instance phase should occupy the provider queue.
				releaseProvisioningWindow(true)
			}

			finalizeErr := r.finalizePlan(executionPlan, outcome)
			if finalizeErr != nil {
				if errors.Is(finalizeErr, errExecutionStopped) {
					releaseProvisioningWindow(false)
					attemptDetail["status"] = "failed"
					attemptDetail["error"] = finalizeErr.Error()
					entryAttempts = append(entryAttempts, attemptDetail)
					return nil, "", entryAttempts, finalizeErr
				}
				executionDecision := classifyPlanExecutionFailure(finalizeErr)
				attemptDetail["error"] = finalizeErr.Error()
				attemptDetail["failure_class"] = executionDecision.Class
				if executionDecision.RetrySameEntry && attemptNumber < maxAttempts {
					retryBackoff := blockedOutletRetryBackoff(attemptNumber)
					retryDetail := map[string]interface{}{
						"entry_id":            candidate.EntryID,
						"entry_name":          candidate.EntryName,
						"attempt":             attemptNumber,
						"next_attempt":        attemptNumber + 1,
						"failure_class":       executionDecision.Class,
						"error_message":       finalizeErr.Error(),
						"provider":            selectedPlan.Provider,
						"provider_entry":      candidate.EntryID,
						"retry_after_seconds": int(retryBackoff / time.Second),
					}
					retryStep := r.startStep("retry_same_entry", "Retry Same Provider Entry", retryDetail)
					if err := waitContextOrDelay(r.ctx, retryBackoff); err != nil {
						r.finishStep(retryStep, models.FailoverStepStatusFailed, err.Error(), retryDetail)
						releaseProvisioningWindow(false)
						attemptDetail["status"] = "failed"
						entryAttempts = append(entryAttempts, attemptDetail)
						return nil, "", entryAttempts, err
					}
					r.finishStep(
						retryStep,
						models.FailoverStepStatusSuccess,
						"retryable new-outlet failure detected; retrying the same provider entry",
						retryDetail,
					)
					attemptDetail["status"] = "retry"
					entryAttempts = append(entryAttempts, attemptDetail)
					continue
				}
				postProvisionFailureSeen = true
				postProvisionFailureErr = finalizeErr
				if executionDecision.Cooldown > 0 {
					applyProviderEntryFailure(r.task.UserID, executionPlan.Provider, candidate.EntryID, providerFailureDecision{
						Class:    executionDecision.Class,
						Cooldown: executionDecision.Cooldown,
					}, finalizeErr)
				}
				releaseProvisioningWindow(false)
				attemptDetail["status"] = "failed"
				entryAttempts = append(entryAttempts, attemptDetail)
				goto nextCandidate
			}

			releaseProvisioningWindow(isProvisionPlan)
			clearProviderEntryCooldown(r.task.UserID, plan.Provider, candidate.EntryID)
			attemptDetail["status"] = "success"
			entryAttempts = append(entryAttempts, attemptDetail)
			return outcome, candidate.EntryID, entryAttempts, nil
		}

	nextCandidate:
	}

	if postProvisionFailureSeen && postProvisionFailureErr != nil {
		return nil, "", entryAttempts, &noPlanFallbackError{err: postProvisionFailureErr}
	}
	return nil, "", entryAttempts, buildProviderPoolUnavailableError(entryAttempts)
}

func buildProviderPoolUnavailableError(entryAttempts []map[string]interface{}) error {
	base := "no provider entry in the selected pool is currently available"
	summary := summarizeProviderEntryAttempts(entryAttempts)
	if summary == "" {
		return errors.New(base)
	}
	return fmt.Errorf("%s: %s", base, summary)
}

func summarizeProviderEntryAttempts(entryAttempts []map[string]interface{}) string {
	if len(entryAttempts) == 0 {
		return ""
	}

	type entrySummary struct {
		label  string
		reason string
	}

	order := make([]string, 0, len(entryAttempts))
	summaries := make(map[string]entrySummary, len(entryAttempts))
	for index := len(entryAttempts) - 1; index >= 0; index-- {
		attempt := entryAttempts[index]
		entryID := strings.TrimSpace(stringMapValue(attempt, "entry_id"))
		entryName := strings.TrimSpace(stringMapValue(attempt, "entry_name"))
		key := firstNonEmpty(entryID, entryName)
		if key == "" {
			key = fmt.Sprintf("entry-%d", index)
		}
		if _, exists := summaries[key]; exists {
			continue
		}

		reason := describeProviderEntryAttempt(attempt)
		if reason == "" {
			continue
		}

		summaries[key] = entrySummary{
			label:  firstNonEmpty(entryName, entryID, key),
			reason: reason,
		}
		order = append(order, key)
	}

	if len(order) == 0 {
		return ""
	}

	parts := make([]string, 0, len(order))
	for index := len(order) - 1; index >= 0; index-- {
		summary := summaries[order[index]]
		if summary.label == "" {
			parts = append(parts, summary.reason)
			continue
		}
		parts = append(parts, fmt.Sprintf("%s: %s", summary.label, summary.reason))
	}
	return strings.Join(parts, "; ")
}

func describeProviderEntryAttempt(attempt map[string]interface{}) string {
	availability := mapValue(attempt, "availability")
	availabilityStatus := strings.TrimSpace(stringMapValue(availability, "status"))
	switch availabilityStatus {
	case "cooldown":
		reason := strings.TrimSpace(stringMapValue(availability, "reason"))
		until := strings.TrimSpace(stringMapValue(availability, "cooldown_until"))
		switch {
		case reason != "" && until != "":
			return fmt.Sprintf("cooldown until %s (%s)", until, reason)
		case until != "":
			return fmt.Sprintf("cooldown until %s", until)
		case reason != "":
			return "cooldown (" + reason + ")"
		default:
			return "cooldown"
		}
	case "full":
		used := intMapValue(availability, "used")
		limit := intMapValue(availability, "limit")
		free := intMapValue(availability, "free")
		switch {
		case limit > 0:
			return fmt.Sprintf("capacity full (%d/%d used, %d free)", used, limit, free)
		case free == 0:
			return "no available capacity"
		}
	case "reserved":
		return "reserved by another running task"
	}

	failureClass := strings.TrimSpace(stringMapValue(attempt, "failure_class"))
	errorMessage := strings.TrimSpace(stringMapValue(attempt, "error"))
	switch failureClass {
	case "rate_limited":
		if errorMessage != "" {
			return "rate limited (" + errorMessage + ")"
		}
		return "rate limited"
	case "quota_exhausted":
		if errorMessage != "" {
			return "quota exhausted (" + errorMessage + ")"
		}
		return "quota exhausted"
	case "billing_locked":
		if errorMessage != "" {
			return "billing locked (" + errorMessage + ")"
		}
		return "billing locked"
	case "auth_invalid":
		if errorMessage != "" {
			return "credential invalid (" + errorMessage + ")"
		}
		return "credential invalid"
	case "outlet_blocked":
		if errorMessage != "" {
			return "new outlet blocked (" + errorMessage + ")"
		}
		return "new outlet blocked"
	case "post_provision_error":
		if errorMessage != "" {
			return errorMessage
		}
		return "post-provision step failed"
	}

	if errorMessage != "" {
		return errorMessage
	}
	status := strings.TrimSpace(stringMapValue(attempt, "status"))
	if status != "" && status != "success" {
		return status
	}
	return ""
}

func mapValue(source map[string]interface{}, key string) map[string]interface{} {
	if source == nil {
		return nil
	}
	value, ok := source[key]
	if !ok {
		return nil
	}
	object, ok := value.(map[string]interface{})
	if !ok {
		return nil
	}
	return object
}

func stringMapValue(source map[string]interface{}, key string) string {
	if source == nil {
		return ""
	}
	value, ok := source[key]
	if !ok || value == nil {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return typed
	case fmt.Stringer:
		return typed.String()
	default:
		return fmt.Sprintf("%v", typed)
	}
}

func intMapValue(source map[string]interface{}, key string) int {
	if source == nil {
		return 0
	}
	value, ok := source[key]
	if !ok || value == nil {
		return 0
	}
	switch typed := value.(type) {
	case int:
		return typed
	case int8:
		return int(typed)
	case int16:
		return int(typed)
	case int32:
		return int(typed)
	case int64:
		return int(typed)
	case uint:
		return int(typed)
	case uint8:
		return int(typed)
	case uint16:
		return int(typed)
	case uint32:
		return int(typed)
	case uint64:
		return int(typed)
	case float32:
		return int(typed)
	case float64:
		return int(typed)
	case json.Number:
		parsed, _ := typed.Int64()
		return int(parsed)
	default:
		return 0
	}
}

func (r *executionRunner) executePlanAction(plan models.FailoverPlan) (*actionOutcome, error) {
	var (
		outcome *actionOutcome
		err     error
	)

	switch plan.ActionType {
	case models.FailoverActionProvisionInstance:
		outcome, err = r.executeProvisionPlan(plan)
	case models.FailoverActionRebindPublicIP:
		outcome, err = r.executeRebindPlan(plan)
	default:
		return nil, fmt.Errorf("unsupported failover action: %s", plan.ActionType)
	}
	if err != nil {
		return nil, err
	}

	return outcome, nil
}

func providerEntryMaxAttempts(task models.FailoverTask, plan models.FailoverPlan) int {
	if planMayProvision(plan) {
		if task.ProvisionRetryLimit > 0 {
			return task.ProvisionRetryLimit
		}
		return models.FailoverProvisionRetryLimitDefault
	}
	return 1
}

func provisionPlanFailureFallbackLimit(task models.FailoverTask, plan models.FailoverPlan) int {
	if planMayProvision(plan) {
		if task.ProvisionFailureFallbackLimit > 0 {
			return task.ProvisionFailureFallbackLimit
		}
		return models.FailoverProvisionFailureFallbackLimitDefault
	}
	return 1
}

func classifyPlanExecutionFailure(err error) planExecutionFailureDecision {
	var blockedErr *blockedOutletError
	if errors.As(err, &blockedErr) {
		return planExecutionFailureDecision{
			Class:          "outlet_blocked",
			RetrySameEntry: true,
			Cooldown:       providerEntryCooldownTransient,
		}
	}

	return planExecutionFailureDecision{
		Class:          "post_provision_error",
		RetrySameEntry: false,
	}
}

func blockedOutletRetryBackoff(attemptNumber int) time.Duration {
	return blockedOutletRetryBaseBackoff
}

func (r *executionRunner) finalizePlan(plan models.FailoverPlan, outcome *actionOutcome) (err error) {
	defer func() {
		if err == nil || outcome == nil || outcome.Rollback == nil {
			return
		}
		if rollbackErr := r.rollbackOutcome(outcome, err); rollbackErr != nil {
			err = rollbackErr
		}
	}()
	defer func() {
		if err == nil || outcome == nil || outcome.Rollback != nil {
			return
		}
		r.syncTaskOutletTracking(outcome)
	}()

	targetClientUUID := strings.TrimSpace(outcome.TargetClientUUID)
	if targetClientUUID == "" && strings.TrimSpace(outcome.AutoConnectGroup) != "" {
		waitStep := r.startStep("wait_agent", "Wait For Agent", map[string]interface{}{
			"group": outcome.AutoConnectGroup,
		})
		_ = failoverdb.UpdateExecutionFields(r.execution.ID, map[string]interface{}{
			"status": models.FailoverExecutionStatusWaitingAgent,
		})

		clientUUID, waitErr := waitForClientByGroup(
			r.ctx,
			r.task.UserID,
			outcome.AutoConnectGroup,
			r.task.WatchClientUUID,
			r.startedAt,
			plan.WaitAgentTimeoutSec,
			expectedClientAddresses(outcome),
		)
		if waitErr != nil {
			r.finishStep(waitStep, models.FailoverStepStatusFailed, waitErr.Error(), nil)
			return waitErr
		}
		targetClientUUID = clientUUID
		outcome.NewClientUUID = clientUUID
		outcome.TargetClientUUID = clientUUID
		_ = failoverdb.UpdateExecutionFields(r.execution.ID, map[string]interface{}{
			"new_client_uuid": clientUUID,
		})
		r.finishStep(waitStep, models.FailoverStepStatusSuccess, "agent connected", map[string]interface{}{
			"client_uuid": clientUUID,
		})
	}

	if validationErr := r.validateProvisionedOutlet(plan, outcome, targetClientUUID); validationErr != nil {
		return validationErr
	}

	scriptClipboardIDs := plan.EffectiveScriptClipboardIDs()
	if strings.TrimSpace(plan.ActionType) == models.FailoverActionProvisionInstance && len(scriptClipboardIDs) > 0 {
		if targetClientUUID == "" {
			return errors.New("script execution requires a target client but none became available")
		}

		scriptStep := r.startStep("run_scripts", "Run Scripts", map[string]interface{}{
			"clipboard_ids": scriptClipboardIDs,
			"client_uuid":   targetClientUUID,
		})
		_ = failoverdb.UpdateExecutionFields(r.execution.ID, map[string]interface{}{
			"status": models.FailoverExecutionStatusRunningScript,
		})
		if err := r.runScripts(plan, targetClientUUID); err != nil {
			r.finishStep(scriptStep, models.FailoverStepStatusFailed, err.Error(), nil)
			return err
		}
		r.finishStep(scriptStep, models.FailoverStepStatusSuccess, "scripts finished successfully", map[string]interface{}{
			"count": len(scriptClipboardIDs),
		})
	}

	if strings.TrimSpace(r.task.DNSProvider) == "" || strings.TrimSpace(r.task.DNSEntryID) == "" {
		dnsStep := r.startStep("switch_dns", "Switch DNS", map[string]interface{}{
			"configured": false,
		})
		skippedResult := map[string]interface{}{
			"message": "dns switching is not configured for this task",
		}
		_ = failoverdb.UpdateExecutionFields(r.execution.ID, map[string]interface{}{
			"dns_status": models.FailoverDNSStatusSkipped,
			"dns_result": marshalJSON(skippedResult),
		})
		r.finishStep(dnsStep, models.FailoverStepStatusSkipped, "dns switching skipped", skippedResult)
		return nil
	}

	dnsStep := r.startStep("switch_dns", "Switch DNS", map[string]interface{}{
		"provider": r.task.DNSProvider,
	})
	_ = failoverdb.UpdateExecutionFields(r.execution.ID, map[string]interface{}{
		"status": models.FailoverExecutionStatusSwitchingDNS,
	})
	dnsResult, err := failoverDNSApplyFunc(r.ctx, r.task.UserID, r.task.DNSProvider, r.task.DNSEntryID, r.task.DNSPayload, outcome.IPv4, outcome.IPv6)
	if err != nil {
		_ = failoverdb.UpdateExecutionFields(r.execution.ID, map[string]interface{}{
			"dns_status": models.FailoverDNSStatusFailed,
			"dns_result": marshalJSON(map[string]interface{}{"error": err.Error()}),
		})
		r.finishStep(dnsStep, models.FailoverStepStatusFailed, err.Error(), nil)
		return err
	}
	dnsVerification, verifyErr := failoverDNSVerifyFunc(r.ctx, r.task.UserID, r.task.DNSProvider, r.task.DNSEntryID, r.task.DNSPayload, outcome.IPv4, outcome.IPv6)
	if dnsResult == nil {
		dnsResult = &dnsUpdateResult{
			Provider: strings.TrimSpace(r.task.DNSProvider),
		}
	}
	dnsResult.Verification = dnsVerification
	if verifyErr != nil {
		failedResult := map[string]interface{}{
			"error": verifyErr.Error(),
		}
		if dnsResult != nil {
			failedResult["applied"] = dnsResult
		}
		if dnsVerification != nil {
			failedResult["verification"] = dnsVerification
		}
		_ = failoverdb.UpdateExecutionFields(r.execution.ID, map[string]interface{}{
			"dns_status": models.FailoverDNSStatusFailed,
			"dns_result": marshalJSON(failedResult),
		})
		r.finishStep(dnsStep, models.FailoverStepStatusFailed, verifyErr.Error(), failedResult)
		return verifyErr
	}
	_ = failoverdb.UpdateExecutionFields(r.execution.ID, map[string]interface{}{
		"dns_status": models.FailoverDNSStatusSuccess,
		"dns_result": marshalJSON(dnsResult),
	})
	r.finishStep(dnsStep, models.FailoverStepStatusSuccess, "dns updated and verified", dnsResult)
	return nil
}

func planHasScripts(plan models.FailoverPlan) bool {
	return len(plan.EffectiveScriptClipboardIDs()) > 0
}

func joinScriptSnapshotNames(names []string) string {
	filtered := make([]string, 0, len(names))
	for _, name := range names {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		filtered = append(filtered, name)
	}
	return truncateUTF8String(strings.Join(filtered, ", "), 255)
}

func latestScriptTaskID(taskIDs []string) string {
	for index := len(taskIDs) - 1; index >= 0; index-- {
		taskID := strings.TrimSpace(taskIDs[index])
		if taskID != "" {
			return taskID
		}
	}
	return ""
}

func joinScriptOutputs(names, outputs []string) string {
	if len(outputs) == 0 {
		return ""
	}
	if len(outputs) == 1 {
		return outputs[0]
	}

	var builder strings.Builder
	for index, output := range outputs {
		if index > 0 {
			builder.WriteString("\n\n")
		}

		name := strings.TrimSpace(fmt.Sprintf("Script %d", index+1))
		if index < len(names) && strings.TrimSpace(names[index]) != "" {
			name = strings.TrimSpace(names[index])
		}
		builder.WriteString("==> ")
		builder.WriteString(name)
		builder.WriteString(" <==")
		if output != "" {
			builder.WriteByte('\n')
			builder.WriteString(output)
		}
	}
	return builder.String()
}

func truncateUTF8String(value string, limit int) string {
	if limit <= 0 || len(value) <= limit {
		return value
	}
	if limit <= 3 {
		return value[:limit]
	}

	var builder strings.Builder
	remaining := limit - 3
	used := 0
	for _, r := range value {
		runeLen := utf8.RuneLen(r)
		if runeLen < 0 || used+runeLen > remaining {
			break
		}
		builder.WriteRune(r)
		used += runeLen
	}
	builder.WriteString("...")
	return builder.String()
}

func (r *executionRunner) validateProvisionedOutlet(plan models.FailoverPlan, outcome *actionOutcome, targetClientUUID string) error {
	if strings.TrimSpace(plan.ActionType) != models.FailoverActionProvisionInstance {
		return nil
	}

	validateStep := r.startStep("validate_outlet", "Validate New Outlet", map[string]interface{}{
		"client_uuid": targetClientUUID,
	})

	if strings.TrimSpace(targetClientUUID) == "" {
		r.finishStep(validateStep, models.FailoverStepStatusSkipped, "connectivity validation skipped because no target client is available", nil)
		return nil
	}

	report, err := waitForHealthyClientConnectivity(r.ctx, r.task.UserID, targetClientUUID, r.startedAt)
	if err != nil {
		detail := map[string]interface{}{
			"client_uuid": targetClientUUID,
		}
		if report != nil && report.CNConnectivity != nil {
			detail["status"] = strings.TrimSpace(report.CNConnectivity.Status)
			detail["message"] = strings.TrimSpace(report.CNConnectivity.Message)
			detail["checked_at"] = report.CNConnectivity.CheckedAt
			detail["consecutive_failures"] = report.CNConnectivity.ConsecutiveFailures
		}
		r.finishStep(validateStep, models.FailoverStepStatusFailed, err.Error(), detail)
		return err
	}

	successDetail := map[string]interface{}{
		"client_uuid": targetClientUUID,
	}
	if report != nil && report.CNConnectivity != nil {
		successDetail["status"] = strings.TrimSpace(report.CNConnectivity.Status)
		successDetail["target"] = strings.TrimSpace(report.CNConnectivity.Target)
		successDetail["latency"] = report.CNConnectivity.Latency
		successDetail["checked_at"] = report.CNConnectivity.CheckedAt
	}
	r.finishStep(validateStep, models.FailoverStepStatusSuccess, "new outlet connectivity looks healthy", successDetail)
	return nil
}

func waitForHealthyClientConnectivity(ctx context.Context, userUUID, clientUUID string, startedAt time.Time) (*common.Report, error) {
	timeout := failoverConnectivityValidationTimeout(userUUID)
	deadline := time.Now().Add(timeout)
	clientUUID = strings.TrimSpace(clientUUID)
	var lastReport *common.Report

	for time.Now().Before(deadline) {
		if err := waitContextOrDelay(ctx, 0); err != nil {
			return nil, err
		}
		report := ws.GetLatestReport()[clientUUID]
		if report != nil && report.CNConnectivity != nil {
			lastReport = cloneReport(report)
			reportTime := report.UpdatedAt
			if report.CNConnectivity.CheckedAt.After(reportTime) {
				reportTime = report.CNConnectivity.CheckedAt
			}
			if reportTime.After(startedAt) || report.CNConnectivity.CheckedAt.After(startedAt) {
				status := strings.ToLower(strings.TrimSpace(report.CNConnectivity.Status))
				switch status {
				case "ok":
					return cloneReport(report), nil
				case "blocked_suspected":
					return cloneReport(report), &blockedOutletError{
						ClientUUID: clientUUID,
						Status:     report.CNConnectivity.Status,
						Message:    report.CNConnectivity.Message,
					}
				}
			}
		}
		if err := waitContextOrDelay(ctx, 5*time.Second); err != nil {
			return nil, err
		}
	}

	if lastReport != nil && lastReport.CNConnectivity != nil {
		return lastReport, fmt.Errorf("timed out waiting for a healthy cn_connectivity report from client %s (last status: %s)", clientUUID, strings.TrimSpace(lastReport.CNConnectivity.Status))
	}
	return nil, fmt.Errorf("timed out waiting for cn_connectivity report from client %s", clientUUID)
}

func resolveAutoConnectPoolGroup(plan models.FailoverPlan, credentialGroup string) string {
	if group := strings.TrimSpace(plan.ProviderEntryGroup); group != "" {
		return group
	}
	if entryID := strings.TrimSpace(plan.ProviderEntryID); entryID != "" && entryID != activeProviderEntryID {
		return strings.TrimSpace(credentialGroup)
	}
	return ""
}

func persistDigitalOceanRootPassword(userUUID string, addition *digitalocean.Addition, token *digitalocean.TokenRecord, dropletID int, dropletName, passwordMode, rootPassword string) error {
	if addition == nil || token == nil || dropletID <= 0 || strings.TrimSpace(rootPassword) == "" {
		return nil
	}

	latestAddition, latestToken, err := reloadDigitalOceanAdditionTokenState(userUUID, token)
	if err != nil {
		log.Printf("failover: failed to reload DigitalOcean token state for droplet %d: %v", dropletID, err)
		return err
	}
	if err := latestToken.SaveDropletPassword(dropletID, dropletName, passwordMode, rootPassword, time.Now()); err != nil {
		log.Printf("failover: failed to save DigitalOcean root password for droplet %d: %v", dropletID, err)
		return err
	}
	if err := saveDigitalOceanAddition(userUUID, latestAddition); err != nil {
		latestToken.RemoveSavedDropletPassword(dropletID)
		log.Printf("failover: failed to persist DigitalOcean root password for droplet %d: %v", dropletID, err)
		return err
	}
	return nil
}

func removeSavedDigitalOceanRootPassword(userUUID string, addition *digitalocean.Addition, token *digitalocean.TokenRecord, dropletID int) {
	if addition == nil || token == nil || dropletID <= 0 {
		return
	}

	targetAddition := addition
	targetToken := token
	if latestAddition, latestToken, err := reloadDigitalOceanAdditionTokenState(userUUID, token); err == nil {
		targetAddition = latestAddition
		targetToken = latestToken
	} else {
		log.Printf("failover: failed to reload DigitalOcean token state for droplet %d cleanup, falling back to in-memory state: %v", dropletID, err)
	}

	if !targetToken.RemoveSavedDropletPassword(dropletID) {
		return
	}
	if err := saveDigitalOceanAddition(userUUID, targetAddition); err != nil {
		log.Printf("failover: failed to remove saved DigitalOcean root password for droplet %d: %v", dropletID, err)
	}
}

func persistLinodeRootPassword(userUUID string, addition *linodecloud.Addition, token *linodecloud.TokenRecord, instanceID int, instanceLabel, passwordMode, rootPassword string) error {
	if addition == nil || token == nil || instanceID <= 0 || strings.TrimSpace(rootPassword) == "" {
		return nil
	}

	latestAddition, latestToken, err := reloadLinodeAdditionTokenState(userUUID, token)
	if err != nil {
		log.Printf("failover: failed to reload Linode token state for instance %d: %v", instanceID, err)
		return err
	}
	if err := latestToken.SaveInstancePassword(instanceID, instanceLabel, passwordMode, rootPassword, time.Now()); err != nil {
		log.Printf("failover: failed to save Linode root password for instance %d: %v", instanceID, err)
		return err
	}
	if err := saveLinodeAddition(userUUID, latestAddition); err != nil {
		latestToken.RemoveSavedInstancePassword(instanceID)
		log.Printf("failover: failed to persist Linode root password for instance %d: %v", instanceID, err)
		return err
	}
	return nil
}

func removeSavedLinodeRootPassword(userUUID string, addition *linodecloud.Addition, token *linodecloud.TokenRecord, instanceID int) {
	if addition == nil || token == nil || instanceID <= 0 {
		return
	}

	targetAddition := addition
	targetToken := token
	if latestAddition, latestToken, err := reloadLinodeAdditionTokenState(userUUID, token); err == nil {
		targetAddition = latestAddition
		targetToken = latestToken
	} else {
		log.Printf("failover: failed to reload Linode token state for instance %d cleanup, falling back to in-memory state: %v", instanceID, err)
	}

	if !targetToken.RemoveSavedInstancePassword(instanceID) {
		return
	}
	if err := saveLinodeAddition(userUUID, targetAddition); err != nil {
		log.Printf("failover: failed to remove saved Linode root password for instance %d: %v", instanceID, err)
	}
}

func persistVultrRootPassword(userUUID string, addition *vultrcloud.Addition, token *vultrcloud.TokenRecord, instanceID, instanceLabel, passwordMode, rootPassword string) error {
	if addition == nil || token == nil || strings.TrimSpace(instanceID) == "" || strings.TrimSpace(rootPassword) == "" {
		return nil
	}

	latestAddition, latestToken, err := reloadVultrAdditionTokenState(userUUID, token)
	if err != nil {
		log.Printf("failover: failed to reload Vultr token state for instance %s: %v", instanceID, err)
		return err
	}
	if err := latestToken.SaveInstancePassword(instanceID, instanceLabel, passwordMode, rootPassword, time.Now()); err != nil {
		log.Printf("failover: failed to save Vultr root password for instance %s: %v", instanceID, err)
		return err
	}
	if err := saveVultrAddition(userUUID, latestAddition); err != nil {
		latestToken.RemoveSavedInstancePassword(instanceID)
		log.Printf("failover: failed to persist Vultr root password for instance %s: %v", instanceID, err)
		return err
	}
	return nil
}

func removeSavedVultrRootPassword(userUUID string, addition *vultrcloud.Addition, token *vultrcloud.TokenRecord, instanceID string) {
	if addition == nil || token == nil || strings.TrimSpace(instanceID) == "" {
		return
	}

	targetAddition := addition
	targetToken := token
	if latestAddition, latestToken, err := reloadVultrAdditionTokenState(userUUID, token); err == nil {
		targetAddition = latestAddition
		targetToken = latestToken
	} else {
		log.Printf("failover: failed to reload Vultr token state for instance %s cleanup, falling back to in-memory state: %v", instanceID, err)
	}

	if !targetToken.RemoveSavedInstancePassword(instanceID) {
		return
	}
	if err := saveVultrAddition(userUUID, targetAddition); err != nil {
		log.Printf("failover: failed to remove saved Vultr root password for instance %s: %v", instanceID, err)
	}
}

func persistAzureRootPassword(userUUID string, addition *azurecloud.Addition, credential *azurecloud.CredentialRecord, instanceID, instanceName, username, passwordMode, rootPassword string) error {
	if addition == nil || credential == nil || strings.TrimSpace(instanceID) == "" || strings.TrimSpace(rootPassword) == "" {
		return nil
	}

	latestAddition, latestCredential, err := reloadAzureAdditionCredentialState(userUUID, credential)
	if err != nil {
		log.Printf("failover: failed to reload Azure credential state for instance %s: %v", instanceID, err)
		return err
	}
	if err := latestCredential.SaveInstancePassword(instanceID, instanceName, username, passwordMode, rootPassword, time.Now()); err != nil {
		log.Printf("failover: failed to save Azure root password for instance %s: %v", instanceID, err)
		return err
	}
	if err := saveAzureAddition(userUUID, latestAddition); err != nil {
		latestCredential.RemoveSavedInstancePassword(instanceID)
		log.Printf("failover: failed to persist Azure root password for instance %s: %v", instanceID, err)
		return err
	}
	return nil
}

func removeSavedAzureRootPassword(userUUID string, addition *azurecloud.Addition, credential *azurecloud.CredentialRecord, instanceID string) {
	if addition == nil || credential == nil || strings.TrimSpace(instanceID) == "" {
		return
	}

	targetAddition := addition
	targetCredential := credential
	if latestAddition, latestCredential, err := reloadAzureAdditionCredentialState(userUUID, credential); err == nil {
		targetAddition = latestAddition
		targetCredential = latestCredential
	} else {
		log.Printf("failover: failed to reload Azure credential state for instance %s cleanup, falling back to in-memory state: %v", instanceID, err)
	}

	if !targetCredential.RemoveSavedInstancePassword(instanceID) {
		return
	}
	if err := saveAzureAddition(userUUID, targetAddition); err != nil {
		log.Printf("failover: failed to remove saved Azure root password for instance %s: %v", instanceID, err)
	}
}

func buildAWSResourceCredentialID(region, resourceID string) string {
	resourceID = strings.TrimSpace(resourceID)
	if resourceID == "" {
		return ""
	}

	region = strings.TrimSpace(strings.ToLower(region))
	if region == "" {
		return resourceID
	}
	return region + "::" + resourceID
}

func normalizeAWSRootPasswordMode(mode string) string {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "none":
		return "none"
	case "custom":
		return "custom"
	case "random":
		return "random"
	default:
		return ""
	}
}

func resolveAWSRootPasswordUserData(mode, rootPassword, userData string) (string, string, error) {
	userData = strings.TrimSpace(userData)
	switch normalizeAWSRootPasswordMode(mode) {
	case "", "none":
		return userData, "", nil
	case "custom":
		rootPassword = strings.TrimSpace(rootPassword)
		if rootPassword == "" {
			return "", "", errors.New("root password is required when custom password mode is selected")
		}
		resolvedUserData, err := awscloud.BuildRootPasswordUserData(rootPassword, userData)
		return resolvedUserData, "", err
	case "random":
		generatedPassword, err := awscloud.GenerateRandomPassword(20)
		if err != nil {
			return "", "", err
		}
		resolvedUserData, err := awscloud.BuildRootPasswordUserData(generatedPassword, userData)
		return resolvedUserData, generatedPassword, err
	default:
		return userData, "", nil
	}
}

func persistAWSRootPassword(
	userUUID string,
	addition *awscloud.Addition,
	credential *awscloud.CredentialRecord,
	resourceType string,
	region string,
	resourceID string,
	resourceName string,
	passwordMode string,
	rootPassword string,
) error {
	resourceType = strings.TrimSpace(resourceType)
	credentialResourceID := buildAWSResourceCredentialID(region, resourceID)
	rootPassword = strings.TrimSpace(rootPassword)
	if addition == nil || credential == nil || resourceType == "" || credentialResourceID == "" || rootPassword == "" {
		return nil
	}

	latestAddition, latestCredential, err := reloadAWSAdditionCredentialState(userUUID, credential)
	if err != nil {
		log.Printf("failover: failed to reload AWS credential state for %s %s: %v", resourceType, strings.TrimSpace(resourceID), err)
		return err
	}
	if err := latestCredential.SaveResourcePassword(resourceType, credentialResourceID, resourceName, passwordMode, rootPassword, time.Now()); err != nil {
		log.Printf("failover: failed to save AWS root password for %s %s: %v", resourceType, strings.TrimSpace(resourceID), err)
		return err
	}
	if err := saveAWSAddition(userUUID, latestAddition); err != nil {
		latestCredential.RemoveSavedResourcePassword(resourceType, credentialResourceID)
		log.Printf("failover: failed to persist AWS root password for %s %s: %v", resourceType, strings.TrimSpace(resourceID), err)
		return err
	}
	return nil
}

func removeSavedAWSRootPassword(
	userUUID string,
	addition *awscloud.Addition,
	credential *awscloud.CredentialRecord,
	resourceType string,
	region string,
	resourceID string,
) {
	resourceType = strings.TrimSpace(resourceType)
	credentialResourceID := buildAWSResourceCredentialID(region, resourceID)
	if addition == nil || credential == nil || resourceType == "" || credentialResourceID == "" {
		return
	}

	targetAddition := addition
	targetCredential := credential
	if latestAddition, latestCredential, err := reloadAWSAdditionCredentialState(userUUID, credential); err == nil {
		targetAddition = latestAddition
		targetCredential = latestCredential
	} else {
		log.Printf("failover: failed to reload AWS credential state for %s %s cleanup, falling back to in-memory state: %v", resourceType, strings.TrimSpace(resourceID), err)
	}

	if !targetCredential.RemoveSavedResourcePassword(resourceType, credentialResourceID) {
		return
	}
	if err := saveAWSAddition(userUUID, targetAddition); err != nil {
		log.Printf("failover: failed to remove saved AWS root password for %s %s: %v", resourceType, strings.TrimSpace(resourceID), err)
	}
}

func failoverConnectivityValidationTimeout(userUUID string) time.Duration {
	interval, err := config.GetAsForUser[int](userUUID, config.CNConnectivityIntervalKey, 60)
	if err != nil || interval <= 0 {
		interval = 60
	}
	timeoutSeconds := interval*2 + 20
	if timeoutSeconds < 90 {
		timeoutSeconds = 90
	}
	return time.Duration(timeoutSeconds) * time.Second
}

func (r *executionRunner) rollbackOutcome(outcome *actionOutcome, originalErr error) error {
	if outcome == nil || outcome.Rollback == nil {
		return originalErr
	}

	rollbackStep := r.startStep("rollback_new", "Cleanup Failed New Instance", map[string]interface{}{
		"label": outcome.RollbackLabel,
		"error": func() string {
			if originalErr == nil {
				return ""
			}
			return originalErr.Error()
		}(),
	})

	if err := normalizeExecutionStopError(outcome.Rollback(r.ctx)); err != nil {
		detail := map[string]interface{}{
			"label": outcome.RollbackLabel,
			"error": err.Error(),
		}
		if pendingCleanup, saveErr := r.queuePendingRollbackCleanup(outcome, err); saveErr != nil {
			detail["compensation_error"] = saveErr.Error()
		} else if pendingCleanup != nil {
			detail["compensation_cleanup_id"] = pendingCleanup.ID
			detail["compensation_status"] = pendingCleanup.Status
			if pendingCleanup.NextRetryAt != nil {
				detail["compensation_next_retry_at"] = pendingCleanup.NextRetryAt
			}
		}
		r.finishStep(rollbackStep, models.FailoverStepStatusFailed, err.Error(), detail)
		return fmt.Errorf("%w; rollback failed: %v", originalErr, err)
	}

	detail := map[string]interface{}{
		"label": outcome.RollbackLabel,
	}
	r.invalidateProvisionedEntrySnapshot(outcome)
	r.finishStep(rollbackStep, models.FailoverStepStatusSuccess, "failed new instance deleted", detail)
	return originalErr
}

func (r *executionRunner) invalidateProvisionedEntrySnapshot(outcome *actionOutcome) {
	if outcome == nil || strings.TrimSpace(r.task.UserID) == "" {
		return
	}

	provider := strings.TrimSpace(stringMapValue(outcome.NewInstanceRef, "provider"))
	entryID := strings.TrimSpace(providerEntryIDFromRef(outcome.NewInstanceRef))
	if provider == "" || entryID == "" {
		return
	}

	invalidateProviderEntrySnapshot(r.task.UserID, provider, entryID)
}

func (r *executionRunner) queuePendingRollbackCleanup(outcome *actionOutcome, rollbackErr error) (*models.FailoverPendingCleanup, error) {
	if r == nil || outcome == nil || rollbackErr == nil {
		return nil, nil
	}

	ref := cloneJSONMap(outcome.NewInstanceRef)
	provider, resourceType, resourceID, providerEntryID := pendingCleanupIdentityFromRef(ref)
	if provider == "" || resourceType == "" || resourceID == "" {
		return nil, nil
	}

	now := time.Now()
	lastAttemptedAt := models.FromTime(now)
	nextRetryAt := models.FromTime(now.Add(pendingCleanupRetryBackoff(provider, rollbackErr, 1)))
	cleanup := &models.FailoverPendingCleanup{
		UserID: strings.TrimSpace(r.task.UserID),
		TaskID: r.task.ID,
		ExecutionID: func() uint {
			if r.execution != nil {
				return r.execution.ID
			}
			return 0
		}(),
		Provider:        provider,
		ProviderEntryID: providerEntryID,
		ResourceType:    resourceType,
		ResourceID:      resourceID,
		InstanceRef:     marshalJSON(ref),
		CleanupLabel:    strings.TrimSpace(outcome.RollbackLabel),
		Status:          models.FailoverPendingCleanupStatusPending,
		AttemptCount:    1,
		LastError:       strings.TrimSpace(rollbackErr.Error()),
		LastAttemptedAt: &lastAttemptedAt,
		NextRetryAt:     &nextRetryAt,
	}
	return failoverdb.CreateOrUpdatePendingCleanup(cleanup)
}

func (r *executionRunner) executeProvisionPlan(plan models.FailoverPlan) (*actionOutcome, error) {
	provisionStep := r.startStep("provision", "Provision Instance", map[string]interface{}{
		"provider": plan.Provider,
	})
	_ = failoverdb.UpdateExecutionFields(r.execution.ID, map[string]interface{}{
		"status": models.FailoverExecutionStatusProvisioning,
	})

	var (
		outcome *actionOutcome
		err     error
	)
	switch plan.Provider {
	case "aws":
		outcome, err = provisionAWSInstance(r.ctx, r.task.UserID, plan)
	case "azure":
		outcome, err = provisionAzureInstance(r.ctx, r.task.UserID, plan)
	case "digitalocean":
		outcome, err = provisionDigitalOceanDroplet(r.ctx, r.task.UserID, plan)
	case "linode":
		outcome, err = provisionLinodeInstance(r.ctx, r.task.UserID, plan)
	case "vultr":
		outcome, err = provisionVultrInstance(r.ctx, r.task.UserID, plan)
	default:
		err = fmt.Errorf("unsupported provision provider: %s", plan.Provider)
	}
	err = normalizeExecutionStopError(err)
	if err != nil {
		r.finishStep(provisionStep, models.FailoverStepStatusFailed, err.Error(), nil)
		return nil, err
	}

	_ = failoverdb.UpdateExecutionFields(r.execution.ID, map[string]interface{}{
		"new_instance_ref": marshalJSON(outcome.NewInstanceRef),
		"new_addresses":    marshalJSON(outcome.NewAddresses),
	})
	r.finishStep(provisionStep, models.FailoverStepStatusSuccess, "instance provisioned", outcome.NewInstanceRef)
	return outcome, nil
}

func (r *executionRunner) executeRebindPlan(plan models.FailoverPlan) (*actionOutcome, error) {
	rebindStep := r.startStep("rebind_ip", "Rebind Public IP", map[string]interface{}{
		"provider": plan.Provider,
	})
	_ = failoverdb.UpdateExecutionFields(r.execution.ID, map[string]interface{}{
		"status": models.FailoverExecutionStatusRebindingIP,
	})

	var (
		outcome *actionOutcome
		err     error
	)
	switch plan.Provider {
	case "aws":
		outcome, err = rebindAWSIPAddress(r.ctx, r.task, plan)
	default:
		err = fmt.Errorf("unsupported rebind provider: %s", plan.Provider)
	}
	err = normalizeExecutionStopError(err)
	if err != nil {
		r.finishStep(rebindStep, models.FailoverStepStatusFailed, err.Error(), nil)
		return nil, err
	}

	_ = failoverdb.UpdateExecutionFields(r.execution.ID, map[string]interface{}{
		"new_addresses":    marshalJSON(outcome.NewAddresses),
		"old_instance_ref": marshalJSON(outcome.OldInstanceRef),
	})
	r.finishStep(rebindStep, models.FailoverStepStatusSuccess, "public ip rebound", outcome.NewAddresses)
	return outcome, nil
}

func (r *executionRunner) runScripts(plan models.FailoverPlan, clientUUID string) error {
	scriptClipboardIDs := plan.EffectiveScriptClipboardIDs()
	if len(scriptClipboardIDs) == 0 {
		return nil
	}

	primaryScriptClipboardID := models.FirstFailoverScriptClipboardID(scriptClipboardIDs)
	encodedScriptClipboardIDs := models.EncodeFailoverScriptClipboardIDs(scriptClipboardIDs)
	var primaryScriptClipboardValue interface{}
	if primaryScriptClipboardID != nil {
		primaryScriptClipboardValue = *primaryScriptClipboardID
	}
	scriptNames := make([]string, 0, len(scriptClipboardIDs))
	scriptTaskIDs := make([]string, 0, len(scriptClipboardIDs))
	scriptOutputs := make([]string, 0, len(scriptClipboardIDs))
	scriptOutputTruncated := false
	var lastExitCode *int
	var lastFinishedAt *models.LocalTime

	for index, scriptClipboardID := range scriptClipboardIDs {
		clipboard, err := clipboarddb.GetClipboardByIDForUser(scriptClipboardID, r.task.UserID)
		if err != nil {
			return err
		}

		scriptNames = append(scriptNames, clipboard.Name)
		step := r.startStep(
			fmt.Sprintf("run_script:%d:%d", clipboard.Id, index+1),
			fmt.Sprintf("Run Script %d", index+1),
			map[string]interface{}{
				"clipboard_id": clipboard.Id,
				"script_name":  clipboard.Name,
				"client_uuid":  clientUUID,
				"index":        index + 1,
				"total":        len(scriptClipboardIDs),
			},
		)
		_ = failoverdb.UpdateExecutionFields(r.execution.ID, map[string]interface{}{
			"script_clipboard_id":     primaryScriptClipboardValue,
			"script_clipboard_ids":    encodedScriptClipboardIDs,
			"script_name_snapshot":    joinScriptSnapshotNames(scriptNames),
			"script_status":           models.FailoverScriptStatusRunning,
			"script_task_id":          latestScriptTaskID(scriptTaskIDs),
			"script_exit_code":        nil,
			"script_finished_at":      nil,
			"script_output":           joinScriptOutputs(scriptNames[:len(scriptOutputs)], scriptOutputs),
			"script_output_truncated": scriptOutputTruncated,
		})

		result, err := dispatchScriptToClient(r.ctx, r.task.UserID, clientUUID, clipboard.Text, time.Duration(plan.ScriptTimeoutSec)*time.Second)
		result = ensureCommandResult(result)
		scriptTaskIDs = append(scriptTaskIDs, result.TaskID)
		scriptOutputs = append(scriptOutputs, result.Output)
		scriptOutputTruncated = scriptOutputTruncated || result.Truncated
		lastExitCode = result.ExitCode
		lastFinishedAt = result.FinishedAt

		if err != nil {
			status := models.FailoverScriptStatusFailed
			if errors.Is(err, context.DeadlineExceeded) {
				status = models.FailoverScriptStatusTimeout
			}
			_ = failoverdb.UpdateExecutionFields(r.execution.ID, map[string]interface{}{
				"script_clipboard_id":     primaryScriptClipboardValue,
				"script_clipboard_ids":    encodedScriptClipboardIDs,
				"script_name_snapshot":    joinScriptSnapshotNames(scriptNames),
				"script_task_id":          latestScriptTaskID(scriptTaskIDs),
				"script_status":           status,
				"script_exit_code":        lastExitCode,
				"script_finished_at":      lastFinishedAt,
				"script_output":           joinScriptOutputs(scriptNames, scriptOutputs),
				"script_output_truncated": scriptOutputTruncated,
			})
			r.finishStep(step, models.FailoverStepStatusFailed, err.Error(), map[string]interface{}{
				"clipboard_id":            clipboard.Id,
				"script_name":             clipboard.Name,
				"task_id":                 result.TaskID,
				"exit_code":               result.ExitCode,
				"output_truncated":        result.Truncated,
				"script_output_available": strings.TrimSpace(result.Output) != "",
			})
			return err
		}

		if execErr := commandResultExecutionError(result); execErr != nil {
			_ = failoverdb.UpdateExecutionFields(r.execution.ID, map[string]interface{}{
				"script_clipboard_id":     primaryScriptClipboardValue,
				"script_clipboard_ids":    encodedScriptClipboardIDs,
				"script_name_snapshot":    joinScriptSnapshotNames(scriptNames),
				"script_task_id":          latestScriptTaskID(scriptTaskIDs),
				"script_status":           models.FailoverScriptStatusFailed,
				"script_exit_code":        lastExitCode,
				"script_finished_at":      lastFinishedAt,
				"script_output":           joinScriptOutputs(scriptNames, scriptOutputs),
				"script_output_truncated": scriptOutputTruncated,
			})
			r.finishStep(step, models.FailoverStepStatusFailed, execErr.Error(), map[string]interface{}{
				"clipboard_id":            clipboard.Id,
				"script_name":             clipboard.Name,
				"task_id":                 result.TaskID,
				"exit_code":               result.ExitCode,
				"output_truncated":        result.Truncated,
				"script_output_available": strings.TrimSpace(result.Output) != "",
			})
			return execErr
		}

		r.finishStep(step, models.FailoverStepStatusSuccess, "script finished successfully", map[string]interface{}{
			"clipboard_id":            clipboard.Id,
			"script_name":             clipboard.Name,
			"task_id":                 result.TaskID,
			"exit_code":               result.ExitCode,
			"output_truncated":        result.Truncated,
			"script_output_available": strings.TrimSpace(result.Output) != "",
		})
	}

	_ = failoverdb.UpdateExecutionFields(r.execution.ID, map[string]interface{}{
		"script_clipboard_id":     primaryScriptClipboardValue,
		"script_clipboard_ids":    encodedScriptClipboardIDs,
		"script_name_snapshot":    joinScriptSnapshotNames(scriptNames),
		"script_task_id":          latestScriptTaskID(scriptTaskIDs),
		"script_status":           models.FailoverScriptStatusSuccess,
		"script_exit_code":        lastExitCode,
		"script_finished_at":      lastFinishedAt,
		"script_output":           joinScriptOutputs(scriptNames, scriptOutputs),
		"script_output_truncated": scriptOutputTruncated,
	})
	return nil
}

func (r *executionRunner) succeedExecution(outcome *actionOutcome) {
	now := time.Now()
	cleanupStatus := models.FailoverCleanupStatusSkipped
	cleanupResult := buildCleanupNotRequestedResult()
	deleteStrategy := effectiveTaskDeleteStrategy(r.task)

	if deleteStrategy != models.FailoverDeleteStrategyKeep && outcome != nil && outcome.Cleanup != nil {
		cleanupStep := r.startStep("cleanup_old", "Cleanup Old Instance", map[string]interface{}{
			"strategy": deleteStrategy,
			"label":    outcome.CleanupLabel,
		})
		_ = failoverdb.UpdateExecutionFields(r.execution.ID, map[string]interface{}{
			"status": models.FailoverExecutionStatusCleaningOld,
		})

		if deleteStrategy == models.FailoverDeleteStrategyDeleteAfterSuccessDelay && r.task.DeleteDelaySeconds > 0 {
			if err := waitContextOrDelay(r.ctx, time.Duration(r.task.DeleteDelaySeconds)*time.Second); err != nil {
				cleanupStatus = models.FailoverCleanupStatusWarning
				cleanupResult = buildCleanupInterruptedResult(outcome.OldInstanceRef, outcome.CleanupLabel, err)
				r.finishStep(cleanupStep, models.FailoverStepStatusSkipped, cleanupStepMessageCleanupStatusUnknown, cleanupResult)
				goto finalizeExecution
			}
		}

		if err := normalizeExecutionStopError(outcome.Cleanup(r.ctx)); err != nil {
			if errors.Is(err, errExecutionStopped) {
				cleanupStatus = models.FailoverCleanupStatusWarning
				cleanupResult = buildCleanupInterruptedResult(outcome.OldInstanceRef, outcome.CleanupLabel, err)
				r.finishStep(cleanupStep, models.FailoverStepStatusSkipped, cleanupStepMessageCleanupStatusUnknown, cleanupResult)
			} else {
				cleanupStatus = models.FailoverCleanupStatusFailed
				cleanupResult = buildCleanupDeleteFailedResult(outcome.OldInstanceRef, outcome.CleanupLabel, err)
				r.finishStep(cleanupStep, models.FailoverStepStatusFailed, err.Error(), cleanupResult)
			}
		} else {
			oldProvider := strings.TrimSpace(stringMapValue(outcome.OldInstanceRef, "provider"))
			oldEntryID := strings.TrimSpace(providerEntryIDFromRef(outcome.OldInstanceRef))
			if oldProvider != "" && oldEntryID != "" {
				invalidateProviderEntrySnapshot(r.task.UserID, oldProvider, oldEntryID)
			}
			cleanupStatus = models.FailoverCleanupStatusSuccess
			cleanupResult = buildCleanupDeletedResult(outcome.OldInstanceRef, outcome.CleanupLabel)
			r.finishStep(cleanupStep, models.FailoverStepStatusSuccess, "old instance deleted", cleanupResult)
		}
	} else if deleteStrategy != models.FailoverDeleteStrategyKeep && outcome != nil && outcome.CleanupAssessment != nil {
		cleanupStep := r.startStep("cleanup_old", "Cleanup Old Instance", map[string]interface{}{
			"strategy":       deleteStrategy,
			"label":          outcome.CleanupLabel,
			"classification": stringMapValue(outcome.CleanupAssessment.Result, "classification"),
		})
		cleanupStatus = firstNonEmpty(strings.TrimSpace(outcome.CleanupAssessment.Status), models.FailoverCleanupStatusWarning)
		cleanupResult = cloneJSONMap(outcome.CleanupAssessment.Result)
		stepStatus := firstNonEmpty(strings.TrimSpace(outcome.CleanupAssessment.StepStatus), models.FailoverStepStatusSkipped)
		stepMessage := firstNonEmpty(
			strings.TrimSpace(outcome.CleanupAssessment.StepMessage),
			strings.TrimSpace(stringMapValue(cleanupResult, "summary")),
			"old instance cleanup requires manual review",
		)
		r.finishStep(cleanupStep, stepStatus, stepMessage, cleanupResult)
	}

finalizeExecution:
	fields := map[string]interface{}{
		"status":         models.FailoverExecutionStatusSuccess,
		"finished_at":    models.FromTime(now),
		"cleanup_status": cleanupStatus,
		"cleanup_result": marshalJSON(cleanupResult),
	}
	if outcome != nil {
		fields["new_client_uuid"] = emptyToNilString(firstNonEmpty(outcome.NewClientUUID, outcome.TargetClientUUID))
		fields["new_instance_ref"] = marshalJSON(outcome.NewInstanceRef)
		fields["new_addresses"] = marshalJSON(outcome.NewAddresses)
		fields["old_instance_ref"] = marshalJSON(outcome.OldInstanceRef)
	}
	_ = failoverdb.UpdateExecutionFields(r.execution.ID, fields)

	taskUpdates := map[string]interface{}{
		"last_status":           models.FailoverTaskStatusCooldown,
		"last_message":          "failover completed",
		"last_succeeded_at":     models.FromTime(now),
		"trigger_failure_count": 0,
	}
	for key, value := range buildTaskOutletTrackingFields(outcome) {
		taskUpdates[key] = value
	}
	_ = failoverdb.UpdateTaskFields(r.task.ID, taskUpdates)
	applyTaskFieldUpdates(&r.task, taskUpdates)
	r.succeeded = true
}

func (r *executionRunner) failExecution(message string) {
	now := time.Now()
	if err := failoverdb.FailRunningStepsForExecution(r.execution.ID, message); err != nil {
		log.Printf("failover: failed to mark running steps failed for execution %d: %v", r.execution.ID, err)
	}
	_ = failoverdb.UpdateExecutionFields(r.execution.ID, map[string]interface{}{
		"status":        models.FailoverExecutionStatusFailed,
		"error_message": strings.TrimSpace(message),
		"finished_at":   models.FromTime(now),
	})
	_ = failoverdb.UpdateTaskFields(r.task.ID, map[string]interface{}{
		"last_status":    models.FailoverTaskStatusFailed,
		"last_message":   strings.TrimSpace(message),
		"last_failed_at": models.FromTime(now),
	})
}

func (r *executionRunner) startStep(key, label string, detail interface{}) *models.FailoverExecutionStep {
	r.stepSort++
	step, err := failoverdb.CreateExecutionStep(&models.FailoverExecutionStep{
		ExecutionID: r.execution.ID,
		Sort:        r.stepSort,
		StepKey:     key,
		StepLabel:   label,
		Status:      models.FailoverStepStatusRunning,
		Detail:      marshalJSON(detail),
		StartedAt:   ptrLocalTime(time.Now()),
	})
	if err != nil {
		log.Printf("failover: failed to create step for execution %d: %v", r.execution.ID, err)
		return nil
	}
	return step
}

func (r *executionRunner) finishStep(step *models.FailoverExecutionStep, status, message string, detail interface{}) {
	if step == nil {
		return
	}
	fields := map[string]interface{}{
		"status":      status,
		"message":     strings.TrimSpace(message),
		"detail":      marshalJSON(detail),
		"finished_at": models.FromTime(time.Now()),
	}
	if err := failoverdb.UpdateExecutionStepFields(step.ID, fields); err != nil {
		log.Printf("failover: failed to update step %d: %v", step.ID, err)
	}
}

func effectiveCurrentInstanceRef(outcome *actionOutcome) map[string]interface{} {
	if outcome == nil {
		return nil
	}
	if len(outcome.NewInstanceRef) > 0 {
		return outcome.NewInstanceRef
	}
	if len(outcome.OldInstanceRef) > 0 {
		return outcome.OldInstanceRef
	}
	return nil
}

func buildTaskOutletTrackingFields(outcome *actionOutcome) map[string]interface{} {
	if outcome == nil {
		return nil
	}

	fields := map[string]interface{}{}
	if nextClientUUID := strings.TrimSpace(firstNonEmpty(outcome.NewClientUUID, outcome.TargetClientUUID)); nextClientUUID != "" {
		fields["watch_client_uuid"] = nextClientUUID
	}
	if nextAddress := primaryOutcomeAddress(outcome); nextAddress != "" {
		fields["current_address"] = nextAddress
	}
	if nextRef := effectiveCurrentInstanceRef(outcome); len(nextRef) > 0 {
		fields["current_instance_ref"] = marshalJSON(nextRef)
	}
	if len(fields) == 0 {
		return nil
	}
	return fields
}

func applyTaskFieldUpdates(task *models.FailoverTask, fields map[string]interface{}) {
	if task == nil || len(fields) == 0 {
		return
	}
	if watchClientUUID := strings.TrimSpace(stringMapValue(fields, "watch_client_uuid")); watchClientUUID != "" {
		task.WatchClientUUID = watchClientUUID
	}
	if currentAddress := strings.TrimSpace(stringMapValue(fields, "current_address")); currentAddress != "" {
		task.CurrentAddress = currentAddress
	}
	if rawCurrentInstanceRef, ok := fields["current_instance_ref"]; ok && rawCurrentInstanceRef != nil {
		task.CurrentInstanceRef = strings.TrimSpace(fmt.Sprintf("%v", rawCurrentInstanceRef))
	}
	if lastStatus := strings.TrimSpace(stringMapValue(fields, "last_status")); lastStatus != "" {
		task.LastStatus = lastStatus
	}
	if rawLastMessage, ok := fields["last_message"]; ok && rawLastMessage != nil {
		task.LastMessage = strings.TrimSpace(fmt.Sprintf("%v", rawLastMessage))
	}
}

func (r *executionRunner) syncTaskOutletTracking(outcome *actionOutcome) {
	fields := buildTaskOutletTrackingFields(outcome)
	if len(fields) == 0 {
		return
	}
	if r != nil && r.task.ID > 0 {
		_ = failoverdb.UpdateTaskFields(r.task.ID, fields)
	}
	if r != nil {
		applyTaskFieldUpdates(&r.task, fields)
	}
}

func parseJSONMap(raw string) map[string]interface{} {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	var decoded map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		return nil
	}
	if len(decoded) == 0 {
		return nil
	}
	return decoded
}

func cloneJSONMap(source map[string]interface{}) map[string]interface{} {
	if len(source) == 0 {
		return nil
	}
	cloned := make(map[string]interface{}, len(source))
	for key, value := range source {
		cloned[key] = value
	}
	return cloned
}

func providerEntryIDFromRef(ref map[string]interface{}) string {
	return firstNonEmpty(stringMapValue(ref, "provider_entry_id"), stringMapValue(ref, "entry_id"))
}

func providerEntryNameFromRef(ref map[string]interface{}) string {
	return firstNonEmpty(stringMapValue(ref, "provider_entry_name"), stringMapValue(ref, "entry_name"))
}

func resolvedCurrentInstanceRef(ref map[string]interface{}, provider, entryID string) map[string]interface{} {
	resolvedRef := cloneJSONMap(ref)
	if len(resolvedRef) == 0 {
		resolvedRef = map[string]interface{}{}
	}
	resolvedRef["provider"] = strings.TrimSpace(provider)
	resolvedRef["provider_entry_id"] = strings.TrimSpace(entryID)
	if name := providerEntryNameFromRef(ref); name != "" {
		resolvedRef["provider_entry_name"] = name
	}
	return resolvedRef
}

func cleanupResultBase(classification, label string, ref map[string]interface{}) map[string]interface{} {
	result := map[string]interface{}{
		"classification": strings.TrimSpace(classification),
	}
	if provider := strings.TrimSpace(stringMapValue(ref, "provider")); provider != "" {
		result["provider"] = provider
	}
	if entryID := strings.TrimSpace(providerEntryIDFromRef(ref)); entryID != "" {
		result["provider_entry_id"] = entryID
	}
	if entryName := strings.TrimSpace(providerEntryNameFromRef(ref)); entryName != "" {
		result["provider_entry_name"] = entryName
	}
	if strings.TrimSpace(label) != "" {
		result["cleanup_label"] = strings.TrimSpace(label)
	}
	if len(ref) > 0 {
		result["instance_ref"] = cloneJSONMap(ref)
	}
	return result
}

func cleanupAssessmentWithResult(status, stepStatus, stepMessage string, result map[string]interface{}) *cleanupAssessment {
	return &cleanupAssessment{
		Status:      strings.TrimSpace(status),
		StepStatus:  strings.TrimSpace(stepStatus),
		StepMessage: strings.TrimSpace(stepMessage),
		Result:      cloneJSONMap(result),
	}
}

func cloneCleanupAssessment(source *cleanupAssessment) *cleanupAssessment {
	if source == nil {
		return nil
	}
	return &cleanupAssessment{
		Status:      strings.TrimSpace(source.Status),
		StepStatus:  strings.TrimSpace(source.StepStatus),
		StepMessage: strings.TrimSpace(source.StepMessage),
		Result:      cloneJSONMap(source.Result),
	}
}

func buildCleanupNotRequestedResult() map[string]interface{} {
	return map[string]interface{}{
		"classification": cleanupClassificationNotRequested,
		"summary":        "old instance cleanup was not requested for this task",
		"billing_risk":   "none",
	}
}

func buildCleanupDeletedResult(ref map[string]interface{}, label string) map[string]interface{} {
	result := cleanupResultBase(cleanupClassificationInstanceDeleted, label, ref)
	result["summary"] = "old instance deleted successfully"
	result["instance_state"] = "deleted"
	result["billing_risk"] = "none"
	return result
}

func buildCleanupDeleteFailedResult(ref map[string]interface{}, label string, err error) map[string]interface{} {
	result := cleanupResultBase(cleanupClassificationInstanceConfirmedDeleteFail, label, ref)
	result["summary"] = "old instance cleanup failed after confirming the old instance still exists; it is likely still billing until removed"
	result["instance_state"] = "confirmed_present"
	result["billing_risk"] = "likely"
	result["manual_action_required"] = true
	if err != nil && strings.TrimSpace(err.Error()) != "" {
		result["error_message"] = strings.TrimSpace(err.Error())
	}
	return result
}

func buildCleanupInterruptedResult(ref map[string]interface{}, label string, err error) map[string]interface{} {
	result := cleanupResultBase(cleanupClassificationCleanupStatusUnknown, label, ref)
	result["summary"] = "old instance cleanup was interrupted before completion; the old instance status could not be confirmed, review the original cloud account manually"
	result["instance_state"] = "unknown"
	result["billing_risk"] = "unknown"
	result["manual_action_required"] = true
	result["interrupted"] = true
	if err != nil && strings.TrimSpace(err.Error()) != "" {
		result["error_message"] = strings.TrimSpace(err.Error())
	}
	return result
}

func buildInstanceMissingCleanupAssessment(ref map[string]interface{}, label string) *cleanupAssessment {
	result := cleanupResultBase(cleanupClassificationInstanceMissing, label, ref)
	result["summary"] = "old instance was already missing; no cleanup action was required"
	result["instance_state"] = "missing"
	result["billing_risk"] = "none"
	return cleanupAssessmentWithResult(
		models.FailoverCleanupStatusSkipped,
		models.FailoverStepStatusSkipped,
		cleanupStepMessageInstanceMissing,
		result,
	)
}

func buildProviderEntryMissingCleanupAssessment(ref map[string]interface{}, label string) *cleanupAssessment {
	result := cleanupResultBase(cleanupClassificationProviderEntryMissing, label, ref)
	result["summary"] = "old instance cleanup could not run because the original cloud credential entry was deleted; the old instance status could not be confirmed, review the original cloud account manually"
	result["credential_state"] = "deleted"
	result["instance_state"] = "unknown"
	result["billing_risk"] = "unknown"
	result["manual_action_required"] = true
	return cleanupAssessmentWithResult(
		models.FailoverCleanupStatusWarning,
		models.FailoverStepStatusSkipped,
		cleanupStepMessageProviderEntryMissing,
		result,
	)
}

func buildProviderEntryQueryCleanupAssessment(provider string, ref map[string]interface{}, label string, err error) *cleanupAssessment {
	decision := classifyProviderFailure(provider, err)
	classification := cleanupClassificationCleanupStatusUnknown
	stepMessage := cleanupStepMessageCleanupStatusUnknown
	summary := "old instance cleanup status could not be confirmed because the original cloud credential could not query the provider API; review the original cloud account manually"
	credentialState := "query_failed"

	switch decision.Class {
	case "auth_invalid":
		classification = cleanupClassificationProviderEntryUnhealthy
		stepMessage = cleanupStepMessageProviderEntryUnhealthy
		summary = "old instance cleanup could not run because the original cloud credential is no longer valid; the old instance status could not be confirmed, review the original cloud account manually"
		credentialState = "unavailable"
	case "billing_locked":
		classification = cleanupClassificationProviderEntryUnhealthy
		stepMessage = cleanupStepMessageProviderEntryUnhealthy
		summary = "old instance cleanup could not run because the original cloud account is locked or billing-restricted; the old instance status could not be confirmed, review the original cloud account manually"
		credentialState = "unavailable"
	case "rate_limited":
		summary = "old instance cleanup status could not be confirmed because the provider API rate-limited the original cloud credential; review the original cloud account manually"
	}

	result := cleanupResultBase(classification, label, ref)
	result["summary"] = summary
	result["credential_state"] = credentialState
	result["instance_state"] = "unknown"
	result["billing_risk"] = "unknown"
	result["manual_action_required"] = true
	if decision.Class != "" {
		result["provider_failure_class"] = decision.Class
	}
	if err != nil && strings.TrimSpace(err.Error()) != "" {
		result["error_message"] = strings.TrimSpace(err.Error())
	}

	return cleanupAssessmentWithResult(
		models.FailoverCleanupStatusWarning,
		models.FailoverStepStatusSkipped,
		stepMessage,
		result,
	)
}

func buildUnresolvedCurrentInstanceCleanupAssessment(ref map[string]interface{}, address string) *cleanupAssessment {
	result := cleanupResultBase(cleanupClassificationCleanupStatusUnknown, "", ref)
	result["summary"] = "old instance cleanup could not be prepared because the original instance could not be identified from the saved reference or current address; review the original cloud account manually"
	result["instance_state"] = "unknown"
	result["billing_risk"] = "unknown"
	result["manual_action_required"] = true
	if strings.TrimSpace(address) != "" {
		result["current_address"] = strings.TrimSpace(address)
	}
	return cleanupAssessmentWithResult(
		models.FailoverCleanupStatusWarning,
		models.FailoverStepStatusSkipped,
		cleanupStepMessageCleanupStatusUnknown,
		result,
	)
}

func hasCurrentOutletCleanupEvidence(ref map[string]interface{}, address string) bool {
	return len(ref) > 0 || strings.TrimSpace(address) != ""
}

func currentInstanceCleanupLabelFromRef(ref map[string]interface{}, address string) string {
	provider := strings.ToLower(strings.TrimSpace(stringMapValue(ref, "provider")))
	address = strings.TrimSpace(address)
	switch provider {
	case "digitalocean":
		if dropletID := intMapValue(ref, "droplet_id"); dropletID > 0 {
			return fmt.Sprintf("delete digitalocean droplet %d", dropletID)
		}
		if address != "" {
			return "delete digitalocean instance at " + address
		}
		return "delete digitalocean instance"
	case "linode":
		if instanceID := intMapValue(ref, "instance_id"); instanceID > 0 {
			return fmt.Sprintf("delete linode instance %d", instanceID)
		}
		if address != "" {
			return "delete linode instance at " + address
		}
		return "delete linode instance"
	case "vultr":
		if instanceID := strings.TrimSpace(stringMapValue(ref, "instance_id")); instanceID != "" {
			return "delete vultr instance " + instanceID
		}
		if address != "" {
			return "delete vultr instance at " + address
		}
		return "delete vultr instance"
	case "azure":
		if instanceName := strings.TrimSpace(firstNonEmpty(stringMapValue(ref, "name"), stringMapValue(ref, "instance_name"))); instanceName != "" {
			return "delete azure vm " + instanceName
		}
		if address != "" {
			return "delete azure vm at " + address
		}
		return "delete azure vm"
	case "aws":
		service := strings.ToLower(strings.TrimSpace(firstNonEmpty(stringMapValue(ref, "service"), "ec2")))
		if service == "lightsail" {
			if instanceName := strings.TrimSpace(stringMapValue(ref, "instance_name")); instanceName != "" {
				return "delete aws lightsail instance " + instanceName
			}
			if address != "" {
				return "delete aws lightsail instance at " + address
			}
			return "delete aws lightsail instance"
		}
		if instanceID := strings.TrimSpace(stringMapValue(ref, "instance_id")); instanceID != "" {
			return "terminate aws ec2 instance " + instanceID
		}
		if address != "" {
			return "terminate aws ec2 instance at " + address
		}
		return "terminate aws ec2 instance"
	default:
		if address != "" {
			return "cleanup old instance at " + address
		}
		return "cleanup old instance"
	}
}

func currentRefMatchesProviderEntry(ref map[string]interface{}, provider, entryID string) bool {
	if len(ref) == 0 {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(stringMapValue(ref, "provider")), strings.TrimSpace(provider)) &&
		strings.TrimSpace(providerEntryIDFromRef(ref)) == strings.TrimSpace(entryID)
}

func currentInstanceCleanupMatchesProviderEntry(cleanup *currentInstanceCleanup, provider, entryID string) bool {
	if cleanup == nil {
		return false
	}
	return currentRefMatchesProviderEntry(cleanup.Ref, provider, entryID)
}

func isCurrentInstanceCredentialMissingError(provider string, err error) bool {
	if err == nil {
		return false
	}
	if isProviderEntryNotFoundError(err) {
		return true
	}

	message := strings.ToLower(strings.TrimSpace(err.Error()))
	switch strings.ToLower(strings.TrimSpace(provider)) {
	case "digitalocean":
		return strings.Contains(message, "digitalocean provider is not configured") ||
			strings.Contains(message, "digitalocean token is not configured")
	case "linode":
		return strings.Contains(message, "linode provider is not configured") ||
			strings.Contains(message, "linode token is not configured")
	case "vultr":
		return strings.Contains(message, "vultr provider is not configured") ||
			strings.Contains(message, "vultr token is not configured")
	case "aws":
		return strings.Contains(message, "aws provider is not configured") ||
			strings.Contains(message, "aws credential is not configured")
	case "azure":
		return strings.Contains(message, "azure provider is not configured") ||
			strings.Contains(message, "azure credential is not configured")
	default:
		return false
	}
}

func instanceMissingCurrentInstanceCleanup(ref map[string]interface{}, provider, entryID, label string) *currentInstanceCleanup {
	return &currentInstanceCleanup{
		Ref:        resolvedCurrentInstanceRef(ref, provider, entryID),
		Label:      strings.TrimSpace(label),
		Missing:    true,
		Assessment: buildInstanceMissingCleanupAssessment(resolvedCurrentInstanceRef(ref, provider, entryID), label),
	}
}

func providerEntryMissingCurrentInstanceCleanup(ref map[string]interface{}, provider, entryID, label string) *currentInstanceCleanup {
	return &currentInstanceCleanup{
		Ref:        resolvedCurrentInstanceRef(ref, provider, entryID),
		Label:      strings.TrimSpace(label),
		Assessment: buildProviderEntryMissingCleanupAssessment(resolvedCurrentInstanceRef(ref, provider, entryID), label),
	}
}

func providerEntryQueryCurrentInstanceCleanup(ref map[string]interface{}, provider, entryID, label string, err error) *currentInstanceCleanup {
	resolvedRef := resolvedCurrentInstanceRef(ref, provider, entryID)
	return &currentInstanceCleanup{
		Ref:        resolvedRef,
		Label:      strings.TrimSpace(label),
		Assessment: buildProviderEntryQueryCleanupAssessment(provider, resolvedRef, label, err),
	}
}

func sameAddress(target string, values ...string) bool {
	target = strings.TrimSpace(target)
	if target == "" {
		return false
	}
	normalizedTarget := normalizeIPAddress(target)
	for _, value := range values {
		trimmedValue := strings.TrimSpace(value)
		if trimmedValue == target {
			return true
		}
		if normalizedTarget == "" {
			continue
		}
		if normalizedValue := normalizeIPAddress(trimmedValue); normalizedValue != "" && normalizedValue == normalizedTarget {
			return true
		}
	}
	return false
}

func samePublicIPv4Address(target, publicIPv4 string) bool {
	return sameAddress(target, strings.TrimSpace(publicIPv4))
}

func effectiveTaskDeleteStrategy(task models.FailoverTask) string {
	for _, plan := range task.Plans {
		if plan.Enabled && planMayProvision(plan) {
			if strings.TrimSpace(task.DeleteStrategy) == models.FailoverDeleteStrategyDeleteAfterSuccessDelay {
				return models.FailoverDeleteStrategyDeleteAfterSuccessDelay
			}
			return models.FailoverDeleteStrategyDeleteAfterSuccess
		}
	}
	return models.FailoverDeleteStrategyKeep
}

func (r *executionRunner) attachCurrentOutletCleanup(outcome *actionOutcome, plan models.FailoverPlan, candidate providerPoolCandidate) error {
	if outcome == nil || outcome.Cleanup != nil {
		return nil
	}
	currentRef := parseJSONMap(r.task.CurrentInstanceRef)
	cleanup, err := resolveCurrentInstanceCleanupFromRef(r.ctx, r.task.UserID, currentRef)
	if err != nil {
		return normalizeExecutionStopError(err)
	}
	address := strings.TrimSpace(r.task.CurrentAddress)
	if cleanup == nil {
		resolvedAddress, addressErr := r.ensureCurrentOutletAddress()
		if addressErr != nil {
			return addressErr
		}
		address = strings.TrimSpace(resolvedAddress)
		if address != "" {
			cleanup, err = resolveCurrentInstanceCleanupByRefAddress(r.ctx, r.task.UserID, currentRef, address)
			if err != nil {
				return normalizeExecutionStopError(err)
			}
		}
		if address != "" {
			if cleanup == nil {
				cleanup, err = r.resolveCurrentInstanceCleanupByAddress(r.ctx, plan, candidate)
			}
			if err != nil {
				return normalizeExecutionStopError(err)
			}
		}
	}
	if cleanup == nil {
		if effectiveTaskDeleteStrategy(r.task) != models.FailoverDeleteStrategyKeep && hasCurrentOutletCleanupEvidence(currentRef, address) {
			outcome.OldInstanceRef = cloneJSONMap(currentRef)
			outcome.CleanupAssessment = buildUnresolvedCurrentInstanceCleanupAssessment(currentRef, address)
		}
		return nil
	}
	if len(cleanup.Ref) > 0 {
		rawRef := marshalJSON(cleanup.Ref)
		r.task.CurrentInstanceRef = rawRef
		_ = failoverdb.UpdateTaskFields(r.task.ID, map[string]interface{}{
			"current_instance_ref": rawRef,
		})
	}
	outcome.OldInstanceRef = cloneJSONMap(cleanup.Ref)
	outcome.CleanupLabel = cleanup.Label
	outcome.Cleanup = cleanup.Cleanup
	outcome.CleanupAssessment = cloneCleanupAssessment(cleanup.Assessment)
	return nil
}

func (r *executionRunner) ensureCurrentOutletAddress() (string, error) {
	address := strings.TrimSpace(r.task.CurrentAddress)
	if address != "" {
		return address, nil
	}

	clientUUID := strings.TrimSpace(r.task.WatchClientUUID)
	userUUID := strings.TrimSpace(r.task.UserID)
	if clientUUID == "" || userUUID == "" {
		return "", nil
	}

	client, err := clientdb.GetClientByUUIDForUser(clientUUID, userUUID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return "", nil
		}
		return "", err
	}

	address = strings.TrimSpace(firstNonEmpty(client.IPv4, client.IPv6))
	if address == "" {
		return "", nil
	}

	r.task.CurrentAddress = address
	_ = failoverdb.UpdateTaskFields(r.task.ID, map[string]interface{}{
		"current_address": address,
	})
	return address, nil
}

func (r *executionRunner) recycleCurrentOutletForCandidate(plan models.FailoverPlan, candidate providerPoolCandidate) (map[string]interface{}, error) {
	currentRef := parseJSONMap(r.task.CurrentInstanceRef)
	cleanup, err := resolveCurrentInstanceCleanupFromRef(r.ctx, r.task.UserID, currentRef)
	if err != nil {
		return nil, normalizeExecutionStopError(err)
	}
	staleCurrentOutlet := cleanup != nil && cleanup.Missing
	if cleanup != nil && !currentInstanceCleanupMatchesProviderEntry(cleanup, plan.Provider, candidate.EntryID) {
		cleanup = nil
		staleCurrentOutlet = false
	}
	if staleCurrentOutlet {
		cleanup = nil
	}
	if cleanup == nil {
		address, addressErr := r.ensureCurrentOutletAddress()
		if addressErr != nil {
			return nil, addressErr
		}
		if address != "" && currentRefMatchesProviderEntry(currentRef, plan.Provider, candidate.EntryID) {
			cleanup, err = resolveCurrentInstanceCleanupByRefAddress(r.ctx, r.task.UserID, currentRef, address)
			if err != nil {
				return nil, normalizeExecutionStopError(err)
			}
			staleCurrentOutlet = cleanup != nil && cleanup.Missing
			if cleanup != nil && !currentInstanceCleanupMatchesProviderEntry(cleanup, plan.Provider, candidate.EntryID) {
				cleanup = nil
				staleCurrentOutlet = false
			}
			if staleCurrentOutlet {
				cleanup = nil
			}
		}
		if cleanup == nil && address != "" {
			cleanup, err = r.resolveCurrentInstanceCleanupByAddress(r.ctx, plan, candidate)
			if err != nil {
				return nil, normalizeExecutionStopError(err)
			}
		}
	}
	if cleanup == nil && staleCurrentOutlet {
		reclaimStep := r.startStep("reclaim_current", "Pre-Provision Old Instance Cleanup", map[string]interface{}{
			"provider": plan.Provider,
			"entry_id": candidate.EntryID,
			"ref":      currentRef,
		})
		detail := map[string]interface{}{
			"message": "current outlet was already missing; continuing with provisioning",
			"missing": true,
		}
		if len(currentRef) > 0 {
			detail["ref"] = currentRef
		}
		r.clearCurrentOutletTracking()
		r.finishStep(reclaimStep, models.FailoverStepStatusSkipped, "current outlet was already missing; skipping delete", detail)
		return detail, nil
	}
	if cleanup == nil || cleanup.Cleanup == nil {
		return nil, nil
	}

	reclaimStep := r.startStep("reclaim_current", "Reclaim Current Outlet Capacity", map[string]interface{}{
		"provider": plan.Provider,
		"entry_id": candidate.EntryID,
		"ref":      cleanup.Ref,
	})
	if err := normalizeExecutionStopError(cleanup.Cleanup(r.ctx)); err != nil {
		r.finishStep(reclaimStep, models.FailoverStepStatusFailed, err.Error(), map[string]interface{}{
			"label": cleanup.Label,
			"ref":   cleanup.Ref,
		})
		return nil, err
	}

	detail := map[string]interface{}{
		"label": cleanup.Label,
		"ref":   cleanup.Ref,
	}
	if len(cleanup.Addresses) > 0 {
		detail["addresses"] = cleanup.Addresses
	}
	r.clearCurrentOutletTracking()
	r.finishStep(reclaimStep, models.FailoverStepStatusSuccess, "current failed outlet deleted to free capacity", detail)
	return detail, nil
}

func (r *executionRunner) clearCurrentOutletTracking() {
	r.task.CurrentInstanceRef = ""
	r.task.CurrentAddress = ""
	_ = failoverdb.UpdateTaskFields(r.task.ID, map[string]interface{}{
		"current_instance_ref": "",
		"current_address":      "",
	})
}

func resolveDigitalOceanCurrentInstanceCleanupForToken(ctx context.Context, userUUID, address string, tokenAddition *digitalocean.Addition, token *digitalocean.TokenRecord, entryID, entryName string) (*currentInstanceCleanup, error) {
	if tokenAddition == nil || token == nil {
		return nil, nil
	}
	client, err := digitalocean.NewClientFromToken(token.Token)
	if err != nil {
		return nil, err
	}
	droplets, err := client.ListDroplets(contextOrBackground(ctx))
	if err != nil {
		return nil, normalizeExecutionStopError(err)
	}
	for _, droplet := range droplets {
		if !sameAddress(address, digitalOceanPublicIPv4(&droplet), digitalOceanPublicIPv6(&droplet)) {
			continue
		}
		ref := map[string]interface{}{
			"provider":            "digitalocean",
			"provider_entry_id":   strings.TrimSpace(entryID),
			"provider_entry_name": firstNonEmpty(strings.TrimSpace(entryName), strings.TrimSpace(token.Name)),
			"droplet_id":          droplet.ID,
			"name":                droplet.Name,
			"region":              strings.TrimSpace(droplet.Region.Slug),
		}
		return &currentInstanceCleanup{
			Ref: ref,
			Addresses: map[string]interface{}{
				"ipv4": droplet.Networks.V4,
				"ipv6": droplet.Networks.V6,
			},
			Label: fmt.Sprintf("delete digitalocean droplet %d", droplet.ID),
			Cleanup: func(ctx context.Context) error {
				if err := client.DeleteDroplet(contextOrBackground(ctx), droplet.ID); err != nil {
					if isDigitalOceanNotFoundError(err) {
						removeSavedDigitalOceanRootPassword(userUUID, tokenAddition, token, droplet.ID)
						return nil
					}
					return normalizeExecutionStopError(err)
				}
				removeSavedDigitalOceanRootPassword(userUUID, tokenAddition, token, droplet.ID)
				return nil
			},
		}, nil
	}
	return nil, nil
}

func resolveLinodeCurrentInstanceCleanupForToken(ctx context.Context, userUUID, address string, tokenAddition *linodecloud.Addition, token *linodecloud.TokenRecord, entryID, entryName string) (*currentInstanceCleanup, error) {
	if tokenAddition == nil || token == nil {
		return nil, nil
	}
	client, err := linodecloud.NewClientFromToken(token.Token)
	if err != nil {
		return nil, err
	}
	instances, err := client.ListInstances(contextOrBackground(ctx))
	if err != nil {
		return nil, normalizeExecutionStopError(err)
	}
	for _, instance := range instances {
		if !sameAddress(address, append(append([]string(nil), instance.IPv4...), strings.TrimSpace(instance.IPv6))...) {
			continue
		}
		ref := map[string]interface{}{
			"provider":            "linode",
			"provider_entry_id":   strings.TrimSpace(entryID),
			"provider_entry_name": firstNonEmpty(strings.TrimSpace(entryName), strings.TrimSpace(token.Name)),
			"instance_id":         instance.ID,
			"label":               instance.Label,
			"region":              instance.Region,
		}
		return &currentInstanceCleanup{
			Ref: ref,
			Addresses: map[string]interface{}{
				"ipv4": instance.IPv4,
				"ipv6": instance.IPv6,
			},
			Label: fmt.Sprintf("delete linode instance %d", instance.ID),
			Cleanup: func(ctx context.Context) error {
				if err := client.DeleteInstance(contextOrBackground(ctx), instance.ID); err != nil {
					if isLinodeNotFoundError(err) {
						removeSavedLinodeRootPassword(userUUID, tokenAddition, token, instance.ID)
						return nil
					}
					return normalizeExecutionStopError(err)
				}
				removeSavedLinodeRootPassword(userUUID, tokenAddition, token, instance.ID)
				return nil
			},
		}, nil
	}
	return nil, nil
}

func resolveVultrCurrentInstanceCleanupForToken(ctx context.Context, userUUID, address string, tokenAddition *vultrcloud.Addition, token *vultrcloud.TokenRecord, entryID, entryName string) (*currentInstanceCleanup, error) {
	if tokenAddition == nil || token == nil {
		return nil, nil
	}
	client, err := vultrcloud.NewClientFromToken(token.Token)
	if err != nil {
		return nil, err
	}
	instances, err := client.ListInstances(contextOrBackground(ctx))
	if err != nil {
		return nil, normalizeExecutionStopError(err)
	}
	for _, instance := range instances {
		if !sameAddress(address, instance.MainIP, instance.V6MainIP) {
			continue
		}
		ref := map[string]interface{}{
			"provider":            "vultr",
			"provider_entry_id":   strings.TrimSpace(entryID),
			"provider_entry_name": firstNonEmpty(strings.TrimSpace(entryName), strings.TrimSpace(token.Name)),
			"instance_id":         instance.ID,
			"label":               instance.Label,
			"region":              instance.Region,
		}
		return &currentInstanceCleanup{
			Ref: ref,
			Addresses: map[string]interface{}{
				"main_ip":       instance.MainIP,
				"v6_main_ip":    instance.V6MainIP,
				"internal_ip":   instance.InternalIP,
				"status":        instance.Status,
				"server_status": instance.ServerStatus,
			},
			Label: "delete vultr instance " + instance.ID,
			Cleanup: func(ctx context.Context) error {
				if err := client.DeleteInstance(contextOrBackground(ctx), instance.ID); err != nil {
					if isVultrNotFoundError(err) {
						removeSavedVultrRootPassword(userUUID, tokenAddition, token, instance.ID)
						return nil
					}
					return normalizeExecutionStopError(err)
				}
				removeSavedVultrRootPassword(userUUID, tokenAddition, token, instance.ID)
				return nil
			},
		}, nil
	}
	return nil, nil
}

func (r *executionRunner) resolveCurrentInstanceCleanupByAddress(ctx context.Context, plan models.FailoverPlan, candidate providerPoolCandidate) (*currentInstanceCleanup, error) {
	address := strings.TrimSpace(r.task.CurrentAddress)
	if address == "" {
		return nil, nil
	}

	switch strings.ToLower(strings.TrimSpace(plan.Provider)) {
	case "digitalocean":
		addition, token, err := loadDigitalOceanToken(r.task.UserID, candidate.EntryID)
		if err != nil {
			return nil, err
		}
		cleanup, err := resolveDigitalOceanCurrentInstanceCleanupForToken(ctx, r.task.UserID, address, addition, token, candidate.EntryID, candidate.EntryName)
		if err != nil {
			return nil, err
		}
		if cleanup != nil {
			return cleanup, nil
		}
		return nil, nil
	case "linode":
		addition, token, err := loadLinodeToken(r.task.UserID, candidate.EntryID)
		if err != nil {
			return nil, err
		}
		cleanup, err := resolveLinodeCurrentInstanceCleanupForToken(ctx, r.task.UserID, address, addition, token, candidate.EntryID, candidate.EntryName)
		if err != nil {
			return nil, err
		}
		if cleanup != nil {
			return cleanup, nil
		}
		return nil, nil
	case "vultr":
		addition, token, err := loadVultrToken(r.task.UserID, candidate.EntryID)
		if err != nil {
			return nil, err
		}
		cleanup, err := resolveVultrCurrentInstanceCleanupForToken(ctx, r.task.UserID, address, addition, token, candidate.EntryID, candidate.EntryName)
		if err != nil {
			return nil, err
		}
		if cleanup != nil {
			return cleanup, nil
		}
		return nil, nil
	case "azure":
		addition, credential, err := loadAzureCredential(r.task.UserID, candidate.EntryID)
		if err != nil {
			return nil, err
		}
		client, err := azurecloud.NewClientFromCredential(credential)
		if err != nil {
			return nil, err
		}
		instances, err := client.ListVirtualMachines(contextOrBackground(ctx))
		if err != nil {
			return nil, normalizeExecutionStopError(err)
		}
		for _, instance := range instances {
			found := false
			for _, publicIP := range instance.PublicIPs {
				if sameAddress(address, publicIP) {
					found = true
					break
				}
			}
			if !found {
				continue
			}
			ref := map[string]interface{}{
				"provider":            "azure",
				"provider_entry_id":   candidate.EntryID,
				"provider_entry_name": candidate.EntryName,
				"region":              instance.Location,
				"location":            instance.Location,
				"resource_group":      instance.ResourceGroup,
				"instance_id":         instance.InstanceID,
				"name":                instance.Name,
			}
			return &currentInstanceCleanup{
				Ref: ref,
				Addresses: map[string]interface{}{
					"public_ips":  instance.PublicIPs,
					"private_ips": instance.PrivateIPs,
				},
				Label: "delete azure vm " + instance.Name,
				Cleanup: func(ctx context.Context) error {
					if _, err := client.DeleteVirtualMachine(contextOrBackground(ctx), instance.ResourceGroup, instance.Name); err != nil {
						if isAzureNotFoundError(err) {
							removeSavedAzureRootPassword(r.task.UserID, addition, credential, instance.InstanceID)
							return nil
						}
						return normalizeExecutionStopError(err)
					}
					removeSavedAzureRootPassword(r.task.UserID, addition, credential, instance.InstanceID)
					return nil
				},
			}, nil
		}
		return nil, nil
	case "aws":
		addition, credential, err := loadAWSCredential(r.task.UserID, candidate.EntryID)
		if err != nil {
			return nil, err
		}
		region := resolveAWSPlanRegion(plan, addition, credential)
		service := resolveAWSPlanService(plan)
		if service == "lightsail" {
			instances, err := awscloud.ListLightsailInstances(contextOrBackground(ctx), credential, region)
			if err != nil {
				return nil, normalizeExecutionStopError(err)
			}
			for _, instance := range instances {
				if !samePublicIPv4Address(address, instance.PublicIP) {
					continue
				}
				ref := map[string]interface{}{
					"provider":            "aws",
					"service":             "lightsail",
					"provider_entry_id":   candidate.EntryID,
					"provider_entry_name": candidate.EntryName,
					"region":              region,
					"instance_name":       instance.Name,
				}
				return &currentInstanceCleanup{
					Ref: ref,
					Addresses: map[string]interface{}{
						"public_ip":      instance.PublicIP,
						"private_ip":     instance.PrivateIP,
						"ipv6_addresses": instance.IPv6Addresses,
					},
					Label: "delete aws lightsail instance " + instance.Name,
					Cleanup: func(ctx context.Context) error {
						if err := awscloud.DeleteLightsailInstance(contextOrBackground(ctx), credential, region, instance.Name); err != nil {
							if isAWSResourceNotFoundError("lightsail", err) {
								removeSavedAWSRootPassword(r.task.UserID, addition, credential, "lightsail", region, instance.Name)
								return nil
							}
							return normalizeExecutionStopError(err)
						}
						removeSavedAWSRootPassword(r.task.UserID, addition, credential, "lightsail", region, instance.Name)
						return nil
					},
				}, nil
			}
			return nil, nil
		}
		instances, err := awscloud.ListInstances(contextOrBackground(ctx), credential, region)
		if err != nil {
			return nil, normalizeExecutionStopError(err)
		}
		for _, instance := range instances {
			if !samePublicIPv4Address(address, instance.PublicIP) {
				continue
			}
			ref := map[string]interface{}{
				"provider":            "aws",
				"service":             "ec2",
				"provider_entry_id":   candidate.EntryID,
				"provider_entry_name": candidate.EntryName,
				"region":              region,
				"instance_id":         instance.InstanceID,
				"name":                instance.Name,
			}
			return &currentInstanceCleanup{
				Ref: ref,
				Addresses: map[string]interface{}{
					"public_ip":      instance.PublicIP,
					"private_ip":     instance.PrivateIP,
					"ipv6_addresses": instance.IPv6Addresses,
				},
				Label: "terminate aws ec2 instance " + instance.InstanceID,
				Cleanup: func(ctx context.Context) error {
					if err := awscloud.TerminateInstance(contextOrBackground(ctx), credential, region, instance.InstanceID); err != nil {
						if isAWSResourceNotFoundError("ec2", err) {
							removeSavedAWSRootPassword(r.task.UserID, addition, credential, "ec2", region, instance.InstanceID)
							return nil
						}
						return normalizeExecutionStopError(err)
					}
					removeSavedAWSRootPassword(r.task.UserID, addition, credential, "ec2", region, instance.InstanceID)
					return nil
				},
			}, nil
		}
		return nil, nil
	default:
		return nil, nil
	}
}

func resolveCurrentInstanceCleanupFromRef(ctx context.Context, userUUID string, ref map[string]interface{}) (*currentInstanceCleanup, error) {
	if len(ref) == 0 {
		return nil, nil
	}

	provider := strings.ToLower(strings.TrimSpace(stringMapValue(ref, "provider")))
	entryID := strings.TrimSpace(providerEntryIDFromRef(ref))
	if provider == "" || entryID == "" {
		return nil, nil
	}

	switch provider {
	case "digitalocean":
		dropletID := intMapValue(ref, "droplet_id")
		if dropletID <= 0 {
			return nil, nil
		}
		resolvedRef := resolvedCurrentInstanceRef(ref, "digitalocean", entryID)
		addition, token, err := loadDigitalOceanToken(userUUID, entryID)
		if err != nil {
			if isCurrentInstanceCredentialMissingError("digitalocean", err) {
				return providerEntryMissingCurrentInstanceCleanup(ref, "digitalocean", entryID, fmt.Sprintf("delete digitalocean droplet %d", dropletID)), nil
			}
			return nil, err
		}
		client, err := digitalocean.NewClientFromToken(token.Token)
		if err != nil {
			return nil, err
		}
		droplets, err := client.ListDroplets(contextOrBackground(ctx))
		if err != nil {
			err = normalizeExecutionStopError(err)
			if errors.Is(err, errExecutionStopped) {
				return nil, err
			}
			return providerEntryQueryCurrentInstanceCleanup(ref, "digitalocean", entryID, fmt.Sprintf("delete digitalocean droplet %d", dropletID), err), nil
		}
		exists := false
		for _, droplet := range droplets {
			if droplet.ID == dropletID {
				exists = true
				break
			}
		}
		if !exists {
			return instanceMissingCurrentInstanceCleanup(ref, "digitalocean", entryID, fmt.Sprintf("delete digitalocean droplet %d", dropletID)), nil
		}
		return &currentInstanceCleanup{
			Ref:   resolvedRef,
			Label: fmt.Sprintf("delete digitalocean droplet %d", dropletID),
			Cleanup: func(ctx context.Context) error {
				if err := client.DeleteDroplet(contextOrBackground(ctx), dropletID); err != nil {
					if isDigitalOceanNotFoundError(err) {
						removeSavedDigitalOceanRootPassword(userUUID, addition, token, dropletID)
						return nil
					}
					return normalizeExecutionStopError(err)
				}
				removeSavedDigitalOceanRootPassword(userUUID, addition, token, dropletID)
				return nil
			},
		}, nil
	case "linode":
		instanceID := intMapValue(ref, "instance_id")
		if instanceID <= 0 {
			return nil, nil
		}
		resolvedRef := resolvedCurrentInstanceRef(ref, "linode", entryID)
		addition, token, err := loadLinodeToken(userUUID, entryID)
		if err != nil {
			if isCurrentInstanceCredentialMissingError("linode", err) {
				return providerEntryMissingCurrentInstanceCleanup(ref, "linode", entryID, fmt.Sprintf("delete linode instance %d", instanceID)), nil
			}
			return nil, err
		}
		client, err := linodecloud.NewClientFromToken(token.Token)
		if err != nil {
			return nil, err
		}
		if _, err := client.GetInstance(contextOrBackground(ctx), instanceID); err != nil {
			if isLinodeNotFoundError(err) {
				return instanceMissingCurrentInstanceCleanup(ref, "linode", entryID, fmt.Sprintf("delete linode instance %d", instanceID)), nil
			}
			err = normalizeExecutionStopError(err)
			if errors.Is(err, errExecutionStopped) {
				return nil, err
			}
			return providerEntryQueryCurrentInstanceCleanup(ref, "linode", entryID, fmt.Sprintf("delete linode instance %d", instanceID), err), nil
		}
		return &currentInstanceCleanup{
			Ref:   resolvedRef,
			Label: fmt.Sprintf("delete linode instance %d", instanceID),
			Cleanup: func(ctx context.Context) error {
				if err := client.DeleteInstance(contextOrBackground(ctx), instanceID); err != nil {
					if isLinodeNotFoundError(err) {
						removeSavedLinodeRootPassword(userUUID, addition, token, instanceID)
						return nil
					}
					return normalizeExecutionStopError(err)
				}
				removeSavedLinodeRootPassword(userUUID, addition, token, instanceID)
				return nil
			},
		}, nil
	case "vultr":
		instanceID := strings.TrimSpace(stringMapValue(ref, "instance_id"))
		if instanceID == "" {
			return nil, nil
		}
		resolvedRef := resolvedCurrentInstanceRef(ref, "vultr", entryID)
		addition, token, err := loadVultrToken(userUUID, entryID)
		if err != nil {
			if isCurrentInstanceCredentialMissingError("vultr", err) {
				return providerEntryMissingCurrentInstanceCleanup(ref, "vultr", entryID, "delete vultr instance "+instanceID), nil
			}
			return nil, err
		}
		client, err := vultrcloud.NewClientFromToken(token.Token)
		if err != nil {
			return nil, err
		}
		if _, err := client.GetInstance(contextOrBackground(ctx), instanceID); err != nil {
			if isVultrNotFoundError(err) {
				return instanceMissingCurrentInstanceCleanup(ref, "vultr", entryID, "delete vultr instance "+instanceID), nil
			}
			err = normalizeExecutionStopError(err)
			if errors.Is(err, errExecutionStopped) {
				return nil, err
			}
			return providerEntryQueryCurrentInstanceCleanup(ref, "vultr", entryID, "delete vultr instance "+instanceID, err), nil
		}
		return &currentInstanceCleanup{
			Ref:   resolvedRef,
			Label: "delete vultr instance " + instanceID,
			Cleanup: func(ctx context.Context) error {
				if err := client.DeleteInstance(contextOrBackground(ctx), instanceID); err != nil {
					if isVultrNotFoundError(err) {
						removeSavedVultrRootPassword(userUUID, addition, token, instanceID)
						return nil
					}
					return normalizeExecutionStopError(err)
				}
				removeSavedVultrRootPassword(userUUID, addition, token, instanceID)
				return nil
			},
		}, nil
	case "azure":
		instanceID := strings.TrimSpace(stringMapValue(ref, "instance_id"))
		resourceGroup := strings.TrimSpace(stringMapValue(ref, "resource_group"))
		instanceName := strings.TrimSpace(firstNonEmpty(stringMapValue(ref, "name"), stringMapValue(ref, "instance_name")))
		if instanceID != "" && (resourceGroup == "" || instanceName == "") {
			if decodedGroup, decodedName, decodeErr := azurecloud.DecodeInstanceID(instanceID); decodeErr == nil {
				resourceGroup = firstNonEmpty(resourceGroup, decodedGroup)
				instanceName = firstNonEmpty(instanceName, decodedName)
			}
		}
		if resourceGroup == "" || instanceName == "" {
			return nil, nil
		}
		if instanceID == "" {
			instanceID = azurecloud.EncodeInstanceID(resourceGroup, instanceName)
		}
		resolvedRef := resolvedCurrentInstanceRef(ref, "azure", entryID)
		resolvedRef["resource_group"] = resourceGroup
		resolvedRef["name"] = instanceName
		resolvedRef["instance_id"] = instanceID
		addition, credential, err := loadAzureCredential(userUUID, entryID)
		if err != nil {
			if isCurrentInstanceCredentialMissingError("azure", err) {
				return providerEntryMissingCurrentInstanceCleanup(ref, "azure", entryID, "delete azure vm "+instanceName), nil
			}
			return nil, err
		}
		client, err := azurecloud.NewClientFromCredential(credential)
		if err != nil {
			return nil, err
		}
		if _, err := client.GetVirtualMachineDetail(contextOrBackground(ctx), resourceGroup, instanceName); err != nil {
			if isAzureNotFoundError(err) {
				return instanceMissingCurrentInstanceCleanup(ref, "azure", entryID, "delete azure vm "+instanceName), nil
			}
			err = normalizeExecutionStopError(err)
			if errors.Is(err, errExecutionStopped) {
				return nil, err
			}
			return providerEntryQueryCurrentInstanceCleanup(ref, "azure", entryID, "delete azure vm "+instanceName, err), nil
		}
		return &currentInstanceCleanup{
			Ref:   resolvedRef,
			Label: "delete azure vm " + instanceName,
			Cleanup: func(ctx context.Context) error {
				if _, err := client.DeleteVirtualMachine(contextOrBackground(ctx), resourceGroup, instanceName); err != nil {
					if isAzureNotFoundError(err) {
						removeSavedAzureRootPassword(userUUID, addition, credential, instanceID)
						return nil
					}
					return normalizeExecutionStopError(err)
				}
				removeSavedAzureRootPassword(userUUID, addition, credential, instanceID)
				return nil
			},
		}, nil
	case "aws":
		service := strings.ToLower(strings.TrimSpace(firstNonEmpty(stringMapValue(ref, "service"), "ec2")))
		region := strings.TrimSpace(stringMapValue(ref, "region"))
		if region == "" {
			return nil, nil
		}
		resolvedRef := resolvedCurrentInstanceRef(ref, "aws", entryID)
		addition, credential, err := loadAWSCredential(userUUID, entryID)
		if err != nil {
			if isCurrentInstanceCredentialMissingError("aws", err) {
				switch service {
				case "lightsail":
					instanceName := strings.TrimSpace(stringMapValue(ref, "instance_name"))
					if instanceName == "" {
						return nil, nil
					}
					return providerEntryMissingCurrentInstanceCleanup(ref, "aws", entryID, "delete aws lightsail instance "+instanceName), nil
				default:
					instanceID := strings.TrimSpace(stringMapValue(ref, "instance_id"))
					if instanceID == "" {
						return nil, nil
					}
					return providerEntryMissingCurrentInstanceCleanup(ref, "aws", entryID, "terminate aws ec2 instance "+instanceID), nil
				}
			}
			return nil, err
		}
		switch service {
		case "lightsail":
			instanceName := strings.TrimSpace(stringMapValue(ref, "instance_name"))
			if instanceName == "" {
				return nil, nil
			}
			instances, err := awscloud.ListLightsailInstances(contextOrBackground(ctx), credential, region)
			if err != nil {
				err = normalizeExecutionStopError(err)
				if errors.Is(err, errExecutionStopped) {
					return nil, err
				}
				return providerEntryQueryCurrentInstanceCleanup(ref, "aws", entryID, "delete aws lightsail instance "+instanceName, err), nil
			}
			exists := false
			for _, instance := range instances {
				if strings.TrimSpace(instance.Name) == instanceName {
					exists = true
					break
				}
			}
			if !exists {
				return instanceMissingCurrentInstanceCleanup(ref, "aws", entryID, "delete aws lightsail instance "+instanceName), nil
			}
			return &currentInstanceCleanup{
				Ref:   resolvedRef,
				Label: "delete aws lightsail instance " + instanceName,
				Cleanup: func(ctx context.Context) error {
					if err := awscloud.DeleteLightsailInstance(contextOrBackground(ctx), credential, region, instanceName); err != nil {
						if isAWSResourceNotFoundError("lightsail", err) {
							removeSavedAWSRootPassword(userUUID, addition, credential, "lightsail", region, instanceName)
							return nil
						}
						return normalizeExecutionStopError(err)
					}
					removeSavedAWSRootPassword(userUUID, addition, credential, "lightsail", region, instanceName)
					return nil
				},
			}, nil
		default:
			instanceID := strings.TrimSpace(stringMapValue(ref, "instance_id"))
			if instanceID == "" {
				return nil, nil
			}
			resolvedRef["service"] = "ec2"
			instances, err := awscloud.ListInstances(contextOrBackground(ctx), credential, region)
			if err != nil {
				err = normalizeExecutionStopError(err)
				if errors.Is(err, errExecutionStopped) {
					return nil, err
				}
				return providerEntryQueryCurrentInstanceCleanup(ref, "aws", entryID, "terminate aws ec2 instance "+instanceID, err), nil
			}
			exists := false
			for _, instance := range instances {
				if strings.TrimSpace(instance.InstanceID) == instanceID {
					exists = true
					break
				}
			}
			if !exists {
				return instanceMissingCurrentInstanceCleanup(ref, "aws", entryID, "terminate aws ec2 instance "+instanceID), nil
			}
			return &currentInstanceCleanup{
				Ref:   resolvedRef,
				Label: "terminate aws ec2 instance " + instanceID,
				Cleanup: func(ctx context.Context) error {
					if err := awscloud.TerminateInstance(contextOrBackground(ctx), credential, region, instanceID); err != nil {
						if isAWSResourceNotFoundError("ec2", err) {
							removeSavedAWSRootPassword(userUUID, addition, credential, "ec2", region, instanceID)
							return nil
						}
						return normalizeExecutionStopError(err)
					}
					removeSavedAWSRootPassword(userUUID, addition, credential, "ec2", region, instanceID)
					return nil
				},
			}, nil
		}
	default:
		return nil, nil
	}
}

func resolveCurrentInstanceCleanupByRefAddress(ctx context.Context, userUUID string, ref map[string]interface{}, address string) (*currentInstanceCleanup, error) {
	if len(ref) == 0 || strings.TrimSpace(address) == "" {
		return nil, nil
	}

	provider := strings.ToLower(strings.TrimSpace(stringMapValue(ref, "provider")))
	entryID := strings.TrimSpace(providerEntryIDFromRef(ref))
	if provider == "" || entryID == "" {
		return nil, nil
	}

	label := currentInstanceCleanupLabelFromRef(ref, address)
	switch provider {
	case "digitalocean":
		addition, token, err := loadDigitalOceanToken(userUUID, entryID)
		if err != nil {
			if isCurrentInstanceCredentialMissingError("digitalocean", err) {
				return providerEntryMissingCurrentInstanceCleanup(ref, "digitalocean", entryID, label), nil
			}
			return nil, err
		}
		cleanup, err := resolveDigitalOceanCurrentInstanceCleanupForToken(ctx, userUUID, address, addition, token, entryID, providerEntryNameFromRef(ref))
		if err != nil {
			err = normalizeExecutionStopError(err)
			if errors.Is(err, errExecutionStopped) {
				return nil, err
			}
			return providerEntryQueryCurrentInstanceCleanup(ref, "digitalocean", entryID, label, err), nil
		}
		if cleanup != nil {
			return cleanup, nil
		}
		return instanceMissingCurrentInstanceCleanup(ref, "digitalocean", entryID, label), nil
	case "linode":
		addition, token, err := loadLinodeToken(userUUID, entryID)
		if err != nil {
			if isCurrentInstanceCredentialMissingError("linode", err) {
				return providerEntryMissingCurrentInstanceCleanup(ref, "linode", entryID, label), nil
			}
			return nil, err
		}
		cleanup, err := resolveLinodeCurrentInstanceCleanupForToken(ctx, userUUID, address, addition, token, entryID, providerEntryNameFromRef(ref))
		if err != nil {
			err = normalizeExecutionStopError(err)
			if errors.Is(err, errExecutionStopped) {
				return nil, err
			}
			return providerEntryQueryCurrentInstanceCleanup(ref, "linode", entryID, label, err), nil
		}
		if cleanup != nil {
			return cleanup, nil
		}
		return instanceMissingCurrentInstanceCleanup(ref, "linode", entryID, label), nil
	case "vultr":
		addition, token, err := loadVultrToken(userUUID, entryID)
		if err != nil {
			if isCurrentInstanceCredentialMissingError("vultr", err) {
				return providerEntryMissingCurrentInstanceCleanup(ref, "vultr", entryID, label), nil
			}
			return nil, err
		}
		cleanup, err := resolveVultrCurrentInstanceCleanupForToken(ctx, userUUID, address, addition, token, entryID, providerEntryNameFromRef(ref))
		if err != nil {
			err = normalizeExecutionStopError(err)
			if errors.Is(err, errExecutionStopped) {
				return nil, err
			}
			return providerEntryQueryCurrentInstanceCleanup(ref, "vultr", entryID, label, err), nil
		}
		if cleanup != nil {
			return cleanup, nil
		}
		return instanceMissingCurrentInstanceCleanup(ref, "vultr", entryID, label), nil
	case "azure":
		addition, credential, err := loadAzureCredential(userUUID, entryID)
		if err != nil {
			if isCurrentInstanceCredentialMissingError("azure", err) {
				return providerEntryMissingCurrentInstanceCleanup(ref, "azure", entryID, label), nil
			}
			return nil, err
		}
		client, err := azurecloud.NewClientFromCredential(credential)
		if err != nil {
			return nil, err
		}
		instances, err := client.ListVirtualMachines(contextOrBackground(ctx))
		if err != nil {
			err = normalizeExecutionStopError(err)
			if errors.Is(err, errExecutionStopped) {
				return nil, err
			}
			return providerEntryQueryCurrentInstanceCleanup(ref, "azure", entryID, label, err), nil
		}
		for _, instance := range instances {
			found := false
			for _, publicIP := range instance.PublicIPs {
				if sameAddress(address, publicIP) {
					found = true
					break
				}
			}
			if !found {
				continue
			}
			resolvedRef := resolvedCurrentInstanceRef(ref, "azure", entryID)
			resolvedRef["region"] = instance.Location
			resolvedRef["location"] = instance.Location
			resolvedRef["resource_group"] = instance.ResourceGroup
			resolvedRef["instance_id"] = instance.InstanceID
			resolvedRef["name"] = instance.Name
			return &currentInstanceCleanup{
				Ref: resolvedRef,
				Addresses: map[string]interface{}{
					"public_ips":  instance.PublicIPs,
					"private_ips": instance.PrivateIPs,
				},
				Label: "delete azure vm " + instance.Name,
				Cleanup: func(ctx context.Context) error {
					if _, err := client.DeleteVirtualMachine(contextOrBackground(ctx), instance.ResourceGroup, instance.Name); err != nil {
						if isAzureNotFoundError(err) {
							removeSavedAzureRootPassword(userUUID, addition, credential, instance.InstanceID)
							return nil
						}
						return normalizeExecutionStopError(err)
					}
					removeSavedAzureRootPassword(userUUID, addition, credential, instance.InstanceID)
					return nil
				},
			}, nil
		}
		return instanceMissingCurrentInstanceCleanup(ref, "azure", entryID, label), nil
	case "aws":
		service := strings.ToLower(strings.TrimSpace(firstNonEmpty(stringMapValue(ref, "service"), "ec2")))
		region := strings.TrimSpace(stringMapValue(ref, "region"))
		if region == "" {
			return nil, nil
		}
		addition, credential, err := loadAWSCredential(userUUID, entryID)
		if err != nil {
			if isCurrentInstanceCredentialMissingError("aws", err) {
				return providerEntryMissingCurrentInstanceCleanup(ref, "aws", entryID, label), nil
			}
			return nil, err
		}
		if service == "lightsail" {
			instances, err := awscloud.ListLightsailInstances(contextOrBackground(ctx), credential, region)
			if err != nil {
				err = normalizeExecutionStopError(err)
				if errors.Is(err, errExecutionStopped) {
					return nil, err
				}
				return providerEntryQueryCurrentInstanceCleanup(ref, "aws", entryID, label, err), nil
			}
			for _, instance := range instances {
				if !samePublicIPv4Address(address, instance.PublicIP) {
					continue
				}
				resolvedRef := resolvedCurrentInstanceRef(ref, "aws", entryID)
				resolvedRef["service"] = "lightsail"
				resolvedRef["region"] = region
				resolvedRef["instance_name"] = instance.Name
				return &currentInstanceCleanup{
					Ref: resolvedRef,
					Addresses: map[string]interface{}{
						"public_ip":      instance.PublicIP,
						"private_ip":     instance.PrivateIP,
						"ipv6_addresses": instance.IPv6Addresses,
					},
					Label: "delete aws lightsail instance " + instance.Name,
					Cleanup: func(ctx context.Context) error {
						if err := awscloud.DeleteLightsailInstance(contextOrBackground(ctx), credential, region, instance.Name); err != nil {
							if isAWSResourceNotFoundError("lightsail", err) {
								removeSavedAWSRootPassword(userUUID, addition, credential, "lightsail", region, instance.Name)
								return nil
							}
							return normalizeExecutionStopError(err)
						}
						removeSavedAWSRootPassword(userUUID, addition, credential, "lightsail", region, instance.Name)
						return nil
					},
				}, nil
			}
			return instanceMissingCurrentInstanceCleanup(ref, "aws", entryID, label), nil
		}
		instances, err := awscloud.ListInstances(contextOrBackground(ctx), credential, region)
		if err != nil {
			err = normalizeExecutionStopError(err)
			if errors.Is(err, errExecutionStopped) {
				return nil, err
			}
			return providerEntryQueryCurrentInstanceCleanup(ref, "aws", entryID, label, err), nil
		}
		for _, instance := range instances {
			if !samePublicIPv4Address(address, instance.PublicIP) {
				continue
			}
			resolvedRef := resolvedCurrentInstanceRef(ref, "aws", entryID)
			resolvedRef["service"] = "ec2"
			resolvedRef["region"] = region
			resolvedRef["instance_id"] = instance.InstanceID
			if strings.TrimSpace(instance.Name) != "" {
				resolvedRef["name"] = instance.Name
			}
			return &currentInstanceCleanup{
				Ref: resolvedRef,
				Addresses: map[string]interface{}{
					"public_ip":      instance.PublicIP,
					"private_ip":     instance.PrivateIP,
					"ipv6_addresses": instance.IPv6Addresses,
				},
				Label: "terminate aws ec2 instance " + instance.InstanceID,
				Cleanup: func(ctx context.Context) error {
					if err := awscloud.TerminateInstance(contextOrBackground(ctx), credential, region, instance.InstanceID); err != nil {
						if isAWSResourceNotFoundError("ec2", err) {
							removeSavedAWSRootPassword(userUUID, addition, credential, "ec2", region, instance.InstanceID)
							return nil
						}
						return normalizeExecutionStopError(err)
					}
					removeSavedAWSRootPassword(userUUID, addition, credential, "ec2", region, instance.InstanceID)
					return nil
				},
			}, nil
		}
		return instanceMissingCurrentInstanceCleanup(ref, "aws", entryID, label), nil
	default:
		return nil, nil
	}
}

func primaryOutcomeAddress(outcome *actionOutcome) string {
	if outcome == nil {
		return ""
	}
	if ipv4 := strings.TrimSpace(outcome.IPv4); ipv4 != "" {
		return ipv4
	}
	return strings.TrimSpace(outcome.IPv6)
}

type commandResult struct {
	TaskID     string
	Output     string
	ExitCode   *int
	FinishedAt *models.LocalTime
	Truncated  bool
}

func ensureCommandResult(result *commandResult) *commandResult {
	if result != nil {
		return result
	}
	return &commandResult{}
}

func commandResultExecutionError(result *commandResult) error {
	if result == nil || result.ExitCode == nil || *result.ExitCode == 0 {
		return nil
	}

	message := fmt.Sprintf("script exited with code %d", *result.ExitCode)
	if excerpt := firstMeaningfulOutputLine(result.Output); excerpt != "" {
		message += ": " + excerpt
	}
	return errors.New(message)
}

func firstMeaningfulOutputLine(output string) string {
	for _, line := range strings.Split(strings.ReplaceAll(output, "\r\n", "\n"), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if len(trimmed) > 200 {
			return trimmed[:200]
		}
		return trimmed
	}
	return ""
}

func dispatchScriptToClient(ctx context.Context, userUUID, clientUUID, command string, timeout time.Duration) (*commandResult, error) {
	command = strings.TrimSpace(command)
	if command == "" {
		return nil, errors.New("script content is empty")
	}

	client := ws.GetConnectedClients()[clientUUID]
	if client == nil {
		return nil, fmt.Errorf("client is offline: %s", clientUUID)
	}

	taskID := utils.GenerateRandomString(16)
	if err := tasks.CreateTaskForUser(userUUID, taskID, []string{clientUUID}); err != nil {
		return nil, err
	}

	payload, err := json.Marshal(map[string]string{
		"message": "exec",
		"command": command,
		"task_id": taskID,
	})
	if err != nil {
		return nil, err
	}
	writeTimeout := 10 * time.Second
	if timeout > 0 && timeout < writeTimeout {
		writeTimeout = timeout
	}
	if err := client.WriteMessageWithDeadline(websocket.TextMessage, payload, time.Now().Add(writeTimeout)); err != nil {
		return &commandResult{TaskID: taskID}, err
	}

	ctx = contextOrBackground(ctx)
	var cancel context.CancelFunc
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, timeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return &commandResult{TaskID: taskID}, normalizeExecutionStopError(ctx.Err())
		case <-ticker.C:
			result, err := tasks.GetSpecificTaskResultForUser(userUUID, taskID, clientUUID)
			if err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					continue
				}
				return &commandResult{TaskID: taskID}, err
			}
			if result.FinishedAt == nil {
				continue
			}
			output, truncated := truncateOutput(result.Result, 65535)
			return &commandResult{
				TaskID:     taskID,
				Output:     output,
				ExitCode:   result.ExitCode,
				FinishedAt: result.FinishedAt,
				Truncated:  truncated,
			}, nil
		}
	}
}

func truncateOutput(output string, limit int) (string, bool) {
	if limit <= 0 || len(output) <= limit {
		return output, false
	}
	return output[:limit], true
}

func waitForClientByGroup(
	ctx context.Context,
	userUUID,
	group,
	excludeUUID string,
	startedAt time.Time,
	timeoutSeconds int,
	expectedAddresses map[string]struct{},
) (string, error) {
	if timeoutSeconds <= 0 {
		timeoutSeconds = 600
	}
	deadline := time.Now().Add(time.Duration(timeoutSeconds) * time.Second)
	group = strings.TrimSpace(group)

	for time.Now().Before(deadline) {
		if err := waitContextOrDelay(ctx, 0); err != nil {
			return "", err
		}
		clientList, err := clientdb.GetAllClientBasicInfoByUser(userUUID)
		if err != nil {
			return "", err
		}
		online := ws.GetConnectedClients()

		candidates := make([]models.Client, 0)
		for _, client := range clientList {
			if strings.TrimSpace(client.Group) != group || client.UUID == excludeUUID {
				continue
			}
			if _, ok := online[client.UUID]; !ok {
				continue
			}
			candidates = append(candidates, client)
		}
		if clientUUID := pickPreferredAutoConnectClient(candidates, startedAt, expectedAddresses); clientUUID != "" {
			return clientUUID, nil
		}
		if err := waitContextOrDelay(ctx, 5*time.Second); err != nil {
			return "", err
		}
	}
	return "", fmt.Errorf("timed out waiting for agent group %q", group)
}

func expectedClientAddresses(outcome *actionOutcome) map[string]struct{} {
	addresses := make(map[string]struct{})
	if outcome == nil {
		return addresses
	}

	for _, value := range []string{
		outcome.IPv4,
		outcome.IPv6,
		primaryOutcomeAddress(outcome),
	} {
		addExpectedClientAddress(addresses, value)
	}
	collectExpectedAddresses(addresses, outcome.NewAddresses)
	return addresses
}

func addExpectedClientAddress(addresses map[string]struct{}, value string) {
	normalized := normalizeIPAddress(value)
	if normalized == "" {
		return
	}
	addresses[normalized] = struct{}{}
}

func collectExpectedAddresses(addresses map[string]struct{}, value interface{}) {
	switch raw := value.(type) {
	case map[string]interface{}:
		for _, nested := range raw {
			collectExpectedAddresses(addresses, nested)
		}
	case []interface{}:
		for _, nested := range raw {
			collectExpectedAddresses(addresses, nested)
		}
	case []string:
		for _, nested := range raw {
			addExpectedClientAddress(addresses, nested)
		}
	case string:
		addExpectedClientAddress(addresses, raw)
	}
}

func normalizeIPAddress(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	ip := net.ParseIP(value)
	if ip != nil {
		return ip.String()
	}
	ip, _, err := net.ParseCIDR(value)
	if err != nil || ip == nil {
		return ""
	}
	return ip.String()
}

func clientMatchesExpectedAddress(client models.Client, expectedAddresses map[string]struct{}) bool {
	if len(expectedAddresses) == 0 {
		return false
	}
	for _, value := range []string{client.IPv4, client.IPv6} {
		normalized := normalizeIPAddress(value)
		if normalized == "" {
			continue
		}
		if _, ok := expectedAddresses[normalized]; ok {
			return true
		}
	}
	return false
}

func clientCreatedForExecution(client models.Client, startedAt time.Time) bool {
	createdAt := client.CreatedAt.ToTime()
	if createdAt.IsZero() {
		return false
	}
	return !createdAt.Before(startedAt)
}

func pickPreferredAutoConnectClient(
	candidates []models.Client,
	startedAt time.Time,
	expectedAddresses map[string]struct{},
) string {
	if len(candidates) == 0 {
		return ""
	}

	ipMatches := make([]models.Client, 0, len(candidates))
	newClients := make([]models.Client, 0, len(candidates))
	for _, client := range candidates {
		if clientMatchesExpectedAddress(client, expectedAddresses) {
			ipMatches = append(ipMatches, client)
		}
		if clientCreatedForExecution(client, startedAt) {
			newClients = append(newClients, client)
		}
	}

	if len(ipMatches) > 0 {
		sortClientsNewestFirst(ipMatches)
		return ipMatches[0].UUID
	}
	if len(expectedAddresses) > 0 {
		return ""
	}
	if len(newClients) > 0 {
		sortClientsNewestFirst(newClients)
		return newClients[0].UUID
	}
	return ""
}

func sortClientsNewestFirst(clients []models.Client) {
	for i := 0; i < len(clients); i++ {
		for j := i + 1; j < len(clients); j++ {
			if clients[j].CreatedAt.ToTime().After(clients[i].CreatedAt.ToTime()) {
				clients[i], clients[j] = clients[j], clients[i]
			}
		}
	}
}

func buildTriggerSnapshot(report *common.Report) string {
	if report == nil || report.CNConnectivity == nil {
		return "null"
	}
	return marshalJSON(map[string]interface{}{
		"status":               report.CNConnectivity.Status,
		"target":               report.CNConnectivity.Target,
		"latency":              report.CNConnectivity.Latency,
		"message":              report.CNConnectivity.Message,
		"checked_at":           report.CNConnectivity.CheckedAt,
		"consecutive_failures": report.CNConnectivity.ConsecutiveFailures,
		"report_updated_at":    report.UpdatedAt,
	})
}

func cloneReport(report *common.Report) *common.Report {
	if report == nil {
		return nil
	}
	payload, err := json.Marshal(report)
	if err != nil {
		return report
	}
	var cloned common.Report
	if err := json.Unmarshal(payload, &cloned); err != nil {
		return report
	}
	return &cloned
}

func claimTaskRun(taskID uint) bool {
	runningTasksMu.Lock()
	defer runningTasksMu.Unlock()
	if _, exists := runningTasks[taskID]; exists {
		return false
	}
	runningTasks[taskID] = struct{}{}
	return true
}

func releaseTaskRun(taskID uint) {
	runningTasksMu.Lock()
	defer runningTasksMu.Unlock()
	delete(runningTasks, taskID)
}

func claimTargetRun(targetKey string, taskID uint) (uint, bool) {
	targetKey = strings.TrimSpace(targetKey)
	if targetKey == "" || taskID == 0 {
		return 0, true
	}

	runningTargetMu.Lock()
	defer runningTargetMu.Unlock()
	if activeTaskID, exists := runningTargets[targetKey]; exists && activeTaskID != taskID {
		return activeTaskID, false
	}
	runningTargets[targetKey] = taskID
	return 0, true
}

func releaseTargetRun(targetKey string, taskID uint) {
	targetKey = strings.TrimSpace(targetKey)
	if targetKey == "" || taskID == 0 {
		return
	}

	runningTargetMu.Lock()
	defer runningTargetMu.Unlock()
	if activeTaskID, exists := runningTargets[targetKey]; exists && activeTaskID == taskID {
		delete(runningTargets, targetKey)
	}
}

func failoverTargetRunKey(task models.FailoverTask) (string, error) {
	userUUID := strings.TrimSpace(task.UserID)
	if userUUID == "" {
		return "", nil
	}

	if refKey := currentInstanceRunKey(parseJSONMap(task.CurrentInstanceRef)); refKey != "" {
		return userUUID + "|ref|" + refKey, nil
	}

	address := strings.TrimSpace(task.CurrentAddress)
	if address == "" {
		resolvedAddress, err := resolveTaskCurrentAddress(task)
		if err == nil {
			address = resolvedAddress
		}
	}
	if normalizedAddress := normalizeIPAddress(address); normalizedAddress != "" {
		return userUUID + "|addr|" + normalizedAddress, nil
	}

	if watchClientUUID := strings.TrimSpace(task.WatchClientUUID); watchClientUUID != "" {
		return userUUID + "|watch|" + watchClientUUID, nil
	}

	return "", nil
}

func currentInstanceRunKey(ref map[string]interface{}) string {
	if len(ref) == 0 {
		return ""
	}

	provider := strings.ToLower(strings.TrimSpace(stringMapValue(ref, "provider")))
	entryID := strings.TrimSpace(providerEntryIDFromRef(ref))
	switch provider {
	case "digitalocean":
		if dropletID := intMapValue(ref, "droplet_id"); dropletID > 0 {
			return fmt.Sprintf("digitalocean|%s|droplet|%d", entryID, dropletID)
		}
	case "linode":
		if instanceID := intMapValue(ref, "instance_id"); instanceID > 0 {
			return fmt.Sprintf("linode|%s|instance|%d", entryID, instanceID)
		}
	case "vultr":
		if instanceID := strings.TrimSpace(stringMapValue(ref, "instance_id")); instanceID != "" {
			return fmt.Sprintf("vultr|%s|instance|%s", entryID, instanceID)
		}
	case "azure":
		instanceID := strings.TrimSpace(stringMapValue(ref, "instance_id"))
		if instanceID == "" {
			resourceGroup := strings.TrimSpace(stringMapValue(ref, "resource_group"))
			instanceName := strings.TrimSpace(firstNonEmpty(stringMapValue(ref, "name"), stringMapValue(ref, "instance_name")))
			if resourceGroup != "" && instanceName != "" {
				instanceID = azurecloud.EncodeInstanceID(resourceGroup, instanceName)
			}
		}
		if instanceID != "" {
			return fmt.Sprintf("azure|%s|vm|%s", entryID, instanceID)
		}
	case "aws":
		service := strings.ToLower(strings.TrimSpace(firstNonEmpty(stringMapValue(ref, "service"), "ec2")))
		switch service {
		case "lightsail":
			if instanceName := strings.TrimSpace(stringMapValue(ref, "instance_name")); instanceName != "" {
				return fmt.Sprintf("aws|%s|lightsail|%s|%s", entryID, strings.TrimSpace(stringMapValue(ref, "region")), instanceName)
			}
		default:
			if instanceID := strings.TrimSpace(stringMapValue(ref, "instance_id")); instanceID != "" {
				return fmt.Sprintf("aws|%s|ec2|%s|%s", entryID, strings.TrimSpace(stringMapValue(ref, "region")), instanceID)
			}
		}
	}

	return ""
}

func registerExecutionCancel(executionID uint, cancel context.CancelFunc) {
	if executionID == 0 || cancel == nil {
		return
	}
	executionStopMu.Lock()
	defer executionStopMu.Unlock()
	executionCancels[executionID] = cancel
}

func unregisterExecutionCancel(executionID uint) {
	if executionID == 0 {
		return
	}
	executionStopMu.Lock()
	defer executionStopMu.Unlock()
	delete(executionCancels, executionID)
}

func cancelExecution(executionID uint) {
	if executionID == 0 {
		return
	}
	executionStopMu.Lock()
	cancel := executionCancels[executionID]
	executionStopMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (r *executionRunner) checkStopped() error {
	if r == nil || r.ctx == nil {
		return nil
	}
	select {
	case <-r.ctx.Done():
		return errExecutionStopped
	default:
		return nil
	}
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
			return errExecutionStopped
		default:
			return nil
		}
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return errExecutionStopped
	case <-timer.C:
		return nil
	}
}

func contextOrBackground(ctx context.Context) context.Context {
	if ctx != nil {
		return ctx
	}
	return context.Background()
}

func runAWSProvisionFollowUp(ctx context.Context, action func(context.Context) error) error {
	const (
		attempts = 5
		delay    = 2 * time.Second
	)

	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		if err := waitContextOrDelay(ctx, 0); err != nil {
			return err
		}
		if err := action(contextOrBackground(ctx)); err == nil {
			return nil
		} else {
			lastErr = err
		}
		if attempt+1 < attempts {
			if err := waitContextOrDelay(ctx, delay); err != nil {
				return err
			}
		}
	}
	return lastErr
}

func firstUsablePublicIPv6(values []string) string {
	for _, value := range values {
		normalized := normalizeIPAddress(value)
		if normalized == "" {
			continue
		}
		ip := net.ParseIP(normalized)
		if ip == nil || ip.To16() == nil || ip.To4() != nil {
			continue
		}
		if !ip.IsGlobalUnicast() || ip.IsLinkLocalUnicast() || ip.IsLoopback() || ip.IsMulticast() || ip.IsUnspecified() || ip.IsPrivate() {
			continue
		}
		return normalized
	}
	return ""
}

func buildAWSProvisionCleanupError(ctx context.Context, service, resourceID, cleanupLabel string, cleanup func(context.Context) error, cause error) error {
	if cause == nil {
		return nil
	}
	if cleanup == nil {
		return cause
	}

	cleanupErr := cleanup(contextOrBackground(ctx))
	if cleanupErr != nil {
		return &provisionCleanupError{
			Provider:     "aws",
			ResourceType: strings.TrimSpace(service) + " instance",
			ResourceID:   strings.TrimSpace(resourceID),
			CleanupLabel: strings.TrimSpace(cleanupLabel),
			CleanupError: normalizeExecutionStopError(cleanupErr),
			Cause:        cause,
		}
	}
	return &provisionCleanupError{
		Provider:     "aws",
		ResourceType: strings.TrimSpace(service) + " instance",
		ResourceID:   strings.TrimSpace(resourceID),
		CleanupLabel: strings.TrimSpace(cleanupLabel),
		Cause:        cause,
	}
}

func requireUsableAWSIPv6(ctx context.Context, service, resourceID, cleanupLabel string, cleanup func(context.Context) error, addresses []string) (string, error) {
	if ipv6 := firstUsablePublicIPv6(addresses); ipv6 != "" {
		return ipv6, nil
	}

	normalized := make([]string, 0, len(addresses))
	for _, value := range addresses {
		if trimmed := normalizeIPAddress(value); trimmed != "" {
			normalized = append(normalized, trimmed)
		}
	}

	message := fmt.Sprintf(
		"AWS %s provisioning requested IPv6, but the new instance never reported a usable global IPv6 address",
		strings.TrimSpace(service),
	)
	if len(normalized) > 0 {
		message += " (reported: " + strings.Join(normalized, ", ") + ")"
	}
	message += ". Check that the subnet and its route table are fully IPv6-enabled, and that the guest OS refreshed its network configuration."
	return "", buildAWSProvisionCleanupError(ctx, service, resourceID, cleanupLabel, cleanup, errors.New(message))
}

func lightsailProvisionWantsIPv6(ipAddressType string) bool {
	switch strings.ToLower(strings.TrimSpace(ipAddressType)) {
	case "dualstack", "ipv6":
		return true
	default:
		return false
	}
}

func normalizeExecutionStopError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, errExecutionStopped) || errors.Is(err, context.Canceled) {
		return errExecutionStopped
	}
	return err
}

func ptrLocalTime(t time.Time) *models.LocalTime {
	value := models.FromTime(t)
	return &value
}

func marshalJSON(value interface{}) string {
	if value == nil {
		return ""
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	return string(payload)
}

func emptyToNilString(value string) interface{} {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return value
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

func provisionAWSInstance(ctx context.Context, userUUID string, plan models.FailoverPlan) (*actionOutcome, error) {
	var payload awsProvisionPayload
	if err := json.Unmarshal([]byte(plan.Payload), &payload); err != nil {
		return nil, fmt.Errorf("invalid aws provision payload: %w", err)
	}
	service := strings.ToLower(strings.TrimSpace(payload.Service))
	if service == "" {
		service = "ec2"
	}

	addition, credential, err := loadAWSCredential(userUUID, plan.ProviderEntryID)
	if err != nil {
		return nil, err
	}
	region := strings.TrimSpace(payload.Region)
	if region == "" {
		region = strings.TrimSpace(addition.ActiveRegion)
	}
	if region == "" {
		region = strings.TrimSpace(credential.DefaultRegion)
	}
	if region == "" {
		region = awscloud.DefaultRegion
	}

	switch service {
	case "ec2":
		name := strings.TrimSpace(payload.Name)
		if name == "" {
			name = "failover-ec2-" + strconv.FormatInt(time.Now().Unix(), 10)
		}
		userData := strings.TrimSpace(payload.UserData)
		userData, generatedRootPassword, err := resolveAWSRootPasswordUserData(
			payload.RootPasswordMode,
			payload.RootPassword,
			userData,
		)
		if err != nil {
			return nil, err
		}
		if payload.AssignIPv6 {
			userData, err = buildAWSIPv6RefreshUserData(userData)
			if err != nil {
				return nil, err
			}
		}
		autoConnectGroup := ""
		if plan.AutoConnectGroup != "" || planHasScripts(plan) {
			userData, autoConnectGroup, err = buildAutoConnectUserData(autoConnectOptions{
				UserUUID:          userUUID,
				UserData:          userData,
				Provider:          "aws",
				CredentialName:    credential.Name,
				PoolGroup:         resolveAutoConnectPoolGroup(plan, credential.Group),
				Group:             plan.AutoConnectGroup,
				WrapInShellScript: true,
			})
			if err != nil {
				return nil, err
			}
		}
		createResult, err := awscloud.CreateInstance(ctx, credential, region, awscloud.CreateInstanceRequest{
			Name:             name,
			ImageID:          strings.TrimSpace(payload.ImageID),
			InstanceType:     strings.TrimSpace(payload.InstanceType),
			KeyName:          strings.TrimSpace(payload.KeyName),
			SubnetID:         strings.TrimSpace(payload.SubnetID),
			SecurityGroupIDs: trimStrings(payload.SecurityGroupIDs),
			UserData:         userData,
			AssignPublicIP:   payload.AssignPublicIP,
			AssignIPv6:       payload.AssignIPv6,
			Tags:             payload.Tags,
		})
		if err != nil {
			return nil, err
		}
		cleanupInstanceID := strings.TrimSpace(createResult.Instance.InstanceID)
		cleanupLabel := "terminate failed aws ec2 instance " + cleanupInstanceID
		cleanupProvisionedInstance := func(runCtx context.Context) error {
			if cleanupInstanceID == "" {
				return nil
			}
			if err := awscloud.TerminateInstance(contextOrBackground(runCtx), credential, region, cleanupInstanceID); err != nil {
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
			warnings := strings.Join(trimStrings(createResult.Warnings), "; ")
			if warnings == "" {
				warnings = "Komari could not enable IPv6 automatically on the selected AWS network."
			}
			return nil, buildAWSProvisionCleanupError(
				ctx,
				"ec2",
				cleanupInstanceID,
				cleanupLabel,
				cleanupProvisionedInstance,
				errors.New(warnings),
			)
		}
		instance := createResult.Instance
		instance, detail, err := waitForAWSEC2Instance(ctx, region, credential, strings.TrimSpace(instance.InstanceID))
		if err != nil {
			return nil, buildAWSProvisionCleanupError(ctx, "ec2", cleanupInstanceID, cleanupLabel, cleanupProvisionedInstance, err)
		}
		if payload.AssignIPv6 {
			if err := runAWSProvisionFollowUp(ctx, func(runCtx context.Context) error {
				_, followUpErr := awscloud.EnsureInstanceIPv6Address(runCtx, credential, region, strings.TrimSpace(instance.InstanceID))
				return followUpErr
			}); err != nil {
				return nil, buildAWSProvisionCleanupError(ctx, "ec2", cleanupInstanceID, cleanupLabel, cleanupProvisionedInstance, normalizeExecutionStopError(err))
			}
			instance, detail, err = waitForAWSEC2Instance(ctx, region, credential, strings.TrimSpace(instance.InstanceID))
			if err != nil {
				return nil, buildAWSProvisionCleanupError(ctx, "ec2", cleanupInstanceID, cleanupLabel, cleanupProvisionedInstance, err)
			}
		}
		if payload.AllowAllTraffic {
			if err := runAWSProvisionFollowUp(ctx, func(runCtx context.Context) error {
				_, followUpErr := awscloud.AllowAllSecurityGroupTraffic(runCtx, credential, region, strings.TrimSpace(instance.InstanceID))
				return followUpErr
			}); err != nil {
				return nil, buildAWSProvisionCleanupError(ctx, "ec2", cleanupInstanceID, cleanupLabel, cleanupProvisionedInstance, normalizeExecutionStopError(err))
			}
			if refreshedDetail, detailErr := awscloud.GetInstanceDetail(contextOrBackground(ctx), credential, region, strings.TrimSpace(instance.InstanceID)); detailErr == nil {
				detail = refreshedDetail
			}
		}
		provisionedIPv6 := firstUsablePublicIPv6(instance.IPv6Addresses)
		if payload.AssignIPv6 {
			provisionedIPv6, err = requireUsableAWSIPv6(ctx, "ec2", cleanupInstanceID, cleanupLabel, cleanupProvisionedInstance, instance.IPv6Addresses)
			if err != nil {
				return nil, err
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
			return nil, buildAWSProvisionCleanupError(ctx, "ec2", cleanupInstanceID, cleanupLabel, cleanupProvisionedInstance, passwordSaveErr)
		}
		detailAddresses := make([]awscloud.Address, 0)
		if detail != nil {
			detailAddresses = detail.Addresses
		}
		outcome := &actionOutcome{
			IPv4:             strings.TrimSpace(instance.PublicIP),
			IPv6:             provisionedIPv6,
			TargetClientUUID: "",
			AutoConnectGroup: autoConnectGroup,
			NewInstanceRef: map[string]interface{}{
				"provider":            "aws",
				"service":             "ec2",
				"provider_entry_id":   credential.ID,
				"provider_entry_name": credential.Name,
				"region":              region,
				"instance_id":         instance.InstanceID,
				"name":                instance.Name,
			},
			NewAddresses: map[string]interface{}{
				"public_ip":      instance.PublicIP,
				"private_ip":     instance.PrivateIP,
				"ipv6_addresses": instance.IPv6Addresses,
				"addresses":      detailAddresses,
			},
			RollbackLabel: cleanupLabel,
			Rollback: func(ctx context.Context) error {
				if err := awscloud.TerminateInstance(contextOrBackground(ctx), credential, region, strings.TrimSpace(instance.InstanceID)); err != nil {
					if isAWSResourceNotFoundError("ec2", err) {
						removeSavedAWSRootPassword(userUUID, addition, credential, "ec2", region, instance.InstanceID)
						return nil
					}
					return normalizeExecutionStopError(err)
				}
				removeSavedAWSRootPassword(userUUID, addition, credential, "ec2", region, instance.InstanceID)
				return nil
			},
		}
		if rootPassword != "" {
			outcome.NewInstanceRef["root_password_mode"] = passwordMode
			outcome.NewInstanceRef["root_password_saved"] = passwordSaveErr == nil
			if passwordSaveErr != nil {
				outcome.NewInstanceRef["root_password_save_error"] = passwordSaveErr.Error()
			}
		}
		if cleanupInstanceID := strings.TrimSpace(payload.CleanupInstanceID); cleanupInstanceID != "" {
			outcome.OldInstanceRef = map[string]interface{}{
				"provider":            "aws",
				"service":             "ec2",
				"provider_entry_id":   credential.ID,
				"provider_entry_name": credential.Name,
				"region":              region,
				"instance_id":         cleanupInstanceID,
			}
			outcome.CleanupLabel = "terminate aws ec2 instance " + cleanupInstanceID
			outcome.Cleanup = func(ctx context.Context) error {
				if err := awscloud.TerminateInstance(contextOrBackground(ctx), credential, region, cleanupInstanceID); err != nil {
					if isAWSResourceNotFoundError("ec2", err) {
						removeSavedAWSRootPassword(userUUID, addition, credential, "ec2", region, cleanupInstanceID)
						return nil
					}
					return normalizeExecutionStopError(err)
				}
				removeSavedAWSRootPassword(userUUID, addition, credential, "ec2", region, cleanupInstanceID)
				return nil
			}
		}
		return outcome, nil
	case "lightsail":
		name := strings.TrimSpace(payload.Name)
		if name == "" {
			name = "failover-ls-" + strconv.FormatInt(time.Now().Unix(), 10)
		}
		userData := strings.TrimSpace(payload.UserData)
		userData, generatedRootPassword, err := resolveAWSRootPasswordUserData(
			payload.RootPasswordMode,
			payload.RootPassword,
			userData,
		)
		if err != nil {
			return nil, err
		}
		if lightsailProvisionWantsIPv6(payload.IPAddressType) {
			userData, err = buildAWSIPv6RefreshUserData(userData)
			if err != nil {
				return nil, err
			}
		}
		autoConnectGroup := ""
		if plan.AutoConnectGroup != "" || planHasScripts(plan) {
			userData, autoConnectGroup, err = buildAutoConnectUserData(autoConnectOptions{
				UserUUID:          userUUID,
				UserData:          userData,
				Provider:          "aws",
				CredentialName:    credential.Name,
				PoolGroup:         resolveAutoConnectPoolGroup(plan, credential.Group),
				Group:             plan.AutoConnectGroup,
				WrapInShellScript: true,
			})
			if err != nil {
				return nil, err
			}
		}
		if err := awscloud.CreateLightsailInstance(ctx, credential, region, awscloud.CreateLightsailInstanceRequest{
			Name:             name,
			AvailabilityZone: strings.TrimSpace(payload.AvailabilityZone),
			BlueprintID:      strings.TrimSpace(payload.BlueprintID),
			BundleID:         strings.TrimSpace(payload.BundleID),
			KeyPairName:      strings.TrimSpace(payload.KeyPairName),
			UserData:         userData,
			IPAddressType:    strings.TrimSpace(payload.IPAddressType),
			Tags:             payload.Tags,
		}); err != nil {
			return nil, err
		}
		cleanupLabel := "delete failed aws lightsail instance " + name
		cleanupProvisionedInstance := func(runCtx context.Context) error {
			if err := awscloud.DeleteLightsailInstance(contextOrBackground(runCtx), credential, region, name); err != nil {
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
			return nil, buildAWSProvisionCleanupError(ctx, "lightsail", name, cleanupLabel, cleanupProvisionedInstance, err)
		}
		if payload.AllowAllTraffic {
			if err := runAWSProvisionFollowUp(ctx, func(runCtx context.Context) error {
				return awscloud.OpenLightsailAllPublicPorts(runCtx, credential, region, name)
			}); err != nil {
				return nil, buildAWSProvisionCleanupError(ctx, "lightsail", name, cleanupLabel, cleanupProvisionedInstance, normalizeExecutionStopError(err))
			}
			detail, err = waitForLightsailInstance(ctx, region, credential, name)
			if err != nil {
				return nil, buildAWSProvisionCleanupError(ctx, "lightsail", name, cleanupLabel, cleanupProvisionedInstance, err)
			}
		}
		provisionedIPv6 := firstUsablePublicIPv6(detail.Instance.IPv6Addresses)
		if lightsailProvisionWantsIPv6(payload.IPAddressType) {
			provisionedIPv6, err = requireUsableAWSIPv6(ctx, "lightsail", name, cleanupLabel, cleanupProvisionedInstance, detail.Instance.IPv6Addresses)
			if err != nil {
				return nil, err
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
			return nil, buildAWSProvisionCleanupError(ctx, "lightsail", name, cleanupLabel, cleanupProvisionedInstance, passwordSaveErr)
		}
		outcome := &actionOutcome{
			IPv4:             strings.TrimSpace(detail.Instance.PublicIP),
			IPv6:             provisionedIPv6,
			AutoConnectGroup: autoConnectGroup,
			NewInstanceRef: map[string]interface{}{
				"provider":            "aws",
				"service":             "lightsail",
				"provider_entry_id":   credential.ID,
				"provider_entry_name": credential.Name,
				"region":              region,
				"instance_name":       name,
			},
			NewAddresses: map[string]interface{}{
				"public_ip":      detail.Instance.PublicIP,
				"private_ip":     detail.Instance.PrivateIP,
				"ipv6_addresses": detail.Instance.IPv6Addresses,
			},
			RollbackLabel: cleanupLabel,
			Rollback: func(ctx context.Context) error {
				if err := awscloud.DeleteLightsailInstance(contextOrBackground(ctx), credential, region, name); err != nil {
					if isAWSResourceNotFoundError("lightsail", err) {
						removeSavedAWSRootPassword(userUUID, addition, credential, "lightsail", region, name)
						return nil
					}
					return normalizeExecutionStopError(err)
				}
				removeSavedAWSRootPassword(userUUID, addition, credential, "lightsail", region, name)
				return nil
			},
		}
		if rootPassword != "" {
			outcome.NewInstanceRef["root_password_mode"] = passwordMode
			outcome.NewInstanceRef["root_password_saved"] = passwordSaveErr == nil
			if passwordSaveErr != nil {
				outcome.NewInstanceRef["root_password_save_error"] = passwordSaveErr.Error()
			}
		}
		if cleanupName := strings.TrimSpace(payload.CleanupInstanceName); cleanupName != "" {
			outcome.OldInstanceRef = map[string]interface{}{
				"provider":            "aws",
				"service":             "lightsail",
				"provider_entry_id":   credential.ID,
				"provider_entry_name": credential.Name,
				"region":              region,
				"instance_name":       cleanupName,
			}
			outcome.CleanupLabel = "delete aws lightsail instance " + cleanupName
			outcome.Cleanup = func(ctx context.Context) error {
				if err := awscloud.DeleteLightsailInstance(contextOrBackground(ctx), credential, region, cleanupName); err != nil {
					if isAWSResourceNotFoundError("lightsail", err) {
						removeSavedAWSRootPassword(userUUID, addition, credential, "lightsail", region, cleanupName)
						return nil
					}
					return normalizeExecutionStopError(err)
				}
				removeSavedAWSRootPassword(userUUID, addition, credential, "lightsail", region, cleanupName)
				return nil
			}
		}
		return outcome, nil
	default:
		return nil, fmt.Errorf("unsupported aws provision service: %s", payload.Service)
	}
}

func provisionDigitalOceanDroplet(ctx context.Context, userUUID string, plan models.FailoverPlan) (*actionOutcome, error) {
	var payload digitalOceanProvisionPayload
	if err := json.Unmarshal([]byte(plan.Payload), &payload); err != nil {
		return nil, fmt.Errorf("invalid digitalocean provision payload: %w", err)
	}

	addition, token, err := loadDigitalOceanToken(userUUID, plan.ProviderEntryID)
	if err != nil {
		return nil, err
	}
	client, err := digitalocean.NewClientFromToken(token.Token)
	if err != nil {
		return nil, err
	}

	name := strings.TrimSpace(payload.Name)
	if name == "" {
		name = "failover-do-" + strconv.FormatInt(time.Now().Unix(), 10)
	}
	userData := strings.TrimSpace(payload.UserData)
	autoConnectGroup := ""
	if plan.AutoConnectGroup != "" || planHasScripts(plan) {
		userData, autoConnectGroup, err = buildAutoConnectUserData(autoConnectOptions{
			UserUUID:          userUUID,
			UserData:          userData,
			Provider:          "digitalocean",
			CredentialName:    token.Name,
			PoolGroup:         resolveAutoConnectPoolGroup(plan, token.Group),
			Group:             plan.AutoConnectGroup,
			WrapInShellScript: false,
		})
		if err != nil {
			return nil, err
		}
	}

	passwordMode := strings.ToLower(strings.TrimSpace(payload.RootPasswordMode))
	rootPassword := ""
	switch passwordMode {
	case "", "random":
		passwordMode = "random"
		rootPassword, err = digitalocean.GenerateRandomPassword(20)
		if err != nil {
			return nil, err
		}
		userData, err = digitalocean.BuildRootPasswordUserData(rootPassword, userData)
		if err != nil {
			return nil, err
		}
	case "none":
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
		Tags:       trimStrings(payload.Tags),
		UserData:   userData,
		VPCUUID:    strings.TrimSpace(payload.VPCUUID),
	})
	if err != nil {
		return nil, normalizeExecutionStopError(err)
	}
	droplet, err = waitForDigitalOceanDroplet(ctx, client, droplet.ID)
	if err != nil {
		return nil, normalizeExecutionStopError(err)
	}
	passwordSaveErr := persistDigitalOceanRootPassword(userUUID, addition, token, droplet.ID, droplet.Name, passwordMode, rootPassword)
	if rootPassword != "" && passwordSaveErr != nil {
		cleanupErr := client.DeleteDroplet(contextOrBackground(ctx), droplet.ID)
		if cleanupErr != nil && !isDigitalOceanNotFoundError(cleanupErr) {
			return nil, &provisionCleanupError{
				Provider:     "digitalocean",
				ResourceType: "droplet",
				ResourceID:   strconv.Itoa(droplet.ID),
				CleanupLabel: fmt.Sprintf("delete failed digitalocean droplet %d", droplet.ID),
				CleanupError: normalizeExecutionStopError(cleanupErr),
				Cause:        passwordSaveErr,
			}
		}
		return nil, &provisionCleanupError{
			Provider:     "digitalocean",
			ResourceType: "droplet",
			ResourceID:   strconv.Itoa(droplet.ID),
			CleanupLabel: fmt.Sprintf("delete failed digitalocean droplet %d", droplet.ID),
			Cause:        passwordSaveErr,
		}
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
		if passwordSaveErr != nil {
			newInstanceRef["root_password_save_error"] = passwordSaveErr.Error()
		}
	}
	outcome := &actionOutcome{
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
			if err := client.DeleteDroplet(contextOrBackground(ctx), droplet.ID); err != nil {
				return normalizeExecutionStopError(err)
			}
			removeSavedDigitalOceanRootPassword(userUUID, addition, token, droplet.ID)
			return nil
		},
	}
	if payload.CleanupDropletID > 0 {
		outcome.OldInstanceRef = map[string]interface{}{
			"provider":            "digitalocean",
			"provider_entry_id":   token.ID,
			"provider_entry_name": token.Name,
			"droplet_id":          payload.CleanupDropletID,
		}
		outcome.CleanupLabel = fmt.Sprintf("delete digitalocean droplet %d", payload.CleanupDropletID)
		outcome.Cleanup = func(ctx context.Context) error {
			if err := client.DeleteDroplet(contextOrBackground(ctx), payload.CleanupDropletID); err != nil {
				return normalizeExecutionStopError(err)
			}
			removeSavedDigitalOceanRootPassword(userUUID, addition, token, payload.CleanupDropletID)
			return nil
		}
	}
	return outcome, nil
}

func provisionAzureInstance(ctx context.Context, userUUID string, plan models.FailoverPlan) (*actionOutcome, error) {
	var payload azureProvisionPayload
	if err := json.Unmarshal([]byte(plan.Payload), &payload); err != nil {
		return nil, fmt.Errorf("invalid azure provision payload: %w", err)
	}

	addition, credential, err := loadAzureCredential(userUUID, plan.ProviderEntryID)
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
		name = "failover-azure-" + strconv.FormatInt(time.Now().Unix(), 10)
	}
	username := strings.TrimSpace(payload.AdminUsername)
	if username == "" {
		username = "azureuser"
	}

	userData := strings.TrimSpace(payload.UserData)
	autoConnectGroup := ""
	if plan.AutoConnectGroup != "" || planHasScripts(plan) {
		userData, autoConnectGroup, err = buildAutoConnectUserData(autoConnectOptions{
			UserUUID:          userUUID,
			UserData:          userData,
			Provider:          "azure",
			CredentialName:    credential.Name,
			Group:             plan.AutoConnectGroup,
			WrapInShellScript: true,
		})
		if err != nil {
			return nil, err
		}
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
			"provider":   "failover",
		},
	})
	if err != nil {
		return nil, normalizeExecutionStopError(err)
	}
	instance := detail.Instance
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
	cleanupLabel := "delete failed azure vm " + instance.Name
	cleanupProvisionedInstance := func(runCtx context.Context) error {
		if _, cleanupErr := client.DeleteVirtualMachine(contextOrBackground(runCtx), instance.ResourceGroup, instance.Name); cleanupErr != nil {
			if isAzureNotFoundError(cleanupErr) {
				removeSavedAzureRootPassword(userUUID, addition, credential, instance.InstanceID)
				return nil
			}
			return normalizeExecutionStopError(cleanupErr)
		}
		removeSavedAzureRootPassword(userUUID, addition, credential, instance.InstanceID)
		return nil
	}
	if rootPassword != "" && passwordSaveErr != nil {
		cleanupErr := cleanupProvisionedInstance(contextOrBackground(ctx))
		if cleanupErr != nil {
			return nil, &provisionCleanupError{
				Provider:     "azure",
				ResourceType: "vm",
				ResourceID:   instance.InstanceID,
				CleanupLabel: cleanupLabel,
				CleanupError: cleanupErr,
				Cause:        passwordSaveErr,
			}
		}
		return nil, &provisionCleanupError{
			Provider:     "azure",
			ResourceType: "vm",
			ResourceID:   instance.InstanceID,
			CleanupLabel: cleanupLabel,
			Cause:        passwordSaveErr,
		}
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
		if passwordSaveErr != nil {
			newInstanceRef["root_password_save_error"] = passwordSaveErr.Error()
		}
	}
	outcome := &actionOutcome{
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
	}

	cleanupResourceGroup := strings.TrimSpace(payload.CleanupResourceGroup)
	cleanupName := strings.TrimSpace(payload.CleanupName)
	cleanupInstanceID := strings.TrimSpace(payload.CleanupInstanceID)
	if cleanupInstanceID != "" && (cleanupResourceGroup == "" || cleanupName == "") {
		if decodedGroup, decodedName, decodeErr := azurecloud.DecodeInstanceID(cleanupInstanceID); decodeErr == nil {
			cleanupResourceGroup = firstNonEmpty(cleanupResourceGroup, decodedGroup)
			cleanupName = firstNonEmpty(cleanupName, decodedName)
		}
	}
	if cleanupResourceGroup != "" && cleanupName != "" {
		oldInstanceID := firstNonEmpty(cleanupInstanceID, azurecloud.EncodeInstanceID(cleanupResourceGroup, cleanupName))
		outcome.OldInstanceRef = map[string]interface{}{
			"provider":            "azure",
			"provider_entry_id":   credential.ID,
			"provider_entry_name": credential.Name,
			"resource_group":      cleanupResourceGroup,
			"instance_id":         oldInstanceID,
			"name":                cleanupName,
		}
		outcome.CleanupLabel = "delete azure vm " + cleanupName
		outcome.Cleanup = func(ctx context.Context) error {
			if _, err := client.DeleteVirtualMachine(contextOrBackground(ctx), cleanupResourceGroup, cleanupName); err != nil {
				if isAzureNotFoundError(err) {
					removeSavedAzureRootPassword(userUUID, addition, credential, oldInstanceID)
					return nil
				}
				return normalizeExecutionStopError(err)
			}
			removeSavedAzureRootPassword(userUUID, addition, credential, oldInstanceID)
			return nil
		}
	}
	return outcome, nil
}

func provisionLinodeInstance(ctx context.Context, userUUID string, plan models.FailoverPlan) (*actionOutcome, error) {
	var payload linodeProvisionPayload
	if err := json.Unmarshal([]byte(plan.Payload), &payload); err != nil {
		return nil, fmt.Errorf("invalid linode provision payload: %w", err)
	}

	addition, token, err := loadLinodeToken(userUUID, plan.ProviderEntryID)
	if err != nil {
		return nil, err
	}
	client, err := linodecloud.NewClientFromToken(token.Token)
	if err != nil {
		return nil, err
	}

	label := strings.TrimSpace(payload.Label)
	if label == "" {
		label = "failover-linode-" + strconv.FormatInt(time.Now().Unix(), 10)
	}
	userData := strings.TrimSpace(payload.UserData)
	autoConnectGroup := ""
	if plan.AutoConnectGroup != "" || planHasScripts(plan) {
		userData, autoConnectGroup, err = buildAutoConnectUserData(autoConnectOptions{
			UserUUID:          userUUID,
			UserData:          userData,
			Provider:          "linode",
			CredentialName:    token.Name,
			PoolGroup:         resolveAutoConnectPoolGroup(plan, token.Group),
			Group:             plan.AutoConnectGroup,
			WrapInShellScript: true,
		})
		if err != nil {
			return nil, err
		}
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
		AuthorizedKeys: trimStrings(payload.AuthorizedKeys),
		BackupsEnabled: payload.BackupsEnabled,
		Booted:         true,
		Tags:           trimStrings(payload.Tags),
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
	instance, err = waitForLinodeInstance(ctx, client, instance.ID)
	if err != nil {
		return nil, normalizeExecutionStopError(err)
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
		cleanupErr := client.DeleteInstance(contextOrBackground(ctx), instance.ID)
		if cleanupErr != nil && !isLinodeNotFoundError(cleanupErr) {
			return nil, &provisionCleanupError{
				Provider:     "linode",
				ResourceType: "instance",
				ResourceID:   strconv.Itoa(instance.ID),
				CleanupLabel: fmt.Sprintf("delete failed linode instance %d", instance.ID),
				CleanupError: normalizeExecutionStopError(cleanupErr),
				Cause:        passwordSaveErr,
			}
		}
		return nil, &provisionCleanupError{
			Provider:     "linode",
			ResourceType: "instance",
			ResourceID:   strconv.Itoa(instance.ID),
			CleanupLabel: fmt.Sprintf("delete failed linode instance %d", instance.ID),
			Cause:        passwordSaveErr,
		}
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
		if passwordSaveErr != nil {
			newInstanceRef["root_password_save_error"] = passwordSaveErr.Error()
		}
	}
	outcome := &actionOutcome{
		IPv4:             normalizeIPAddress(firstString(instance.IPv4)),
		IPv6:             normalizeIPAddress(instance.IPv6),
		AutoConnectGroup: autoConnectGroup,
		NewInstanceRef:   newInstanceRef,
		NewAddresses: map[string]interface{}{
			"ipv4": instance.IPv4,
			"ipv6": instance.IPv6,
		},
		RollbackLabel: fmt.Sprintf("delete failed linode instance %d", instance.ID),
		Rollback: func(ctx context.Context) error {
			if err := client.DeleteInstance(contextOrBackground(ctx), instance.ID); err != nil {
				return normalizeExecutionStopError(err)
			}
			removeSavedLinodeRootPassword(userUUID, addition, token, instance.ID)
			return nil
		},
	}
	if payload.CleanupInstanceID > 0 {
		outcome.OldInstanceRef = map[string]interface{}{
			"provider":            "linode",
			"provider_entry_id":   token.ID,
			"provider_entry_name": token.Name,
			"instance_id":         payload.CleanupInstanceID,
		}
		outcome.CleanupLabel = fmt.Sprintf("delete linode instance %d", payload.CleanupInstanceID)
		outcome.Cleanup = func(ctx context.Context) error {
			if err := client.DeleteInstance(contextOrBackground(ctx), payload.CleanupInstanceID); err != nil {
				return normalizeExecutionStopError(err)
			}
			removeSavedLinodeRootPassword(userUUID, addition, token, payload.CleanupInstanceID)
			return nil
		}
	}
	return outcome, nil
}

func provisionVultrInstance(ctx context.Context, userUUID string, plan models.FailoverPlan) (*actionOutcome, error) {
	var payload vultrProvisionPayload
	if err := json.Unmarshal([]byte(plan.Payload), &payload); err != nil {
		return nil, fmt.Errorf("invalid vultr provision payload: %w", err)
	}

	addition, token, err := loadVultrToken(userUUID, plan.ProviderEntryID)
	if err != nil {
		return nil, err
	}
	client, err := vultrcloud.NewClientFromToken(token.Token)
	if err != nil {
		return nil, err
	}

	label := strings.TrimSpace(payload.Label)
	if label == "" {
		label = "failover-vultr-" + strconv.FormatInt(time.Now().Unix(), 10)
	}
	hostname := strings.TrimSpace(payload.Hostname)
	if hostname == "" {
		hostname = label
	}
	userData := strings.TrimSpace(payload.UserData)
	autoConnectGroup := ""
	if plan.AutoConnectGroup != "" || planHasScripts(plan) {
		userData, autoConnectGroup, err = buildAutoConnectUserData(autoConnectOptions{
			UserUUID:          userUUID,
			UserData:          userData,
			Provider:          "vultr",
			CredentialName:    token.Name,
			PoolGroup:         resolveAutoConnectPoolGroup(plan, token.Group),
			Group:             plan.AutoConnectGroup,
			WrapInShellScript: true,
		})
		if err != nil {
			return nil, err
		}
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
		SSHKeyIDs:         trimStrings(payload.SSHKeyIDs),
		EnableIPv6:        payload.EnableIPv6,
		DisablePublicIPv4: payload.DisablePublicIPv4,
		Backups:           backups,
		DDOSProtection:    payload.DDOSProtection,
		ActivationEmail:   payload.ActivationEmail,
		Tags:              trimStrings(payload.Tags),
	}
	if request.Region == "" {
		return nil, errors.New("vultr region is required")
	}
	if request.Plan == "" {
		return nil, errors.New("vultr plan is required")
	}
	if request.OSID <= 0 {
		return nil, errors.New("vultr os_id is required")
	}
	if userData != "" {
		request.UserData = vultrcloud.EncodeUserData(userData)
	}

	instance, err := client.CreateInstance(ctx, request)
	if err != nil {
		return nil, normalizeExecutionStopError(err)
	}
	instance, err = waitForVultrInstance(ctx, client, instance.ID)
	if err != nil {
		return nil, normalizeExecutionStopError(err)
	}

	passwordToSave := rootPassword
	if passwordToSave == "" && passwordMode == "provider_default" && instance != nil {
		passwordToSave = strings.TrimSpace(instance.DefaultPassword)
	}
	passwordSaveErr := persistVultrRootPassword(userUUID, addition, token, instance.ID, instance.Label, passwordMode, passwordToSave)
	cleanupLabel := "delete failed vultr instance " + instance.ID
	cleanupProvisionedInstance := func(runCtx context.Context) error {
		if cleanupErr := client.DeleteInstance(contextOrBackground(runCtx), instance.ID); cleanupErr != nil {
			if isVultrNotFoundError(cleanupErr) {
				removeSavedVultrRootPassword(userUUID, addition, token, instance.ID)
				return nil
			}
			return normalizeExecutionStopError(cleanupErr)
		}
		removeSavedVultrRootPassword(userUUID, addition, token, instance.ID)
		return nil
	}
	if passwordToSave != "" && passwordSaveErr != nil {
		cleanupErr := cleanupProvisionedInstance(contextOrBackground(ctx))
		if cleanupErr != nil {
			return nil, &provisionCleanupError{
				Provider:     "vultr",
				ResourceType: "instance",
				ResourceID:   instance.ID,
				CleanupLabel: cleanupLabel,
				CleanupError: cleanupErr,
				Cause:        passwordSaveErr,
			}
		}
		return nil, &provisionCleanupError{
			Provider:     "vultr",
			ResourceType: "instance",
			ResourceID:   instance.ID,
			CleanupLabel: cleanupLabel,
			Cause:        passwordSaveErr,
		}
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
		if passwordSaveErr != nil {
			newInstanceRef["root_password_save_error"] = passwordSaveErr.Error()
		}
	}
	outcome := &actionOutcome{
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
		RollbackLabel: cleanupLabel,
		Rollback:      cleanupProvisionedInstance,
	}
	if cleanupInstanceID := strings.TrimSpace(payload.CleanupInstanceID); cleanupInstanceID != "" {
		outcome.OldInstanceRef = map[string]interface{}{
			"provider":            "vultr",
			"provider_entry_id":   token.ID,
			"provider_entry_name": token.Name,
			"instance_id":         cleanupInstanceID,
		}
		outcome.CleanupLabel = "delete vultr instance " + cleanupInstanceID
		outcome.Cleanup = func(ctx context.Context) error {
			if err := client.DeleteInstance(contextOrBackground(ctx), cleanupInstanceID); err != nil {
				if isVultrNotFoundError(err) {
					removeSavedVultrRootPassword(userUUID, addition, token, cleanupInstanceID)
					return nil
				}
				return normalizeExecutionStopError(err)
			}
			removeSavedVultrRootPassword(userUUID, addition, token, cleanupInstanceID)
			return nil
		}
	}
	return outcome, nil
}

func rebindAWSIPAddress(ctx context.Context, task models.FailoverTask, plan models.FailoverPlan) (*actionOutcome, error) {
	var payload awsRebindPayload
	if err := json.Unmarshal([]byte(plan.Payload), &payload); err != nil {
		return nil, fmt.Errorf("invalid aws rebind payload: %w", err)
	}
	payload, hasTarget := resolveAWSRebindPayload(task, payload, plan.ProviderEntryID)
	if !hasTarget {
		if normalizeAWSFailoverService(payload.Service) == "lightsail" {
			return nil, errors.New("aws lightsail rebind requires instance_name or a tracked current instance")
		}
		return nil, errors.New("aws ec2 rebind requires instance_id or a tracked current instance")
	}

	addition, credential, err := loadAWSCredential(task.UserID, plan.ProviderEntryID)
	if err != nil {
		return nil, err
	}
	region := resolveAWSFailoverRegion(payload.Region, addition, credential)

	service := normalizeAWSFailoverService(payload.Service)

	switch service {
	case "ec2":
		instanceID := strings.TrimSpace(payload.InstanceID)
		detail, err := awscloud.GetInstanceDetail(contextOrBackground(ctx), credential, region, instanceID)
		if err != nil {
			return nil, normalizeExecutionStopError(err)
		}
		address, err := awscloud.AllocateAndAssociateAddress(contextOrBackground(ctx), credential, region, instanceID, strings.TrimSpace(payload.PrivateIP))
		if err != nil {
			return nil, normalizeExecutionStopError(err)
		}
		return &actionOutcome{
			IPv4:             strings.TrimSpace(address.PublicIP),
			IPv6:             firstString(detail.Instance.IPv6Addresses),
			TargetClientUUID: task.WatchClientUUID,
			NewClientUUID:    task.WatchClientUUID,
			OldInstanceRef: map[string]interface{}{
				"provider":            "aws",
				"service":             "ec2",
				"provider_entry_id":   credential.ID,
				"provider_entry_name": credential.Name,
				"region":              region,
				"instance_id":         instanceID,
			},
			NewAddresses: map[string]interface{}{
				"allocation_id":  address.AllocationID,
				"association_id": address.AssociationID,
				"public_ip":      address.PublicIP,
				"private_ip":     address.PrivateIP,
				"ipv6_addresses": detail.Instance.IPv6Addresses,
				"old_addresses":  detail.Addresses,
			},
		}, nil
	case "lightsail":
		instanceName := strings.TrimSpace(payload.InstanceName)
		staticIPName := strings.TrimSpace(payload.StaticIPName)
		if staticIPName == "" {
			staticIPName = fmt.Sprintf("%s-ip-%d", instanceName, time.Now().Unix())
		}
		detail, err := awscloud.GetLightsailInstanceDetail(contextOrBackground(ctx), credential, region, instanceName)
		if err != nil {
			return nil, normalizeExecutionStopError(err)
		}
		if err := awscloud.AllocateLightsailStaticIP(contextOrBackground(ctx), credential, region, staticIPName); err != nil {
			return nil, normalizeExecutionStopError(err)
		}
		if err := awscloud.AttachLightsailStaticIP(contextOrBackground(ctx), credential, region, staticIPName, instanceName); err != nil {
			return nil, normalizeExecutionStopError(err)
		}
		detail, err = waitForLightsailInstance(ctx, region, credential, instanceName)
		if err != nil {
			return nil, normalizeExecutionStopError(err)
		}
		return &actionOutcome{
			IPv4:             strings.TrimSpace(detail.Instance.PublicIP),
			IPv6:             firstString(detail.Instance.IPv6Addresses),
			TargetClientUUID: task.WatchClientUUID,
			NewClientUUID:    task.WatchClientUUID,
			OldInstanceRef: map[string]interface{}{
				"provider":            "aws",
				"service":             "lightsail",
				"provider_entry_id":   credential.ID,
				"provider_entry_name": credential.Name,
				"region":              region,
				"instance_name":       instanceName,
			},
			NewAddresses: map[string]interface{}{
				"static_ip_name": staticIPName,
				"public_ip":      detail.Instance.PublicIP,
				"ipv6_addresses": detail.Instance.IPv6Addresses,
			},
		}, nil
	default:
		return nil, fmt.Errorf("unsupported aws rebind service: %s", payload.Service)
	}
}

func waitForAWSEC2Instance(ctx context.Context, region string, credential *awscloud.CredentialRecord, instanceID string) (*awscloud.Instance, *awscloud.InstanceDetail, error) {
	deadline := time.Now().Add(5 * time.Minute)
	for time.Now().Before(deadline) {
		if err := waitContextOrDelay(ctx, 0); err != nil {
			return nil, nil, err
		}
		instance, err := awscloud.GetInstance(ctx, credential, region, instanceID)
		if err != nil {
			if normalizedErr := normalizeExecutionStopError(err); errors.Is(normalizedErr, errExecutionStopped) {
				return nil, nil, normalizedErr
			}
		}
		if err == nil && instance != nil && strings.TrimSpace(instance.PublicIP) != "" {
			detail, detailErr := awscloud.GetInstanceDetail(ctx, credential, region, instanceID)
			if detailErr != nil {
				if normalizedErr := normalizeExecutionStopError(detailErr); errors.Is(normalizedErr, errExecutionStopped) {
					return nil, nil, normalizedErr
				}
				return instance, nil, nil
			}
			return instance, detail, nil
		}
		if err := waitContextOrDelay(ctx, 5*time.Second); err != nil {
			return nil, nil, err
		}
	}
	instance, err := awscloud.GetInstance(ctx, credential, region, instanceID)
	if err != nil {
		return nil, nil, normalizeExecutionStopError(err)
	}
	detail, _ := awscloud.GetInstanceDetail(ctx, credential, region, instanceID)
	return instance, detail, nil
}

func waitForLightsailInstance(ctx context.Context, region string, credential *awscloud.CredentialRecord, instanceName string) (*awscloud.LightsailInstanceDetail, error) {
	deadline := time.Now().Add(5 * time.Minute)
	for time.Now().Before(deadline) {
		if err := waitContextOrDelay(ctx, 0); err != nil {
			return nil, err
		}
		detail, err := awscloud.GetLightsailInstanceDetail(ctx, credential, region, instanceName)
		if err != nil {
			if normalizedErr := normalizeExecutionStopError(err); errors.Is(normalizedErr, errExecutionStopped) {
				return nil, normalizedErr
			}
		}
		if err == nil && detail != nil && strings.TrimSpace(detail.Instance.PublicIP) != "" {
			return detail, nil
		}
		if err := waitContextOrDelay(ctx, 5*time.Second); err != nil {
			return nil, err
		}
	}
	detail, err := awscloud.GetLightsailInstanceDetail(ctx, credential, region, instanceName)
	return detail, normalizeExecutionStopError(err)
}

func waitForDigitalOceanDroplet(ctx context.Context, client *digitalocean.Client, dropletID int) (*digitalocean.Droplet, error) {
	deadline := time.Now().Add(5 * time.Minute)
	for time.Now().Before(deadline) {
		if err := waitContextOrDelay(ctx, 0); err != nil {
			return nil, err
		}
		droplets, err := client.ListDroplets(ctx)
		if err != nil {
			return nil, normalizeExecutionStopError(err)
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
		return nil, normalizeExecutionStopError(err)
	}
	for _, droplet := range droplets {
		if droplet.ID == dropletID {
			return &droplet, nil
		}
	}
	return nil, fmt.Errorf("digitalocean droplet not found: %d", dropletID)
}

func waitForLinodeInstance(ctx context.Context, client *linodecloud.Client, instanceID int) (*linodecloud.Instance, error) {
	deadline := time.Now().Add(5 * time.Minute)
	for time.Now().Before(deadline) {
		if err := waitContextOrDelay(ctx, 0); err != nil {
			return nil, err
		}
		instance, err := client.GetInstance(ctx, instanceID)
		if err != nil {
			if normalizedErr := normalizeExecutionStopError(err); errors.Is(normalizedErr, errExecutionStopped) {
				return nil, normalizedErr
			}
		}
		if err == nil && instance != nil && firstString(instance.IPv4) != "" {
			return instance, nil
		}
		if err := waitContextOrDelay(ctx, 5*time.Second); err != nil {
			return nil, err
		}
	}
	instance, err := client.GetInstance(ctx, instanceID)
	return instance, normalizeExecutionStopError(err)
}

func waitForVultrInstance(ctx context.Context, client *vultrcloud.Client, instanceID string) (*vultrcloud.Instance, error) {
	deadline := time.Now().Add(5 * time.Minute)
	for time.Now().Before(deadline) {
		if err := waitContextOrDelay(ctx, 0); err != nil {
			return nil, err
		}
		instance, err := client.GetInstance(ctx, instanceID)
		if err != nil {
			if normalizedErr := normalizeExecutionStopError(err); errors.Is(normalizedErr, errExecutionStopped) {
				return nil, normalizedErr
			}
		}
		if err == nil && instance != nil && vultrPublicIPv4(instance) != "" {
			return instance, nil
		}
		if err := waitContextOrDelay(ctx, 5*time.Second); err != nil {
			return nil, err
		}
	}
	instance, err := client.GetInstance(ctx, instanceID)
	return instance, normalizeExecutionStopError(err)
}

func vultrPublicIPv4(instance *vultrcloud.Instance) string {
	if instance == nil {
		return ""
	}
	return normalizeIPAddress(instance.MainIP)
}

func vultrPublicIPv6(instance *vultrcloud.Instance) string {
	if instance == nil {
		return ""
	}
	return normalizeIPAddress(instance.V6MainIP)
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

func firstString(values []string) string {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			return value
		}
	}
	return ""
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

func trimStrings(values []string) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			result = append(result, value)
		}
	}
	return result
}
