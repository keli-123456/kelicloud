package admin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	smithy "github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/gin-gonic/gin"
	"github.com/komari-monitor/komari/api"
	dbawsfollowup "github.com/komari-monitor/komari/database/awsfollowup"
	"github.com/komari-monitor/komari/database/models"
	"github.com/komari-monitor/komari/utils/awsfollowup"
	awscloud "github.com/komari-monitor/komari/utils/cloudprovider/aws"
	"gorm.io/gorm"
)

const awsProviderName = "aws"

const (
	awsCreateFollowUpSyncAttempts = 5
	awsCreateFollowUpSyncDelay    = 2 * time.Second
	awsCreateFollowUpNextRunDelay = 15 * time.Second
	awsAccountIdentityTimeout     = 12 * time.Second
	awsAccountQuotaTimeout        = 10 * time.Second
)

type createAWSInstancePayload struct {
	Region           string         `json:"region,omitempty"`
	Name             string         `json:"name"`
	ImageID          string         `json:"image_id" binding:"required"`
	InstanceType     string         `json:"instance_type" binding:"required"`
	KeyName          string         `json:"key_name,omitempty"`
	SubnetID         string         `json:"subnet_id,omitempty"`
	SecurityGroupIDs []string       `json:"security_group_ids,omitempty"`
	UserData         string         `json:"user_data,omitempty"`
	AssignPublicIP   bool           `json:"assign_public_ip"`
	AssignIPv6       bool           `json:"assign_ipv6"`
	AllowAllTraffic  bool           `json:"allow_all_traffic"`
	RootPasswordMode string         `json:"root_password_mode,omitempty"`
	RootPassword     string         `json:"root_password,omitempty"`
	Tags             []awscloud.Tag `json:"tags,omitempty"`
	AutoConnect      bool           `json:"auto_connect"`
	AutoConnectGroup string         `json:"auto_connect_group,omitempty"`
}

type createAWSLightsailInstancePayload struct {
	Region           string         `json:"region,omitempty"`
	Name             string         `json:"name"`
	AvailabilityZone string         `json:"availability_zone" binding:"required"`
	BlueprintID      string         `json:"blueprint_id" binding:"required"`
	BundleID         string         `json:"bundle_id" binding:"required"`
	KeyPairName      string         `json:"key_pair_name,omitempty"`
	UserData         string         `json:"user_data,omitempty"`
	IPAddressType    string         `json:"ip_address_type,omitempty"`
	AllowAllTraffic  bool           `json:"allow_all_traffic"`
	RootPasswordMode string         `json:"root_password_mode,omitempty"`
	RootPassword     string         `json:"root_password,omitempty"`
	Tags             []awscloud.Tag `json:"tags,omitempty"`
	AutoConnect      bool           `json:"auto_connect"`
	AutoConnectGroup string         `json:"auto_connect_group,omitempty"`
}

type awsInstanceView struct {
	awscloud.Instance
	SavedRootPassword          bool   `json:"saved_root_password"`
	SavedRootPasswordUpdatedAt string `json:"saved_root_password_updated_at,omitempty"`
}

type awsInstanceDetailView struct {
	Instance           *awsInstanceView         `json:"instance"`
	VPCID              string                   `json:"vpc_id"`
	SubnetID           string                   `json:"subnet_id"`
	Architecture       string                   `json:"architecture"`
	PlatformDetails    string                   `json:"platform_details"`
	VirtualizationType string                   `json:"virtualization_type"`
	RootDeviceName     string                   `json:"root_device_name"`
	MonitoringState    string                   `json:"monitoring_state"`
	StateReason        string                   `json:"state_reason"`
	PublicDNSName      string                   `json:"public_dns_name"`
	PrivateDNSName     string                   `json:"private_dns_name"`
	SecurityGroups     []awscloud.SecurityGroup `json:"security_groups"`
	Volumes            []awscloud.Volume        `json:"volumes"`
	Addresses          []awscloud.Address       `json:"addresses"`
	ConsoleOutput      string                   `json:"console_output"`
}

type createAWSInstanceResponse struct {
	Instance          *awsInstanceView `json:"instance"`
	GeneratedPassword string           `json:"generated_password,omitempty"`
	Warning           string           `json:"warning,omitempty"`
	PasswordSaved     bool             `json:"password_saved"`
	PasswordSaveError string           `json:"password_save_error,omitempty"`
}

type awsLightsailInstanceView struct {
	awscloud.LightsailInstance
	SavedRootPassword          bool   `json:"saved_root_password"`
	SavedRootPasswordUpdatedAt string `json:"saved_root_password_updated_at,omitempty"`
}

type awsLightsailInstanceDetailView struct {
	Instance  *awsLightsailInstanceView    `json:"instance"`
	Ports     []awscloud.LightsailPort     `json:"ports"`
	StaticIPs []awscloud.LightsailStaticIP `json:"static_ips"`
	Snapshots []awscloud.LightsailSnapshot `json:"snapshots"`
}

type createAWSLightsailInstanceResponse struct {
	Name              string `json:"name"`
	Status            string `json:"status"`
	Warning           string `json:"warning,omitempty"`
	GeneratedPassword string `json:"generated_password,omitempty"`
	PasswordSaved     bool   `json:"password_saved"`
	PasswordSaveError string `json:"password_save_error,omitempty"`
}

type awsAccountView struct {
	AccountID     string                    `json:"account_id"`
	ARN           string                    `json:"arn"`
	UserID        string                    `json:"user_id"`
	Region        string                    `json:"region"`
	EC2Quota      *awscloud.EC2QuotaSummary `json:"ec2_quota,omitempty"`
	EC2QuotaError string                    `json:"ec2_quota_error,omitempty"`
}

type awsFollowUpTaskView struct {
	ID             uint              `json:"id"`
	CredentialID   string            `json:"credential_id"`
	CredentialName string            `json:"credential_name"`
	Region         string            `json:"region"`
	TaskType       string            `json:"task_type"`
	ResourceID     string            `json:"resource_id"`
	Status         string            `json:"status"`
	Attempts       int               `json:"attempts"`
	MaxAttempts    int               `json:"max_attempts"`
	LastError      string            `json:"last_error,omitempty"`
	LastAttemptAt  *models.LocalTime `json:"last_attempt_at,omitempty"`
	NextRunAt      models.LocalTime  `json:"next_run_at"`
	CompletedAt    *models.LocalTime `json:"completed_at,omitempty"`
	CreatedAt      models.LocalTime  `json:"created_at"`
	UpdatedAt      models.LocalTime  `json:"updated_at"`
}

func GetAWSCredentials(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	_, addition, err := loadAWSAddition(scope, true)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}

	api.RespondSuccess(c, addition.ToPoolViewWithoutQuota())
}

func SaveAWSCredentials(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	var payload struct {
		Credentials        []awscloud.CredentialImport `json:"credentials"`
		ActiveCredentialID string                      `json:"active_credential_id"`
		ActiveRegion       string                      `json:"active_region"`
	}
	if err := c.ShouldBindJSON(&payload); err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid credential payload: "+err.Error())
		return
	}

	if len(payload.Credentials) == 0 {
		api.RespondError(c, http.StatusBadRequest, "At least one credential is required")
		return
	}

	_, addition, err := loadAWSAddition(scope, true)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}

	imported := addition.UpsertCredentials(payload.Credentials)
	if imported == 0 {
		api.RespondError(c, http.StatusBadRequest, "No valid credentials were provided")
		return
	}

	if payload.ActiveCredentialID != "" {
		if !addition.SetActiveCredential(payload.ActiveCredentialID) {
			api.RespondError(c, http.StatusBadRequest, "Active credential not found")
			return
		}
	}
	if strings.TrimSpace(payload.ActiveRegion) != "" {
		addition.SetActiveRegion(payload.ActiveRegion)
	}

	if err := saveAWSAddition(scope, addition); err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to save AWS credentials: "+err.Error())
		return
	}

	logCloudAudit(c, fmt.Sprintf("import aws credentials: %d", imported))
	api.RespondSuccess(c, addition.ToPoolViewWithoutQuota())
}

func SetAWSActiveCredential(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	var payload struct {
		CredentialID string `json:"credential_id" binding:"required"`
	}
	if err := c.ShouldBindJSON(&payload); err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid active credential payload: "+err.Error())
		return
	}

	_, addition, err := loadAWSAddition(scope, false)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}

	if !addition.SetActiveCredential(payload.CredentialID) {
		api.RespondError(c, http.StatusNotFound, "AWS credential not found")
		return
	}

	if err := saveAWSAddition(scope, addition); err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to update active credential: "+err.Error())
		return
	}

	logCloudAudit(c, "set active aws credential: "+payload.CredentialID)
	api.RespondSuccess(c, addition.ToPoolViewWithoutQuota())
}

func SetAWSActiveRegion(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	var payload struct {
		Region string `json:"region" binding:"required"`
	}
	if err := c.ShouldBindJSON(&payload); err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid active region payload: "+err.Error())
		return
	}

	_, addition, err := loadAWSAddition(scope, false)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}

	addition.SetActiveRegion(payload.Region)
	if err := saveAWSAddition(scope, addition); err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to update active region: "+err.Error())
		return
	}

	logCloudAudit(c, "set active aws region: "+payload.Region)
	api.RespondSuccess(c, addition.ToPoolViewWithoutQuota())
}

func CheckAWSCredentials(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	var payload struct {
		CredentialIDs []string `json:"credential_ids"`
	}
	if c.Request.ContentLength > 0 {
		if err := c.ShouldBindJSON(&payload); err != nil {
			api.RespondError(c, http.StatusBadRequest, "Invalid credential check payload: "+err.Error())
			return
		}
	}

	_, addition, err := loadAWSAddition(scope, false)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}

	if len(addition.Credentials) == 0 {
		api.RespondSuccess(c, addition.ToPoolView())
		return
	}

	selected := make(map[string]struct{}, len(payload.CredentialIDs))
	for _, credentialID := range payload.CredentialIDs {
		credentialID = strings.TrimSpace(credentialID)
		if credentialID != "" {
			selected[credentialID] = struct{}{}
		}
	}

	checkedAt := time.Now().UTC()
	var wg sync.WaitGroup
	var mu sync.Mutex
	limiter := make(chan struct{}, 3)

	for index := range addition.Credentials {
		credentialID := addition.Credentials[index].ID
		if len(selected) > 0 {
			if _, exists := selected[credentialID]; !exists {
				continue
			}
		}

		wg.Add(1)
		go func(credentialIndex int) {
			defer wg.Done()

			limiter <- struct{}{}
			defer func() {
				<-limiter
			}()

			record := addition.Credentials[credentialIndex]
			region := strings.TrimSpace(addition.ActiveRegion)
			if region == "" {
				region = record.DefaultRegion
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			identity, err := awscloud.GetIdentity(ctx, &record, region)
			var quota *awscloud.EC2QuotaSummary
			var quotaErr error
			if err == nil {
				quota, quotaErr = awscloud.GetEC2QuotaSummary(ctx, &record, region)
			}
			mu.Lock()
			addition.Credentials[credentialIndex].SetCheckResult(checkedAt, identity, quota, quotaErr, err)
			mu.Unlock()
		}(index)
	}

	wg.Wait()

	if err := saveAWSAddition(scope, addition); err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to save credential health: "+err.Error())
		return
	}

	logCloudAudit(c, fmt.Sprintf("check aws credentials: %d", len(addition.Credentials)))
	api.RespondSuccess(c, addition.ToPoolView())
}

func DeleteAWSCredential(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	credentialID := strings.TrimSpace(c.Param("id"))
	if credentialID == "" {
		api.RespondError(c, http.StatusBadRequest, "Invalid credential id")
		return
	}

	_, addition, err := loadAWSAddition(scope, false)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}

	if !addition.RemoveCredential(credentialID) {
		api.RespondError(c, http.StatusNotFound, "AWS credential not found")
		return
	}

	if err := saveAWSAddition(scope, addition); err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to delete credential: "+err.Error())
		return
	}

	cancelledCount, cancelErr := dbawsfollowup.CancelPendingTasksByCredential(
		scope.UserUUID,
		credentialID,
		time.Now(),
		"AWS credential removed; follow-up task cancelled",
	)
	if cancelErr != nil {
		log.Printf("aws follow-up: failed to cancel tasks for deleted credential %s: %v", credentialID, cancelErr)
	}

	logCloudAudit(c, fmt.Sprintf("delete aws credential: %s (cancelled %d follow-up tasks)", credentialID, cancelledCount))
	api.RespondSuccess(c, addition.ToPoolView())
}

func ListAWSFollowUpTasks(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	_, addition, err := loadAWSAddition(scope, true)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}

	tasks, err := dbawsfollowup.ListTasksByUser(scope.UserUUID, 200, false)
	if err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to load AWS follow-up tasks: "+err.Error())
		return
	}

	credentialNames := make(map[string]string, len(addition.Credentials))
	for _, credential := range addition.Credentials {
		credentialNames[credential.ID] = credential.Name
	}

	views := make([]awsFollowUpTaskView, 0, len(tasks))
	for _, task := range tasks {
		credentialName := credentialNames[task.CredentialID]
		if credentialName == "" {
			credentialName = task.CredentialID
		}
		views = append(views, awsFollowUpTaskView{
			ID:             task.ID,
			CredentialID:   task.CredentialID,
			CredentialName: credentialName,
			Region:         task.Region,
			TaskType:       task.TaskType,
			ResourceID:     task.ResourceID,
			Status:         task.Status,
			Attempts:       task.Attempts,
			MaxAttempts:    task.MaxAttempts,
			LastError:      task.LastError,
			LastAttemptAt:  task.LastAttemptAt,
			NextRunAt:      task.NextRunAt,
			CompletedAt:    task.CompletedAt,
			CreatedAt:      task.CreatedAt,
			UpdatedAt:      task.UpdatedAt,
		})
	}

	api.RespondSuccess(c, views)
}

func RetryAWSFollowUpTask(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	taskIDValue := strings.TrimSpace(c.Param("id"))
	taskID, err := strconv.ParseUint(taskIDValue, 10, 64)
	if err != nil || taskID == 0 {
		api.RespondError(c, http.StatusBadRequest, "Invalid follow-up task id")
		return
	}

	if err := dbawsfollowup.RetryTaskByID(scope.UserUUID, uint(taskID), time.Now()); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			api.RespondError(c, http.StatusNotFound, "AWS follow-up task not found or is not retryable")
			return
		}
		api.RespondError(c, http.StatusInternalServerError, "Failed to retry AWS follow-up task: "+err.Error())
		return
	}

	logCloudAudit(c, fmt.Sprintf("retry aws follow-up task: %d", taskID))
	api.RespondSuccess(c, gin.H{"id": taskID, "status": models.AWSFollowUpTaskStatusPending})
}

func ClearAWSFollowUpTerminalTasks(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	deletedCount, err := dbawsfollowup.DeleteTerminalTasksByUser(scope.UserUUID)
	if err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to clear AWS follow-up tasks: "+err.Error())
		return
	}

	logCloudAudit(c, fmt.Sprintf("clear aws follow-up terminal tasks: %d", deletedCount))
	api.RespondSuccess(c, gin.H{"deleted_count": deletedCount})
}

func GetAWSCredentialSecret(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	credentialID := strings.TrimSpace(c.Param("id"))
	if credentialID == "" {
		api.RespondError(c, http.StatusBadRequest, "Invalid credential id")
		return
	}

	_, addition, err := loadAWSAddition(scope, false)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}

	credential := addition.FindCredential(credentialID)
	if credential == nil {
		api.RespondError(c, http.StatusNotFound, "AWS credential not found")
		return
	}

	api.RespondSuccess(c, credential.SecretView())
}

func GetAWSInstancePassword(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	instanceID := strings.TrimSpace(c.Param("id"))
	if instanceID == "" {
		api.RespondError(c, http.StatusBadRequest, "Invalid instance id")
		return
	}

	_, addition, err := loadAWSAddition(scope, false)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}

	activeCredential := addition.ActiveCredential()
	if activeCredential == nil {
		api.RespondError(c, http.StatusBadRequest, "AWS credential is not configured")
		return
	}

	resourceKey := buildAWSResourceCredentialID(resolveAWSRegion(addition, activeCredential, ""), instanceID)
	passwordView, err := activeCredential.RevealResourcePassword("ec2", resourceKey)
	if err != nil {
		switch {
		case errors.Is(err, awscloud.ErrSavedRootPasswordNotFound):
			api.RespondError(c, http.StatusNotFound, err.Error())
		case errors.Is(err, awscloud.ErrRootPasswordVaultDisabled), errors.Is(err, awscloud.ErrRootPasswordDecryptFailed):
			api.RespondError(c, http.StatusBadRequest, err.Error())
		default:
			api.RespondError(c, http.StatusInternalServerError, "Failed to load saved root password: "+err.Error())
		}
		return
	}

	api.RespondSuccess(c, passwordView)
}

func GetAWSLightsailInstancePassword(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	instanceName := strings.TrimSpace(c.Param("name"))
	if instanceName == "" {
		api.RespondError(c, http.StatusBadRequest, "Invalid Lightsail instance name")
		return
	}

	_, addition, err := loadAWSAddition(scope, false)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}

	activeCredential := addition.ActiveCredential()
	if activeCredential == nil {
		api.RespondError(c, http.StatusBadRequest, "AWS credential is not configured")
		return
	}

	resourceKey := buildAWSResourceCredentialID(resolveAWSRegion(addition, activeCredential, ""), instanceName)
	passwordView, err := activeCredential.RevealResourcePassword("lightsail", resourceKey)
	if err != nil {
		switch {
		case errors.Is(err, awscloud.ErrSavedRootPasswordNotFound):
			api.RespondError(c, http.StatusNotFound, err.Error())
		case errors.Is(err, awscloud.ErrRootPasswordVaultDisabled), errors.Is(err, awscloud.ErrRootPasswordDecryptFailed):
			api.RespondError(c, http.StatusBadRequest, err.Error())
		default:
			api.RespondError(c, http.StatusInternalServerError, "Failed to load saved root password: "+err.Error())
		}
		return
	}

	api.RespondSuccess(c, passwordView)
}

func GetAWSAccount(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	addition, credential, region, _, cancel, err := getAWSActiveCredential(c, scope)
	if err != nil {
		respondAWSError(c, err)
		return
	}
	defer cancel()

	includeQuota := c.Query("include_quota") == "1" || strings.EqualFold(c.Query("include_quota"), "true")

	identityCtx, identityCancel := context.WithTimeout(c.Request.Context(), awsAccountIdentityTimeout)
	defer identityCancel()

	identity, err := awscloud.GetIdentity(identityCtx, credential, region)
	if err != nil {
		respondAWSError(c, err)
		return
	}

	var quota *awscloud.EC2QuotaSummary
	var quotaErr error
	if includeQuota {
		quotaCtx, quotaCancel := context.WithTimeout(c.Request.Context(), awsAccountQuotaTimeout)
		defer quotaCancel()
		quota, quotaErr = awscloud.GetEC2QuotaSummary(quotaCtx, credential, region)
	}

	if includeQuota && identity != nil {
		credential.SetCheckResult(time.Now(), identity, quota, quotaErr, nil)
		_ = saveAWSAddition(scope, addition)
	}

	api.RespondSuccess(c, awsAccountView{
		AccountID:     identity.AccountID,
		ARN:           identity.ARN,
		UserID:        identity.UserID,
		Region:        region,
		EC2Quota:      quota,
		EC2QuotaError: errorString(quotaErr),
	})
}

func GetAWSCatalog(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	regionOverride := strings.TrimSpace(c.Query("region"))
	_, credential, region, _, cancel, err := getAWSActiveCredentialWithRegion(c, scope, regionOverride)
	if err != nil {
		respondAWSError(c, err)
		return
	}
	cancel()

	ctx, createCancel := context.WithTimeout(c.Request.Context(), 90*time.Second)
	defer createCancel()

	regions, err := awscloud.ListRegions(ctx, credential)
	if err != nil {
		respondAWSError(c, err)
		return
	}
	instanceTypes, err := awscloud.ListInstanceTypes(ctx, credential, region)
	if err != nil {
		respondAWSError(c, err)
		return
	}
	instanceTypeOfferings, err := awscloud.ListInstanceTypeOfferings(ctx, credential, region)
	if err != nil {
		respondAWSError(c, err)
		return
	}
	images, err := awscloud.ListSuggestedImages(ctx, credential, region)
	if err != nil {
		respondAWSError(c, err)
		return
	}
	keyPairs, err := awscloud.ListKeyPairs(ctx, credential, region)
	if err != nil {
		respondAWSError(c, err)
		return
	}
	subnets, err := awscloud.ListSubnets(ctx, credential, region)
	if err != nil {
		respondAWSError(c, err)
		return
	}
	securityGroups, err := awscloud.ListSecurityGroups(ctx, credential, region)
	if err != nil {
		respondAWSError(c, err)
		return
	}
	elasticAddresses, err := awscloud.ListElasticAddresses(ctx, credential, region)
	if err != nil {
		respondAWSError(c, err)
		return
	}

	if regions == nil {
		regions = make([]awscloud.Region, 0)
	}
	if instanceTypes == nil {
		instanceTypes = make([]awscloud.InstanceType, 0)
	}
	if instanceTypeOfferings == nil {
		instanceTypeOfferings = make([]awscloud.InstanceTypeOffering, 0)
	}
	if images == nil {
		images = make([]awscloud.Image, 0)
	}
	if keyPairs == nil {
		keyPairs = make([]awscloud.KeyPair, 0)
	}
	if subnets == nil {
		subnets = make([]awscloud.Subnet, 0)
	}
	if securityGroups == nil {
		securityGroups = make([]awscloud.SecurityGroup, 0)
	}
	if elasticAddresses == nil {
		elasticAddresses = make([]awscloud.Address, 0)
	}

	api.RespondSuccess(c, gin.H{
		"active_region":           region,
		"regions":                 regions,
		"instance_types":          instanceTypes,
		"instance_type_offerings": instanceTypeOfferings,
		"images":                  images,
		"key_pairs":               keyPairs,
		"subnets":                 subnets,
		"security_groups":         securityGroups,
		"elastic_addresses":       elasticAddresses,
	})
}

func GetAWSInstanceDetail(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	_, credential, region, ctx, cancel, err := getAWSActiveCredential(c, scope)
	if err != nil {
		respondAWSError(c, err)
		return
	}
	defer cancel()

	instanceID := strings.TrimSpace(c.Param("id"))
	if instanceID == "" {
		api.RespondError(c, http.StatusBadRequest, "Invalid instance id")
		return
	}

	detail, err := awscloud.GetInstanceDetail(ctx, credential, region, instanceID)
	if err != nil {
		respondAWSError(c, err)
		return
	}

	api.RespondSuccess(c, buildAWSInstanceDetailView(detail, credential, region))
}

func GetAWSLightsailCatalog(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	regionOverride := strings.TrimSpace(c.Query("region"))
	_, credential, region, ctx, cancel, err := getAWSActiveCredentialWithRegion(c, scope, regionOverride)
	if err != nil {
		respondAWSError(c, err)
		return
	}
	defer cancel()

	regions, err := awscloud.ListLightsailRegions(ctx, credential, region)
	if err != nil {
		respondAWSError(c, err)
		return
	}
	bundles, err := awscloud.ListLightsailBundles(ctx, credential, region)
	if err != nil {
		respondAWSError(c, err)
		return
	}
	blueprints, err := awscloud.ListLightsailBlueprints(ctx, credential, region)
	if err != nil {
		respondAWSError(c, err)
		return
	}
	keyPairs, err := awscloud.ListLightsailKeyPairs(ctx, credential, region)
	if err != nil {
		respondAWSError(c, err)
		return
	}
	staticIPs, err := awscloud.ListLightsailStaticIPs(ctx, credential, region)
	if err != nil {
		respondAWSError(c, err)
		return
	}

	if regions == nil {
		regions = make([]awscloud.LightsailRegion, 0)
	}
	if bundles == nil {
		bundles = make([]awscloud.LightsailBundle, 0)
	}
	if blueprints == nil {
		blueprints = make([]awscloud.LightsailBlueprint, 0)
	}
	if keyPairs == nil {
		keyPairs = make([]awscloud.LightsailKeyPair, 0)
	}
	if staticIPs == nil {
		staticIPs = make([]awscloud.LightsailStaticIP, 0)
	}

	api.RespondSuccess(c, gin.H{
		"active_region": region,
		"regions":       regions,
		"bundles":       bundles,
		"blueprints":    blueprints,
		"key_pairs":     keyPairs,
		"static_ips":    staticIPs,
	})
}

func ListAWSLightsailInstances(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	addition, credential, region, ctx, cancel, err := getAWSActiveCredential(c, scope)
	if err != nil {
		respondAWSError(c, err)
		return
	}
	defer cancel()

	instances, err := awscloud.ListLightsailInstances(ctx, credential, region)
	if err != nil {
		respondAWSError(c, err)
		return
	}
	if instances == nil {
		instances = make([]awscloud.LightsailInstance, 0)
	}

	views := make([]awsLightsailInstanceView, 0, len(instances))
	validResourceIDs := make([]string, 0, len(instances))
	for index := range instances {
		if resourceKey := buildAWSResourceCredentialID(region, instances[index].Name); resourceKey != "" {
			validResourceIDs = append(validResourceIDs, resourceKey)
		}
		if view := buildAWSLightsailInstanceView(&instances[index], credential, region); view != nil {
			views = append(views, *view)
		}
	}
	if credential != nil && credential.PruneResourceCredentials("lightsail", validResourceIDs, buildAWSResourceCredentialScope(region)) {
		_ = saveAWSAddition(scope, addition)
	}

	api.RespondSuccess(c, views)
}

func GetAWSLightsailInstanceDetail(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	_, credential, region, ctx, cancel, err := getAWSActiveCredential(c, scope)
	if err != nil {
		respondAWSError(c, err)
		return
	}
	defer cancel()

	instanceName := strings.TrimSpace(c.Param("name"))
	if instanceName == "" {
		api.RespondError(c, http.StatusBadRequest, "Invalid Lightsail instance name")
		return
	}

	detail, err := awscloud.GetLightsailInstanceDetail(ctx, credential, region, instanceName)
	if err != nil {
		respondAWSError(c, err)
		return
	}

	api.RespondSuccess(c, buildAWSLightsailInstanceDetailView(detail, credential, region))
}

func ListAWSInstances(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	addition, credential, region, ctx, cancel, err := getAWSActiveCredential(c, scope)
	if err != nil {
		respondAWSError(c, err)
		return
	}
	defer cancel()

	instances, err := awscloud.ListInstances(ctx, credential, region)
	if err != nil {
		respondAWSError(c, err)
		return
	}
	if instances == nil {
		instances = make([]awscloud.Instance, 0)
	}

	views := make([]awsInstanceView, 0, len(instances))
	validResourceIDs := make([]string, 0, len(instances))
	for index := range instances {
		if resourceKey := buildAWSResourceCredentialID(region, instances[index].InstanceID); resourceKey != "" {
			validResourceIDs = append(validResourceIDs, resourceKey)
		}
		if view := buildAWSInstanceView(&instances[index], credential, region); view != nil {
			views = append(views, *view)
		}
	}
	if credential != nil && credential.PruneResourceCredentials("ec2", validResourceIDs, buildAWSResourceCredentialScope(region)) {
		_ = saveAWSAddition(scope, addition)
	}

	api.RespondSuccess(c, views)
}

func CreateAWSInstance(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	var payload createAWSInstancePayload
	if err := c.ShouldBindJSON(&payload); err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid instance request: "+err.Error())
		return
	}

	regionOverride := strings.TrimSpace(payload.Region)
	addition, credential, region, ctx, cancel, err := getAWSActiveCredentialWithRegion(c, scope, regionOverride)
	if err != nil {
		respondAWSError(c, err)
		return
	}
	defer cancel()

	resolvedUserData, generatedRootPassword, err := resolveAWSRootPasswordUserData(
		payload.RootPasswordMode,
		payload.RootPassword,
		payload.UserData,
	)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}

	autoConnectGroup := ""
	if payload.AutoConnect {
		resolvedUserData, autoConnectGroup, err = prepareCloudAutoConnectUserData(c, resolvedUserData, autoConnectUserDataOptions{
			Enabled:           true,
			Group:             payload.AutoConnectGroup,
			Provider:          awsProviderName,
			CredentialName:    credential.Name,
			WrapInShellScript: true,
		})
		if err != nil {
			api.RespondError(c, http.StatusBadRequest, err.Error())
			return
		}
	}

	name := strings.TrimSpace(payload.Name)
	if name == "" {
		name = fmt.Sprintf("komari-ec2-%d", time.Now().Unix())
	}

	request := awscloud.CreateInstanceRequest{
		Name:             name,
		ImageID:          strings.TrimSpace(payload.ImageID),
		InstanceType:     strings.TrimSpace(payload.InstanceType),
		KeyName:          strings.TrimSpace(payload.KeyName),
		SubnetID:         strings.TrimSpace(payload.SubnetID),
		SecurityGroupIDs: trimStringSlice(payload.SecurityGroupIDs),
		UserData:         resolvedUserData,
		AssignPublicIP:   payload.AssignPublicIP,
		AssignIPv6:       payload.AssignIPv6,
		Tags:             payload.Tags,
	}

	createResult, err := awscloud.CreateInstance(ctx, credential, region, request)
	if err != nil {
		respondAWSError(c, err)
		return
	}

	instance := createResult.Instance
	warnings := append(make([]string, 0, len(createResult.Warnings)+2), createResult.Warnings...)
	if createResult.AssignIPv6 {
		followUpErr := runAWSCreateFollowUp(ctx, awsCreateFollowUpSyncAttempts, awsCreateFollowUpSyncDelay, func(runCtx context.Context) error {
			_, err := awscloud.EnsureInstanceIPv6Address(runCtx, credential, region, instance.InstanceID)
			return err
		})
		if followUpErr != nil {
			if enqueueErr := awsfollowup.EnqueueEC2AssignIPv6(scope.UserUUID, credential.ID, region, instance.InstanceID, time.Now().Add(awsCreateFollowUpNextRunDelay)); enqueueErr != nil {
				warnings = append(warnings, fmt.Sprintf("IPv6 setup is still pending, and Komari could not queue the background retry: %v (enqueue error: %v)", followUpErr, enqueueErr))
			} else {
				warnings = append(warnings, fmt.Sprintf("IPv6 setup is still pending and Komari will keep retrying in the background: %v", followUpErr))
			}
		} else if refreshedInstance, refreshErr := awscloud.GetInstance(ctx, credential, region, instance.InstanceID); refreshErr == nil {
			instance = refreshedInstance
		}
	}
	if payload.AllowAllTraffic {
		followUpErr := runAWSCreateFollowUp(ctx, awsCreateFollowUpSyncAttempts, awsCreateFollowUpSyncDelay, func(runCtx context.Context) error {
			_, err := awscloud.AllowAllSecurityGroupTraffic(runCtx, credential, region, instance.InstanceID)
			return err
		})
		if followUpErr != nil {
			if enqueueErr := awsfollowup.EnqueueEC2AllowAllTraffic(scope.UserUUID, credential.ID, region, instance.InstanceID, time.Now().Add(awsCreateFollowUpNextRunDelay)); enqueueErr != nil {
				warnings = append(warnings, fmt.Sprintf("Security group opening is still pending, and Komari could not queue the background retry: %v (enqueue error: %v)", followUpErr, enqueueErr))
			} else {
				warnings = append(warnings, fmt.Sprintf("Security group opening is still pending and Komari will keep retrying in the background: %v", followUpErr))
			}
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

	passwordSaved := false
	passwordSaveError := ""
	responseCredential := credential
	if instance != nil && credential != nil && rootPassword != "" {
		persistedAddition, saveErr := persistAWSResourcePassword(
			scope,
			addition,
			credential,
			"ec2",
			buildAWSResourceCredentialID(region, instance.InstanceID),
			firstNonEmptyAWS(instance.Name, instance.InstanceID),
			passwordMode,
			rootPassword,
		)
		if saveErr != nil {
			passwordSaveError = saveErr.Error()
		} else {
			passwordSaved = true
			if persistedCredential := findAWSCredentialForPersistenceVerification(
				persistedAddition,
				credential.ID,
				credential.AccessKeyID,
				credential.DefaultRegion,
			); persistedCredential != nil {
				responseCredential = persistedCredential
			}
		}
	}

	logMessage := fmt.Sprintf("create aws ec2 instance: %s (%s/%s", request.Name, region, request.InstanceType)
	if autoConnectGroup != "" {
		logMessage += ", auto_connect_group=" + autoConnectGroup
	}
	logMessage += ")"
	logCloudAudit(c, logMessage)
	api.RespondSuccess(c, createAWSInstanceResponse{
		Instance:          buildAWSInstanceView(instance, responseCredential, region),
		Warning:           strings.Join(warnings, "; "),
		GeneratedPassword: generatedRootPassword,
		PasswordSaved:     passwordSaved,
		PasswordSaveError: passwordSaveError,
	})
}

func CreateAWSLightsailInstance(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	var payload createAWSLightsailInstancePayload
	if err := c.ShouldBindJSON(&payload); err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid Lightsail instance request: "+err.Error())
		return
	}

	regionOverride := strings.TrimSpace(payload.Region)
	addition, credential, region, ctx, cancel, err := getAWSActiveCredentialWithRegion(c, scope, regionOverride)
	if err != nil {
		respondAWSError(c, err)
		return
	}
	defer cancel()

	resolvedUserData, generatedRootPassword, err := resolveAWSRootPasswordUserData(
		payload.RootPasswordMode,
		payload.RootPassword,
		payload.UserData,
	)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}

	autoConnectGroup := ""
	if payload.AutoConnect {
		resolvedUserData, autoConnectGroup, err = prepareCloudAutoConnectUserData(c, resolvedUserData, autoConnectUserDataOptions{
			Enabled:           true,
			Group:             payload.AutoConnectGroup,
			Provider:          awsProviderName,
			CredentialName:    credential.Name,
			WrapInShellScript: true,
		})
		if err != nil {
			api.RespondError(c, http.StatusBadRequest, err.Error())
			return
		}
	}

	name := strings.TrimSpace(payload.Name)
	if name == "" {
		name = fmt.Sprintf("komari-lightsail-%d", time.Now().Unix())
	}

	request := awscloud.CreateLightsailInstanceRequest{
		Name:             name,
		AvailabilityZone: strings.TrimSpace(payload.AvailabilityZone),
		BlueprintID:      strings.TrimSpace(payload.BlueprintID),
		BundleID:         strings.TrimSpace(payload.BundleID),
		KeyPairName:      strings.TrimSpace(payload.KeyPairName),
		UserData:         resolvedUserData,
		IPAddressType:    strings.TrimSpace(payload.IPAddressType),
		Tags:             payload.Tags,
	}

	if err := awscloud.CreateLightsailInstance(ctx, credential, region, request); err != nil {
		respondAWSError(c, err)
		return
	}

	warnings := make([]string, 0, 1)
	if payload.AllowAllTraffic {
		followUpErr := runAWSCreateFollowUp(ctx, awsCreateFollowUpSyncAttempts, awsCreateFollowUpSyncDelay, func(runCtx context.Context) error {
			return awscloud.OpenLightsailAllPublicPorts(runCtx, credential, region, request.Name)
		})
		if followUpErr != nil {
			if enqueueErr := awsfollowup.EnqueueLightsailAllowAllPorts(scope.UserUUID, credential.ID, region, request.Name, time.Now().Add(awsCreateFollowUpNextRunDelay)); enqueueErr != nil {
				warnings = append(warnings, fmt.Sprintf("Lightsail firewall opening is still pending, and Komari could not queue the background retry: %v (enqueue error: %v)", followUpErr, enqueueErr))
			} else {
				warnings = append(warnings, fmt.Sprintf("Lightsail firewall opening is still pending and Komari will keep retrying in the background: %v", followUpErr))
			}
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

	passwordSaved := false
	passwordSaveError := ""
	if credential != nil && rootPassword != "" {
		if _, saveErr := persistAWSResourcePassword(
			scope,
			addition,
			credential,
			"lightsail",
			buildAWSResourceCredentialID(region, request.Name),
			request.Name,
			passwordMode,
			rootPassword,
		); saveErr != nil {
			passwordSaveError = saveErr.Error()
		} else {
			passwordSaved = true
		}
	}

	logMessage := fmt.Sprintf("create aws lightsail instance: %s (%s/%s", request.Name, request.AvailabilityZone, request.BundleID)
	if autoConnectGroup != "" {
		logMessage += ", auto_connect_group=" + autoConnectGroup
	}
	logMessage += ")"
	logCloudAudit(c, logMessage)
	api.RespondSuccess(c, createAWSLightsailInstanceResponse{
		Name:              request.Name,
		Status:            "submitted",
		Warning:           strings.Join(warnings, "; "),
		GeneratedPassword: generatedRootPassword,
		PasswordSaved:     passwordSaved,
		PasswordSaveError: passwordSaveError,
	})
}

func DeleteAWSInstance(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	addition, credential, region, ctx, cancel, err := getAWSActiveCredential(c, scope)
	if err != nil {
		respondAWSError(c, err)
		return
	}
	defer cancel()

	instanceID := strings.TrimSpace(c.Param("id"))
	if instanceID == "" {
		api.RespondError(c, http.StatusBadRequest, "Invalid instance id")
		return
	}

	if err := awscloud.TerminateInstance(ctx, credential, region, instanceID); err != nil {
		respondAWSError(c, err)
		return
	}
	if credential != nil && credential.RemoveSavedResourcePassword("ec2", buildAWSResourceCredentialID(region, instanceID)) {
		_ = saveAWSAddition(scope, addition)
	}

	logCloudAudit(c, fmt.Sprintf("terminate aws ec2 instance: %s", instanceID))
	api.RespondSuccess(c, nil)
}

func DeleteAWSLightsailInstance(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	addition, credential, region, ctx, cancel, err := getAWSActiveCredential(c, scope)
	if err != nil {
		respondAWSError(c, err)
		return
	}
	defer cancel()

	instanceName := strings.TrimSpace(c.Param("name"))
	if instanceName == "" {
		api.RespondError(c, http.StatusBadRequest, "Invalid Lightsail instance name")
		return
	}

	if err := awscloud.DeleteLightsailInstance(ctx, credential, region, instanceName); err != nil {
		respondAWSError(c, err)
		return
	}
	if credential != nil && credential.RemoveSavedResourcePassword("lightsail", buildAWSResourceCredentialID(region, instanceName)) {
		_ = saveAWSAddition(scope, addition)
	}

	logCloudAudit(c, fmt.Sprintf("delete aws lightsail instance: %s", instanceName))
	api.RespondSuccess(c, nil)
}

func PostAWSInstanceAction(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	_, credential, region, ctx, cancel, err := getAWSActiveCredential(c, scope)
	if err != nil {
		respondAWSError(c, err)
		return
	}
	defer cancel()

	instanceID := strings.TrimSpace(c.Param("id"))
	if instanceID == "" {
		api.RespondError(c, http.StatusBadRequest, "Invalid instance id")
		return
	}

	var payload struct {
		Type          string         `json:"type" binding:"required"`
		Name          string         `json:"name,omitempty"`
		Description   string         `json:"description,omitempty"`
		NoReboot      bool           `json:"no_reboot,omitempty"`
		InstanceType  string         `json:"instance_type,omitempty"`
		Tags          []awscloud.Tag `json:"tags,omitempty"`
		AllocationID  string         `json:"allocation_id,omitempty"`
		AssociationID string         `json:"association_id,omitempty"`
		PrivateIP     string         `json:"private_ip,omitempty"`
	}
	if err := c.ShouldBindJSON(&payload); err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid action request: "+err.Error())
		return
	}

	actionType := strings.ToLower(strings.TrimSpace(payload.Type))
	response := gin.H{"type": actionType, "instance_id": instanceID, "status": "submitted"}
	switch actionType {
	case "start":
		err = awscloud.StartInstance(ctx, credential, region, instanceID)
	case "stop":
		err = awscloud.StopInstance(ctx, credential, region, instanceID)
	case "reboot":
		err = awscloud.RebootInstance(ctx, credential, region, instanceID)
	case "terminate":
		err = awscloud.TerminateInstance(ctx, credential, region, instanceID)
	case "create_image":
		name := strings.TrimSpace(payload.Name)
		if name == "" {
			name = fmt.Sprintf("komari-%s-%d", instanceID, time.Now().Unix())
		}
		imageID, actionErr := awscloud.CreateImage(ctx, credential, region, instanceID, name, payload.Description, payload.NoReboot)
		err = actionErr
		response["image_id"] = imageID
	case "change_type":
		instanceType := strings.TrimSpace(payload.InstanceType)
		if instanceType == "" {
			api.RespondError(c, http.StatusBadRequest, "instance_type is required")
			return
		}
		err = awscloud.ModifyInstanceType(ctx, credential, region, instanceID, instanceType)
		response["instance_type"] = instanceType
	case "enable_monitoring":
		err = awscloud.SetDetailedMonitoring(ctx, credential, region, instanceID, true)
	case "disable_monitoring":
		err = awscloud.SetDetailedMonitoring(ctx, credential, region, instanceID, false)
	case "sync_tags":
		err = awscloud.ReplaceInstanceTags(ctx, credential, region, instanceID, payload.Tags)
		response["tags"] = payload.Tags
	case "create_snapshots":
		snapshotIDs, actionErr := awscloud.CreateVolumeSnapshots(ctx, credential, region, instanceID, payload.Description)
		err = actionErr
		response["snapshot_ids"] = snapshotIDs
	case "allocate_address":
		address, actionErr := awscloud.AllocateAndAssociateAddress(ctx, credential, region, instanceID, payload.PrivateIP)
		err = actionErr
		response["address"] = address
	case "associate_address":
		if strings.TrimSpace(payload.AllocationID) == "" {
			api.RespondError(c, http.StatusBadRequest, "allocation_id is required")
			return
		}
		address, actionErr := awscloud.AssociateAddress(ctx, credential, region, payload.AllocationID, instanceID, payload.PrivateIP)
		err = actionErr
		response["address"] = address
	case "disassociate_address":
		if strings.TrimSpace(payload.AssociationID) == "" {
			api.RespondError(c, http.StatusBadRequest, "association_id is required")
			return
		}
		err = awscloud.DisassociateAddress(ctx, credential, region, payload.AssociationID)
	case "release_address":
		if strings.TrimSpace(payload.AllocationID) == "" {
			api.RespondError(c, http.StatusBadRequest, "allocation_id is required")
			return
		}
		err = awscloud.ReleaseAddress(ctx, credential, region, payload.AllocationID)
	case "replace_address":
		address, releasedAllocationIDs, actionErr := awscloud.ReplaceAddress(
			ctx,
			credential,
			region,
			instanceID,
			payload.PrivateIP,
		)
		err = actionErr
		response["address"] = address
		response["released_allocation_ids"] = releasedAllocationIDs
	case "allow_all_traffic":
		groupIDs, actionErr := awscloud.AllowAllSecurityGroupTraffic(ctx, credential, region, instanceID)
		err = actionErr
		response["security_group_ids"] = groupIDs
	default:
		api.RespondError(c, http.StatusBadRequest, "Unsupported instance action: "+payload.Type)
		return
	}
	if err != nil {
		respondAWSError(c, err)
		return
	}

	logCloudAudit(c, fmt.Sprintf("aws ec2 instance action: %s (%s)", actionType, instanceID))
	api.RespondSuccess(c, response)
}

func PostAWSLightsailInstanceAction(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	_, credential, region, ctx, cancel, err := getAWSActiveCredential(c, scope)
	if err != nil {
		respondAWSError(c, err)
		return
	}
	defer cancel()

	instanceName := strings.TrimSpace(c.Param("name"))
	if instanceName == "" {
		api.RespondError(c, http.StatusBadRequest, "Invalid Lightsail instance name")
		return
	}

	var payload struct {
		Type         string         `json:"type" binding:"required"`
		SnapshotName string         `json:"snapshot_name,omitempty"`
		StaticIPName string         `json:"static_ip_name,omitempty"`
		Tags         []awscloud.Tag `json:"tags,omitempty"`
	}
	if err := c.ShouldBindJSON(&payload); err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid Lightsail action request: "+err.Error())
		return
	}

	actionType := strings.ToLower(strings.TrimSpace(payload.Type))
	response := gin.H{"type": actionType, "instance_name": instanceName, "status": "submitted"}
	switch actionType {
	case "start":
		err = awscloud.StartLightsailInstance(ctx, credential, region, instanceName)
	case "stop":
		err = awscloud.StopLightsailInstance(ctx, credential, region, instanceName)
	case "reboot":
		err = awscloud.RebootLightsailInstance(ctx, credential, region, instanceName)
	case "create_snapshot":
		snapshotName := strings.TrimSpace(payload.SnapshotName)
		if snapshotName == "" {
			snapshotName = fmt.Sprintf("%s-%d", instanceName, time.Now().Unix())
		}
		err = awscloud.CreateLightsailSnapshot(ctx, credential, region, instanceName, snapshotName, payload.Tags)
		response["snapshot_name"] = snapshotName
	case "allocate_static_ip":
		staticIPName := strings.TrimSpace(payload.StaticIPName)
		if staticIPName == "" {
			staticIPName = fmt.Sprintf("%s-ip-%d", instanceName, time.Now().Unix())
		}
		err = awscloud.AllocateLightsailStaticIP(ctx, credential, region, staticIPName)
		response["static_ip_name"] = staticIPName
	case "attach_static_ip":
		if strings.TrimSpace(payload.StaticIPName) == "" {
			api.RespondError(c, http.StatusBadRequest, "static_ip_name is required")
			return
		}
		err = awscloud.AttachLightsailStaticIP(ctx, credential, region, payload.StaticIPName, instanceName)
		response["static_ip_name"] = strings.TrimSpace(payload.StaticIPName)
	case "detach_static_ip":
		if strings.TrimSpace(payload.StaticIPName) == "" {
			api.RespondError(c, http.StatusBadRequest, "static_ip_name is required")
			return
		}
		err = awscloud.DetachLightsailStaticIP(ctx, credential, region, payload.StaticIPName)
		response["static_ip_name"] = strings.TrimSpace(payload.StaticIPName)
	case "release_static_ip":
		if strings.TrimSpace(payload.StaticIPName) == "" {
			api.RespondError(c, http.StatusBadRequest, "static_ip_name is required")
			return
		}
		err = awscloud.ReleaseLightsailStaticIP(ctx, credential, region, payload.StaticIPName)
		response["static_ip_name"] = strings.TrimSpace(payload.StaticIPName)
	case "replace_static_ip":
		staticIPName := strings.TrimSpace(payload.StaticIPName)
		if staticIPName == "" {
			api.RespondError(c, http.StatusBadRequest, "static_ip_name is required")
			return
		}
		releasedStaticIPName, actionErr := awscloud.ReplaceLightsailStaticIP(
			ctx,
			credential,
			region,
			staticIPName,
			instanceName,
		)
		err = actionErr
		response["static_ip_name"] = staticIPName
		response["released_static_ip_name"] = releasedStaticIPName
	case "allow_all_traffic":
		err = awscloud.OpenLightsailAllPublicPorts(ctx, credential, region, instanceName)
	default:
		api.RespondError(c, http.StatusBadRequest, "Unsupported Lightsail action: "+payload.Type)
		return
	}
	if err != nil {
		respondAWSError(c, err)
		return
	}

	logCloudAudit(c, fmt.Sprintf("aws lightsail instance action: %s (%s)", actionType, instanceName))
	api.RespondSuccess(c, response)
}

func runAWSCreateFollowUp(ctx context.Context, attempts int, delay time.Duration, action func(context.Context) error) error {
	if attempts < 1 {
		attempts = 1
	}
	if delay <= 0 {
		delay = time.Second
	}

	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		if err := action(ctx); err == nil {
			return nil
		} else {
			lastErr = err
		}

		if attempt == attempts-1 {
			break
		}

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			if lastErr != nil {
				return lastErr
			}
			return ctx.Err()
		case <-timer.C:
		}
	}

	return lastErr
}

func resolveAWSRegion(addition *awscloud.Addition, activeCredential *awscloud.CredentialRecord, regionOverride string) string {
	region := strings.TrimSpace(regionOverride)
	if region == "" && addition != nil {
		region = strings.TrimSpace(addition.ActiveRegion)
	}
	if region == "" && activeCredential != nil {
		region = strings.TrimSpace(activeCredential.DefaultRegion)
	}
	if region == "" {
		region = awscloud.DefaultRegion
	}
	return region
}

func getAWSActiveCredentialWithRegion(c *gin.Context, scope ownerScope, regionOverride string) (*awscloud.Addition, *awscloud.CredentialRecord, string, context.Context, context.CancelFunc, error) {
	_, addition, err := loadAWSAddition(scope, false)
	if err != nil {
		return nil, nil, "", nil, nil, err
	}

	activeCredential := addition.ActiveCredential()
	if activeCredential == nil {
		return nil, nil, "", nil, nil, fmt.Errorf("AWS credential is not configured")
	}

	region := resolveAWSRegion(addition, activeCredential, regionOverride)

	ctx, cancel := context.WithTimeout(c.Request.Context(), 45*time.Second)
	return addition, activeCredential, region, ctx, cancel, nil
}

func getAWSActiveCredential(c *gin.Context, scope ownerScope) (*awscloud.Addition, *awscloud.CredentialRecord, string, context.Context, context.CancelFunc, error) {
	return getAWSActiveCredentialWithRegion(c, scope, "")
}

func resolveAWSRootPasswordUserData(mode, rootPassword, userData string) (string, string, error) {
	userData = strings.TrimSpace(userData)
	switch strings.ToLower(strings.TrimSpace(mode)) {
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

func respondAWSError(c *gin.Context, err error) {
	var responseErr *smithyhttp.ResponseError
	if errors.As(err, &responseErr) {
		statusCode := responseErr.HTTPStatusCode()
		if statusCode < 400 || statusCode > 599 {
			statusCode = http.StatusBadGateway
		}
		api.RespondError(c, statusCode, err.Error())
		return
	}

	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		api.RespondError(c, http.StatusBadRequest, apiErr.ErrorMessage())
		return
	}

	api.RespondError(c, http.StatusBadRequest, err.Error())
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func loadAWSAddition(scope ownerScope, allowMissing bool) (*models.CloudProvider, *awscloud.Addition, error) {
	config, err := getCloudProviderConfigForScope(scope, awsProviderName)
	if err != nil {
		if allowMissing {
			addition := &awscloud.Addition{}
			addition.Normalize()
			return nil, addition, nil
		}
		return nil, nil, fmt.Errorf("AWS provider is not configured")
	}

	addition := &awscloud.Addition{}
	raw := strings.TrimSpace(config.Addition)
	if raw == "" {
		raw = "{}"
	}
	if err := json.Unmarshal([]byte(raw), addition); err != nil {
		return nil, nil, fmt.Errorf("AWS configuration is invalid: %w", err)
	}

	addition.Normalize()
	return config, addition, nil
}

func saveAWSAddition(scope ownerScope, addition *awscloud.Addition) error {
	if addition == nil {
		addition = &awscloud.Addition{}
	}
	addition.Normalize()

	payload, err := json.Marshal(addition)
	if err != nil {
		return err
	}

	return saveCloudProviderConfigForScope(scope, awsProviderName, string(payload))
}

func saveAWSAdditionPreservingSecrets(scope ownerScope, addition *awscloud.Addition) error {
	if addition == nil {
		addition = &awscloud.Addition{}
	}

	if _, current, err := loadAWSAddition(scope, true); err == nil {
		addition.MergePersistentStateFrom(current)
	}

	return saveAWSAddition(scope, addition)
}

func persistAWSResourcePassword(
	scope ownerScope,
	addition *awscloud.Addition,
	credential *awscloud.CredentialRecord,
	resourceType string,
	resourceID string,
	resourceName string,
	passwordMode string,
	rootPassword string,
) (*awscloud.Addition, error) {
	resourceType = strings.TrimSpace(resourceType)
	resourceID = strings.TrimSpace(resourceID)
	rootPassword = strings.TrimSpace(rootPassword)
	if addition == nil || credential == nil || resourceType == "" || resourceID == "" || rootPassword == "" {
		return nil, errors.New("root password persistence requires an active credential and resource")
	}

	if err := credential.SaveResourcePassword(resourceType, resourceID, resourceName, passwordMode, rootPassword, time.Now()); err != nil {
		return nil, err
	}

	verifyPersistedPassword := func() (*awscloud.Addition, error) {
		_, persistedAddition, err := loadAWSAddition(scope, false)
		if err != nil {
			return nil, err
		}
		persistedCredential := findAWSCredentialForPersistenceVerification(
			persistedAddition,
			credential.ID,
			credential.AccessKeyID,
			credential.DefaultRegion,
		)
		if persistedCredential == nil || !persistedCredential.HasSavedResourcePassword(resourceType, resourceID) {
			return nil, errors.New("saved root password was not found after persistence")
		}
		return persistedAddition, nil
	}

	if err := saveAWSAdditionPreservingSecrets(scope, addition); err != nil {
		credential.RemoveSavedResourcePassword(resourceType, resourceID)
		return nil, fmt.Errorf("Failed to save root password: %w", err)
	}

	if persistedAddition, err := verifyPersistedPassword(); err == nil {
		return persistedAddition, nil
	}

	if err := saveAWSAddition(scope, addition); err != nil {
		credential.RemoveSavedResourcePassword(resourceType, resourceID)
		return nil, fmt.Errorf("Failed to save root password: %w", err)
	}

	persistedAddition, err := verifyPersistedPassword()
	if err != nil {
		credential.RemoveSavedResourcePassword(resourceType, resourceID)
		return nil, fmt.Errorf("Failed to verify saved root password: %w", err)
	}
	return persistedAddition, nil
}

func findAWSCredentialForPersistenceVerification(addition *awscloud.Addition, credentialID, accessKeyID, defaultRegion string) *awscloud.CredentialRecord {
	if addition == nil {
		return nil
	}

	credentialID = strings.TrimSpace(credentialID)
	if credentialID != "" {
		if credential := addition.FindCredential(credentialID); credential != nil {
			return credential
		}
	}

	accessKeyID = strings.TrimSpace(accessKeyID)
	defaultRegion = strings.TrimSpace(defaultRegion)
	if accessKeyID == "" {
		return nil
	}

	for index := range addition.Credentials {
		if strings.TrimSpace(addition.Credentials[index].AccessKeyID) == accessKeyID &&
			strings.TrimSpace(addition.Credentials[index].DefaultRegion) == defaultRegion {
			return &addition.Credentials[index]
		}
	}

	return nil
}

func buildAWSInstanceView(instance *awscloud.Instance, credential *awscloud.CredentialRecord, region string) *awsInstanceView {
	if instance == nil {
		return nil
	}

	view := &awsInstanceView{
		Instance: *instance,
	}
	if credential != nil {
		resourceKey := buildAWSResourceCredentialID(region, instance.InstanceID)
		if credential.HasSavedResourcePassword("ec2", resourceKey) {
			view.SavedRootPassword = true
			view.SavedRootPasswordUpdatedAt = credential.SavedResourcePasswordUpdatedAt("ec2", resourceKey)
		}
	}
	return view
}

func buildAWSInstanceDetailView(detail *awscloud.InstanceDetail, credential *awscloud.CredentialRecord, region string) *awsInstanceDetailView {
	if detail == nil {
		return nil
	}

	return &awsInstanceDetailView{
		Instance:           buildAWSInstanceView(&detail.Instance, credential, region),
		VPCID:              detail.VpcID,
		SubnetID:           detail.SubnetID,
		Architecture:       detail.Architecture,
		PlatformDetails:    detail.PlatformDetails,
		VirtualizationType: detail.VirtualizationType,
		RootDeviceName:     detail.RootDeviceName,
		MonitoringState:    detail.MonitoringState,
		StateReason:        detail.StateReason,
		PublicDNSName:      detail.PublicDNSName,
		PrivateDNSName:     detail.PrivateDNSName,
		SecurityGroups:     detail.SecurityGroups,
		Volumes:            detail.Volumes,
		Addresses:          detail.Addresses,
		ConsoleOutput:      detail.ConsoleOutput,
	}
}

func buildAWSLightsailInstanceView(instance *awscloud.LightsailInstance, credential *awscloud.CredentialRecord, region string) *awsLightsailInstanceView {
	if instance == nil {
		return nil
	}

	view := &awsLightsailInstanceView{
		LightsailInstance: *instance,
	}
	if credential != nil {
		resourceKey := buildAWSResourceCredentialID(region, instance.Name)
		if credential.HasSavedResourcePassword("lightsail", resourceKey) {
			view.SavedRootPassword = true
			view.SavedRootPasswordUpdatedAt = credential.SavedResourcePasswordUpdatedAt("lightsail", resourceKey)
		}
	}
	return view
}

func buildAWSLightsailInstanceDetailView(detail *awscloud.LightsailInstanceDetail, credential *awscloud.CredentialRecord, region string) *awsLightsailInstanceDetailView {
	if detail == nil {
		return nil
	}

	return &awsLightsailInstanceDetailView{
		Instance:  buildAWSLightsailInstanceView(&detail.Instance, credential, region),
		Ports:     detail.Ports,
		StaticIPs: detail.StaticIPs,
		Snapshots: detail.Snapshots,
	}
}

func buildAWSResourceCredentialID(region, resourceID string) string {
	resourceID = strings.TrimSpace(resourceID)
	if resourceID == "" {
		return ""
	}

	scopePrefix := buildAWSResourceCredentialScope(region)
	if scopePrefix == "" {
		return resourceID
	}
	return scopePrefix + resourceID
}

func buildAWSResourceCredentialScope(region string) string {
	region = strings.TrimSpace(strings.ToLower(region))
	if region == "" {
		return ""
	}
	return region + "::"
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

func firstNonEmptyAWS(values ...string) string {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			return value
		}
	}
	return ""
}
