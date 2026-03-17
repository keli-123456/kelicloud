package admin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	smithy "github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/gin-gonic/gin"
	"github.com/komari-monitor/komari/api"
	"github.com/komari-monitor/komari/database/models"
	awscloud "github.com/komari-monitor/komari/utils/cloudprovider/aws"
)

const awsProviderName = "aws"

type createAWSInstancePayload struct {
	Name             string         `json:"name"`
	ImageID          string         `json:"image_id" binding:"required"`
	InstanceType     string         `json:"instance_type" binding:"required"`
	KeyName          string         `json:"key_name,omitempty"`
	SubnetID         string         `json:"subnet_id,omitempty"`
	SecurityGroupIDs []string       `json:"security_group_ids,omitempty"`
	UserData         string         `json:"user_data,omitempty"`
	AssignPublicIP   bool           `json:"assign_public_ip"`
	Tags             []awscloud.Tag `json:"tags,omitempty"`
	AutoConnect      bool           `json:"auto_connect"`
	AutoConnectGroup string         `json:"auto_connect_group,omitempty"`
}

type createAWSLightsailInstancePayload struct {
	Name             string         `json:"name" binding:"required"`
	AvailabilityZone string         `json:"availability_zone" binding:"required"`
	BlueprintID      string         `json:"blueprint_id" binding:"required"`
	BundleID         string         `json:"bundle_id" binding:"required"`
	KeyPairName      string         `json:"key_pair_name,omitempty"`
	UserData         string         `json:"user_data,omitempty"`
	IPAddressType    string         `json:"ip_address_type,omitempty"`
	Tags             []awscloud.Tag `json:"tags,omitempty"`
	AutoConnect      bool           `json:"auto_connect"`
	AutoConnectGroup string         `json:"auto_connect_group,omitempty"`
}

type awsAccountView struct {
	AccountID     string                    `json:"account_id"`
	ARN           string                    `json:"arn"`
	UserID        string                    `json:"user_id"`
	Region        string                    `json:"region"`
	EC2Quota      *awscloud.EC2QuotaSummary `json:"ec2_quota,omitempty"`
	EC2QuotaError string                    `json:"ec2_quota_error,omitempty"`
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

	api.RespondSuccess(c, addition.ToPoolView())
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
	api.RespondSuccess(c, addition.ToPoolView())
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
	api.RespondSuccess(c, addition.ToPoolView())
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
	api.RespondSuccess(c, addition.ToPoolView())
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
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			identity, err := awscloud.GetIdentity(ctx, &record, record.DefaultRegion)
			var quota *awscloud.EC2QuotaSummary
			var quotaErr error
			if err == nil {
				quota, quotaErr = awscloud.GetEC2QuotaSummary(ctx, &record, record.DefaultRegion)
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

	logCloudAudit(c, "delete aws credential: "+credentialID)
	api.RespondSuccess(c, addition.ToPoolView())
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

func GetAWSAccount(c *gin.Context) {
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

	identity, err := awscloud.GetIdentity(ctx, credential, region)
	if err != nil {
		respondAWSError(c, err)
		return
	}

	quota, quotaErr := awscloud.GetEC2QuotaSummary(ctx, credential, region)

	if identity != nil {
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

	_, credential, region, ctx, cancel, err := getAWSActiveCredential(c, scope)
	if err != nil {
		respondAWSError(c, err)
		return
	}
	defer cancel()

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
		"active_region":     region,
		"regions":           regions,
		"instance_types":    instanceTypes,
		"images":            images,
		"key_pairs":         keyPairs,
		"subnets":           subnets,
		"security_groups":   securityGroups,
		"elastic_addresses": elasticAddresses,
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

	api.RespondSuccess(c, detail)
}

func GetAWSLightsailCatalog(c *gin.Context) {
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

	_, credential, region, ctx, cancel, err := getAWSActiveCredential(c, scope)
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

	api.RespondSuccess(c, instances)
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

	api.RespondSuccess(c, detail)
}

func ListAWSInstances(c *gin.Context) {
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

	instances, err := awscloud.ListInstances(ctx, credential, region)
	if err != nil {
		respondAWSError(c, err)
		return
	}
	if instances == nil {
		instances = make([]awscloud.Instance, 0)
	}

	api.RespondSuccess(c, instances)
}

func CreateAWSInstance(c *gin.Context) {
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

	var payload createAWSInstancePayload
	if err := c.ShouldBindJSON(&payload); err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid instance request: "+err.Error())
		return
	}

	resolvedUserData := strings.TrimSpace(payload.UserData)
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

	request := awscloud.CreateInstanceRequest{
		Name:             strings.TrimSpace(payload.Name),
		ImageID:          strings.TrimSpace(payload.ImageID),
		InstanceType:     strings.TrimSpace(payload.InstanceType),
		KeyName:          strings.TrimSpace(payload.KeyName),
		SubnetID:         strings.TrimSpace(payload.SubnetID),
		SecurityGroupIDs: trimStringSlice(payload.SecurityGroupIDs),
		UserData:         resolvedUserData,
		AssignPublicIP:   payload.AssignPublicIP,
		Tags:             payload.Tags,
	}

	instance, err := awscloud.CreateInstance(ctx, credential, region, request)
	if err != nil {
		respondAWSError(c, err)
		return
	}

	logMessage := fmt.Sprintf("create aws ec2 instance: %s (%s/%s", request.Name, region, request.InstanceType)
	if autoConnectGroup != "" {
		logMessage += ", auto_connect_group=" + autoConnectGroup
	}
	logMessage += ")"
	logCloudAudit(c, logMessage)
	api.RespondSuccess(c, gin.H{
		"instance": instance,
	})
}

func CreateAWSLightsailInstance(c *gin.Context) {
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

	var payload createAWSLightsailInstancePayload
	if err := c.ShouldBindJSON(&payload); err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid Lightsail instance request: "+err.Error())
		return
	}

	resolvedUserData := strings.TrimSpace(payload.UserData)
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

	request := awscloud.CreateLightsailInstanceRequest{
		Name:             strings.TrimSpace(payload.Name),
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

	logMessage := fmt.Sprintf("create aws lightsail instance: %s (%s/%s", request.Name, request.AvailabilityZone, request.BundleID)
	if autoConnectGroup != "" {
		logMessage += ", auto_connect_group=" + autoConnectGroup
	}
	logMessage += ")"
	logCloudAudit(c, logMessage)
	api.RespondSuccess(c, gin.H{
		"name":   request.Name,
		"status": "submitted",
	})
}

func DeleteAWSInstance(c *gin.Context) {
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

	if err := awscloud.TerminateInstance(ctx, credential, region, instanceID); err != nil {
		respondAWSError(c, err)
		return
	}

	logCloudAudit(c, fmt.Sprintf("terminate aws ec2 instance: %s", instanceID))
	api.RespondSuccess(c, nil)
}

func DeleteAWSLightsailInstance(c *gin.Context) {
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

	if err := awscloud.DeleteLightsailInstance(ctx, credential, region, instanceName); err != nil {
		respondAWSError(c, err)
		return
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

func getAWSActiveCredential(c *gin.Context, scope ownerScope) (*awscloud.Addition, *awscloud.CredentialRecord, string, context.Context, context.CancelFunc, error) {
	_, addition, err := loadAWSAddition(scope, false)
	if err != nil {
		return nil, nil, "", nil, nil, err
	}

	activeCredential := addition.ActiveCredential()
	if activeCredential == nil {
		return nil, nil, "", nil, nil, fmt.Errorf("AWS credential is not configured")
	}

	region := addition.ActiveRegion
	if strings.TrimSpace(region) == "" {
		region = activeCredential.DefaultRegion
	}
	region = strings.TrimSpace(region)
	if region == "" {
		region = awscloud.DefaultRegion
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 45*time.Second)
	return addition, activeCredential, region, ctx, cancel, nil
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
