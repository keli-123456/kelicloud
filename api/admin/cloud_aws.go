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
	"github.com/komari-monitor/komari/database"
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
}

type awsAccountView struct {
	AccountID string `json:"account_id"`
	ARN       string `json:"arn"`
	UserID    string `json:"user_id"`
	Region    string `json:"region"`
}

func GetAWSCredentials(c *gin.Context) {
	_, addition, err := loadAWSAddition(true)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}

	api.RespondSuccess(c, addition.ToPoolView())
}

func SaveAWSCredentials(c *gin.Context) {
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

	_, addition, err := loadAWSAddition(true)
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

	if err := saveAWSAddition(addition); err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to save AWS credentials: "+err.Error())
		return
	}

	logCloudAudit(c, fmt.Sprintf("import aws credentials: %d", imported))
	api.RespondSuccess(c, addition.ToPoolView())
}

func SetAWSActiveCredential(c *gin.Context) {
	var payload struct {
		CredentialID string `json:"credential_id" binding:"required"`
	}
	if err := c.ShouldBindJSON(&payload); err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid active credential payload: "+err.Error())
		return
	}

	_, addition, err := loadAWSAddition(false)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}

	if !addition.SetActiveCredential(payload.CredentialID) {
		api.RespondError(c, http.StatusNotFound, "AWS credential not found")
		return
	}

	if err := saveAWSAddition(addition); err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to update active credential: "+err.Error())
		return
	}

	logCloudAudit(c, "set active aws credential: "+payload.CredentialID)
	api.RespondSuccess(c, addition.ToPoolView())
}

func SetAWSActiveRegion(c *gin.Context) {
	var payload struct {
		Region string `json:"region" binding:"required"`
	}
	if err := c.ShouldBindJSON(&payload); err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid active region payload: "+err.Error())
		return
	}

	_, addition, err := loadAWSAddition(false)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}

	addition.SetActiveRegion(payload.Region)
	if err := saveAWSAddition(addition); err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to update active region: "+err.Error())
		return
	}

	logCloudAudit(c, "set active aws region: "+payload.Region)
	api.RespondSuccess(c, addition.ToPoolView())
}

func CheckAWSCredentials(c *gin.Context) {
	var payload struct {
		CredentialIDs []string `json:"credential_ids"`
	}
	if c.Request.ContentLength > 0 {
		if err := c.ShouldBindJSON(&payload); err != nil {
			api.RespondError(c, http.StatusBadRequest, "Invalid credential check payload: "+err.Error())
			return
		}
	}

	_, addition, err := loadAWSAddition(false)
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
			mu.Lock()
			addition.Credentials[credentialIndex].SetCheckResult(checkedAt, identity, err)
			mu.Unlock()
		}(index)
	}

	wg.Wait()

	if err := saveAWSAddition(addition); err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to save credential health: "+err.Error())
		return
	}

	logCloudAudit(c, fmt.Sprintf("check aws credentials: %d", len(addition.Credentials)))
	api.RespondSuccess(c, addition.ToPoolView())
}

func DeleteAWSCredential(c *gin.Context) {
	credentialID := strings.TrimSpace(c.Param("id"))
	if credentialID == "" {
		api.RespondError(c, http.StatusBadRequest, "Invalid credential id")
		return
	}

	_, addition, err := loadAWSAddition(false)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}

	if !addition.RemoveCredential(credentialID) {
		api.RespondError(c, http.StatusNotFound, "AWS credential not found")
		return
	}

	if err := saveAWSAddition(addition); err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to delete credential: "+err.Error())
		return
	}

	logCloudAudit(c, "delete aws credential: "+credentialID)
	api.RespondSuccess(c, addition.ToPoolView())
}

func GetAWSCredentialSecret(c *gin.Context) {
	credentialID := strings.TrimSpace(c.Param("id"))
	if credentialID == "" {
		api.RespondError(c, http.StatusBadRequest, "Invalid credential id")
		return
	}

	_, addition, err := loadAWSAddition(false)
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
	addition, credential, region, ctx, cancel, err := getAWSActiveCredential(c)
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

	if identity != nil {
		credential.SetCheckResult(time.Now(), identity, nil)
		_ = saveAWSAddition(addition)
	}

	api.RespondSuccess(c, awsAccountView{
		AccountID: identity.AccountID,
		ARN:       identity.ARN,
		UserID:    identity.UserID,
		Region:    region,
	})
}

func GetAWSCatalog(c *gin.Context) {
	_, credential, region, ctx, cancel, err := getAWSActiveCredential(c)
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

	api.RespondSuccess(c, gin.H{
		"active_region":   region,
		"regions":         regions,
		"instance_types":  instanceTypes,
		"images":          images,
		"key_pairs":       keyPairs,
		"subnets":         subnets,
		"security_groups": securityGroups,
	})
}

func ListAWSInstances(c *gin.Context) {
	_, credential, region, ctx, cancel, err := getAWSActiveCredential(c)
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
	_, credential, region, ctx, cancel, err := getAWSActiveCredential(c)
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

	request := awscloud.CreateInstanceRequest{
		Name:             strings.TrimSpace(payload.Name),
		ImageID:          strings.TrimSpace(payload.ImageID),
		InstanceType:     strings.TrimSpace(payload.InstanceType),
		KeyName:          strings.TrimSpace(payload.KeyName),
		SubnetID:         strings.TrimSpace(payload.SubnetID),
		SecurityGroupIDs: trimStringSlice(payload.SecurityGroupIDs),
		UserData:         strings.TrimSpace(payload.UserData),
		AssignPublicIP:   payload.AssignPublicIP,
		Tags:             payload.Tags,
	}

	instance, err := awscloud.CreateInstance(ctx, credential, region, request)
	if err != nil {
		respondAWSError(c, err)
		return
	}

	logCloudAudit(c, fmt.Sprintf("create aws ec2 instance: %s (%s/%s)", request.Name, region, request.InstanceType))
	api.RespondSuccess(c, gin.H{
		"instance": instance,
	})
}

func DeleteAWSInstance(c *gin.Context) {
	_, credential, region, ctx, cancel, err := getAWSActiveCredential(c)
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

func PostAWSInstanceAction(c *gin.Context) {
	_, credential, region, ctx, cancel, err := getAWSActiveCredential(c)
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
		Type string `json:"type" binding:"required"`
	}
	if err := c.ShouldBindJSON(&payload); err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid action request: "+err.Error())
		return
	}

	actionType := strings.ToLower(strings.TrimSpace(payload.Type))
	switch actionType {
	case "start":
		err = awscloud.StartInstance(ctx, credential, region, instanceID)
	case "stop":
		err = awscloud.StopInstance(ctx, credential, region, instanceID)
	case "reboot":
		err = awscloud.RebootInstance(ctx, credential, region, instanceID)
	case "terminate":
		err = awscloud.TerminateInstance(ctx, credential, region, instanceID)
	default:
		api.RespondError(c, http.StatusBadRequest, "Unsupported instance action: "+payload.Type)
		return
	}
	if err != nil {
		respondAWSError(c, err)
		return
	}

	logCloudAudit(c, fmt.Sprintf("aws ec2 instance action: %s (%s)", actionType, instanceID))
	api.RespondSuccess(c, gin.H{"type": actionType, "instance_id": instanceID, "status": "submitted"})
}

func getAWSActiveCredential(c *gin.Context) (*awscloud.Addition, *awscloud.CredentialRecord, string, context.Context, context.CancelFunc, error) {
	_, addition, err := loadAWSAddition(false)
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

func loadAWSAddition(allowMissing bool) (*models.CloudProvider, *awscloud.Addition, error) {
	config, err := database.GetCloudProviderConfigByName(awsProviderName)
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

func saveAWSAddition(addition *awscloud.Addition) error {
	if addition == nil {
		addition = &awscloud.Addition{}
	}
	addition.Normalize()

	payload, err := json.Marshal(addition)
	if err != nil {
		return err
	}

	return database.SaveCloudProviderConfig(&models.CloudProvider{
		Name:     awsProviderName,
		Addition: string(payload),
	})
}
