package admin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/komari-monitor/komari/api"
	"github.com/komari-monitor/komari/database/models"
	vultrcloud "github.com/komari-monitor/komari/utils/cloudprovider/vultr"
)

const vultrProviderName = "vultr"

type createVultrInstancePayload struct {
	Label             string   `json:"label"`
	Hostname          string   `json:"hostname"`
	Region            string   `json:"region" binding:"required"`
	Plan              string   `json:"plan" binding:"required"`
	OSID              int      `json:"os_id" binding:"required"`
	SSHKeyIDs         []string `json:"sshkey_id,omitempty"`
	EnableIPv6        bool     `json:"enable_ipv6"`
	DisablePublicIPv4 bool     `json:"disable_public_ipv4"`
	BackupsEnabled    bool     `json:"backups_enabled"`
	DDOSProtection    bool     `json:"ddos_protection"`
	ActivationEmail   bool     `json:"activation_email"`
	Tags              []string `json:"tags,omitempty"`
	UserData          string   `json:"user_data,omitempty"`
	AutoConnect       bool     `json:"auto_connect"`
	AutoConnectGroup  string   `json:"auto_connect_group,omitempty"`
}

type vultrInstanceView struct {
	vultrcloud.Instance
	SavedRootPassword          bool   `json:"saved_root_password"`
	SavedRootPasswordUpdatedAt string `json:"saved_root_password_updated_at,omitempty"`
}

type createVultrInstanceResponse struct {
	Instance          *vultrInstanceView `json:"instance"`
	GeneratedPassword string             `json:"generated_password,omitempty"`
	PasswordSaved     bool               `json:"password_saved"`
	PasswordSaveError string             `json:"password_save_error,omitempty"`
}

func GetVultrTokens(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	_, addition, err := loadVultrAddition(scope, true)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}

	api.RespondSuccess(c, addition.ToPoolView())
}

func SaveVultrTokens(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	var payload struct {
		Tokens        []vultrcloud.TokenImport `json:"tokens"`
		ActiveTokenID string                   `json:"active_token_id"`
	}
	if err := c.ShouldBindJSON(&payload); err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid token payload: "+err.Error())
		return
	}
	if len(payload.Tokens) == 0 {
		api.RespondError(c, http.StatusBadRequest, "At least one token is required")
		return
	}

	_, addition, err := loadVultrAddition(scope, true)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}

	imported := addition.UpsertTokens(payload.Tokens)
	if imported == 0 {
		api.RespondError(c, http.StatusBadRequest, "No valid tokens were provided")
		return
	}

	if payload.ActiveTokenID != "" {
		if !addition.SetActiveToken(payload.ActiveTokenID) {
			api.RespondError(c, http.StatusBadRequest, "Active token not found")
			return
		}
	}

	if err := saveVultrAdditionPreservingSecrets(scope, addition); err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to save Vultr tokens: "+err.Error())
		return
	}

	logCloudAudit(c, fmt.Sprintf("import vultr tokens: %d", imported))
	api.RespondSuccess(c, addition.ToPoolView())
}

func SetVultrActiveToken(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	var payload struct {
		TokenID string `json:"token_id" binding:"required"`
	}
	if err := c.ShouldBindJSON(&payload); err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid active token payload: "+err.Error())
		return
	}

	_, addition, err := loadVultrAddition(scope, false)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}
	if !addition.SetActiveToken(payload.TokenID) {
		api.RespondError(c, http.StatusNotFound, "Vultr token not found")
		return
	}

	if err := saveVultrAdditionPreservingSecrets(scope, addition); err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to update active token: "+err.Error())
		return
	}

	logCloudAudit(c, "set active vultr token: "+payload.TokenID)
	api.RespondSuccess(c, addition.ToPoolView())
}

func CheckVultrTokens(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	var payload struct {
		TokenIDs []string `json:"token_ids"`
	}
	if c.Request.ContentLength > 0 {
		if err := c.ShouldBindJSON(&payload); err != nil {
			api.RespondError(c, http.StatusBadRequest, "Invalid token check payload: "+err.Error())
			return
		}
	}

	_, addition, err := loadVultrAddition(scope, false)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}
	if len(addition.Tokens) == 0 {
		api.RespondSuccess(c, addition.ToPoolView())
		return
	}

	selected := make(map[string]struct{}, len(payload.TokenIDs))
	for _, tokenID := range payload.TokenIDs {
		tokenID = strings.TrimSpace(tokenID)
		if tokenID != "" {
			selected[tokenID] = struct{}{}
		}
	}

	checkedAt := time.Now().UTC()
	var wg sync.WaitGroup
	var mu sync.Mutex
	limiter := make(chan struct{}, 4)

	for index := range addition.Tokens {
		tokenID := addition.Tokens[index].ID
		if len(selected) > 0 {
			if _, exists := selected[tokenID]; !exists {
				continue
			}
		}

		wg.Add(1)
		go func(tokenIndex int) {
			defer wg.Done()
			limiter <- struct{}{}
			defer func() { <-limiter }()

			record := addition.Tokens[tokenIndex]
			client, err := vultrcloud.NewClientFromToken(record.Token)
			var account *vultrcloud.Account
			if err == nil {
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				account, err = client.GetAccount(ctx)
				if err == nil && account == nil {
					err = errors.New("vultr account response is empty")
				}
			}

			mu.Lock()
			addition.Tokens[tokenIndex].SetCheckResult(checkedAt, account, err)
			mu.Unlock()
		}(index)
	}

	wg.Wait()

	if err := saveVultrAdditionPreservingSecrets(scope, addition); err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to save token health: "+err.Error())
		return
	}

	logCloudAudit(c, fmt.Sprintf("check vultr tokens: %d", len(addition.Tokens)))
	api.RespondSuccess(c, addition.ToPoolView())
}

func DeleteVultrToken(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	tokenID := strings.TrimSpace(c.Param("id"))
	if tokenID == "" {
		api.RespondError(c, http.StatusBadRequest, "Invalid token id")
		return
	}

	_, addition, err := loadVultrAddition(scope, false)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}
	if !addition.RemoveToken(tokenID) {
		api.RespondError(c, http.StatusNotFound, "Vultr token not found")
		return
	}
	if err := saveVultrAddition(scope, addition); err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to delete token: "+err.Error())
		return
	}

	logCloudAudit(c, "delete vultr token: "+tokenID)
	api.RespondSuccess(c, addition.ToPoolView())
}

func GetVultrTokenSecret(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	tokenID := strings.TrimSpace(c.Param("id"))
	if tokenID == "" {
		api.RespondError(c, http.StatusBadRequest, "Invalid token id")
		return
	}

	_, addition, err := loadVultrAddition(scope, false)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}
	token := addition.FindToken(tokenID)
	if token == nil {
		api.RespondError(c, http.StatusNotFound, "Vultr token not found")
		return
	}

	logCloudAuditWithType(c, "view vultr token secret: "+tokenID, "warn")
	api.RespondSuccess(c, token.TokenSecretView())
}

func GetVultrAccount(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	client, ctx, cancel, err := getVultrClient(c, scope)
	if err != nil {
		respondVultrError(c, err)
		return
	}
	defer cancel()

	account, err := client.GetAccount(ctx)
	if err != nil {
		respondVultrError(c, err)
		return
	}
	api.RespondSuccess(c, account)
}

func GetVultrCatalog(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	client, ctx, cancel, err := getVultrClient(c, scope)
	if err != nil {
		respondVultrError(c, err)
		return
	}
	defer cancel()

	regions, err := client.ListRegions(ctx)
	if err != nil {
		respondVultrError(c, err)
		return
	}
	plans, err := client.ListPlans(ctx)
	if err != nil {
		respondVultrError(c, err)
		return
	}
	oses, err := client.ListOS(ctx)
	if err != nil {
		respondVultrError(c, err)
		return
	}
	sshKeys, err := client.ListSSHKeys(ctx)
	if err != nil {
		respondVultrError(c, err)
		return
	}

	sort.Slice(regions, func(i, j int) bool {
		return regions[i].ID < regions[j].ID
	})
	sort.Slice(plans, func(i, j int) bool {
		return plans[i].MonthlyCost < plans[j].MonthlyCost
	})
	sort.Slice(oses, func(i, j int) bool {
		left := strings.ToLower(strings.TrimSpace(oses[i].Family + " " + oses[i].Name))
		right := strings.ToLower(strings.TrimSpace(oses[j].Family + " " + oses[j].Name))
		return left < right
	})
	sort.Slice(sshKeys, func(i, j int) bool {
		return sshKeys[i].Name < sshKeys[j].Name
	})

	if regions == nil {
		regions = make([]vultrcloud.Region, 0)
	}
	if plans == nil {
		plans = make([]vultrcloud.Plan, 0)
	}
	if oses == nil {
		oses = make([]vultrcloud.OS, 0)
	}
	if sshKeys == nil {
		sshKeys = make([]vultrcloud.SSHKey, 0)
	}

	api.RespondSuccess(c, gin.H{
		"regions":  regions,
		"plans":    plans,
		"os":       oses,
		"ssh_keys": sshKeys,
	})
}

func ListVultrInstances(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	client, addition, activeToken, ctx, cancel, err := getVultrActiveTokenClient(c, scope)
	if err != nil {
		respondVultrError(c, err)
		return
	}
	defer cancel()

	instances, err := client.ListInstances(ctx)
	if err != nil {
		respondVultrError(c, err)
		return
	}

	sort.Slice(instances, func(i, j int) bool {
		return instances[i].DateCreated > instances[j].DateCreated
	})
	if instances == nil {
		instances = make([]vultrcloud.Instance, 0)
	}

	views := make([]vultrInstanceView, 0, len(instances))
	validInstanceIDs := make(map[string]struct{}, len(instances))
	credentialsChanged := false
	for index := range instances {
		instanceID := strings.TrimSpace(instances[index].ID)
		if instanceID == "" {
			continue
		}
		validInstanceIDs[instanceID] = struct{}{}
		if activeToken != nil && activeToken.SyncInstanceCredentialLabel(instanceID, instances[index].Label) {
			credentialsChanged = true
		}
		view := buildVultrInstanceView(&instances[index], addition)
		if view != nil {
			views = append(views, *view)
		}
	}
	if activeToken != nil && activeToken.PruneInstanceCredentials(validInstanceIDs) {
		credentialsChanged = true
	}
	if credentialsChanged {
		_ = saveVultrAddition(scope, addition)
	}

	api.RespondSuccess(c, views)
}

func GetVultrInstanceDetail(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	client, addition, _, ctx, cancel, err := getVultrActiveTokenClient(c, scope)
	if err != nil {
		respondVultrError(c, err)
		return
	}
	defer cancel()

	instanceID := strings.TrimSpace(c.Param("id"))
	if instanceID == "" {
		api.RespondError(c, http.StatusBadRequest, "Invalid instance id")
		return
	}

	instance, err := client.GetInstance(ctx, instanceID)
	if err != nil {
		respondVultrError(c, err)
		return
	}

	api.RespondSuccess(c, gin.H{
		"instance": buildVultrInstanceView(instance, addition),
	})
}

func GetVultrInstancePassword(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	instanceID := strings.TrimSpace(c.Param("id"))
	if instanceID == "" {
		api.RespondError(c, http.StatusBadRequest, "Invalid instance id")
		return
	}

	_, addition, err := loadVultrAddition(scope, false)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}

	password, err := addition.RevealInstancePassword(instanceID)
	if err != nil {
		api.RespondError(c, http.StatusNotFound, err.Error())
		return
	}

	logCloudAuditWithType(c, fmt.Sprintf("view vultr instance password: %s", instanceID), "warn")
	api.RespondSuccess(c, password)
}

func CreateVultrInstance(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	client, addition, activeToken, ctx, cancel, err := getVultrActiveTokenClient(c, scope)
	if err != nil {
		respondVultrError(c, err)
		return
	}
	defer cancel()

	var payload createVultrInstancePayload
	if err := c.ShouldBindJSON(&payload); err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid instance request: "+err.Error())
		return
	}

	payload.Label = strings.TrimSpace(payload.Label)
	payload.Hostname = strings.TrimSpace(payload.Hostname)
	payload.Region = strings.TrimSpace(payload.Region)
	payload.Plan = strings.TrimSpace(payload.Plan)
	payload.SSHKeyIDs = trimStringSlice(payload.SSHKeyIDs)
	payload.Tags = trimStringSlice(payload.Tags)
	payload.UserData = strings.TrimSpace(payload.UserData)
	payload.AutoConnectGroup = strings.TrimSpace(payload.AutoConnectGroup)
	if payload.Label == "" {
		payload.Label = fmt.Sprintf("komari-vultr-%d", time.Now().Unix())
	}
	if payload.Hostname == "" {
		payload.Hostname = payload.Label
	}

	resolvedUserData := payload.UserData
	autoConnectGroup := ""
	if payload.AutoConnect {
		resolvedUserData, autoConnectGroup, err = prepareCloudAutoConnectUserData(c, payload.UserData, autoConnectUserDataOptions{
			Enabled:           true,
			Group:             payload.AutoConnectGroup,
			Provider:          vultrProviderName,
			CredentialName:    activeToken.Name,
			WrapInShellScript: true,
		})
		if err != nil {
			api.RespondError(c, http.StatusBadRequest, err.Error())
			return
		}
	}

	backups := ""
	if payload.BackupsEnabled {
		backups = "enabled"
	}
	request := vultrcloud.CreateInstanceRequest{
		Label:             payload.Label,
		Hostname:          payload.Hostname,
		Region:            payload.Region,
		Plan:              payload.Plan,
		OSID:              payload.OSID,
		SSHKeyIDs:         payload.SSHKeyIDs,
		EnableIPv6:        payload.EnableIPv6,
		DisablePublicIPv4: payload.DisablePublicIPv4,
		Backups:           backups,
		DDOSProtection:    payload.DDOSProtection,
		ActivationEmail:   payload.ActivationEmail,
		Tags:              payload.Tags,
	}
	if resolvedUserData != "" {
		request.UserData = vultrcloud.EncodeUserData(resolvedUserData)
	}

	instance, err := client.CreateInstance(ctx, request)
	if err != nil {
		respondVultrError(c, err)
		return
	}

	generatedPassword := ""
	passwordSaved := false
	passwordSaveError := ""
	if instance != nil {
		generatedPassword = strings.TrimSpace(instance.DefaultPassword)
	}
	if instance != nil && generatedPassword != "" && activeToken != nil {
		if saveErr := activeToken.SaveInstancePassword(instance.ID, instance.Label, "provider_default", generatedPassword, time.Now()); saveErr != nil {
			passwordSaveError = saveErr.Error()
		} else if saveErr := saveVultrAdditionPreservingSecrets(scope, addition); saveErr != nil {
			activeToken.RemoveSavedInstancePassword(instance.ID)
			passwordSaveError = "Failed to save root password: " + saveErr.Error()
		} else {
			passwordSaved = true
		}
	}

	logMessage := fmt.Sprintf("create vultr instance: %s (%s/%s/%d", request.Label, request.Region, request.Plan, request.OSID)
	if autoConnectGroup != "" {
		logMessage += ", auto_connect_group=" + autoConnectGroup
	}
	logMessage += ")"
	logCloudAudit(c, logMessage)
	api.RespondSuccess(c, createVultrInstanceResponse{
		Instance:          buildVultrInstanceView(instance, addition),
		GeneratedPassword: generatedPassword,
		PasswordSaved:     passwordSaved,
		PasswordSaveError: passwordSaveError,
	})
}

func DeleteVultrInstance(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	client, addition, activeToken, ctx, cancel, err := getVultrActiveTokenClient(c, scope)
	if err != nil {
		respondVultrError(c, err)
		return
	}
	defer cancel()

	instanceID := strings.TrimSpace(c.Param("id"))
	if instanceID == "" {
		api.RespondError(c, http.StatusBadRequest, "Invalid instance id")
		return
	}
	if err := client.DeleteInstance(ctx, instanceID); err != nil {
		respondVultrError(c, err)
		return
	}
	if activeToken != nil && activeToken.RemoveSavedInstancePassword(instanceID) {
		_ = saveVultrAddition(scope, addition)
	}

	logCloudAudit(c, "delete vultr instance: "+instanceID)
	api.RespondSuccess(c, nil)
}

func PostVultrInstanceAction(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	client, _, _, ctx, cancel, err := getVultrActiveTokenClient(c, scope)
	if err != nil {
		respondVultrError(c, err)
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
	case "start", "boot", "power_on":
		err = client.StartInstance(ctx, instanceID)
		actionType = "start"
	case "halt", "stop", "shutdown", "power_off":
		err = client.HaltInstance(ctx, instanceID)
		actionType = "halt"
	case "reboot", "restart":
		err = client.RebootInstance(ctx, instanceID)
		actionType = "reboot"
	default:
		api.RespondError(c, http.StatusBadRequest, "Unsupported instance action: "+payload.Type)
		return
	}
	if err != nil {
		respondVultrError(c, err)
		return
	}

	logCloudAudit(c, fmt.Sprintf("vultr instance action: %s (%s)", actionType, instanceID))
	api.RespondSuccess(c, gin.H{
		"type":        actionType,
		"resource_id": instanceID,
		"status":      "submitted",
	})
}

func getVultrClient(c *gin.Context, scope ownerScope) (*vultrcloud.Client, context.Context, context.CancelFunc, error) {
	client, _, _, ctx, cancel, err := getVultrActiveTokenClient(c, scope)
	return client, ctx, cancel, err
}

func getVultrActiveTokenClient(c *gin.Context, scope ownerScope) (*vultrcloud.Client, *vultrcloud.Addition, *vultrcloud.TokenRecord, context.Context, context.CancelFunc, error) {
	_, addition, err := loadVultrAddition(scope, false)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}

	activeToken := addition.ActiveToken()
	if activeToken == nil {
		return nil, nil, nil, nil, nil, fmt.Errorf("Vultr token is not configured")
	}

	client, err := vultrcloud.NewClientFromToken(activeToken.Token)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
	return client, addition, activeToken, ctx, cancel, nil
}

func respondVultrError(c *gin.Context, err error) {
	var apiErr *vultrcloud.APIError
	if errors.As(err, &apiErr) {
		statusCode := apiErr.StatusCode
		if statusCode < 400 || statusCode > 599 {
			statusCode = http.StatusBadGateway
		}
		api.RespondError(c, statusCode, apiErr.Error())
		return
	}
	api.RespondError(c, http.StatusBadRequest, err.Error())
}

func buildVultrInstanceView(instance *vultrcloud.Instance, addition *vultrcloud.Addition) *vultrInstanceView {
	if instance == nil {
		return nil
	}
	view := &vultrInstanceView{Instance: *instance}
	if addition != nil && addition.HasSavedInstancePassword(instance.ID) {
		view.SavedRootPassword = true
		view.SavedRootPasswordUpdatedAt = addition.SavedInstancePasswordUpdatedAt(instance.ID)
	}
	return view
}

func loadVultrAddition(scope ownerScope, allowMissing bool) (*models.CloudProvider, *vultrcloud.Addition, error) {
	config, err := getCloudProviderConfigForScope(scope, vultrProviderName)
	if err != nil {
		if allowMissing {
			addition := &vultrcloud.Addition{}
			addition.Normalize()
			return nil, addition, nil
		}
		return nil, nil, fmt.Errorf("Vultr provider is not configured")
	}

	addition := &vultrcloud.Addition{}
	raw := strings.TrimSpace(config.Addition)
	if raw == "" {
		raw = "{}"
	}
	if err := json.Unmarshal([]byte(raw), addition); err != nil {
		return nil, nil, fmt.Errorf("Vultr configuration is invalid: %w", err)
	}

	addition.Normalize()
	return config, addition, nil
}

func saveVultrAddition(scope ownerScope, addition *vultrcloud.Addition) error {
	if addition == nil {
		addition = &vultrcloud.Addition{}
	}
	addition.Normalize()
	payload, err := json.Marshal(addition)
	if err != nil {
		return err
	}
	return saveCloudProviderConfigForScope(scope, vultrProviderName, string(payload))
}

func saveVultrAdditionPreservingSecrets(scope ownerScope, addition *vultrcloud.Addition) error {
	if addition == nil {
		addition = &vultrcloud.Addition{}
	}
	if _, current, err := loadVultrAddition(scope, true); err == nil {
		addition.MergePersistentStateFrom(current)
	}
	return saveVultrAddition(scope, addition)
}
