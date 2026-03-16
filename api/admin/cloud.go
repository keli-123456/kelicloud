package admin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/komari-monitor/komari/api"
	"github.com/komari-monitor/komari/database"
	"github.com/komari-monitor/komari/database/models"
	"github.com/komari-monitor/komari/utils/cloudprovider/digitalocean"
	"github.com/komari-monitor/komari/utils/cloudprovider/factory"
	"gorm.io/gorm"
)

const digitalOceanProviderName = "digitalocean"

type createDigitalOceanDropletPayload struct {
	Name             string   `json:"name" binding:"required"`
	Region           string   `json:"region" binding:"required"`
	Size             string   `json:"size" binding:"required"`
	Image            string   `json:"image" binding:"required"`
	Backups          bool     `json:"backups"`
	IPv6             bool     `json:"ipv6"`
	Monitoring       bool     `json:"monitoring"`
	Tags             []string `json:"tags,omitempty"`
	UserData         string   `json:"user_data,omitempty"`
	VPCUUID          string   `json:"vpc_uuid,omitempty"`
	RootPasswordMode string   `json:"root_password_mode,omitempty"`
	RootPassword     string   `json:"root_password,omitempty"`
	AutoConnect      bool     `json:"auto_connect"`
	AutoConnectGroup string   `json:"auto_connect_group,omitempty"`
}

type digitalOceanDropletView struct {
	digitalocean.Droplet
	SavedRootPassword          bool   `json:"saved_root_password"`
	SavedRootPasswordUpdatedAt string `json:"saved_root_password_updated_at,omitempty"`
}

type createDigitalOceanDropletResponse struct {
	Droplet           *digitalOceanDropletView                `json:"droplet"`
	GeneratedPassword string                                  `json:"generated_password,omitempty"`
	ManagedSSHKey     *digitalocean.ManagedSSHKeyMaterialView `json:"managed_ssh_key,omitempty"`
	PasswordSaved     bool                                    `json:"password_saved"`
	PasswordSaveError string                                  `json:"password_save_error,omitempty"`
}

func GetCloudProviders(c *gin.Context) {
	api.RespondSuccess(c, factory.GetProviderConfigs())
}

func GetCloudProvider(c *gin.Context) {
	tenantID, ok := requireCurrentTenantID(c)
	if !ok {
		return
	}

	providerName := strings.TrimSpace(c.Param("provider"))
	if _, exists := factory.GetConstructor(providerName); !exists {
		api.RespondError(c, http.StatusNotFound, "Cloud provider not found: "+providerName)
		return
	}

	config, err := database.GetCloudProviderConfigByTenantAndName(tenantID, providerName)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			response, buildErr := buildCloudProviderResponse(providerName, nil)
			if buildErr != nil {
				api.RespondError(c, http.StatusInternalServerError, "Failed to build cloud provider response: "+buildErr.Error())
				return
			}
			api.RespondSuccess(c, response)
			return
		}
		api.RespondError(c, http.StatusInternalServerError, "Failed to load cloud provider configuration: "+err.Error())
		return
	}

	response, err := buildCloudProviderResponse(providerName, config)
	if err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to parse cloud provider configuration: "+err.Error())
		return
	}

	api.RespondSuccess(c, response)
}

func SetCloudProvider(c *gin.Context) {
	tenantID, ok := requireCurrentTenantID(c)
	if !ok {
		return
	}

	providerName := strings.TrimSpace(c.Param("provider"))
	if _, exists := factory.GetConstructor(providerName); !exists {
		api.RespondError(c, http.StatusNotFound, "Cloud provider not found: "+providerName)
		return
	}

	var payload setCloudProviderPayload
	if err := c.ShouldBindJSON(&payload); err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid configuration: "+err.Error())
		return
	}

	var entries []cloudProviderEntry
	var err error
	if payload.Entries != nil {
		entries = payload.Entries
	} else {
		entries, err = convertLegacyAdditionToCloudProviderEntries(payload.Addition)
		if err != nil {
			api.RespondError(c, http.StatusBadRequest, "Invalid provider configuration: "+err.Error())
			return
		}
	}

	entries, err = validateCloudProviderEntries(providerName, entries)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid provider configuration: "+err.Error())
		return
	}

	addition, err := marshalCloudProviderEntries(entries)
	if err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to encode cloud provider configuration: "+err.Error())
		return
	}

	if err := database.SaveCloudProviderConfigForTenant(&models.CloudProvider{
		TenantID: tenantID,
		Name:     providerName,
		Addition: addition,
	}); err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to save cloud provider configuration: "+err.Error())
		return
	}

	logCloudAudit(c, fmt.Sprintf("update cloud provider config: %s (%d entries)", providerName, len(entries)))
	api.RespondSuccess(c, &cloudProviderResponse{
		Name:    providerName,
		Entries: entries,
	})
}

func GetDigitalOceanTokens(c *gin.Context) {
	tenantID, ok := requireCurrentTenantID(c)
	if !ok {
		return
	}

	_, addition, err := loadDigitalOceanAddition(tenantID, true)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}

	api.RespondSuccess(c, addition.ToPoolView())
}

func SaveDigitalOceanTokens(c *gin.Context) {
	tenantID, ok := requireCurrentTenantID(c)
	if !ok {
		return
	}

	var payload struct {
		Tokens        []digitalocean.TokenImport `json:"tokens"`
		ActiveTokenID string                     `json:"active_token_id"`
	}
	if err := c.ShouldBindJSON(&payload); err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid token payload: "+err.Error())
		return
	}

	if len(payload.Tokens) == 0 {
		api.RespondError(c, http.StatusBadRequest, "At least one token is required")
		return
	}

	_, addition, err := loadDigitalOceanAddition(tenantID, true)
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

	if err := saveDigitalOceanAdditionPreservingSecrets(tenantID, addition); err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to save DigitalOcean tokens: "+err.Error())
		return
	}

	logCloudAudit(c, fmt.Sprintf("import digitalocean tokens: %d", imported))
	api.RespondSuccess(c, addition.ToPoolView())
}

func SetDigitalOceanActiveToken(c *gin.Context) {
	tenantID, ok := requireCurrentTenantID(c)
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

	_, addition, err := loadDigitalOceanAddition(tenantID, false)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}

	if !addition.SetActiveToken(payload.TokenID) {
		api.RespondError(c, http.StatusNotFound, "DigitalOcean token not found")
		return
	}

	if err := saveDigitalOceanAdditionPreservingSecrets(tenantID, addition); err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to update active token: "+err.Error())
		return
	}

	logCloudAudit(c, "set active digitalocean token: "+payload.TokenID)
	api.RespondSuccess(c, addition.ToPoolView())
}

func CheckDigitalOceanTokens(c *gin.Context) {
	tenantID, ok := requireCurrentTenantID(c)
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

	_, addition, err := loadDigitalOceanAddition(tenantID, false)
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
			defer func() {
				<-limiter
			}()

			record := addition.Tokens[tokenIndex]
			client, err := digitalocean.NewClientFromToken(record.Token)
			var account *digitalocean.Account
			if err == nil {
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				account, err = client.GetAccount(ctx)
			}

			mu.Lock()
			addition.Tokens[tokenIndex].SetCheckResult(checkedAt, account, err)
			mu.Unlock()
		}(index)
	}

	wg.Wait()

	if err := saveDigitalOceanAdditionPreservingSecrets(tenantID, addition); err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to save token health: "+err.Error())
		return
	}

	logCloudAudit(c, fmt.Sprintf("check digitalocean tokens: %d", len(addition.Tokens)))
	api.RespondSuccess(c, addition.ToPoolView())
}

func DeleteDigitalOceanToken(c *gin.Context) {
	tenantID, ok := requireCurrentTenantID(c)
	if !ok {
		return
	}

	tokenID := strings.TrimSpace(c.Param("id"))
	if tokenID == "" {
		api.RespondError(c, http.StatusBadRequest, "Invalid token id")
		return
	}

	_, addition, err := loadDigitalOceanAddition(tenantID, false)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}

	if !addition.RemoveToken(tokenID) {
		api.RespondError(c, http.StatusNotFound, "DigitalOcean token not found")
		return
	}

	if err := saveDigitalOceanAddition(tenantID, addition); err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to delete token: "+err.Error())
		return
	}

	logCloudAudit(c, "delete digitalocean token: "+tokenID)
	api.RespondSuccess(c, addition.ToPoolView())
}

func GetDigitalOceanManagedSSHKey(c *gin.Context) {
	tenantID, ok := requireCurrentTenantID(c)
	if !ok {
		return
	}

	tokenID := strings.TrimSpace(c.Param("id"))
	if tokenID == "" {
		api.RespondError(c, http.StatusBadRequest, "Invalid token id")
		return
	}

	_, addition, err := loadDigitalOceanAddition(tenantID, false)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}

	token := addition.FindToken(tokenID)
	if token == nil {
		api.RespondError(c, http.StatusNotFound, "DigitalOcean token not found")
		return
	}

	material := addition.ManagedSSHKeyMaterialViewForToken(token)
	if material == nil {
		api.RespondError(c, http.StatusNotFound, "Managed SSH key is not configured for this token")
		return
	}

	api.RespondSuccess(c, material)
}

func GetDigitalOceanTokenSecret(c *gin.Context) {
	tenantID, ok := requireCurrentTenantID(c)
	if !ok {
		return
	}

	tokenID := strings.TrimSpace(c.Param("id"))
	if tokenID == "" {
		api.RespondError(c, http.StatusBadRequest, "Invalid token id")
		return
	}

	_, addition, err := loadDigitalOceanAddition(tenantID, false)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}

	token := addition.FindToken(tokenID)
	if token == nil {
		api.RespondError(c, http.StatusNotFound, "DigitalOcean token not found")
		return
	}

	api.RespondSuccess(c, token.TokenSecretView())
}

func GetDigitalOceanDropletPassword(c *gin.Context) {
	tenantID, ok := requireCurrentTenantID(c)
	if !ok {
		return
	}

	dropletID, err := strconv.Atoi(strings.TrimSpace(c.Param("id")))
	if err != nil || dropletID <= 0 {
		api.RespondError(c, http.StatusBadRequest, "Invalid droplet id")
		return
	}

	_, addition, err := loadDigitalOceanAddition(tenantID, false)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}

	activeToken := addition.ActiveToken()
	if activeToken == nil {
		api.RespondError(c, http.StatusBadRequest, "DigitalOcean token is not configured")
		return
	}

	passwordView, err := activeToken.RevealDropletPassword(dropletID)
	if err != nil {
		switch {
		case errors.Is(err, digitalocean.ErrSavedDropletPasswordNotFound):
			api.RespondError(c, http.StatusNotFound, err.Error())
		case errors.Is(err, digitalocean.ErrDropletPasswordVaultDisabled), errors.Is(err, digitalocean.ErrDropletPasswordDecryptFailed):
			api.RespondError(c, http.StatusBadRequest, err.Error())
		default:
			api.RespondError(c, http.StatusInternalServerError, "Failed to load saved root password: "+err.Error())
		}
		return
	}

	api.RespondSuccess(c, passwordView)
}

func GetDigitalOceanAccount(c *gin.Context) {
	tenantID, ok := requireCurrentTenantID(c)
	if !ok {
		return
	}

	client, ctx, cancel, err := getDigitalOceanClient(c, tenantID)
	if err != nil {
		respondCloudError(c, err)
		return
	}
	defer cancel()

	account, err := client.GetAccount(ctx)
	if err != nil {
		respondCloudError(c, err)
		return
	}

	api.RespondSuccess(c, account)
}

func GetDigitalOceanCatalog(c *gin.Context) {
	tenantID, ok := requireCurrentTenantID(c)
	if !ok {
		return
	}

	client, ctx, cancel, err := getDigitalOceanClient(c, tenantID)
	if err != nil {
		respondCloudError(c, err)
		return
	}
	defer cancel()

	regions, err := client.ListRegions(ctx)
	if err != nil {
		respondCloudError(c, err)
		return
	}
	sizes, err := client.ListSizes(ctx)
	if err != nil {
		respondCloudError(c, err)
		return
	}
	images, err := client.ListImages(ctx, "distribution")
	if err != nil {
		respondCloudError(c, err)
		return
	}
	sshKeys, err := client.ListSSHKeys(ctx)
	if err != nil {
		respondCloudError(c, err)
		return
	}

	sort.Slice(regions, func(i, j int) bool {
		return regions[i].Slug < regions[j].Slug
	})
	sort.Slice(sizes, func(i, j int) bool {
		return sizes[i].PriceMonthly < sizes[j].PriceMonthly
	})
	sort.Slice(images, func(i, j int) bool {
		left := strings.ToLower(strings.TrimSpace(images[i].Distribution + " " + images[i].Name))
		right := strings.ToLower(strings.TrimSpace(images[j].Distribution + " " + images[j].Name))
		return left < right
	})
	sort.Slice(sshKeys, func(i, j int) bool {
		return sshKeys[i].Name < sshKeys[j].Name
	})
	if regions == nil {
		regions = make([]digitalocean.Region, 0)
	}
	if sizes == nil {
		sizes = make([]digitalocean.Size, 0)
	}
	if images == nil {
		images = make([]digitalocean.Image, 0)
	}
	if sshKeys == nil {
		sshKeys = make([]digitalocean.SSHKey, 0)
	}

	api.RespondSuccess(c, gin.H{
		"regions":  regions,
		"sizes":    sizes,
		"images":   images,
		"ssh_keys": sshKeys,
	})
}

func ListDigitalOceanDroplets(c *gin.Context) {
	tenantID, ok := requireCurrentTenantID(c)
	if !ok {
		return
	}

	client, addition, activeToken, ctx, cancel, err := getDigitalOceanActiveTokenClient(c, tenantID)
	if err != nil {
		respondCloudError(c, err)
		return
	}
	defer cancel()

	droplets, err := client.ListDroplets(ctx)
	if err != nil {
		respondCloudError(c, err)
		return
	}

	sort.Slice(droplets, func(i, j int) bool {
		return droplets[i].CreatedAt > droplets[j].CreatedAt
	})
	if droplets == nil {
		droplets = make([]digitalocean.Droplet, 0)
	}

	views := make([]digitalOceanDropletView, 0, len(droplets))
	validDropletIDs := make(map[int]struct{}, len(droplets))
	credentialsChanged := false

	for index := range droplets {
		validDropletIDs[droplets[index].ID] = struct{}{}
		if activeToken != nil && activeToken.SyncDropletCredentialName(droplets[index].ID, droplets[index].Name) {
			credentialsChanged = true
		}
		view := buildDigitalOceanDropletView(&droplets[index], activeToken)
		if view != nil {
			views = append(views, *view)
		}
	}

	if activeToken != nil && activeToken.PruneDropletCredentials(validDropletIDs) {
		credentialsChanged = true
	}
	if credentialsChanged {
		_ = saveDigitalOceanAddition(tenantID, addition)
	}

	api.RespondSuccess(c, views)
}

func CreateDigitalOceanDroplet(c *gin.Context) {
	tenantID, ok := requireCurrentTenantID(c)
	if !ok {
		return
	}

	client, addition, activeToken, ctx, cancel, err := getDigitalOceanActiveTokenClient(c, tenantID)
	if err != nil {
		respondCloudError(c, err)
		return
	}
	defer cancel()

	var payload createDigitalOceanDropletPayload
	if err := c.ShouldBindJSON(&payload); err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid droplet request: "+err.Error())
		return
	}

	payload.Name = strings.TrimSpace(payload.Name)
	payload.Region = strings.TrimSpace(payload.Region)
	payload.Size = strings.TrimSpace(payload.Size)
	payload.Image = strings.TrimSpace(payload.Image)
	payload.Tags = trimStringSlice(payload.Tags)
	payload.UserData = strings.TrimSpace(payload.UserData)
	payload.VPCUUID = strings.TrimSpace(payload.VPCUUID)
	payload.RootPassword = strings.TrimSpace(payload.RootPassword)
	payload.AutoConnectGroup = strings.TrimSpace(payload.AutoConnectGroup)

	passwordMode := normalizeRootPasswordMode(payload.RootPasswordMode)
	if passwordMode == "" {
		api.RespondError(c, http.StatusBadRequest, "Unsupported root password mode: "+payload.RootPasswordMode)
		return
	}

	resolvedUserData := payload.UserData
	autoConnectGroup := ""
	if payload.AutoConnect {
		resolvedUserData, autoConnectGroup, err = prepareCloudAutoConnectUserData(c, payload.UserData, autoConnectUserDataOptions{
			Enabled:           true,
			Group:             payload.AutoConnectGroup,
			Provider:          digitalOceanProviderName,
			CredentialName:    activeToken.Name,
			WrapInShellScript: false,
		})
		if err != nil {
			api.RespondError(c, http.StatusBadRequest, err.Error())
			return
		}
	}

	request := digitalocean.CreateDropletRequest{
		Name:       payload.Name,
		Region:     payload.Region,
		Size:       payload.Size,
		Image:      payload.Image,
		Backups:    payload.Backups,
		IPv6:       payload.IPv6,
		Monitoring: payload.Monitoring,
		Tags:       payload.Tags,
		UserData:   resolvedUserData,
		VPCUUID:    payload.VPCUUID,
	}

	var generatedPassword string
	var managedSSHKey *digitalocean.ManagedSSHKeyMaterialView
	rootPassword := payload.RootPassword
	if passwordMode == "random" {
		rootPassword, err = digitalocean.GenerateRandomPassword(20)
		if err != nil {
			api.RespondError(c, http.StatusInternalServerError, "Failed to generate root password: "+err.Error())
			return
		}
		generatedPassword = rootPassword
	} else if rootPassword == "" {
		api.RespondError(c, http.StatusBadRequest, "Custom root password cannot be empty")
		return
	}

	managedSSHKey, err = ensureManagedDigitalOceanSSHKey(ctx, tenantID, addition, activeToken, client)
	if err != nil {
		var apiErr *digitalocean.APIError
		if errors.As(err, &apiErr) {
			respondCloudError(c, err)
		} else {
			api.RespondError(c, http.StatusInternalServerError, "Failed to prepare managed SSH key: "+err.Error())
		}
		return
	}

	request.SSHKeys = appendUniqueInt(request.SSHKeys, managedSSHKey.KeyID)
	request.UserData, err = digitalocean.BuildRootPasswordUserData(rootPassword, request.UserData)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}

	droplet, err := client.CreateDroplet(ctx, request)
	if err != nil {
		respondCloudError(c, err)
		return
	}

	passwordSaved := false
	passwordSaveError := ""
	if droplet != nil && rootPassword != "" && activeToken != nil {
		if saveErr := activeToken.SaveDropletPassword(droplet.ID, droplet.Name, passwordMode, rootPassword, time.Now()); saveErr != nil {
			passwordSaveError = saveErr.Error()
		} else if saveErr := saveDigitalOceanAdditionPreservingSecrets(tenantID, addition); saveErr != nil {
			activeToken.RemoveSavedDropletPassword(droplet.ID)
			passwordSaveError = "Failed to save root password: " + saveErr.Error()
		} else {
			passwordSaved = true
		}
	}

	logMessage := fmt.Sprintf("create digitalocean droplet: %s (%s/%s/%s, password_mode=%s", request.Name, request.Region, request.Size, request.Image, passwordMode)
	if autoConnectGroup != "" {
		logMessage += ", auto_connect_group=" + autoConnectGroup
	}
	logMessage += ")"
	logCloudAudit(c, logMessage)
	api.RespondSuccess(c, createDigitalOceanDropletResponse{
		Droplet:           buildDigitalOceanDropletView(droplet, activeToken),
		GeneratedPassword: generatedPassword,
		ManagedSSHKey:     managedSSHKey,
		PasswordSaved:     passwordSaved,
		PasswordSaveError: passwordSaveError,
	})
}

func DeleteDigitalOceanDroplet(c *gin.Context) {
	tenantID, ok := requireCurrentTenantID(c)
	if !ok {
		return
	}

	client, addition, activeToken, ctx, cancel, err := getDigitalOceanActiveTokenClient(c, tenantID)
	if err != nil {
		respondCloudError(c, err)
		return
	}
	defer cancel()

	dropletID, err := strconv.Atoi(strings.TrimSpace(c.Param("id")))
	if err != nil || dropletID <= 0 {
		api.RespondError(c, http.StatusBadRequest, "Invalid droplet id")
		return
	}

	if err := client.DeleteDroplet(ctx, dropletID); err != nil {
		respondCloudError(c, err)
		return
	}

	if activeToken != nil && activeToken.RemoveSavedDropletPassword(dropletID) {
		_ = saveDigitalOceanAddition(tenantID, addition)
	}

	logCloudAudit(c, fmt.Sprintf("delete digitalocean droplet: %d", dropletID))
	api.RespondSuccess(c, nil)
}

func PostDigitalOceanDropletAction(c *gin.Context) {
	tenantID, ok := requireCurrentTenantID(c)
	if !ok {
		return
	}

	client, ctx, cancel, err := getDigitalOceanClient(c, tenantID)
	if err != nil {
		respondCloudError(c, err)
		return
	}
	defer cancel()

	dropletID, err := strconv.Atoi(strings.TrimSpace(c.Param("id")))
	if err != nil || dropletID <= 0 {
		api.RespondError(c, http.StatusBadRequest, "Invalid droplet id")
		return
	}

	var request digitalocean.DropletActionRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid action request: "+err.Error())
		return
	}

	request.Type = strings.TrimSpace(request.Type)
	if !isAllowedDigitalOceanAction(request.Type) {
		api.RespondError(c, http.StatusBadRequest, "Unsupported droplet action: "+request.Type)
		return
	}

	action, err := client.PostDropletAction(ctx, dropletID, request)
	if err != nil {
		respondCloudError(c, err)
		return
	}

	logCloudAudit(c, fmt.Sprintf("digitalocean droplet action: %s (%d)", request.Type, dropletID))
	api.RespondSuccess(c, action)
}

func getDigitalOceanClient(c *gin.Context, tenantID string) (*digitalocean.Client, context.Context, context.CancelFunc, error) {
	client, _, _, ctx, cancel, err := getDigitalOceanActiveTokenClient(c, tenantID)
	return client, ctx, cancel, err
}

func getDigitalOceanActiveTokenClient(c *gin.Context, tenantID string) (*digitalocean.Client, *digitalocean.Addition, *digitalocean.TokenRecord, context.Context, context.CancelFunc, error) {
	_, addition, err := loadDigitalOceanAddition(tenantID, false)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}

	activeToken := addition.ActiveToken()
	if activeToken == nil {
		return nil, nil, nil, nil, nil, fmt.Errorf("DigitalOcean token is not configured")
	}

	client, err := digitalocean.NewClientFromToken(activeToken.Token)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
	return client, addition, activeToken, ctx, cancel, nil
}

func respondCloudError(c *gin.Context, err error) {
	var apiErr *digitalocean.APIError
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

func isAllowedDigitalOceanAction(action string) bool {
	switch action {
	case "power_on", "power_off", "reboot", "shutdown", "power_cycle":
		return true
	default:
		return false
	}
}

func trimStringSlice(values []string) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

func normalizeRootPasswordMode(mode string) string {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "", "random":
		return "random"
	case "custom":
		return "custom"
	default:
		return ""
	}
}

func uniqueIntSlice(values []int) []int {
	result := make([]int, 0, len(values))
	seen := make(map[int]struct{}, len(values))
	for _, value := range values {
		if value <= 0 {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

func appendUniqueInt(values []int, value int) []int {
	if value <= 0 {
		return values
	}
	for _, current := range values {
		if current == value {
			return values
		}
	}
	return append(values, value)
}

func buildDigitalOceanDropletView(droplet *digitalocean.Droplet, token *digitalocean.TokenRecord) *digitalOceanDropletView {
	if droplet == nil {
		return nil
	}

	view := &digitalOceanDropletView{
		Droplet: *droplet,
	}
	if token != nil && token.HasSavedDropletPassword(droplet.ID) {
		view.SavedRootPassword = true
		view.SavedRootPasswordUpdatedAt = token.SavedDropletPasswordUpdatedAt(droplet.ID)
	}

	return view
}

func ensureManagedDigitalOceanSSHKey(ctx context.Context, tenantID string, addition *digitalocean.Addition, token *digitalocean.TokenRecord, client *digitalocean.Client) (*digitalocean.ManagedSSHKeyMaterialView, error) {
	if token == nil {
		return nil, fmt.Errorf("DigitalOcean token is not configured")
	}

	addition.Normalize()

	account, err := client.GetAccount(ctx)
	if err != nil {
		return nil, err
	}

	changed := syncDigitalOceanTokenAccount(token, account)

	if !addition.HasManagedSSHKeyMaterial() {
		material, err := digitalocean.GenerateManagedSSHKeyPair(digitalocean.ManagedSSHKeyName(nil))
		if err != nil {
			return nil, err
		}
		addition.ManagedSSHKeyName = material.Name
		addition.ManagedSSHPublicKey = material.PublicKey
		addition.ManagedSSHPrivateKey = material.PrivateKey
		changed = true
	}

	sshKeys, err := client.ListSSHKeys(ctx)
	if err != nil {
		return nil, err
	}

	accountUUID := ""
	accountEmail := ""
	if account != nil {
		accountUUID = account.UUID
		accountEmail = account.Email
	}

	if registration := addition.FindManagedSSHKeyAccount(accountUUID, accountEmail); registration != nil {
		for _, sshKey := range sshKeys {
			if sshKey.ID != registration.KeyID {
				continue
			}
			if addition.UpsertManagedSSHKeyAccount(accountUUID, accountEmail, &sshKey) {
				changed = true
			}
			if addition.ManagedSSHKeyFingerprint == "" && strings.TrimSpace(sshKey.Fingerprint) != "" {
				addition.ManagedSSHKeyFingerprint = strings.TrimSpace(sshKey.Fingerprint)
				changed = true
			}
			if changed {
				if err := saveDigitalOceanAdditionPreservingSecrets(tenantID, addition); err != nil {
					return nil, err
				}
			}
			return addition.ManagedSSHKeyMaterialViewForToken(token), nil
		}
	}

	for _, sshKey := range sshKeys {
		if strings.TrimSpace(sshKey.PublicKey) != strings.TrimSpace(addition.ManagedSSHPublicKey) {
			continue
		}
		if addition.UpsertManagedSSHKeyAccount(accountUUID, accountEmail, &sshKey) {
			changed = true
		}
		if addition.ManagedSSHKeyFingerprint == "" && strings.TrimSpace(sshKey.Fingerprint) != "" {
			addition.ManagedSSHKeyFingerprint = strings.TrimSpace(sshKey.Fingerprint)
			changed = true
		}
		if changed {
			if err := saveDigitalOceanAdditionPreservingSecrets(tenantID, addition); err != nil {
				return nil, err
			}
		}
		return addition.ManagedSSHKeyMaterialViewForToken(token), nil
	}

	createdSSHKey, err := client.CreateSSHKey(ctx, digitalocean.CreateSSHKeyRequest{
		Name:      addition.ManagedSSHKeyName,
		PublicKey: addition.ManagedSSHPublicKey,
	})
	if err != nil {
		return nil, err
	}

	if addition.UpsertManagedSSHKeyAccount(accountUUID, accountEmail, createdSSHKey) {
		changed = true
	}
	if addition.ManagedSSHKeyFingerprint == "" && strings.TrimSpace(createdSSHKey.Fingerprint) != "" {
		addition.ManagedSSHKeyFingerprint = strings.TrimSpace(createdSSHKey.Fingerprint)
		changed = true
	}
	if changed {
		if err := saveDigitalOceanAdditionPreservingSecrets(tenantID, addition); err != nil {
			return nil, err
		}
	}

	return addition.ManagedSSHKeyMaterialViewForToken(token), nil
}

func syncDigitalOceanTokenAccount(token *digitalocean.TokenRecord, account *digitalocean.Account) bool {
	if token == nil || account == nil {
		return false
	}

	changed := false
	email := strings.TrimSpace(account.Email)
	if token.AccountEmail != email {
		token.AccountEmail = email
		changed = true
	}

	uuid := strings.TrimSpace(account.UUID)
	if token.AccountUUID != uuid {
		token.AccountUUID = uuid
		changed = true
	}

	if token.DropletLimit != account.DropletLimit {
		token.DropletLimit = account.DropletLimit
		changed = true
	}

	return changed
}

func loadDigitalOceanAddition(tenantID string, allowMissing bool) (*models.CloudProvider, *digitalocean.Addition, error) {
	config, err := database.GetCloudProviderConfigByTenantAndName(tenantID, digitalOceanProviderName)
	if err != nil {
		if allowMissing {
			addition := &digitalocean.Addition{}
			addition.Normalize()
			return nil, addition, nil
		}
		return nil, nil, fmt.Errorf("DigitalOcean provider is not configured")
	}

	addition := &digitalocean.Addition{}
	raw := strings.TrimSpace(config.Addition)
	if raw == "" {
		raw = "{}"
	}
	if err := json.Unmarshal([]byte(raw), addition); err != nil {
		return nil, nil, fmt.Errorf("DigitalOcean configuration is invalid: %w", err)
	}

	addition.Normalize()
	return config, addition, nil
}

func saveDigitalOceanAddition(tenantID string, addition *digitalocean.Addition) error {
	if addition == nil {
		addition = &digitalocean.Addition{}
	}
	addition.Normalize()

	payload, err := json.Marshal(addition)
	if err != nil {
		return err
	}

	return database.SaveCloudProviderConfigForTenant(&models.CloudProvider{
		TenantID: tenantID,
		Name:     digitalOceanProviderName,
		Addition: string(payload),
	})
}

func saveDigitalOceanAdditionPreservingSecrets(tenantID string, addition *digitalocean.Addition) error {
	if addition == nil {
		addition = &digitalocean.Addition{}
	}

	if _, current, err := loadDigitalOceanAddition(tenantID, true); err == nil {
		addition.MergePersistentStateFrom(current)
	}

	return saveDigitalOceanAddition(tenantID, addition)
}

func logCloudAudit(c *gin.Context, message string) {
	uuid, exists := c.Get("uuid")
	if !exists {
		return
	}
	userUUID, ok := uuid.(string)
	if !ok || userUUID == "" {
		return
	}
	api.AuditLogForCurrentTenant(c, userUUID, message, "info")
}
