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
	"github.com/komari-monitor/komari/database/auditlog"
	"github.com/komari-monitor/komari/database/models"
	"github.com/komari-monitor/komari/utils/cloudprovider/digitalocean"
	"github.com/komari-monitor/komari/utils/cloudprovider/factory"
)

const digitalOceanProviderName = "digitalocean"

type createDigitalOceanDropletPayload struct {
	Name             string   `json:"name" binding:"required"`
	Region           string   `json:"region" binding:"required"`
	Size             string   `json:"size" binding:"required"`
	Image            string   `json:"image" binding:"required"`
	SSHKeys          []int    `json:"ssh_keys,omitempty"`
	Backups          bool     `json:"backups"`
	IPv6             bool     `json:"ipv6"`
	Monitoring       bool     `json:"monitoring"`
	Tags             []string `json:"tags,omitempty"`
	UserData         string   `json:"user_data,omitempty"`
	VPCUUID          string   `json:"vpc_uuid,omitempty"`
	RootPasswordMode string   `json:"root_password_mode,omitempty"`
	RootPassword     string   `json:"root_password,omitempty"`
}

type createDigitalOceanDropletResponse struct {
	Droplet           *digitalocean.Droplet                   `json:"droplet"`
	GeneratedPassword string                                  `json:"generated_password,omitempty"`
	ManagedSSHKey     *digitalocean.ManagedSSHKeyMaterialView `json:"managed_ssh_key,omitempty"`
}

func GetCloudProviders(c *gin.Context) {
	api.RespondSuccess(c, factory.GetProviderConfigs())
}

func GetCloudProvider(c *gin.Context) {
	providerName := strings.TrimSpace(c.Param("provider"))
	if _, exists := factory.GetConstructor(providerName); !exists {
		api.RespondError(c, http.StatusNotFound, "Cloud provider not found: "+providerName)
		return
	}

	config, err := database.GetCloudProviderConfigByName(providerName)
	if err != nil {
		api.RespondError(c, http.StatusNotFound, "Cloud provider not configured: "+providerName)
		return
	}

	api.RespondSuccess(c, config)
}

func SetCloudProvider(c *gin.Context) {
	providerName := strings.TrimSpace(c.Param("provider"))
	constructor, exists := factory.GetConstructor(providerName)
	if !exists {
		api.RespondError(c, http.StatusNotFound, "Cloud provider not found: "+providerName)
		return
	}

	var payload struct {
		Addition string `json:"addition"`
	}
	if err := c.ShouldBindJSON(&payload); err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid configuration: "+err.Error())
		return
	}
	if strings.TrimSpace(payload.Addition) == "" {
		payload.Addition = "{}"
	}

	provider := constructor()
	if err := json.Unmarshal([]byte(payload.Addition), provider.GetConfiguration()); err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid provider configuration: "+err.Error())
		return
	}

	if err := database.SaveCloudProviderConfig(&models.CloudProvider{
		Name:     providerName,
		Addition: payload.Addition,
	}); err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to save cloud provider configuration: "+err.Error())
		return
	}

	logCloudAudit(c, "update cloud provider config: "+providerName)
	api.RespondSuccess(c, gin.H{"message": "Cloud provider configured successfully"})
}

func GetDigitalOceanTokens(c *gin.Context) {
	_, addition, err := loadDigitalOceanAddition(true)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}

	api.RespondSuccess(c, addition.ToPoolView())
}

func SaveDigitalOceanTokens(c *gin.Context) {
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

	_, addition, err := loadDigitalOceanAddition(true)
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

	if err := saveDigitalOceanAddition(addition); err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to save DigitalOcean tokens: "+err.Error())
		return
	}

	logCloudAudit(c, fmt.Sprintf("import digitalocean tokens: %d", imported))
	api.RespondSuccess(c, addition.ToPoolView())
}

func SetDigitalOceanActiveToken(c *gin.Context) {
	var payload struct {
		TokenID string `json:"token_id" binding:"required"`
	}
	if err := c.ShouldBindJSON(&payload); err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid active token payload: "+err.Error())
		return
	}

	_, addition, err := loadDigitalOceanAddition(false)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}

	if !addition.SetActiveToken(payload.TokenID) {
		api.RespondError(c, http.StatusNotFound, "DigitalOcean token not found")
		return
	}

	if err := saveDigitalOceanAddition(addition); err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to update active token: "+err.Error())
		return
	}

	logCloudAudit(c, "set active digitalocean token: "+payload.TokenID)
	api.RespondSuccess(c, addition.ToPoolView())
}

func CheckDigitalOceanTokens(c *gin.Context) {
	var payload struct {
		TokenIDs []string `json:"token_ids"`
	}
	if c.Request.ContentLength > 0 {
		if err := c.ShouldBindJSON(&payload); err != nil {
			api.RespondError(c, http.StatusBadRequest, "Invalid token check payload: "+err.Error())
			return
		}
	}

	_, addition, err := loadDigitalOceanAddition(false)
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

	if err := saveDigitalOceanAddition(addition); err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to save token health: "+err.Error())
		return
	}

	logCloudAudit(c, fmt.Sprintf("check digitalocean tokens: %d", len(addition.Tokens)))
	api.RespondSuccess(c, addition.ToPoolView())
}

func DeleteDigitalOceanToken(c *gin.Context) {
	tokenID := strings.TrimSpace(c.Param("id"))
	if tokenID == "" {
		api.RespondError(c, http.StatusBadRequest, "Invalid token id")
		return
	}

	_, addition, err := loadDigitalOceanAddition(false)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}

	if !addition.RemoveToken(tokenID) {
		api.RespondError(c, http.StatusNotFound, "DigitalOcean token not found")
		return
	}

	if err := saveDigitalOceanAddition(addition); err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to delete token: "+err.Error())
		return
	}

	logCloudAudit(c, "delete digitalocean token: "+tokenID)
	api.RespondSuccess(c, addition.ToPoolView())
}

func GetDigitalOceanManagedSSHKey(c *gin.Context) {
	tokenID := strings.TrimSpace(c.Param("id"))
	if tokenID == "" {
		api.RespondError(c, http.StatusBadRequest, "Invalid token id")
		return
	}

	_, addition, err := loadDigitalOceanAddition(false)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}

	token := addition.FindToken(tokenID)
	if token == nil {
		api.RespondError(c, http.StatusNotFound, "DigitalOcean token not found")
		return
	}

	material := token.ManagedSSHKeyMaterialView()
	if material == nil {
		api.RespondError(c, http.StatusNotFound, "Managed SSH key is not configured for this token")
		return
	}

	api.RespondSuccess(c, material)
}

func GetDigitalOceanAccount(c *gin.Context) {
	client, ctx, cancel, err := getDigitalOceanClient(c)
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
	client, ctx, cancel, err := getDigitalOceanClient(c)
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
	client, ctx, cancel, err := getDigitalOceanClient(c)
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

	api.RespondSuccess(c, droplets)
}

func CreateDigitalOceanDroplet(c *gin.Context) {
	client, addition, activeToken, ctx, cancel, err := getDigitalOceanActiveTokenClient(c)
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

	passwordMode := normalizeRootPasswordMode(payload.RootPasswordMode)
	if passwordMode == "" {
		api.RespondError(c, http.StatusBadRequest, "Unsupported root password mode: "+payload.RootPasswordMode)
		return
	}

	request := digitalocean.CreateDropletRequest{
		Name:       payload.Name,
		Region:     payload.Region,
		Size:       payload.Size,
		Image:      payload.Image,
		SSHKeys:    uniqueIntSlice(payload.SSHKeys),
		Backups:    payload.Backups,
		IPv6:       payload.IPv6,
		Monitoring: payload.Monitoring,
		Tags:       payload.Tags,
		UserData:   payload.UserData,
		VPCUUID:    payload.VPCUUID,
	}

	var generatedPassword string
	var managedSSHKey *digitalocean.ManagedSSHKeyMaterialView
	if passwordMode != "ssh" {
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

		managedSSHKey, err = ensureManagedDigitalOceanSSHKey(ctx, addition, activeToken, client)
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
	}

	droplet, err := client.CreateDroplet(ctx, request)
	if err != nil {
		respondCloudError(c, err)
		return
	}

	logCloudAudit(c, fmt.Sprintf("create digitalocean droplet: %s (%s/%s/%s, password_mode=%s)", request.Name, request.Region, request.Size, request.Image, passwordMode))
	api.RespondSuccess(c, createDigitalOceanDropletResponse{
		Droplet:           droplet,
		GeneratedPassword: generatedPassword,
		ManagedSSHKey:     managedSSHKey,
	})
}

func DeleteDigitalOceanDroplet(c *gin.Context) {
	client, ctx, cancel, err := getDigitalOceanClient(c)
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

	logCloudAudit(c, fmt.Sprintf("delete digitalocean droplet: %d", dropletID))
	api.RespondSuccess(c, nil)
}

func PostDigitalOceanDropletAction(c *gin.Context) {
	client, ctx, cancel, err := getDigitalOceanClient(c)
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

func getDigitalOceanClient(c *gin.Context) (*digitalocean.Client, context.Context, context.CancelFunc, error) {
	client, _, _, ctx, cancel, err := getDigitalOceanActiveTokenClient(c)
	return client, ctx, cancel, err
}

func getDigitalOceanActiveTokenClient(c *gin.Context) (*digitalocean.Client, *digitalocean.Addition, *digitalocean.TokenRecord, context.Context, context.CancelFunc, error) {
	_, addition, err := loadDigitalOceanAddition(false)
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
	case "", "ssh", "ssh_key", "ssh_only":
		return "ssh"
	case "custom":
		return "custom"
	case "random":
		return "random"
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

func ensureManagedDigitalOceanSSHKey(ctx context.Context, addition *digitalocean.Addition, token *digitalocean.TokenRecord, client *digitalocean.Client) (*digitalocean.ManagedSSHKeyMaterialView, error) {
	if token == nil {
		return nil, fmt.Errorf("DigitalOcean token is not configured")
	}

	if token.ManagedSSHKeyID > 0 && token.HasManagedSSHKeyMaterial() {
		sshKeys, err := client.ListSSHKeys(ctx)
		if err != nil {
			return nil, err
		}
		for _, sshKey := range sshKeys {
			if sshKey.ID == token.ManagedSSHKeyID {
				if token.ManagedSSHKeyName == "" {
					token.ManagedSSHKeyName = sshKey.Name
				}
				if token.ManagedSSHKeyFingerprint == "" {
					token.ManagedSSHKeyFingerprint = sshKey.Fingerprint
				}
				return token.ManagedSSHKeyMaterialView(), nil
			}
		}
	}

	material, err := digitalocean.GenerateManagedSSHKeyPair(digitalocean.ManagedSSHKeyName(token))
	if err != nil {
		return nil, err
	}

	createdSSHKey, err := client.CreateSSHKey(ctx, digitalocean.CreateSSHKeyRequest{
		Name:      material.Name,
		PublicKey: material.PublicKey,
	})
	if err != nil {
		return nil, err
	}

	token.ManagedSSHKeyID = createdSSHKey.ID
	token.ManagedSSHKeyName = createdSSHKey.Name
	token.ManagedSSHKeyFingerprint = createdSSHKey.Fingerprint
	token.ManagedSSHPublicKey = material.PublicKey
	token.ManagedSSHPrivateKey = material.PrivateKey

	if err := saveDigitalOceanAddition(addition); err != nil {
		return nil, err
	}

	return token.ManagedSSHKeyMaterialView(), nil
}

func loadDigitalOceanAddition(allowMissing bool) (*models.CloudProvider, *digitalocean.Addition, error) {
	config, err := database.GetCloudProviderConfigByName(digitalOceanProviderName)
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

func saveDigitalOceanAddition(addition *digitalocean.Addition) error {
	if addition == nil {
		addition = &digitalocean.Addition{}
	}
	addition.Normalize()

	payload, err := json.Marshal(addition)
	if err != nil {
		return err
	}

	return database.SaveCloudProviderConfig(&models.CloudProvider{
		Name:     digitalOceanProviderName,
		Addition: string(payload),
	})
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
	auditlog.Log(c.ClientIP(), userUUID, message, "info")
}
