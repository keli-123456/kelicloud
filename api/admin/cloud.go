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
	client, ctx, cancel, err := getDigitalOceanClient(c)
	if err != nil {
		respondCloudError(c, err)
		return
	}
	defer cancel()

	var request digitalocean.CreateDropletRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid droplet request: "+err.Error())
		return
	}

	request.Name = strings.TrimSpace(request.Name)
	request.Region = strings.TrimSpace(request.Region)
	request.Size = strings.TrimSpace(request.Size)
	request.Image = strings.TrimSpace(request.Image)
	request.Tags = trimStringSlice(request.Tags)
	request.UserData = strings.TrimSpace(request.UserData)
	request.VPCUUID = strings.TrimSpace(request.VPCUUID)

	droplet, err := client.CreateDroplet(ctx, request)
	if err != nil {
		respondCloudError(c, err)
		return
	}

	logCloudAudit(c, fmt.Sprintf("create digitalocean droplet: %s (%s/%s/%s)", request.Name, request.Region, request.Size, request.Image))
	api.RespondSuccess(c, droplet)
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
	config, err := database.GetCloudProviderConfigByName(digitalOceanProviderName)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("DigitalOcean provider is not configured")
	}

	var addition digitalocean.Addition
	if err := json.Unmarshal([]byte(config.Addition), &addition); err != nil {
		return nil, nil, nil, fmt.Errorf("DigitalOcean configuration is invalid: %w", err)
	}

	client, err := digitalocean.NewClient(&addition)
	if err != nil {
		return nil, nil, nil, err
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
	return client, ctx, cancel, nil
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
