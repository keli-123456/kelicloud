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

	"github.com/gin-gonic/gin"
	"github.com/komari-monitor/komari/api"
	"github.com/komari-monitor/komari/database/models"
	azurecloud "github.com/komari-monitor/komari/utils/cloudprovider/azure"
)

const azureProviderName = "azure"

type azureAccountView struct {
	CredentialName          string `json:"credential_name"`
	TenantID                string `json:"tenant_id"`
	ClientID                string `json:"client_id"`
	SubscriptionID          string `json:"subscription_id"`
	SubscriptionDisplayName string `json:"subscription_display_name"`
	SubscriptionState       string `json:"subscription_state"`
	DefaultLocation         string `json:"default_location"`
	ActiveLocation          string `json:"active_location"`
}

type createAzureInstancePayload struct {
	Name             string                    `json:"name"`
	ResourceGroup    string                    `json:"resource_group,omitempty"`
	Size             string                    `json:"size" binding:"required"`
	AdminUsername    string                    `json:"admin_username,omitempty"`
	AdminPassword    string                    `json:"admin_password,omitempty"`
	SSHPublicKey     string                    `json:"ssh_public_key,omitempty"`
	UserData         string                    `json:"user_data,omitempty"`
	PublicIP         bool                      `json:"public_ip"`
	AssignIPv6       bool                      `json:"assign_ipv6"`
	Image            azurecloud.ImageReference `json:"image" binding:"required"`
	AutoConnect      bool                      `json:"auto_connect"`
	AutoConnectGroup string                    `json:"auto_connect_group,omitempty"`
}

type azureInstanceView struct {
	azurecloud.Instance
	SavedRootPassword          bool   `json:"saved_root_password"`
	SavedRootPasswordUpdatedAt string `json:"saved_root_password_updated_at,omitempty"`
}

type azureInstanceDetailView struct {
	Instance          *azureInstanceView            `json:"instance"`
	VMID              string                        `json:"vm_id"`
	Zones             []string                      `json:"zones"`
	LicenseType       string                        `json:"license_type"`
	NetworkInterfaces []azurecloud.NetworkInterface `json:"network_interfaces"`
	OSDisk            *azurecloud.Disk              `json:"os_disk,omitempty"`
	DataDisks         []azurecloud.Disk             `json:"data_disks"`
}

func GetAzureCredentials(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	_, addition, err := loadAzureAddition(scope, true)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}

	api.RespondSuccess(c, addition.ToPoolView())
}

func SaveAzureCredentials(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	var payload struct {
		Credentials        []azurecloud.CredentialImport `json:"credentials"`
		ActiveCredentialID string                        `json:"active_credential_id"`
		ActiveLocation     string                        `json:"active_location"`
	}
	if err := c.ShouldBindJSON(&payload); err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid credential payload: "+err.Error())
		return
	}

	if len(payload.Credentials) == 0 {
		api.RespondError(c, http.StatusBadRequest, "At least one credential is required")
		return
	}

	_, addition, err := loadAzureAddition(scope, true)
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
	if strings.TrimSpace(payload.ActiveLocation) != "" {
		addition.SetActiveLocation(payload.ActiveLocation)
	}

	if err := saveAzureAdditionPreservingSecrets(scope, addition); err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to save Azure credentials: "+err.Error())
		return
	}

	logCloudAudit(c, fmt.Sprintf("import azure credentials: %d", imported))
	api.RespondSuccess(c, addition.ToPoolView())
}

func SetAzureActiveCredential(c *gin.Context) {
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

	_, addition, err := loadAzureAddition(scope, false)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}

	if !addition.SetActiveCredential(payload.CredentialID) {
		api.RespondError(c, http.StatusNotFound, "Azure credential not found")
		return
	}

	if err := saveAzureAdditionPreservingSecrets(scope, addition); err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to update active credential: "+err.Error())
		return
	}

	logCloudAudit(c, "set active azure credential: "+payload.CredentialID)
	api.RespondSuccess(c, addition.ToPoolView())
}

func SetAzureActiveLocation(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	var payload struct {
		Location string `json:"location" binding:"required"`
	}
	if err := c.ShouldBindJSON(&payload); err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid active location payload: "+err.Error())
		return
	}

	_, addition, err := loadAzureAddition(scope, false)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}

	addition.SetActiveLocation(payload.Location)
	if err := saveAzureAdditionPreservingSecrets(scope, addition); err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to update active location: "+err.Error())
		return
	}

	logCloudAudit(c, "set active azure location: "+payload.Location)
	api.RespondSuccess(c, addition.ToPoolView())
}

func CheckAzureCredentials(c *gin.Context) {
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

	_, addition, err := loadAzureAddition(scope, false)
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
			client, err := azurecloud.NewClientFromCredential(&record)
			var subscription *azurecloud.Subscription
			if err == nil {
				ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
				defer cancel()
				subscription, err = client.GetSubscription(ctx)
			}

			mu.Lock()
			addition.Credentials[credentialIndex].SetCheckResult(checkedAt, subscription, err)
			mu.Unlock()
		}(index)
	}

	wg.Wait()

	if err := saveAzureAdditionPreservingSecrets(scope, addition); err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to save Azure credential health: "+err.Error())
		return
	}

	logCloudAudit(c, fmt.Sprintf("check azure credentials: %d", len(addition.Credentials)))
	api.RespondSuccess(c, addition.ToPoolView())
}

func DeleteAzureCredential(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	credentialID := strings.TrimSpace(c.Param("id"))
	if credentialID == "" {
		api.RespondError(c, http.StatusBadRequest, "Invalid credential id")
		return
	}

	_, addition, err := loadAzureAddition(scope, false)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}

	if !addition.RemoveCredential(credentialID) {
		api.RespondError(c, http.StatusNotFound, "Azure credential not found")
		return
	}

	if err := saveAzureAdditionPreservingSecrets(scope, addition); err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to delete Azure credential: "+err.Error())
		return
	}

	logCloudAudit(c, "delete azure credential: "+credentialID)
	api.RespondSuccess(c, addition.ToPoolView())
}

func GetAzureCredentialSecret(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	credentialID := strings.TrimSpace(c.Param("id"))
	if credentialID == "" {
		api.RespondError(c, http.StatusBadRequest, "Invalid credential id")
		return
	}

	_, addition, err := loadAzureAddition(scope, false)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}

	credential := addition.FindCredential(credentialID)
	if credential == nil {
		api.RespondError(c, http.StatusNotFound, "Azure credential not found")
		return
	}

	logCloudAuditWithType(c, "view azure credential secret: "+credentialID, "warn")
	api.RespondSuccess(c, credential.CredentialSecretView())
}

func GetAzureInstancePassword(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	instanceID := strings.TrimSpace(c.Param("id"))
	if instanceID == "" {
		api.RespondError(c, http.StatusBadRequest, "Invalid instance id")
		return
	}

	_, addition, err := loadAzureAddition(scope, false)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}

	activeCredential := addition.ActiveCredential()
	if activeCredential == nil {
		api.RespondError(c, http.StatusBadRequest, "Azure credential is not configured")
		return
	}

	passwordView, err := activeCredential.RevealInstancePassword(instanceID)
	if err != nil {
		switch {
		case errors.Is(err, azurecloud.ErrSavedRootPasswordNotFound):
			api.RespondError(c, http.StatusNotFound, err.Error())
		case errors.Is(err, azurecloud.ErrRootPasswordVaultDisabled), errors.Is(err, azurecloud.ErrRootPasswordDecryptFailed):
			api.RespondError(c, http.StatusBadRequest, err.Error())
		default:
			api.RespondError(c, http.StatusInternalServerError, "Failed to load saved root password: "+err.Error())
		}
		return
	}

	logCloudAuditWithType(c, "view azure vm password: "+instanceID, "warn")
	api.RespondSuccess(c, passwordView)
}

func GetAzureAccount(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	addition, credential, _, ctx, cancel, err := getAzureActiveCredential(c, scope)
	if err != nil {
		respondAzureError(c, err)
		return
	}
	defer cancel()

	subscription, err := getAzureSubscriptionSnapshot(ctx, credential)
	if err != nil {
		respondAzureError(c, err)
		return
	}

	view := azureAccountView{
		CredentialName:  credential.Name,
		TenantID:        credential.TenantID,
		ClientID:        credential.ClientID,
		SubscriptionID:  credential.SubscriptionID,
		DefaultLocation: firstNonEmpty(credential.DefaultLocation, azurecloud.DefaultLocation),
		ActiveLocation:  firstNonEmpty(addition.ActiveLocation, credential.DefaultLocation, azurecloud.DefaultLocation),
	}
	if subscription != nil {
		view.SubscriptionDisplayName = strings.TrimSpace(subscription.DisplayName)
		view.SubscriptionState = strings.TrimSpace(subscription.State)
	}

	api.RespondSuccess(c, view)
}

func GetAzureCatalog(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	addition, credential, location, ctx, cancel, err := getAzureActiveCredential(c, scope)
	if err != nil {
		respondAzureError(c, err)
		return
	}
	defer cancel()

	client, err := azurecloud.NewClientFromCredential(credential)
	if err != nil {
		respondAzureError(c, err)
		return
	}

	locations, err := client.ListLocations(ctx)
	if err != nil {
		respondAzureError(c, err)
		return
	}

	sizes, err := client.ListVirtualMachineSizes(ctx, location)
	if err != nil {
		respondAzureError(c, err)
		return
	}
	if sizes == nil {
		sizes = make([]azurecloud.VMSku, 0)
	}

	api.RespondSuccess(c, gin.H{
		"active_location": firstNonEmpty(addition.ActiveLocation, credential.DefaultLocation, azurecloud.DefaultLocation),
		"locations":       locations,
		"sizes":           sizes,
	})
}

func ListAzureInstances(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	addition, credential, _, ctx, cancel, err := getAzureActiveCredential(c, scope)
	if err != nil {
		respondAzureError(c, err)
		return
	}
	defer cancel()

	client, err := azurecloud.NewClientFromCredential(credential)
	if err != nil {
		respondAzureError(c, err)
		return
	}

	instances, err := client.ListVirtualMachines(ctx)
	if err != nil {
		respondAzureError(c, err)
		return
	}
	if instances == nil {
		instances = make([]azurecloud.Instance, 0)
	}

	if credential != nil {
		validInstanceIDs := make(map[string]struct{}, len(instances))
		for _, instance := range instances {
			instanceID := strings.TrimSpace(instance.InstanceID)
			if instanceID != "" {
				validInstanceIDs[instanceID] = struct{}{}
			}
		}
		if credential.PruneInstanceCredentials(validInstanceIDs) {
			_ = saveAzureAddition(scope, addition)
		}
	}

	views := make([]azureInstanceView, 0, len(instances))
	for _, instance := range instances {
		if view := buildAzureInstanceView(&instance, addition); view != nil {
			views = append(views, *view)
		}
	}

	api.RespondSuccess(c, views)
}

func GetAzureInstanceDetail(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	addition, credential, _, ctx, cancel, err := getAzureActiveCredential(c, scope)
	if err != nil {
		respondAzureError(c, err)
		return
	}
	defer cancel()

	resourceGroup, name, err := azurecloud.DecodeInstanceID(c.Param("id"))
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}

	client, err := azurecloud.NewClientFromCredential(credential)
	if err != nil {
		respondAzureError(c, err)
		return
	}

	detail, err := client.GetVirtualMachineDetail(ctx, resourceGroup, name)
	if err != nil {
		respondAzureError(c, err)
		return
	}

	api.RespondSuccess(c, buildAzureInstanceDetailView(detail, addition))
}

func CreateAzureInstance(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	addition, credential, location, _, _, err := getAzureActiveCredential(c, scope)
	if err != nil {
		respondAzureError(c, err)
		return
	}

	var payload createAzureInstancePayload
	if err := c.ShouldBindJSON(&payload); err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid instance request: "+err.Error())
		return
	}

	payload.Name = strings.TrimSpace(payload.Name)
	payload.ResourceGroup = strings.TrimSpace(payload.ResourceGroup)
	payload.Size = strings.TrimSpace(payload.Size)
	payload.AdminUsername = strings.TrimSpace(payload.AdminUsername)
	payload.AdminPassword = strings.TrimSpace(payload.AdminPassword)
	payload.SSHPublicKey = strings.TrimSpace(payload.SSHPublicKey)
	payload.UserData = strings.TrimSpace(payload.UserData)
	payload.AutoConnectGroup = strings.TrimSpace(payload.AutoConnectGroup)
	payload.Image = azurecloud.ImageReference{
		Publisher: strings.TrimSpace(payload.Image.Publisher),
		Offer:     strings.TrimSpace(payload.Image.Offer),
		SKU:       strings.TrimSpace(payload.Image.SKU),
		Version:   strings.TrimSpace(payload.Image.Version),
	}

	resolvedUserData := payload.UserData
	autoConnectGroup := ""
	if payload.AutoConnect {
		resolvedUserData, autoConnectGroup, err = prepareCloudAutoConnectUserData(c, resolvedUserData, autoConnectUserDataOptions{
			Enabled:           true,
			Group:             payload.AutoConnectGroup,
			Provider:          azureProviderName,
			CredentialName:    credential.Name,
			WrapInShellScript: true,
		})
		if err != nil {
			api.RespondError(c, http.StatusBadRequest, err.Error())
			return
		}
	}

	client, err := azurecloud.NewClientFromCredential(credential)
	if err != nil {
		respondAzureError(c, err)
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 8*time.Minute)
	defer cancel()

	detail, err := client.CreateVirtualMachine(ctx, azurecloud.CreateVirtualMachineRequest{
		Name:          payload.Name,
		ResourceGroup: payload.ResourceGroup,
		Location:      location,
		Size:          payload.Size,
		AdminUsername: payload.AdminUsername,
		AdminPassword: payload.AdminPassword,
		SSHPublicKey:  payload.SSHPublicKey,
		UserData:      resolvedUserData,
		PublicIP:      payload.PublicIP,
		AssignIPv6:    payload.AssignIPv6,
		Image:         payload.Image,
	})
	if err != nil {
		respondAzureError(c, err)
		return
	}
	if detail == nil || strings.TrimSpace(detail.Instance.InstanceID) == "" {
		api.RespondError(c, http.StatusBadGateway, "Azure VM was created, but Komari could not load the created VM details")
		return
	}

	passwordSaved := false
	passwordSaveError := ""
	if credential != nil && strings.TrimSpace(payload.AdminPassword) != "" {
		passwordMode := "custom"
		instanceName := ""
		if detail != nil {
			instanceName = detail.Instance.Name
		}
		if persistedAddition, saveErr := persistAzureInstancePassword(
			scope,
			addition,
			credential,
			detail.Instance.InstanceID,
			instanceName,
			firstNonEmpty(payload.AdminUsername, "azureuser"),
			passwordMode,
			payload.AdminPassword,
		); saveErr != nil {
			passwordSaveError = saveErr.Error()
		} else {
			passwordSaved = true
			addition = persistedAddition
		}
	}

	logMessage := fmt.Sprintf("create azure vm: %s (%s/%s/%s", detail.Instance.Name, detail.Instance.Location, detail.Instance.Size, detail.Instance.Image)
	if autoConnectGroup != "" {
		logMessage += ", auto_connect_group=" + autoConnectGroup
	}
	logMessage += ")"
	logCloudAudit(c, logMessage)

	detailView := buildAzureInstanceDetailView(detail, addition)
	instanceView := buildAzureInstanceView(&detail.Instance, addition)
	if detailView != nil && detailView.Instance != nil {
		instanceView = detailView.Instance
	}
	if instanceView != nil && passwordSaved {
		instanceView.SavedRootPassword = true
		if instanceView.SavedRootPasswordUpdatedAt == "" {
			instanceView.SavedRootPasswordUpdatedAt = time.Now().UTC().Format(time.RFC3339)
		}
	}
	if detailView != nil {
		detailView.Instance = instanceView
	}

	api.RespondSuccess(c, gin.H{
		"instance":            instanceView,
		"detail":              detailView,
		"password_saved":      passwordSaved,
		"password_save_error": passwordSaveError,
	})
}

func DeleteAzureInstance(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	addition, credential, _, ctx, cancel, err := getAzureActiveCredentialWithTimeout(c, scope, 4*time.Minute)
	if err != nil {
		respondAzureError(c, err)
		return
	}
	defer cancel()

	resourceGroup, name, err := azurecloud.DecodeInstanceID(c.Param("id"))
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}

	client, err := azurecloud.NewClientFromCredential(credential)
	if err != nil {
		respondAzureError(c, err)
		return
	}

	deleteResult, err := client.DeleteVirtualMachine(ctx, resourceGroup, name)
	if err != nil {
		respondAzureError(c, err)
		return
	}
	instanceID := strings.TrimSpace(c.Param("id"))
	if credential != nil && credential.RemoveSavedInstancePassword(instanceID) {
		_ = saveAzureAddition(scope, addition)
	}

	logCloudAudit(c, fmt.Sprintf("delete azure vm: %s/%s", resourceGroup, name))
	if deleteResult == nil {
		deleteResult = &azurecloud.DeleteVirtualMachineResult{}
	}
	api.RespondSuccess(c, deleteResult)
}

func PostAzureInstanceAction(c *gin.Context) {
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}

	_, credential, _, ctx, cancel, err := getAzureActiveCredentialWithTimeout(c, scope, 4*time.Minute)
	if err != nil {
		respondAzureError(c, err)
		return
	}
	defer cancel()

	resourceGroup, name, err := azurecloud.DecodeInstanceID(c.Param("id"))
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}

	var payload struct {
		Type string `json:"type" binding:"required"`
	}
	if err := c.ShouldBindJSON(&payload); err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid action request: "+err.Error())
		return
	}

	client, err := azurecloud.NewClientFromCredential(credential)
	if err != nil {
		respondAzureError(c, err)
		return
	}

	actionType := strings.ToLower(strings.TrimSpace(payload.Type))
	response := gin.H{
		"type":   actionType,
		"status": "submitted",
	}
	switch actionType {
	case "start":
		err = client.StartVirtualMachine(ctx, resourceGroup, name)
	case "restart":
		err = client.RestartVirtualMachine(ctx, resourceGroup, name)
	case "deallocate", "stop":
		actionType = "deallocate"
		response["type"] = actionType
		err = client.DeallocateVirtualMachine(ctx, resourceGroup, name)
	case "replace_ip", "replace_public_ip":
		actionType = "replace_public_ip"
		response["type"] = actionType
		var replacement *azurecloud.PublicIPReplacementResult
		replacement, err = client.ReplaceVirtualMachinePublicIPv4(ctx, resourceGroup, name)
		if replacement != nil {
			response["old_public_ip"] = replacement.OldPublicIP
			response["old_public_ip_id"] = replacement.OldPublicIPID
			response["new_public_ip"] = replacement.NewPublicIP
			response["new_public_ip_id"] = replacement.NewPublicIPID
		}
	default:
		api.RespondError(c, http.StatusBadRequest, "Unsupported Azure VM action")
		return
	}
	if err != nil {
		respondAzureError(c, err)
		return
	}

	logCloudAudit(c, fmt.Sprintf("azure vm action: %s (%s/%s)", actionType, resourceGroup, name))
	api.RespondSuccess(c, response)
}

func getAzureActiveCredential(c *gin.Context, scope ownerScope) (*azurecloud.Addition, *azurecloud.CredentialRecord, string, context.Context, context.CancelFunc, error) {
	return getAzureActiveCredentialWithTimeout(c, scope, 60*time.Second)
}

func getAzureActiveCredentialWithTimeout(c *gin.Context, scope ownerScope, timeout time.Duration) (*azurecloud.Addition, *azurecloud.CredentialRecord, string, context.Context, context.CancelFunc, error) {
	_, addition, err := loadAzureAddition(scope, false)
	if err != nil {
		return nil, nil, "", nil, nil, err
	}

	activeCredential := addition.ActiveCredential()
	if activeCredential == nil {
		return nil, nil, "", nil, nil, fmt.Errorf("Azure credential is not configured")
	}
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), timeout)

	if strings.TrimSpace(activeCredential.SubscriptionID) == "" {
		subscription, resolveErr := getAzureSubscriptionSnapshot(ctx, activeCredential)
		if resolveErr != nil {
			cancel()
			return nil, nil, "", nil, nil, resolveErr
		}
		activeCredential.SetCheckResult(time.Now().UTC(), subscription, nil)
		addition.Normalize()
		if saveErr := saveAzureAdditionPreservingSecrets(scope, addition); saveErr != nil {
			cancel()
			return nil, nil, "", nil, nil, fmt.Errorf("Failed to save Azure credential health: %w", saveErr)
		}
		activeCredential = addition.ActiveCredential()
		if activeCredential == nil {
			cancel()
			return nil, nil, "", nil, nil, fmt.Errorf("Azure credential is not configured")
		}
	}

	location := firstNonEmpty(addition.ActiveLocation, activeCredential.DefaultLocation, azurecloud.DefaultLocation)
	return addition, activeCredential, location, ctx, cancel, nil
}

func getAzureSubscriptionSnapshot(ctx context.Context, credential *azurecloud.CredentialRecord) (*azurecloud.Subscription, error) {
	client, err := azurecloud.NewClientFromCredential(credential)
	if err != nil {
		return nil, err
	}
	return client.GetSubscription(ctx)
}

func respondAzureError(c *gin.Context, err error) {
	var apiErr *azurecloud.APIError
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

func loadAzureAddition(scope ownerScope, allowMissing bool) (*models.CloudProvider, *azurecloud.Addition, error) {
	config, err := getCloudProviderConfigForScope(scope, azureProviderName)
	if err != nil {
		if allowMissing {
			addition := &azurecloud.Addition{}
			addition.Normalize()
			return nil, addition, nil
		}
		return nil, nil, fmt.Errorf("Azure provider is not configured")
	}

	addition := &azurecloud.Addition{}
	raw := strings.TrimSpace(config.Addition)
	if raw == "" {
		raw = "{}"
	}
	if err := json.Unmarshal([]byte(raw), addition); err != nil {
		return nil, nil, fmt.Errorf("Azure configuration is invalid: %w", err)
	}

	addition.Normalize()
	return config, addition, nil
}

func saveAzureAddition(scope ownerScope, addition *azurecloud.Addition) error {
	if addition == nil {
		addition = &azurecloud.Addition{}
	}
	addition.Normalize()

	payload, err := json.Marshal(addition)
	if err != nil {
		return err
	}

	return saveCloudProviderConfigForScope(scope, azureProviderName, string(payload))
}

func saveAzureAdditionPreservingSecrets(scope ownerScope, addition *azurecloud.Addition) error {
	if addition == nil {
		addition = &azurecloud.Addition{}
	}

	if _, current, err := loadAzureAddition(scope, true); err == nil {
		addition.MergePersistentStateFrom(current)
	}

	return saveAzureAddition(scope, addition)
}

func buildAzureInstanceView(instance *azurecloud.Instance, addition *azurecloud.Addition) *azureInstanceView {
	if instance == nil {
		return nil
	}

	view := &azureInstanceView{
		Instance: *instance,
	}
	if addition != nil && addition.HasSavedInstancePassword(instance.InstanceID) {
		view.SavedRootPassword = true
		view.SavedRootPasswordUpdatedAt = addition.SavedInstancePasswordUpdatedAt(instance.InstanceID)
	}
	return view
}

func buildAzureInstanceDetailView(detail *azurecloud.InstanceDetail, addition *azurecloud.Addition) *azureInstanceDetailView {
	if detail == nil {
		return nil
	}

	return &azureInstanceDetailView{
		Instance:          buildAzureInstanceView(&detail.Instance, addition),
		VMID:              detail.VMID,
		Zones:             append([]string(nil), detail.Zones...),
		LicenseType:       detail.LicenseType,
		NetworkInterfaces: append([]azurecloud.NetworkInterface(nil), detail.NetworkInterfaces...),
		OSDisk:            detail.OSDisk,
		DataDisks:         append([]azurecloud.Disk(nil), detail.DataDisks...),
	}
}

func persistAzureInstancePassword(
	scope ownerScope,
	addition *azurecloud.Addition,
	credential *azurecloud.CredentialRecord,
	instanceID string,
	instanceName string,
	username string,
	passwordMode string,
	rootPassword string,
) (*azurecloud.Addition, error) {
	instanceID = strings.TrimSpace(instanceID)
	rootPassword = strings.TrimSpace(rootPassword)
	if addition == nil || credential == nil || instanceID == "" || rootPassword == "" {
		return nil, errors.New("root password persistence requires an active credential and resource")
	}

	if err := credential.SaveInstancePassword(instanceID, instanceName, username, passwordMode, rootPassword, time.Now()); err != nil {
		return nil, err
	}

	verifyPersistedPassword := func() (*azurecloud.Addition, error) {
		_, persistedAddition, err := loadAzureAddition(scope, false)
		if err != nil {
			return nil, err
		}
		persistedCredential := findAzureCredentialForPersistenceVerification(
			persistedAddition,
			credential.ID,
			credential.TenantID,
			credential.ClientID,
			credential.SubscriptionID,
		)
		if persistedCredential == nil || !persistedCredential.HasSavedInstancePassword(instanceID) {
			return nil, errors.New("saved root password was not found after persistence")
		}
		return persistedAddition, nil
	}

	if err := saveAzureAdditionPreservingSecrets(scope, addition); err != nil {
		credential.RemoveSavedInstancePassword(instanceID)
		return nil, fmt.Errorf("Failed to save root password: %w", err)
	}

	if persistedAddition, err := verifyPersistedPassword(); err == nil {
		return persistedAddition, nil
	}

	if err := saveAzureAddition(scope, addition); err != nil {
		credential.RemoveSavedInstancePassword(instanceID)
		return nil, fmt.Errorf("Failed to save root password: %w", err)
	}

	persistedAddition, err := verifyPersistedPassword()
	if err != nil {
		credential.RemoveSavedInstancePassword(instanceID)
		return nil, fmt.Errorf("Failed to verify saved root password: %w", err)
	}
	return persistedAddition, nil
}

func findAzureCredentialForPersistenceVerification(
	addition *azurecloud.Addition,
	credentialID string,
	tenantID string,
	clientID string,
	subscriptionID string,
) *azurecloud.CredentialRecord {
	if addition == nil {
		return nil
	}

	credentialID = strings.TrimSpace(credentialID)
	if credentialID != "" {
		if credential := addition.FindCredential(credentialID); credential != nil {
			return credential
		}
	}

	tenantID = strings.TrimSpace(tenantID)
	clientID = strings.TrimSpace(clientID)
	subscriptionID = strings.TrimSpace(subscriptionID)
	if tenantID == "" || clientID == "" || subscriptionID == "" {
		return nil
	}

	for index := range addition.Credentials {
		credential := &addition.Credentials[index]
		if strings.TrimSpace(credential.TenantID) == tenantID &&
			strings.TrimSpace(credential.ClientID) == clientID &&
			strings.TrimSpace(credential.SubscriptionID) == subscriptionID {
			return credential
		}
	}
	return nil
}
