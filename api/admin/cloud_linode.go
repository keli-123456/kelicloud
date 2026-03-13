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
	linodecloud "github.com/komari-monitor/komari/utils/cloudprovider/linode"
)

const linodeProviderName = "linode"

type createLinodeInstancePayload struct {
	Label            string   `json:"label" binding:"required"`
	Region           string   `json:"region" binding:"required"`
	Type             string   `json:"type" binding:"required"`
	Image            string   `json:"image" binding:"required"`
	AuthorizedKeys   []string `json:"authorized_keys,omitempty"`
	BackupsEnabled   bool     `json:"backups_enabled"`
	Booted           bool     `json:"booted"`
	Tags             []string `json:"tags,omitempty"`
	UserData         string   `json:"user_data,omitempty"`
	RootPasswordMode string   `json:"root_password_mode,omitempty"`
	RootPassword     string   `json:"root_password,omitempty"`
	AutoConnect      bool     `json:"auto_connect"`
	AutoConnectGroup string   `json:"auto_connect_group,omitempty"`
}

type linodeAccountView struct {
	Username   string  `json:"username"`
	Email      string  `json:"email"`
	Company    string  `json:"company"`
	Balance    float64 `json:"balance"`
	Restricted bool    `json:"restricted"`
}

type linodeInstanceView struct {
	linodecloud.Instance
	SavedRootPassword          bool   `json:"saved_root_password"`
	SavedRootPasswordUpdatedAt string `json:"saved_root_password_updated_at,omitempty"`
}

type createLinodeInstanceResponse struct {
	Instance          *linodeInstanceView `json:"instance"`
	GeneratedPassword string              `json:"generated_password,omitempty"`
	PasswordSaved     bool                `json:"password_saved"`
	PasswordSaveError string              `json:"password_save_error,omitempty"`
}

type linodeInstanceDetailView struct {
	Instance *linodeInstanceView  `json:"instance"`
	Disks    []linodecloud.Disk   `json:"disks"`
	Configs  []linodecloud.Config `json:"configs"`
	Backups  *linodecloud.Backups `json:"backups,omitempty"`
}

func GetLinodeTokens(c *gin.Context) {
	_, addition, err := loadLinodeAddition(true)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}

	api.RespondSuccess(c, addition.ToPoolView())
}

func SaveLinodeTokens(c *gin.Context) {
	var payload struct {
		Tokens        []linodecloud.TokenImport `json:"tokens"`
		ActiveTokenID string                    `json:"active_token_id"`
	}
	if err := c.ShouldBindJSON(&payload); err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid token payload: "+err.Error())
		return
	}

	if len(payload.Tokens) == 0 {
		api.RespondError(c, http.StatusBadRequest, "At least one token is required")
		return
	}

	_, addition, err := loadLinodeAddition(true)
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

	if err := saveLinodeAddition(addition); err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to save Linode tokens: "+err.Error())
		return
	}

	logCloudAudit(c, fmt.Sprintf("import linode tokens: %d", imported))
	api.RespondSuccess(c, addition.ToPoolView())
}

func SetLinodeActiveToken(c *gin.Context) {
	var payload struct {
		TokenID string `json:"token_id" binding:"required"`
	}
	if err := c.ShouldBindJSON(&payload); err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid active token payload: "+err.Error())
		return
	}

	_, addition, err := loadLinodeAddition(false)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}

	if !addition.SetActiveToken(payload.TokenID) {
		api.RespondError(c, http.StatusNotFound, "Linode token not found")
		return
	}

	if err := saveLinodeAddition(addition); err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to update active token: "+err.Error())
		return
	}

	logCloudAudit(c, "set active linode token: "+payload.TokenID)
	api.RespondSuccess(c, addition.ToPoolView())
}

func CheckLinodeTokens(c *gin.Context) {
	var payload struct {
		TokenIDs []string `json:"token_ids"`
	}
	if c.Request.ContentLength > 0 {
		if err := c.ShouldBindJSON(&payload); err != nil {
			api.RespondError(c, http.StatusBadRequest, "Invalid token check payload: "+err.Error())
			return
		}
	}

	_, addition, err := loadLinodeAddition(false)
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
			client, err := linodecloud.NewClientFromToken(record.Token)
			var profile *linodecloud.Profile
			var account *linodecloud.Account
			if err == nil {
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				profile, err = client.GetProfile(ctx)
				if err == nil {
					account, err = client.GetAccount(ctx)
				}
			}

			mu.Lock()
			addition.Tokens[tokenIndex].SetCheckResult(checkedAt, profile, account, err)
			mu.Unlock()
		}(index)
	}

	wg.Wait()

	if err := saveLinodeAddition(addition); err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to save token health: "+err.Error())
		return
	}

	logCloudAudit(c, fmt.Sprintf("check linode tokens: %d", len(addition.Tokens)))
	api.RespondSuccess(c, addition.ToPoolView())
}

func DeleteLinodeToken(c *gin.Context) {
	tokenID := strings.TrimSpace(c.Param("id"))
	if tokenID == "" {
		api.RespondError(c, http.StatusBadRequest, "Invalid token id")
		return
	}

	_, addition, err := loadLinodeAddition(false)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}

	if !addition.RemoveToken(tokenID) {
		api.RespondError(c, http.StatusNotFound, "Linode token not found")
		return
	}

	if err := saveLinodeAddition(addition); err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to delete token: "+err.Error())
		return
	}

	logCloudAudit(c, "delete linode token: "+tokenID)
	api.RespondSuccess(c, addition.ToPoolView())
}

func GetLinodeTokenSecret(c *gin.Context) {
	tokenID := strings.TrimSpace(c.Param("id"))
	if tokenID == "" {
		api.RespondError(c, http.StatusBadRequest, "Invalid token id")
		return
	}

	_, addition, err := loadLinodeAddition(false)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}

	token := addition.FindToken(tokenID)
	if token == nil {
		api.RespondError(c, http.StatusNotFound, "Linode token not found")
		return
	}

	api.RespondSuccess(c, token.TokenSecretView())
}

func GetLinodeInstancePassword(c *gin.Context) {
	instanceID, err := strconv.Atoi(strings.TrimSpace(c.Param("id")))
	if err != nil || instanceID <= 0 {
		api.RespondError(c, http.StatusBadRequest, "Invalid instance id")
		return
	}

	_, addition, err := loadLinodeAddition(false)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}

	activeToken := addition.ActiveToken()
	if activeToken == nil {
		api.RespondError(c, http.StatusBadRequest, "Linode token is not configured")
		return
	}

	passwordView, err := activeToken.RevealInstancePassword(instanceID)
	if err != nil {
		switch {
		case errors.Is(err, linodecloud.ErrSavedRootPasswordNotFound):
			api.RespondError(c, http.StatusNotFound, err.Error())
		case errors.Is(err, linodecloud.ErrRootPasswordVaultDisabled), errors.Is(err, linodecloud.ErrRootPasswordDecryptFailed):
			api.RespondError(c, http.StatusBadRequest, err.Error())
		default:
			api.RespondError(c, http.StatusInternalServerError, "Failed to load saved root password: "+err.Error())
		}
		return
	}

	api.RespondSuccess(c, passwordView)
}

func GetLinodeAccount(c *gin.Context) {
	client, ctx, cancel, err := getLinodeClient(c)
	if err != nil {
		respondLinodeError(c, err)
		return
	}
	defer cancel()

	profile, err := client.GetProfile(ctx)
	if err != nil {
		respondLinodeError(c, err)
		return
	}
	account, err := client.GetAccount(ctx)
	if err != nil {
		respondLinodeError(c, err)
		return
	}

	api.RespondSuccess(c, linodeAccountView{
		Username:   profile.Username,
		Email:      firstNonEmpty(profile.Email, account.Email),
		Company:    account.Company,
		Balance:    account.Balance,
		Restricted: profile.Restricted,
	})
}

func GetLinodeCatalog(c *gin.Context) {
	client, ctx, cancel, err := getLinodeClient(c)
	if err != nil {
		respondLinodeError(c, err)
		return
	}
	defer cancel()

	regions, err := client.ListRegions(ctx)
	if err != nil {
		respondLinodeError(c, err)
		return
	}
	types, err := client.ListTypes(ctx)
	if err != nil {
		respondLinodeError(c, err)
		return
	}
	images, err := client.ListImages(ctx)
	if err != nil {
		respondLinodeError(c, err)
		return
	}
	sshKeys, err := client.ListSSHKeys(ctx)
	if err != nil {
		respondLinodeError(c, err)
		return
	}

	sort.Slice(regions, func(i, j int) bool {
		return regions[i].ID < regions[j].ID
	})
	sort.Slice(types, func(i, j int) bool {
		return types[i].Price.Monthly < types[j].Price.Monthly
	})
	sort.Slice(images, func(i, j int) bool {
		left := strings.ToLower(strings.TrimSpace(images[i].Vendor + " " + images[i].Label))
		right := strings.ToLower(strings.TrimSpace(images[j].Vendor + " " + images[j].Label))
		return left < right
	})
	sort.Slice(sshKeys, func(i, j int) bool {
		return sshKeys[i].Label < sshKeys[j].Label
	})

	if regions == nil {
		regions = make([]linodecloud.Region, 0)
	}
	if types == nil {
		types = make([]linodecloud.Type, 0)
	}
	if images == nil {
		images = make([]linodecloud.Image, 0)
	}
	if sshKeys == nil {
		sshKeys = make([]linodecloud.SSHKey, 0)
	}

	api.RespondSuccess(c, gin.H{
		"regions":  regions,
		"types":    types,
		"images":   images,
		"ssh_keys": sshKeys,
	})
}

func ListLinodeInstances(c *gin.Context) {
	client, addition, activeToken, ctx, cancel, err := getLinodeActiveTokenClient(c)
	if err != nil {
		respondLinodeError(c, err)
		return
	}
	defer cancel()

	instances, err := client.ListInstances(ctx)
	if err != nil {
		respondLinodeError(c, err)
		return
	}

	sort.Slice(instances, func(i, j int) bool {
		return instances[i].Created > instances[j].Created
	})
	if instances == nil {
		instances = make([]linodecloud.Instance, 0)
	}

	views := make([]linodeInstanceView, 0, len(instances))
	validInstanceIDs := make(map[int]struct{}, len(instances))
	credentialsChanged := false

	for index := range instances {
		validInstanceIDs[instances[index].ID] = struct{}{}
		if activeToken != nil && activeToken.SyncInstanceCredentialLabel(instances[index].ID, instances[index].Label) {
			credentialsChanged = true
		}
		view := buildLinodeInstanceView(&instances[index], activeToken)
		if view != nil {
			views = append(views, *view)
		}
	}

	if activeToken != nil && activeToken.PruneInstanceCredentials(validInstanceIDs) {
		credentialsChanged = true
	}
	if credentialsChanged {
		_ = saveLinodeAddition(addition)
	}

	api.RespondSuccess(c, views)
}

func GetLinodeInstanceDetail(c *gin.Context) {
	client, _, activeToken, ctx, cancel, err := getLinodeActiveTokenClient(c)
	if err != nil {
		respondLinodeError(c, err)
		return
	}
	defer cancel()

	instanceID, err := strconv.Atoi(strings.TrimSpace(c.Param("id")))
	if err != nil || instanceID <= 0 {
		api.RespondError(c, http.StatusBadRequest, "Invalid instance id")
		return
	}

	instance, err := client.GetInstance(ctx, instanceID)
	if err != nil {
		respondLinodeError(c, err)
		return
	}
	disks, err := client.ListInstanceDisks(ctx, instanceID)
	if err != nil {
		respondLinodeError(c, err)
		return
	}
	configs, err := client.ListInstanceConfigs(ctx, instanceID)
	if err != nil {
		respondLinodeError(c, err)
		return
	}
	backups, err := client.GetInstanceBackups(ctx, instanceID)
	if err != nil {
		respondLinodeError(c, err)
		return
	}

	if disks == nil {
		disks = make([]linodecloud.Disk, 0)
	}
	if configs == nil {
		configs = make([]linodecloud.Config, 0)
	}

	api.RespondSuccess(c, linodeInstanceDetailView{
		Instance: buildLinodeInstanceView(instance, activeToken),
		Disks:    disks,
		Configs:  configs,
		Backups:  backups,
	})
}

func CreateLinodeInstance(c *gin.Context) {
	client, addition, activeToken, ctx, cancel, err := getLinodeActiveTokenClient(c)
	if err != nil {
		respondLinodeError(c, err)
		return
	}
	defer cancel()

	var payload createLinodeInstancePayload
	if err := c.ShouldBindJSON(&payload); err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid instance request: "+err.Error())
		return
	}

	payload.Label = strings.TrimSpace(payload.Label)
	payload.Region = strings.TrimSpace(payload.Region)
	payload.Type = strings.TrimSpace(payload.Type)
	payload.Image = strings.TrimSpace(payload.Image)
	payload.AuthorizedKeys = trimStringSlice(payload.AuthorizedKeys)
	payload.Tags = trimStringSlice(payload.Tags)
	payload.UserData = strings.TrimSpace(payload.UserData)
	payload.RootPassword = strings.TrimSpace(payload.RootPassword)
	payload.AutoConnectGroup = strings.TrimSpace(payload.AutoConnectGroup)

	passwordMode := normalizeLinodeRootPasswordMode(payload.RootPasswordMode)
	if passwordMode == "" {
		api.RespondError(c, http.StatusBadRequest, "Unsupported root password mode: "+payload.RootPasswordMode)
		return
	}

	rootPassword := payload.RootPassword
	generatedPassword := ""
	if passwordMode == "random" {
		rootPassword, err = linodecloud.GenerateRandomPassword(20)
		if err != nil {
			api.RespondError(c, http.StatusInternalServerError, "Failed to generate root password: "+err.Error())
			return
		}
		generatedPassword = rootPassword
	}
	if rootPassword == "" {
		api.RespondError(c, http.StatusBadRequest, "Root password cannot be empty")
		return
	}

	resolvedUserData := payload.UserData
	autoConnectGroup := ""
	if payload.AutoConnect {
		resolvedUserData, autoConnectGroup, err = prepareCloudAutoConnectUserData(c, payload.UserData, autoConnectUserDataOptions{
			Enabled:           true,
			Group:             payload.AutoConnectGroup,
			Provider:          linodeProviderName,
			CredentialName:    activeToken.Name,
			WrapInShellScript: true,
		})
		if err != nil {
			api.RespondError(c, http.StatusBadRequest, err.Error())
			return
		}
	}

	request := linodecloud.CreateInstanceRequest{
		Label:          payload.Label,
		Region:         payload.Region,
		Type:           payload.Type,
		Image:          payload.Image,
		RootPass:       rootPassword,
		AuthorizedKeys: payload.AuthorizedKeys,
		BackupsEnabled: payload.BackupsEnabled,
		Booted:         payload.Booted,
		Tags:           payload.Tags,
	}
	if resolvedUserData != "" {
		request.Metadata = &struct {
			UserData string `json:"user_data,omitempty"`
		}{
			UserData: linodecloud.EncodeUserData(resolvedUserData),
		}
	}

	instance, err := client.CreateInstance(ctx, request)
	if err != nil {
		respondLinodeError(c, err)
		return
	}

	passwordSaved := false
	passwordSaveError := ""
	if instance != nil && rootPassword != "" && activeToken != nil {
		if saveErr := activeToken.SaveInstancePassword(instance.ID, instance.Label, passwordMode, rootPassword, time.Now()); saveErr != nil {
			passwordSaveError = saveErr.Error()
		} else if saveErr := saveLinodeAddition(addition); saveErr != nil {
			activeToken.RemoveSavedInstancePassword(instance.ID)
			passwordSaveError = "Failed to save root password: " + saveErr.Error()
		} else {
			passwordSaved = true
		}
	}

	logMessage := fmt.Sprintf("create linode instance: %s (%s/%s/%s, password_mode=%s", request.Label, request.Region, request.Type, request.Image, passwordMode)
	if autoConnectGroup != "" {
		logMessage += ", auto_connect_group=" + autoConnectGroup
	}
	logMessage += ")"
	logCloudAudit(c, logMessage)
	api.RespondSuccess(c, createLinodeInstanceResponse{
		Instance:          buildLinodeInstanceView(instance, activeToken),
		GeneratedPassword: generatedPassword,
		PasswordSaved:     passwordSaved,
		PasswordSaveError: passwordSaveError,
	})
}

func DeleteLinodeInstance(c *gin.Context) {
	client, addition, activeToken, ctx, cancel, err := getLinodeActiveTokenClient(c)
	if err != nil {
		respondLinodeError(c, err)
		return
	}
	defer cancel()

	instanceID, err := strconv.Atoi(strings.TrimSpace(c.Param("id")))
	if err != nil || instanceID <= 0 {
		api.RespondError(c, http.StatusBadRequest, "Invalid instance id")
		return
	}

	if err := client.DeleteInstance(ctx, instanceID); err != nil {
		respondLinodeError(c, err)
		return
	}

	if activeToken != nil && activeToken.RemoveSavedInstancePassword(instanceID) {
		_ = saveLinodeAddition(addition)
	}

	logCloudAudit(c, fmt.Sprintf("delete linode instance: %d", instanceID))
	api.RespondSuccess(c, nil)
}

func PostLinodeInstanceAction(c *gin.Context) {
	client, addition, activeToken, ctx, cancel, err := getLinodeActiveTokenClient(c)
	if err != nil {
		respondLinodeError(c, err)
		return
	}
	defer cancel()

	instanceID, err := strconv.Atoi(strings.TrimSpace(c.Param("id")))
	if err != nil || instanceID <= 0 {
		api.RespondError(c, http.StatusBadRequest, "Invalid instance id")
		return
	}

	var payload struct {
		Type             string   `json:"type" binding:"required"`
		TargetType       string   `json:"target_type,omitempty"`
		Image            string   `json:"image,omitempty"`
		RootPasswordMode string   `json:"root_password_mode,omitempty"`
		RootPassword     string   `json:"root_password,omitempty"`
		AuthorizedKeys   []string `json:"authorized_keys,omitempty"`
		Booted           bool     `json:"booted,omitempty"`
		UserData         string   `json:"user_data,omitempty"`
	}
	if err := c.ShouldBindJSON(&payload); err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid action request: "+err.Error())
		return
	}

	actionType := strings.ToLower(strings.TrimSpace(payload.Type))
	response := gin.H{"type": actionType, "resource_id": instanceID, "status": "submitted"}
	switch actionType {
	case "boot":
		err = client.BootInstance(ctx, instanceID)
	case "shutdown":
		err = client.ShutdownInstance(ctx, instanceID)
	case "reboot":
		err = client.RebootInstance(ctx, instanceID)
	case "resize":
		targetType := strings.TrimSpace(payload.TargetType)
		if targetType == "" {
			api.RespondError(c, http.StatusBadRequest, "target_type is required")
			return
		}
		err = client.ResizeInstance(ctx, instanceID, linodecloud.ResizeInstanceRequest{
			Type: targetType,
		})
		response["target_type"] = targetType
	case "snapshot":
		backup, actionErr := client.CreateInstanceSnapshot(ctx, instanceID)
		err = actionErr
		response["snapshot"] = backup
	case "reset_root_password":
		passwordMode := normalizeLinodeRootPasswordMode(payload.RootPasswordMode)
		if passwordMode == "" {
			api.RespondError(c, http.StatusBadRequest, "Unsupported root password mode: "+payload.RootPasswordMode)
			return
		}
		rootPassword := strings.TrimSpace(payload.RootPassword)
		generatedPassword := ""
		if passwordMode == "random" {
			rootPassword, err = linodecloud.GenerateRandomPassword(20)
			if err != nil {
				api.RespondError(c, http.StatusInternalServerError, "Failed to generate root password: "+err.Error())
				return
			}
			generatedPassword = rootPassword
		}
		if rootPassword == "" {
			api.RespondError(c, http.StatusBadRequest, "Root password cannot be empty")
			return
		}
		err = client.ResetInstanceRootPassword(ctx, instanceID, rootPassword)
		if err == nil && activeToken != nil {
			label := ""
			if instance, lookupErr := client.GetInstance(ctx, instanceID); lookupErr == nil && instance != nil {
				label = instance.Label
			}
			if saveErr := activeToken.SaveInstancePassword(instanceID, label, passwordMode, rootPassword, time.Now()); saveErr != nil {
				response["password_saved"] = false
				response["password_save_error"] = saveErr.Error()
			} else if saveErr := saveLinodeAddition(addition); saveErr != nil {
				activeToken.RemoveSavedInstancePassword(instanceID)
				response["password_saved"] = false
				response["password_save_error"] = "Failed to save root password: " + saveErr.Error()
			} else {
				response["password_saved"] = true
			}
		}
		response["generated_password"] = generatedPassword
	case "rebuild":
		passwordMode := normalizeLinodeRootPasswordMode(payload.RootPasswordMode)
		if passwordMode == "" {
			api.RespondError(c, http.StatusBadRequest, "Unsupported root password mode: "+payload.RootPasswordMode)
			return
		}
		rootPassword := strings.TrimSpace(payload.RootPassword)
		generatedPassword := ""
		if passwordMode == "random" {
			rootPassword, err = linodecloud.GenerateRandomPassword(20)
			if err != nil {
				api.RespondError(c, http.StatusInternalServerError, "Failed to generate root password: "+err.Error())
				return
			}
			generatedPassword = rootPassword
		}
		if rootPassword == "" {
			api.RespondError(c, http.StatusBadRequest, "Root password cannot be empty")
			return
		}

		request := linodecloud.RebuildInstanceRequest{
			Image:          strings.TrimSpace(payload.Image),
			RootPass:       rootPassword,
			AuthorizedKeys: trimStringSlice(payload.AuthorizedKeys),
			Booted:         payload.Booted,
		}
		if request.Image == "" {
			api.RespondError(c, http.StatusBadRequest, "image is required")
			return
		}
		if userData := strings.TrimSpace(payload.UserData); userData != "" {
			request.Metadata = &struct {
				UserData string `json:"user_data,omitempty"`
			}{
				UserData: linodecloud.EncodeUserData(userData),
			}
		}

		rebuilt, actionErr := client.RebuildInstance(ctx, instanceID, request)
		err = actionErr
		if err == nil && activeToken != nil {
			label := ""
			if rebuilt != nil {
				label = rebuilt.Label
			}
			if saveErr := activeToken.SaveInstancePassword(instanceID, label, passwordMode, rootPassword, time.Now()); saveErr != nil {
				response["password_saved"] = false
				response["password_save_error"] = saveErr.Error()
			} else if saveErr := saveLinodeAddition(addition); saveErr != nil {
				activeToken.RemoveSavedInstancePassword(instanceID)
				response["password_saved"] = false
				response["password_save_error"] = "Failed to save root password: " + saveErr.Error()
			} else {
				response["password_saved"] = true
			}
		}
		response["generated_password"] = generatedPassword
		response["instance"] = buildLinodeInstanceView(rebuilt, activeToken)
	default:
		api.RespondError(c, http.StatusBadRequest, "Unsupported instance action: "+payload.Type)
		return
	}
	if err != nil {
		respondLinodeError(c, err)
		return
	}

	logCloudAudit(c, fmt.Sprintf("linode instance action: %s (%d)", actionType, instanceID))
	api.RespondSuccess(c, response)
}

func getLinodeClient(c *gin.Context) (*linodecloud.Client, context.Context, context.CancelFunc, error) {
	client, _, _, ctx, cancel, err := getLinodeActiveTokenClient(c)
	return client, ctx, cancel, err
}

func getLinodeActiveTokenClient(c *gin.Context) (*linodecloud.Client, *linodecloud.Addition, *linodecloud.TokenRecord, context.Context, context.CancelFunc, error) {
	_, addition, err := loadLinodeAddition(false)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}

	activeToken := addition.ActiveToken()
	if activeToken == nil {
		return nil, nil, nil, nil, nil, fmt.Errorf("Linode token is not configured")
	}

	client, err := linodecloud.NewClientFromToken(activeToken.Token)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
	return client, addition, activeToken, ctx, cancel, nil
}

func respondLinodeError(c *gin.Context, err error) {
	var apiErr *linodecloud.APIError
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

func buildLinodeInstanceView(instance *linodecloud.Instance, token *linodecloud.TokenRecord) *linodeInstanceView {
	if instance == nil {
		return nil
	}

	view := &linodeInstanceView{
		Instance: *instance,
	}
	if token != nil && token.HasSavedInstancePassword(instance.ID) {
		view.SavedRootPassword = true
		view.SavedRootPasswordUpdatedAt = token.SavedInstancePasswordUpdatedAt(instance.ID)
	}
	return view
}

func loadLinodeAddition(allowMissing bool) (*models.CloudProvider, *linodecloud.Addition, error) {
	config, err := database.GetCloudProviderConfigByName(linodeProviderName)
	if err != nil {
		if allowMissing {
			addition := &linodecloud.Addition{}
			addition.Normalize()
			return nil, addition, nil
		}
		return nil, nil, fmt.Errorf("Linode provider is not configured")
	}

	addition := &linodecloud.Addition{}
	raw := strings.TrimSpace(config.Addition)
	if raw == "" {
		raw = "{}"
	}
	if err := json.Unmarshal([]byte(raw), addition); err != nil {
		return nil, nil, fmt.Errorf("Linode configuration is invalid: %w", err)
	}

	addition.Normalize()
	return config, addition, nil
}

func saveLinodeAddition(addition *linodecloud.Addition) error {
	if addition == nil {
		addition = &linodecloud.Addition{}
	}
	addition.Normalize()

	payload, err := json.Marshal(addition)
	if err != nil {
		return err
	}

	return database.SaveCloudProviderConfig(&models.CloudProvider{
		Name:     linodeProviderName,
		Addition: string(payload),
	})
}

func normalizeLinodeRootPasswordMode(mode string) string {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "", "custom":
		return "custom"
	case "random":
		return "random"
	default:
		return ""
	}
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
