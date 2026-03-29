package admin

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/komari-monitor/komari/api"
	clientddnsdb "github.com/komari-monitor/komari/database/clientddns"
	clientdb "github.com/komari-monitor/komari/database/clients"
	"github.com/komari-monitor/komari/database/models"
	clientddnssvc "github.com/komari-monitor/komari/utils/clientddns"
	failoversvc "github.com/komari-monitor/komari/utils/failover"
	"gorm.io/gorm"
)

const (
	clientDDNSSyncStatusDisabled = "disabled"
	clientDDNSSyncStatusPending  = "pending"
	clientDDNSSyncStatusSynced   = "synced"
	clientDDNSSyncStatusError    = "error"
)

type clientDDNSBindingRequest struct {
	Enabled     *bool           `json:"enabled"`
	Provider    string          `json:"provider"`
	EntryID     string          `json:"entry_id"`
	AddressMode string          `json:"address_mode"`
	Payload     json.RawMessage `json:"payload"`
}

type clientDDNSBindingView struct {
	ID           uint              `json:"id"`
	ClientUUID   string            `json:"client_uuid"`
	Enabled      bool              `json:"enabled"`
	Provider     string            `json:"provider"`
	EntryID      string            `json:"entry_id"`
	AddressMode  string            `json:"address_mode"`
	Payload      json.RawMessage   `json:"payload"`
	RecordKey    string            `json:"record_key"`
	SyncStatus   string            `json:"sync_status"`
	LastIPv4     string            `json:"last_ipv4"`
	LastIPv6     string            `json:"last_ipv6"`
	LastSyncedAt *models.LocalTime `json:"last_synced_at"`
	LastError    string            `json:"last_error"`
	LastResult   json.RawMessage   `json:"last_result"`
	CreatedAt    models.LocalTime  `json:"created_at"`
	UpdatedAt    models.LocalTime  `json:"updated_at"`
}

func resolveClientOwnerScope(c *gin.Context) (ownerScope, models.Client, bool) {
	clientUUID := strings.TrimSpace(c.Param("uuid"))
	if clientUUID == "" {
		api.RespondError(c, http.StatusBadRequest, "Client UUID is required")
		return ownerScope{}, models.Client{}, false
	}

	requestUserUUID, ok := currentUserUUID(c)
	if !ok {
		api.RespondError(c, http.StatusForbidden, "User context is required")
		return ownerScope{}, models.Client{}, false
	}

	platformAdmin, _ := isPlatformAdmin(c)
	if platformAdmin {
		client, err := clientdb.GetClientByUUID(clientUUID)
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				api.RespondError(c, http.StatusNotFound, "Client not found")
				return ownerScope{}, models.Client{}, false
			}
			api.RespondError(c, http.StatusInternalServerError, "Failed to load client: "+err.Error())
			return ownerScope{}, models.Client{}, false
		}
		return ownerScope{UserUUID: firstNonEmpty(strings.TrimSpace(client.UserID), requestUserUUID)}, client, true
	}

	client, err := clientdb.GetClientByUUIDForUser(clientUUID, requestUserUUID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			api.RespondError(c, http.StatusNotFound, "Client not found")
			return ownerScope{}, models.Client{}, false
		}
		api.RespondError(c, http.StatusInternalServerError, "Failed to load client: "+err.Error())
		return ownerScope{}, models.Client{}, false
	}
	return ownerScope{UserUUID: requestUserUUID}, client, true
}

func buildClientDDNSBindingView(binding models.ClientDDNSBinding) clientDDNSBindingView {
	status := clientDDNSSyncStatusPending
	switch {
	case !binding.Enabled:
		status = clientDDNSSyncStatusDisabled
	case strings.TrimSpace(binding.LastError) != "":
		status = clientDDNSSyncStatusError
	case binding.LastSyncedAt != nil:
		status = clientDDNSSyncStatusSynced
	}

	return clientDDNSBindingView{
		ID:           binding.ID,
		ClientUUID:   binding.ClientUUID,
		Enabled:      binding.Enabled,
		Provider:     binding.Provider,
		EntryID:      binding.EntryID,
		AddressMode:  binding.AddressMode,
		Payload:      rawJSONOrNull(binding.Payload),
		RecordKey:    binding.RecordKey,
		SyncStatus:   status,
		LastIPv4:     binding.LastIPv4,
		LastIPv6:     binding.LastIPv6,
		LastSyncedAt: binding.LastSyncedAt,
		LastError:    binding.LastError,
		LastResult:   rawJSONOrNull(binding.LastResult),
		CreatedAt:    binding.CreatedAt,
		UpdatedAt:    binding.UpdatedAt,
	}
}

func GetClientDDNSBinding(c *gin.Context) {
	scope, client, ok := resolveClientOwnerScope(c)
	if !ok {
		return
	}

	binding, err := clientddnsdb.GetBindingByClientForUser(scope.UserUUID, client.UUID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			api.RespondSuccess(c, nil)
			return
		}
		api.RespondError(c, http.StatusInternalServerError, "Failed to load DDNS binding: "+err.Error())
		return
	}

	api.RespondSuccess(c, buildClientDDNSBindingView(binding))
}

func SaveClientDDNSBinding(c *gin.Context) {
	scope, client, ok := resolveClientOwnerScope(c)
	if !ok {
		return
	}

	var req clientDDNSBindingRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid request body: "+err.Error())
		return
	}

	provider := strings.ToLower(strings.TrimSpace(req.Provider))
	switch provider {
	case models.FailoverDNSProviderCloudflare, models.FailoverDNSProviderAliyun:
	default:
		api.RespondError(c, http.StatusBadRequest, "unsupported ddns provider")
		return
	}

	entryID := strings.TrimSpace(req.EntryID)
	if entryID == "" {
		api.RespondError(c, http.StatusBadRequest, "entry_id is required")
		return
	}
	if err := validateCloudProviderEntryForScope(scope, provider, entryID); err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}

	addressMode, err := normalizeClientDDNSAddressMode(req.AddressMode)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}

	payload, err := normalizeJSONPayload(req.Payload, "{}")
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, "invalid payload: "+err.Error())
		return
	}
	if err := validateFailoverDNSPayload(scope, provider, entryID, payload); err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}

	recordKey, err := buildClientDDNSRecordKey(scope, provider, entryID, payload)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}

	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}
	if enabled {
		conflict, err := clientddnsdb.FindEnabledBindingByRecordKeyForUser(scope.UserUUID, recordKey, client.UUID)
		if err == nil {
			api.RespondError(c, http.StatusBadRequest, fmt.Sprintf("DDNS record is already bound to node %s", conflict.ClientUUID))
			return
		}
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			api.RespondError(c, http.StatusInternalServerError, "Failed to validate DDNS target: "+err.Error())
			return
		}
	}

	binding, err := clientddnsdb.SaveBindingForUser(scope.UserUUID, client.UUID, &models.ClientDDNSBinding{
		Enabled:     enabled,
		Provider:    provider,
		EntryID:     entryID,
		AddressMode: addressMode,
		Payload:     payload,
		RecordKey:   recordKey,
	})
	if err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to save DDNS binding: "+err.Error())
		return
	}

	api.AuditLogForCurrentUser(c, scope.UserUUID, "save client ddns:"+client.UUID, "info")
	api.RespondSuccess(c, buildClientDDNSBindingView(binding))
}

func DeleteClientDDNSBinding(c *gin.Context) {
	scope, client, ok := resolveClientOwnerScope(c)
	if !ok {
		return
	}

	if err := clientddnsdb.DeleteBindingForUser(scope.UserUUID, client.UUID); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			api.RespondError(c, http.StatusNotFound, "DDNS binding not found")
			return
		}
		api.RespondError(c, http.StatusInternalServerError, "Failed to delete DDNS binding: "+err.Error())
		return
	}

	api.AuditLogForCurrentUser(c, scope.UserUUID, "delete client ddns:"+client.UUID, "warn")
	api.RespondSuccess(c, nil)
}

func SyncClientDDNSBinding(c *gin.Context) {
	scope, client, ok := resolveClientOwnerScope(c)
	if !ok {
		return
	}

	binding, err := clientddnssvc.SyncBindingForUser(scope.UserUUID, client.UUID, true)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, "Failed to sync DDNS binding: "+err.Error())
		return
	}

	api.RespondSuccess(c, buildClientDDNSBindingView(binding))
}

func GetClientDDNSCatalog(c *gin.Context) {
	scope, _, ok := resolveClientOwnerScope(c)
	if !ok {
		return
	}

	provider := strings.ToLower(strings.TrimSpace(c.Query("provider")))
	entryID := strings.TrimSpace(c.Query("entry_id"))
	if provider == "" || entryID == "" {
		api.RespondError(c, http.StatusBadRequest, "provider and entry_id are required")
		return
	}

	catalog, err := failoversvc.LoadDNSCatalog(
		scope.UserUUID,
		provider,
		entryID,
		strings.TrimSpace(c.Query("zone_name")),
		strings.TrimSpace(c.Query("domain_name")),
	)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, "Failed to load DNS catalog: "+err.Error())
		return
	}

	api.RespondSuccess(c, catalog)
}

func normalizeClientDDNSAddressMode(value string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", models.ClientDDNSAddressModeIPv4:
		return models.ClientDDNSAddressModeIPv4, nil
	case models.ClientDDNSAddressModeIPv6:
		return models.ClientDDNSAddressModeIPv6, nil
	case models.ClientDDNSAddressModeDual:
		return models.ClientDDNSAddressModeDual, nil
	default:
		return "", fmt.Errorf("unsupported address_mode: %s", value)
	}
}

func buildClientDDNSRecordKey(scope ownerScope, providerName, entryID, payloadJSON string) (string, error) {
	entry, err := findDNSProviderEntryForScope(scope, providerName, entryID)
	if err != nil {
		return "", err
	}

	switch providerName {
	case models.FailoverDNSProviderCloudflare:
		var payload failoverCloudflareDNSPayload
		if strings.TrimSpace(payloadJSON) != "" {
			if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
				return "", fmt.Errorf("cloudflare dns payload is invalid: %w", err)
			}
		}

		zoneName := firstNonEmpty(strings.TrimSpace(payload.ZoneName), trimEntryValue(entry.Values, "zone_name"))
		zoneID := firstNonEmpty(strings.TrimSpace(payload.ZoneID), trimEntryValue(entry.Values, "zone_id"))
		recordName := strings.TrimSpace(payload.RecordName)
		if recordName == "" {
			recordName = zoneName
		}
		recordName = normalizeClientDDNSCloudflareRecordName(recordName, zoneName)
		if recordName == "" {
			return "", fmt.Errorf("cloudflare record_name is required")
		}
		zoneRef := firstNonEmpty(zoneName, zoneID)
		if zoneRef == "" {
			return "", fmt.Errorf("cloudflare zone_name is required")
		}
		return fmt.Sprintf("cloudflare:%s:%s", strings.ToLower(zoneRef), strings.ToLower(recordName)), nil
	case models.FailoverDNSProviderAliyun:
		var payload failoverAliyunDNSPayload
		if strings.TrimSpace(payloadJSON) != "" {
			if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
				return "", fmt.Errorf("aliyun dns payload is invalid: %w", err)
			}
		}

		domainName := firstNonEmpty(strings.TrimSpace(payload.DomainName), trimEntryValue(entry.Values, "domain_name"))
		if domainName == "" {
			return "", fmt.Errorf("aliyun domain_name is required")
		}
		rr := strings.TrimSpace(payload.RR)
		if rr == "" {
			rr = "@"
		}
		lines := normalizeClientDDNSAliyunLines(payload.Line, payload.Lines)
		sort.Strings(lines)
		return fmt.Sprintf("aliyun:%s:%s:%s", strings.ToLower(domainName), strings.ToLower(rr), strings.Join(lines, ",")), nil
	default:
		return "", fmt.Errorf("unsupported ddns provider: %s", providerName)
	}
}

func normalizeClientDDNSCloudflareRecordName(recordName, zoneName string) string {
	recordName = strings.TrimSpace(recordName)
	zoneName = strings.TrimSpace(zoneName)
	if recordName == "@" {
		return zoneName
	}
	if recordName != "" && zoneName != "" && !strings.Contains(recordName, ".") {
		return recordName + "." + zoneName
	}
	return recordName
}

func normalizeClientDDNSAliyunLines(primary string, values []string) []string {
	normalized := make([]string, 0, len(values)+1)
	seen := make(map[string]struct{}, len(values)+1)
	appendValue := func(value string) {
		value = strings.TrimSpace(value)
		if value == "" {
			return
		}
		if _, ok := seen[value]; ok {
			return
		}
		seen[value] = struct{}{}
		normalized = append(normalized, value)
	}

	appendValue(primary)
	for _, value := range values {
		appendValue(value)
	}
	if len(normalized) == 0 {
		normalized = append(normalized, "default")
	}
	return normalized
}
