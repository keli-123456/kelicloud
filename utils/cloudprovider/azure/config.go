package azure

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	CredentialStatusUnknown = "unknown"
	CredentialStatusHealthy = "healthy"
	CredentialStatusError   = "error"
	DefaultLocation         = "eastus"
)

type Addition struct {
	TenantID           string             `json:"tenant_id,omitempty"`
	ClientID           string             `json:"client_id,omitempty"`
	ClientSecret       string             `json:"client_secret,omitempty"`
	SubscriptionID     string             `json:"subscription_id,omitempty"`
	DefaultLocation    string             `json:"default_location,omitempty"`
	ActiveCredentialID string             `json:"active_credential_id,omitempty"`
	ActiveLocation     string             `json:"active_location,omitempty"`
	Credentials        []CredentialRecord `json:"credentials,omitempty"`
}

type CredentialRecord struct {
	ID                      string `json:"id"`
	Name                    string `json:"name"`
	TenantID                string `json:"tenant_id"`
	ClientID                string `json:"client_id"`
	ClientSecret            string `json:"client_secret"`
	SubscriptionID          string `json:"subscription_id"`
	DefaultLocation         string `json:"default_location,omitempty"`
	SubscriptionDisplayName string `json:"subscription_display_name,omitempty"`
	SubscriptionState       string `json:"subscription_state,omitempty"`
	LastStatus              string `json:"last_status,omitempty"`
	LastError               string `json:"last_error,omitempty"`
	LastCheckedAt           string `json:"last_checked_at,omitempty"`
}

type CredentialImport struct {
	ID              string `json:"id,omitempty"`
	Name            string `json:"name"`
	TenantID        string `json:"tenant_id"`
	ClientID        string `json:"client_id"`
	ClientSecret    string `json:"client_secret"`
	SubscriptionID  string `json:"subscription_id"`
	DefaultLocation string `json:"default_location,omitempty"`
}

type CredentialView struct {
	ID                      string `json:"id"`
	Name                    string `json:"name"`
	TenantID                string `json:"tenant_id"`
	MaskedClientID          string `json:"masked_client_id"`
	SubscriptionID          string `json:"subscription_id"`
	DefaultLocation         string `json:"default_location"`
	SubscriptionDisplayName string `json:"subscription_display_name,omitempty"`
	SubscriptionState       string `json:"subscription_state,omitempty"`
	LastStatus              string `json:"last_status"`
	LastError               string `json:"last_error,omitempty"`
	LastCheckedAt           string `json:"last_checked_at,omitempty"`
	IsActive                bool   `json:"is_active"`
}

type CredentialPoolView struct {
	ActiveCredentialID string           `json:"active_credential_id,omitempty"`
	ActiveLocation     string           `json:"active_location"`
	Credentials        []CredentialView `json:"credentials"`
}

type CredentialSecretView struct {
	CredentialID            string `json:"credential_id"`
	CredentialName          string `json:"credential_name"`
	TenantID                string `json:"tenant_id"`
	ClientID                string `json:"client_id"`
	ClientSecret            string `json:"client_secret"`
	MaskedClientID          string `json:"masked_client_id"`
	SubscriptionID          string `json:"subscription_id"`
	DefaultLocation         string `json:"default_location"`
	SubscriptionDisplayName string `json:"subscription_display_name,omitempty"`
	SubscriptionState       string `json:"subscription_state,omitempty"`
}

func (a *Addition) Normalize() {
	if a == nil {
		return
	}

	a.TenantID = strings.TrimSpace(a.TenantID)
	a.ClientID = strings.TrimSpace(a.ClientID)
	a.ClientSecret = strings.TrimSpace(a.ClientSecret)
	a.SubscriptionID = strings.TrimSpace(a.SubscriptionID)
	a.DefaultLocation = normalizeLocation(a.DefaultLocation)
	a.ActiveCredentialID = strings.TrimSpace(a.ActiveCredentialID)
	a.ActiveLocation = normalizeLocation(a.ActiveLocation)

	if len(a.Credentials) == 0 &&
		a.TenantID != "" &&
		a.ClientID != "" &&
		a.ClientSecret != "" &&
		a.SubscriptionID != "" {
		a.Credentials = []CredentialRecord{
			{
				ID:              uuid.NewString(),
				Name:            "Default Credential",
				TenantID:        a.TenantID,
				ClientID:        a.ClientID,
				ClientSecret:    a.ClientSecret,
				SubscriptionID:  a.SubscriptionID,
				DefaultLocation: a.DefaultLocation,
				LastStatus:      CredentialStatusUnknown,
			},
		}
	}

	normalized := make([]CredentialRecord, 0, len(a.Credentials))
	seenIDs := make(map[string]struct{}, len(a.Credentials))
	seenKeys := make(map[string]int, len(a.Credentials))

	for _, credential := range a.Credentials {
		credential.ID = strings.TrimSpace(credential.ID)
		credential.Name = strings.TrimSpace(credential.Name)
		credential.TenantID = strings.TrimSpace(credential.TenantID)
		credential.ClientID = strings.TrimSpace(credential.ClientID)
		credential.ClientSecret = strings.TrimSpace(credential.ClientSecret)
		credential.SubscriptionID = strings.TrimSpace(credential.SubscriptionID)
		credential.DefaultLocation = normalizeLocation(credential.DefaultLocation)
		credential.SubscriptionDisplayName = strings.TrimSpace(credential.SubscriptionDisplayName)
		credential.SubscriptionState = strings.TrimSpace(credential.SubscriptionState)
		credential.LastStatus = normalizeCredentialStatus(credential.LastStatus)
		credential.LastError = strings.TrimSpace(credential.LastError)
		credential.LastCheckedAt = strings.TrimSpace(credential.LastCheckedAt)

		if credential.TenantID == "" || credential.ClientID == "" || credential.ClientSecret == "" || credential.SubscriptionID == "" {
			continue
		}
		if credential.ID == "" {
			credential.ID = uuid.NewString()
		}
		if credential.Name == "" {
			credential.Name = "Credential"
		}
		if _, exists := seenIDs[credential.ID]; exists {
			credential.ID = uuid.NewString()
		}
		seenIDs[credential.ID] = struct{}{}

		key := credential.TenantID + "|" + credential.ClientID + "|" + credential.SubscriptionID
		if existingIndex, exists := seenKeys[key]; exists {
			merged := normalized[existingIndex]
			if credential.Name != "" {
				merged.Name = credential.Name
			}
			if credential.ClientSecret != "" {
				merged.ClientSecret = credential.ClientSecret
			}
			if credential.DefaultLocation != "" {
				merged.DefaultLocation = credential.DefaultLocation
			}
			if credential.SubscriptionDisplayName != "" {
				merged.SubscriptionDisplayName = credential.SubscriptionDisplayName
			}
			if credential.SubscriptionState != "" {
				merged.SubscriptionState = credential.SubscriptionState
			}
			if credential.LastCheckedAt != "" {
				merged.LastCheckedAt = credential.LastCheckedAt
				merged.LastStatus = credential.LastStatus
				merged.LastError = credential.LastError
			}
			normalized[existingIndex] = merged
			if a.ActiveCredentialID == credential.ID {
				a.ActiveCredentialID = merged.ID
			}
			continue
		}

		seenKeys[key] = len(normalized)
		normalized = append(normalized, credential)
	}

	a.Credentials = normalized

	if a.ActiveCredentialID == "" && len(a.Credentials) > 0 {
		a.ActiveCredentialID = a.Credentials[0].ID
	}

	active := a.FindCredential(a.ActiveCredentialID)
	if active != nil {
		a.TenantID = active.TenantID
		a.ClientID = active.ClientID
		a.ClientSecret = active.ClientSecret
		a.SubscriptionID = active.SubscriptionID
		a.DefaultLocation = active.DefaultLocation
		if a.ActiveLocation == "" {
			a.ActiveLocation = active.DefaultLocation
		}
	} else if len(a.Credentials) == 0 {
		a.TenantID = ""
		a.ClientID = ""
		a.ClientSecret = ""
		a.SubscriptionID = ""
		a.DefaultLocation = ""
		a.ActiveLocation = ""
	}

	if a.ActiveLocation == "" {
		a.ActiveLocation = normalizeLocation(a.DefaultLocation)
	}
}

func (a *Addition) FindCredential(id string) *CredentialRecord {
	if a == nil {
		return nil
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return nil
	}
	for index := range a.Credentials {
		if a.Credentials[index].ID == id {
			return &a.Credentials[index]
		}
	}
	return nil
}

func (a *Addition) ActiveCredential() *CredentialRecord {
	if a == nil {
		return nil
	}
	a.Normalize()
	return a.FindCredential(a.ActiveCredentialID)
}

func (a *Addition) SetActiveCredential(id string) bool {
	if a == nil {
		return false
	}
	a.Normalize()
	credential := a.FindCredential(id)
	if credential == nil {
		return false
	}
	a.ActiveCredentialID = credential.ID
	a.TenantID = credential.TenantID
	a.ClientID = credential.ClientID
	a.ClientSecret = credential.ClientSecret
	a.SubscriptionID = credential.SubscriptionID
	a.DefaultLocation = credential.DefaultLocation
	a.ActiveLocation = normalizeLocation(credential.DefaultLocation)
	return true
}

func (a *Addition) SetActiveLocation(location string) {
	if a == nil {
		return
	}
	a.ActiveLocation = normalizeLocation(location)
	if a.ActiveLocation == "" {
		a.ActiveLocation = normalizeLocation(a.DefaultLocation)
	}
}

func (a *Addition) UpsertCredentials(inputs []CredentialImport) int {
	if a == nil {
		return 0
	}
	a.Normalize()

	count := 0
	for _, input := range inputs {
		tenantID := strings.TrimSpace(input.TenantID)
		clientID := strings.TrimSpace(input.ClientID)
		clientSecret := strings.TrimSpace(input.ClientSecret)
		subscriptionID := strings.TrimSpace(input.SubscriptionID)
		if tenantID == "" || clientID == "" || clientSecret == "" || subscriptionID == "" {
			continue
		}

		name := strings.TrimSpace(input.Name)
		inputID := strings.TrimSpace(input.ID)
		defaultLocation := normalizeLocation(input.DefaultLocation)

		var matched *CredentialRecord
		for index := range a.Credentials {
			if inputID != "" && a.Credentials[index].ID == inputID {
				matched = &a.Credentials[index]
				break
			}
			if a.Credentials[index].TenantID == tenantID &&
				a.Credentials[index].ClientID == clientID &&
				a.Credentials[index].SubscriptionID == subscriptionID {
				matched = &a.Credentials[index]
				break
			}
		}

		if matched != nil {
			if name != "" {
				matched.Name = name
			}
			matched.TenantID = tenantID
			matched.ClientID = clientID
			matched.ClientSecret = clientSecret
			matched.SubscriptionID = subscriptionID
			if defaultLocation != "" {
				matched.DefaultLocation = defaultLocation
			}
		} else {
			if name == "" {
				name = nextGeneratedCredentialName(a.Credentials)
			}
			a.Credentials = append(a.Credentials, CredentialRecord{
				ID:              uuid.NewString(),
				Name:            name,
				TenantID:        tenantID,
				ClientID:        clientID,
				ClientSecret:    clientSecret,
				SubscriptionID:  subscriptionID,
				DefaultLocation: defaultLocation,
				LastStatus:      CredentialStatusUnknown,
			})
		}

		count++
	}

	a.Normalize()
	return count
}

func (a *Addition) RemoveCredential(id string) bool {
	if a == nil {
		return false
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return false
	}

	next := make([]CredentialRecord, 0, len(a.Credentials))
	removed := false
	for _, credential := range a.Credentials {
		if credential.ID == id {
			removed = true
			continue
		}
		next = append(next, credential)
	}
	if !removed {
		return false
	}

	a.Credentials = next
	if a.ActiveCredentialID == id {
		a.ActiveCredentialID = ""
	}
	a.Normalize()
	return true
}

func (a *Addition) ToPoolView() CredentialPoolView {
	if a == nil {
		return CredentialPoolView{
			Credentials: make([]CredentialView, 0),
		}
	}
	a.Normalize()

	view := CredentialPoolView{
		ActiveCredentialID: a.ActiveCredentialID,
		ActiveLocation:     a.ActiveLocation,
		Credentials:        make([]CredentialView, 0, len(a.Credentials)),
	}
	for _, credential := range a.Credentials {
		view.Credentials = append(view.Credentials, CredentialView{
			ID:                      credential.ID,
			Name:                    credential.Name,
			TenantID:                credential.TenantID,
			MaskedClientID:          maskValue(credential.ClientID),
			SubscriptionID:          credential.SubscriptionID,
			DefaultLocation:         firstNonEmpty(credential.DefaultLocation, DefaultLocation),
			SubscriptionDisplayName: credential.SubscriptionDisplayName,
			SubscriptionState:       credential.SubscriptionState,
			LastStatus:              normalizeCredentialStatus(credential.LastStatus),
			LastError:               credential.LastError,
			LastCheckedAt:           credential.LastCheckedAt,
			IsActive:                credential.ID == a.ActiveCredentialID,
		})
	}
	return view
}

func (c *CredentialRecord) CredentialSecretView() *CredentialSecretView {
	if c == nil {
		return nil
	}
	return &CredentialSecretView{
		CredentialID:            c.ID,
		CredentialName:          c.Name,
		TenantID:                c.TenantID,
		ClientID:                c.ClientID,
		ClientSecret:            c.ClientSecret,
		MaskedClientID:          maskValue(c.ClientID),
		SubscriptionID:          c.SubscriptionID,
		DefaultLocation:         firstNonEmpty(c.DefaultLocation, DefaultLocation),
		SubscriptionDisplayName: c.SubscriptionDisplayName,
		SubscriptionState:       c.SubscriptionState,
	}
}

func (c *CredentialRecord) SetCheckResult(checkedAt time.Time, subscription *Subscription, err error) {
	if c == nil {
		return
	}

	c.LastCheckedAt = checkedAt.UTC().Format(time.RFC3339)
	if err != nil {
		c.LastStatus = CredentialStatusError
		c.LastError = err.Error()
		c.SubscriptionDisplayName = ""
		c.SubscriptionState = ""
		return
	}

	c.LastStatus = CredentialStatusHealthy
	c.LastError = ""
	if subscription != nil {
		if subscription.DisplayName != "" {
			c.SubscriptionDisplayName = strings.TrimSpace(subscription.DisplayName)
		}
		if subscription.State != "" {
			c.SubscriptionState = strings.TrimSpace(subscription.State)
		}
	}
}

func nextGeneratedCredentialName(credentials []CredentialRecord) string {
	used := make(map[string]struct{}, len(credentials))
	for _, credential := range credentials {
		name := strings.TrimSpace(credential.Name)
		if name != "" {
			used[name] = struct{}{}
		}
	}

	for index := 1; ; index++ {
		candidate := fmt.Sprintf("Credential %d", index)
		if _, exists := used[candidate]; !exists {
			return candidate
		}
	}
}

func normalizeCredentialStatus(status string) string {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case CredentialStatusHealthy:
		return CredentialStatusHealthy
	case CredentialStatusError:
		return CredentialStatusError
	default:
		return CredentialStatusUnknown
	}
}

func normalizeLocation(location string) string {
	location = strings.TrimSpace(location)
	if location == "" {
		return ""
	}
	replacer := strings.NewReplacer(" ", "", "_", "", "-", "")
	return strings.ToLower(replacer.Replace(location))
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

func maskValue(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if len(value) <= 8 {
		return strings.Repeat("*", len(value))
	}
	return value[:4] + strings.Repeat("*", len(value)-8) + value[len(value)-4:]
}
