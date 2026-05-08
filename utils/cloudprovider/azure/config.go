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

	missingInstanceCredentialRetention = 24 * time.Hour
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
	ID                      string                     `json:"id"`
	Name                    string                     `json:"name"`
	Group                   string                     `json:"group,omitempty"`
	TenantID                string                     `json:"tenant_id"`
	ClientID                string                     `json:"client_id"`
	ClientSecret            string                     `json:"client_secret"`
	SubscriptionID          string                     `json:"subscription_id"`
	DefaultLocation         string                     `json:"default_location,omitempty"`
	SubscriptionDisplayName string                     `json:"subscription_display_name,omitempty"`
	SubscriptionState       string                     `json:"subscription_state,omitempty"`
	LastStatus              string                     `json:"last_status,omitempty"`
	LastError               string                     `json:"last_error,omitempty"`
	LastCheckedAt           string                     `json:"last_checked_at,omitempty"`
	InstanceCredentials     []InstanceCredentialRecord `json:"instance_credentials,omitempty"`
}

type CredentialImport struct {
	ID                   string `json:"id,omitempty"`
	Name                 string `json:"name"`
	Group                string `json:"group,omitempty"`
	TenantID             string `json:"tenant_id"`
	TenantIDCamel        string `json:"tenantId,omitempty"`
	ClientID             string `json:"client_id"`
	ClientIDCamel        string `json:"clientId,omitempty"`
	ClientSecret         string `json:"client_secret"`
	ClientSecretCamel    string `json:"clientSecret,omitempty"`
	SubscriptionID       string `json:"subscription_id"`
	SubscriptionIDCamel  string `json:"subscriptionId,omitempty"`
	Subscription         string `json:"subscription,omitempty"`
	DefaultLocation      string `json:"default_location,omitempty"`
	DefaultLocationCamel string `json:"defaultLocation,omitempty"`
	Tenant               string `json:"tenant,omitempty"`
	AppID                string `json:"appId,omitempty"`
	AppIDSnake           string `json:"app_id,omitempty"`
	Password             string `json:"password,omitempty"`
	DisplayName          string `json:"displayName,omitempty"`
	DisplayNameSnake     string `json:"display_name,omitempty"`
	LoginUser            string `json:"login_user,omitempty"`
	LoginUserCamel       string `json:"loginUser,omitempty"`
	LoginPasswd          string `json:"login_passwd,omitempty"`
}

type CredentialView struct {
	ID                      string `json:"id"`
	Name                    string `json:"name"`
	Group                   string `json:"group,omitempty"`
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
	ActiveCredentialID     string           `json:"active_credential_id,omitempty"`
	ActiveLocation         string           `json:"active_location"`
	PasswordStorageEnabled bool             `json:"password_storage_enabled"`
	Credentials            []CredentialView `json:"credentials"`
}

type CredentialSecretView struct {
	CredentialID            string `json:"credential_id"`
	CredentialName          string `json:"credential_name"`
	Group                   string `json:"group,omitempty"`
	TenantID                string `json:"tenant_id"`
	ClientID                string `json:"client_id"`
	ClientSecret            string `json:"client_secret"`
	MaskedClientID          string `json:"masked_client_id"`
	SubscriptionID          string `json:"subscription_id"`
	DefaultLocation         string `json:"default_location"`
	SubscriptionDisplayName string `json:"subscription_display_name,omitempty"`
	SubscriptionState       string `json:"subscription_state,omitempty"`
}

type InstanceCredentialRecord struct {
	InstanceID         string `json:"instance_id"`
	InstanceName       string `json:"instance_name,omitempty"`
	Username           string `json:"username,omitempty"`
	PasswordMode       string `json:"password_mode,omitempty"`
	PasswordCiphertext string `json:"password_ciphertext,omitempty"`
	UpdatedAt          string `json:"updated_at,omitempty"`
}

type InstancePasswordView struct {
	InstanceID    string `json:"instance_id"`
	InstanceName  string `json:"instance_name,omitempty"`
	Username      string `json:"username,omitempty"`
	PasswordMode  string `json:"password_mode,omitempty"`
	RootPassword  string `json:"root_password"`
	UpdatedAt     string `json:"updated_at,omitempty"`
	CredentialID  string `json:"credential_id,omitempty"`
	CredentialKey string `json:"credential_key,omitempty"`
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
		a.ClientSecret != "" {
		a.Credentials = []CredentialRecord{
			{
				ID:              uuid.NewString(),
				Name:            "Default Credential",
				Group:           "",
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
		credential.Group = normalizeCredentialGroup(credential.Group)
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
		credential.InstanceCredentials = normalizeInstanceCredentials(credential.InstanceCredentials)

		if credential.TenantID == "" || credential.ClientID == "" || credential.ClientSecret == "" {
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
			if credential.Group != "" {
				merged.Group = credential.Group
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
			merged.InstanceCredentials = mergeInstanceCredentials(merged.InstanceCredentials, credential.InstanceCredentials)
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

func (a *Addition) MergePersistentStateFrom(previous *Addition) {
	if a == nil || previous == nil {
		return
	}

	a.Normalize()
	previous.Normalize()

	for index := range a.Credentials {
		previousCredential := previous.findCredentialForPersistentStateMerge(
			a.Credentials[index].ID,
			a.Credentials[index].TenantID,
			a.Credentials[index].ClientID,
			a.Credentials[index].SubscriptionID,
		)
		if previousCredential == nil {
			continue
		}

		a.Credentials[index].InstanceCredentials = mergeInstanceCredentials(
			previousCredential.InstanceCredentials,
			a.Credentials[index].InstanceCredentials,
		)
	}

	a.Normalize()
}

func (a *Addition) findCredentialForPersistentStateMerge(id, tenantID, clientID, subscriptionID string) *CredentialRecord {
	if a == nil {
		return nil
	}

	id = strings.TrimSpace(id)
	if id != "" {
		if credential := a.FindCredential(id); credential != nil {
			return credential
		}
	}

	tenantID = strings.TrimSpace(tenantID)
	clientID = strings.TrimSpace(clientID)
	subscriptionID = strings.TrimSpace(subscriptionID)
	if tenantID == "" || clientID == "" {
		return nil
	}

	for index := range a.Credentials {
		currentSubscriptionID := strings.TrimSpace(a.Credentials[index].SubscriptionID)
		if strings.TrimSpace(a.Credentials[index].TenantID) == tenantID &&
			strings.TrimSpace(a.Credentials[index].ClientID) == clientID &&
			((subscriptionID == "" && currentSubscriptionID == "") || currentSubscriptionID == subscriptionID) {
			return &a.Credentials[index]
		}
	}

	return nil
}

func (a *Addition) FindCredentialWithSavedInstancePassword(instanceID string) *CredentialRecord {
	if a == nil {
		return nil
	}
	instanceID = strings.TrimSpace(instanceID)
	if instanceID == "" {
		return nil
	}
	a.Normalize()
	for index := range a.Credentials {
		if a.Credentials[index].HasSavedInstancePassword(instanceID) {
			return &a.Credentials[index]
		}
	}
	return nil
}

func (a *Addition) HasSavedInstancePassword(instanceID string) bool {
	return a.FindCredentialWithSavedInstancePassword(instanceID) != nil
}

func (a *Addition) SavedInstancePasswordUpdatedAt(instanceID string) string {
	credential := a.FindCredentialWithSavedInstancePassword(instanceID)
	if credential == nil {
		return ""
	}
	return credential.SavedInstancePasswordUpdatedAt(instanceID)
}

func (a *Addition) RevealInstancePassword(instanceID string) (*InstancePasswordView, error) {
	credential := a.FindCredentialWithSavedInstancePassword(instanceID)
	if credential == nil {
		return nil, ErrSavedRootPasswordNotFound
	}
	view, err := credential.RevealInstancePassword(instanceID)
	if err != nil {
		return nil, err
	}
	view.CredentialID = credential.ID
	view.CredentialKey = strings.TrimSpace(credential.Name)
	return view, nil
}

func (a *Addition) UpsertCredentials(inputs []CredentialImport) int {
	if a == nil {
		return 0
	}
	a.Normalize()

	count := 0
	for _, input := range inputs {
		tenantID := firstNonEmpty(input.TenantID, input.TenantIDCamel, input.Tenant)
		clientID := firstNonEmpty(input.ClientID, input.ClientIDCamel, input.AppID, input.AppIDSnake)
		clientSecret := firstNonEmpty(input.ClientSecret, input.ClientSecretCamel, input.Password, input.LoginPasswd)
		subscriptionID := firstNonEmpty(input.SubscriptionID, input.SubscriptionIDCamel, input.Subscription)
		inputID := strings.TrimSpace(input.ID)
		hasCredentialValue := tenantID != "" && clientID != "" && clientSecret != ""
		if !hasCredentialValue && inputID == "" {
			continue
		}

		name := firstNonEmpty(input.Name, input.DisplayName, input.DisplayNameSnake, input.LoginUser, input.LoginUserCamel)
		group := normalizeCredentialGroup(input.Group)
		defaultLocation := normalizeLocation(firstNonEmpty(input.DefaultLocation, input.DefaultLocationCamel))

		var matched *CredentialRecord
		for index := range a.Credentials {
			if inputID != "" && a.Credentials[index].ID == inputID {
				matched = &a.Credentials[index]
				break
			}
			if hasCredentialValue && credentialValuesMatch(a.Credentials[index], tenantID, clientID, subscriptionID) {
				matched = &a.Credentials[index]
				break
			}
		}

		if matched != nil {
			if name != "" {
				matched.Name = name
			}
			matched.Group = group
			if hasCredentialValue {
				matched.TenantID = tenantID
				matched.ClientID = clientID
				matched.ClientSecret = clientSecret
				if subscriptionID != "" || strings.TrimSpace(matched.SubscriptionID) == "" {
					matched.SubscriptionID = subscriptionID
				}
			}
			if defaultLocation != "" {
				matched.DefaultLocation = defaultLocation
			}
		} else {
			if !hasCredentialValue {
				continue
			}
			if name == "" {
				name = nextGeneratedCredentialName(a.Credentials)
			}
			a.Credentials = append(a.Credentials, CredentialRecord{
				ID:              uuid.NewString(),
				Name:            name,
				Group:           group,
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

func credentialValuesMatch(credential CredentialRecord, tenantID, clientID, subscriptionID string) bool {
	if strings.TrimSpace(credential.TenantID) != strings.TrimSpace(tenantID) ||
		strings.TrimSpace(credential.ClientID) != strings.TrimSpace(clientID) {
		return false
	}

	currentSubscriptionID := strings.TrimSpace(credential.SubscriptionID)
	subscriptionID = strings.TrimSpace(subscriptionID)
	if subscriptionID == "" {
		return currentSubscriptionID == ""
	}
	return currentSubscriptionID == "" || currentSubscriptionID == subscriptionID
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
		ActiveCredentialID:     a.ActiveCredentialID,
		ActiveLocation:         a.ActiveLocation,
		PasswordStorageEnabled: IsRootPasswordVaultEnabled(),
		Credentials:            make([]CredentialView, 0, len(a.Credentials)),
	}
	for _, credential := range a.Credentials {
		view.Credentials = append(view.Credentials, CredentialView{
			ID:                      credential.ID,
			Name:                    credential.Name,
			Group:                   credential.Group,
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
		Group:                   c.Group,
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
		if strings.TrimSpace(c.SubscriptionID) == "" && strings.TrimSpace(subscription.SubscriptionID) != "" {
			c.SubscriptionID = strings.TrimSpace(subscription.SubscriptionID)
		}
		if subscription.DisplayName != "" {
			c.SubscriptionDisplayName = strings.TrimSpace(subscription.DisplayName)
		}
		if subscription.State != "" {
			c.SubscriptionState = strings.TrimSpace(subscription.State)
		}
	}
}

func (c *CredentialRecord) HasSavedInstancePassword(instanceID string) bool {
	if c == nil {
		return false
	}
	return c.findInstanceCredential(instanceID) != nil
}

func (c *CredentialRecord) SavedInstancePasswordUpdatedAt(instanceID string) string {
	credential := c.findInstanceCredential(instanceID)
	if credential == nil {
		return ""
	}
	return credential.UpdatedAt
}

func (c *CredentialRecord) SaveInstancePassword(instanceID, instanceName, username, passwordMode, rootPassword string, updatedAt time.Time) error {
	if c == nil {
		return ErrSavedRootPasswordNotFound
	}

	instanceID = strings.TrimSpace(instanceID)
	rootPassword = strings.TrimSpace(rootPassword)
	if instanceID == "" || rootPassword == "" {
		return ErrSavedRootPasswordNotFound
	}

	ciphertext, err := encryptRootPassword(rootPassword)
	if err != nil {
		return err
	}

	record := InstanceCredentialRecord{
		InstanceID:         instanceID,
		InstanceName:       strings.TrimSpace(instanceName),
		Username:           firstNonEmpty(username, "root"),
		PasswordMode:       strings.TrimSpace(strings.ToLower(passwordMode)),
		PasswordCiphertext: ciphertext,
		UpdatedAt:          updatedAt.UTC().Format(time.RFC3339),
	}

	if existing := c.findInstanceCredential(instanceID); existing != nil {
		*existing = record
	} else {
		c.InstanceCredentials = append(c.InstanceCredentials, record)
	}
	c.InstanceCredentials = normalizeInstanceCredentials(c.InstanceCredentials)
	return nil
}

func (c *CredentialRecord) RevealInstancePassword(instanceID string) (*InstancePasswordView, error) {
	credential := c.findInstanceCredential(instanceID)
	if credential == nil {
		return nil, ErrSavedRootPasswordNotFound
	}

	rootPassword, err := decryptRootPassword(credential.PasswordCiphertext)
	if err != nil {
		return nil, err
	}

	username := firstNonEmpty(credential.Username, "root")
	return &InstancePasswordView{
		InstanceID:   credential.InstanceID,
		InstanceName: credential.InstanceName,
		Username:     username,
		PasswordMode: credential.PasswordMode,
		RootPassword: rootPassword,
		UpdatedAt:    credential.UpdatedAt,
	}, nil
}

func (c *CredentialRecord) RemoveSavedInstancePassword(instanceID string) bool {
	if c == nil || len(c.InstanceCredentials) == 0 {
		return false
	}

	instanceID = strings.TrimSpace(instanceID)
	if instanceID == "" {
		return false
	}

	next := make([]InstanceCredentialRecord, 0, len(c.InstanceCredentials))
	removed := false
	for _, credential := range c.InstanceCredentials {
		if strings.TrimSpace(credential.InstanceID) == instanceID {
			removed = true
			continue
		}
		next = append(next, credential)
	}
	if !removed {
		return false
	}
	c.InstanceCredentials = next
	return true
}

func (c *CredentialRecord) PruneInstanceCredentials(validInstanceIDs map[string]struct{}) bool {
	if c == nil || len(c.InstanceCredentials) == 0 {
		return false
	}

	cutoff := time.Now().UTC().Add(-missingInstanceCredentialRetention)
	next := make([]InstanceCredentialRecord, 0, len(c.InstanceCredentials))
	changed := false
	for _, credential := range c.InstanceCredentials {
		instanceID := strings.TrimSpace(credential.InstanceID)
		if _, exists := validInstanceIDs[instanceID]; exists {
			next = append(next, credential)
			continue
		}
		if shouldRetainMissingInstanceCredential(credential, cutoff) {
			next = append(next, credential)
			continue
		}
		changed = true
	}

	if changed {
		c.InstanceCredentials = next
	}
	return changed
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

func normalizeCredentialGroup(group string) string {
	group = strings.TrimSpace(group)
	if len(group) > 100 {
		group = group[:100]
	}
	return group
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

func (c *CredentialRecord) findInstanceCredential(instanceID string) *InstanceCredentialRecord {
	if c == nil {
		return nil
	}

	instanceID = strings.TrimSpace(instanceID)
	if instanceID == "" {
		return nil
	}

	for index := range c.InstanceCredentials {
		if strings.TrimSpace(c.InstanceCredentials[index].InstanceID) == instanceID {
			return &c.InstanceCredentials[index]
		}
	}
	return nil
}

func mergeInstanceCredentials(left, right []InstanceCredentialRecord) []InstanceCredentialRecord {
	if len(left) == 0 {
		return normalizeInstanceCredentials(right)
	}
	if len(right) == 0 {
		return normalizeInstanceCredentials(left)
	}

	merged := append(append(make([]InstanceCredentialRecord, 0, len(left)+len(right)), left...), right...)
	return normalizeInstanceCredentials(merged)
}

func normalizeInstanceCredentials(records []InstanceCredentialRecord) []InstanceCredentialRecord {
	if len(records) == 0 {
		return nil
	}

	normalized := make([]InstanceCredentialRecord, 0, len(records))
	seen := make(map[string]int, len(records))
	for _, record := range records {
		record.InstanceID = strings.TrimSpace(record.InstanceID)
		record.InstanceName = strings.TrimSpace(record.InstanceName)
		record.Username = strings.TrimSpace(record.Username)
		record.PasswordMode = strings.TrimSpace(strings.ToLower(record.PasswordMode))
		record.PasswordCiphertext = strings.TrimSpace(record.PasswordCiphertext)
		record.UpdatedAt = strings.TrimSpace(record.UpdatedAt)

		if record.InstanceID == "" || record.PasswordCiphertext == "" {
			continue
		}

		if existingIndex, exists := seen[record.InstanceID]; exists {
			normalized[existingIndex] = record
			continue
		}

		seen[record.InstanceID] = len(normalized)
		normalized = append(normalized, record)
	}

	if len(normalized) == 0 {
		return nil
	}
	return normalized
}

func shouldRetainMissingInstanceCredential(record InstanceCredentialRecord, cutoff time.Time) bool {
	updatedAt := strings.TrimSpace(record.UpdatedAt)
	if updatedAt == "" {
		return false
	}

	parsed, err := time.Parse(time.RFC3339, updatedAt)
	if err != nil {
		return false
	}

	return parsed.After(cutoff)
}
