package aws

import (
	"fmt"
	"strings"
	"time"
)

type ResourceCredentialRecord struct {
	ResourceType       string `json:"resource_type,omitempty"`
	ResourceID         string `json:"resource_id,omitempty"`
	ResourceName       string `json:"resource_name,omitempty"`
	Username           string `json:"username,omitempty"`
	PasswordMode       string `json:"password_mode,omitempty"`
	PasswordCiphertext string `json:"password_ciphertext,omitempty"`
	UpdatedAt          string `json:"updated_at,omitempty"`
}

type ResourcePasswordView struct {
	ResourceType string `json:"resource_type,omitempty"`
	ResourceID   string `json:"resource_id,omitempty"`
	ResourceName string `json:"resource_name,omitempty"`
	Username     string `json:"username,omitempty"`
	PasswordMode string `json:"password_mode,omitempty"`
	RootPassword string `json:"root_password"`
	UpdatedAt    string `json:"updated_at,omitempty"`
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
			a.Credentials[index].AccessKeyID,
			a.Credentials[index].DefaultRegion,
		)
		if previousCredential == nil {
			continue
		}

		a.Credentials[index].ResourceCredentials = mergeResourceCredentials(
			previousCredential.ResourceCredentials,
			a.Credentials[index].ResourceCredentials,
		)
	}

	a.Normalize()
}

func (a *Addition) findCredentialForPersistentStateMerge(id, accessKeyID, defaultRegion string) *CredentialRecord {
	if a == nil {
		return nil
	}

	id = strings.TrimSpace(id)
	if id != "" {
		if credential := a.FindCredential(id); credential != nil {
			return credential
		}
	}

	accessKeyID = strings.TrimSpace(accessKeyID)
	defaultRegion = normalizeRegion(defaultRegion)
	if accessKeyID == "" {
		return nil
	}

	for index := range a.Credentials {
		if strings.TrimSpace(a.Credentials[index].AccessKeyID) == accessKeyID &&
			normalizeRegion(a.Credentials[index].DefaultRegion) == defaultRegion {
			return &a.Credentials[index]
		}
	}

	return nil
}

func (a *Addition) FindCredentialWithSavedResourcePassword(resourceType, resourceID string) *CredentialRecord {
	if a == nil {
		return nil
	}
	a.Normalize()
	for index := range a.Credentials {
		if a.Credentials[index].HasSavedResourcePassword(resourceType, resourceID) {
			return &a.Credentials[index]
		}
	}
	return nil
}

func (a *Addition) HasSavedResourcePassword(resourceType, resourceID string) bool {
	return a.FindCredentialWithSavedResourcePassword(resourceType, resourceID) != nil
}

func (a *Addition) SavedResourcePasswordUpdatedAt(resourceType, resourceID string) string {
	credential := a.FindCredentialWithSavedResourcePassword(resourceType, resourceID)
	if credential == nil {
		return ""
	}
	return credential.SavedResourcePasswordUpdatedAt(resourceType, resourceID)
}

func (a *Addition) RevealResourcePassword(resourceType, resourceID string) (*ResourcePasswordView, error) {
	credential := a.FindCredentialWithSavedResourcePassword(resourceType, resourceID)
	if credential == nil {
		return nil, ErrSavedRootPasswordNotFound
	}
	return credential.RevealResourcePassword(resourceType, resourceID)
}

func (c *CredentialRecord) HasSavedResourcePassword(resourceType, resourceID string) bool {
	if c == nil {
		return false
	}
	return c.findResourceCredential(resourceType, resourceID) != nil
}

func (c *CredentialRecord) SavedResourcePasswordUpdatedAt(resourceType, resourceID string) string {
	credential := c.findResourceCredential(resourceType, resourceID)
	if credential == nil {
		return ""
	}
	return credential.UpdatedAt
}

func (c *CredentialRecord) SaveResourcePassword(resourceType, resourceID, resourceName, passwordMode, rootPassword string, updatedAt time.Time) error {
	if c == nil {
		return ErrSavedRootPasswordNotFound
	}

	resourceType = normalizeResourceType(resourceType)
	resourceID = strings.TrimSpace(resourceID)
	rootPassword = strings.TrimSpace(rootPassword)
	if resourceType == "" || resourceID == "" || rootPassword == "" {
		return ErrSavedRootPasswordNotFound
	}

	ciphertext, err := encryptRootPassword(rootPassword)
	if err != nil {
		return err
	}

	record := ResourceCredentialRecord{
		ResourceType:       resourceType,
		ResourceID:         resourceID,
		ResourceName:       strings.TrimSpace(resourceName),
		Username:           "root",
		PasswordMode:       strings.TrimSpace(strings.ToLower(passwordMode)),
		PasswordCiphertext: ciphertext,
		UpdatedAt:          updatedAt.UTC().Format(time.RFC3339),
	}
	if existing := c.findResourceCredential(resourceType, resourceID); existing != nil {
		*existing = record
	} else {
		c.ResourceCredentials = append(c.ResourceCredentials, record)
	}
	c.ResourceCredentials = normalizeResourceCredentials(c.ResourceCredentials)
	return nil
}

func (c *CredentialRecord) RevealResourcePassword(resourceType, resourceID string) (*ResourcePasswordView, error) {
	credential := c.findResourceCredential(resourceType, resourceID)
	if credential == nil {
		return nil, ErrSavedRootPasswordNotFound
	}

	rootPassword, err := decryptRootPassword(credential.PasswordCiphertext)
	if err != nil {
		return nil, err
	}

	username := credential.Username
	if username == "" {
		username = "root"
	}

	return &ResourcePasswordView{
		ResourceType: credential.ResourceType,
		ResourceID:   credential.ResourceID,
		ResourceName: credential.ResourceName,
		Username:     username,
		PasswordMode: credential.PasswordMode,
		RootPassword: rootPassword,
		UpdatedAt:    credential.UpdatedAt,
	}, nil
}

func (c *CredentialRecord) RemoveSavedResourcePassword(resourceType, resourceID string) bool {
	if c == nil || len(c.ResourceCredentials) == 0 {
		return false
	}

	resourceType = normalizeResourceType(resourceType)
	resourceID = strings.TrimSpace(resourceID)
	if resourceType == "" || resourceID == "" {
		return false
	}

	next := make([]ResourceCredentialRecord, 0, len(c.ResourceCredentials))
	removed := false
	for _, credential := range c.ResourceCredentials {
		if normalizeResourceType(credential.ResourceType) == resourceType && strings.TrimSpace(credential.ResourceID) == resourceID {
			removed = true
			continue
		}
		next = append(next, credential)
	}
	if !removed {
		return false
	}

	c.ResourceCredentials = next
	return true
}

func (c *CredentialRecord) PruneResourceCredentials(resourceType string, validResourceIDs []string, scopePrefix string) bool {
	if c == nil || len(c.ResourceCredentials) == 0 {
		return false
	}

	resourceType = normalizeResourceType(resourceType)
	scopePrefix = strings.TrimSpace(scopePrefix)
	if resourceType == "" {
		return false
	}

	valid := make(map[string]struct{}, len(validResourceIDs))
	for _, resourceID := range validResourceIDs {
		resourceID = strings.TrimSpace(resourceID)
		if resourceID != "" {
			valid[resourceID] = struct{}{}
		}
	}

	next := make([]ResourceCredentialRecord, 0, len(c.ResourceCredentials))
	changed := false
	for _, credential := range c.ResourceCredentials {
		if normalizeResourceType(credential.ResourceType) != resourceType {
			next = append(next, credential)
			continue
		}
		resourceID := strings.TrimSpace(credential.ResourceID)
		if scopePrefix != "" && !strings.HasPrefix(resourceID, scopePrefix) {
			next = append(next, credential)
			continue
		}
		if _, exists := valid[resourceID]; exists {
			next = append(next, credential)
			continue
		}
		changed = true
	}

	if !changed {
		return false
	}

	c.ResourceCredentials = next
	return true
}

func (c *CredentialRecord) SyncSavedResourceName(resourceType, resourceID, resourceName string) bool {
	credential := c.findResourceCredential(resourceType, resourceID)
	if credential == nil {
		return false
	}

	resourceName = strings.TrimSpace(resourceName)
	if credential.ResourceName == resourceName {
		return false
	}

	credential.ResourceName = resourceName
	return true
}

func (c *CredentialRecord) findResourceCredential(resourceType, resourceID string) *ResourceCredentialRecord {
	if c == nil {
		return nil
	}

	resourceType = normalizeResourceType(resourceType)
	resourceID = strings.TrimSpace(resourceID)
	if resourceType == "" || resourceID == "" {
		return nil
	}

	for index := range c.ResourceCredentials {
		record := &c.ResourceCredentials[index]
		if normalizeResourceType(record.ResourceType) == resourceType && strings.TrimSpace(record.ResourceID) == resourceID {
			return record
		}
	}
	return nil
}

func mergeResourceCredentials(left, right []ResourceCredentialRecord) []ResourceCredentialRecord {
	if len(left) == 0 {
		return normalizeResourceCredentials(right)
	}
	if len(right) == 0 {
		return normalizeResourceCredentials(left)
	}

	merged := append(append(make([]ResourceCredentialRecord, 0, len(left)+len(right)), left...), right...)
	return normalizeResourceCredentials(merged)
}

func normalizeResourceCredentials(records []ResourceCredentialRecord) []ResourceCredentialRecord {
	if len(records) == 0 {
		return nil
	}

	normalized := make([]ResourceCredentialRecord, 0, len(records))
	seen := make(map[string]int, len(records))
	for _, record := range records {
		record.ResourceType = normalizeResourceType(record.ResourceType)
		record.ResourceID = strings.TrimSpace(record.ResourceID)
		record.ResourceName = strings.TrimSpace(record.ResourceName)
		record.Username = strings.TrimSpace(record.Username)
		record.PasswordMode = strings.TrimSpace(strings.ToLower(record.PasswordMode))
		record.PasswordCiphertext = strings.TrimSpace(record.PasswordCiphertext)
		record.UpdatedAt = strings.TrimSpace(record.UpdatedAt)

		if record.ResourceType == "" || record.ResourceID == "" || record.PasswordCiphertext == "" {
			continue
		}

		lookupKey := fmt.Sprintf("%s|%s", record.ResourceType, record.ResourceID)
		if existingIndex, exists := seen[lookupKey]; exists {
			normalized[existingIndex] = record
			continue
		}

		seen[lookupKey] = len(normalized)
		normalized = append(normalized, record)
	}

	if len(normalized) == 0 {
		return nil
	}
	return normalized
}

func normalizeResourceType(resourceType string) string {
	switch strings.ToLower(strings.TrimSpace(resourceType)) {
	case "ec2":
		return "ec2"
	case "lightsail":
		return "lightsail"
	default:
		return ""
	}
}
