package aws

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
	DefaultRegion           = "us-east-1"
)

type Addition struct {
	AccessKeyID        string             `json:"access_key_id,omitempty"`
	SecretAccessKey    string             `json:"secret_access_key,omitempty"`
	SessionToken       string             `json:"session_token,omitempty"`
	DefaultRegion      string             `json:"default_region,omitempty"`
	ActiveCredentialID string             `json:"active_credential_id,omitempty"`
	ActiveRegion       string             `json:"active_region,omitempty"`
	Credentials        []CredentialRecord `json:"credentials,omitempty"`
}

type CredentialRecord struct {
	ID                  string                     `json:"id"`
	Name                string                     `json:"name"`
	Group               string                     `json:"group,omitempty"`
	AccessKeyID         string                     `json:"access_key_id"`
	SecretAccessKey     string                     `json:"secret_access_key"`
	SessionToken        string                     `json:"session_token,omitempty"`
	DefaultRegion       string                     `json:"default_region,omitempty"`
	AccountID           string                     `json:"account_id,omitempty"`
	ARN                 string                     `json:"arn,omitempty"`
	UserID              string                     `json:"user_id,omitempty"`
	EC2Quota            *EC2QuotaSummary           `json:"ec2_quota,omitempty"`
	EC2QuotaError       string                     `json:"ec2_quota_error,omitempty"`
	LastStatus          string                     `json:"last_status,omitempty"`
	LastError           string                     `json:"last_error,omitempty"`
	LastCheckedAt       string                     `json:"last_checked_at,omitempty"`
	ResourceCredentials []ResourceCredentialRecord `json:"resource_credentials,omitempty"`
}

type CredentialImport struct {
	ID              string `json:"id,omitempty"`
	Name            string `json:"name"`
	Group           string `json:"group,omitempty"`
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
	SessionToken    string `json:"session_token,omitempty"`
	DefaultRegion   string `json:"default_region,omitempty"`
}

type CredentialView struct {
	ID                string           `json:"id"`
	Name              string           `json:"name"`
	Group             string           `json:"group,omitempty"`
	MaskedAccessKeyID string           `json:"masked_access_key_id"`
	DefaultRegion     string           `json:"default_region"`
	AccountID         string           `json:"account_id,omitempty"`
	ARN               string           `json:"arn,omitempty"`
	UserID            string           `json:"user_id,omitempty"`
	EC2Quota          *EC2QuotaSummary `json:"ec2_quota,omitempty"`
	EC2QuotaError     string           `json:"ec2_quota_error,omitempty"`
	LastStatus        string           `json:"last_status"`
	LastError         string           `json:"last_error,omitempty"`
	LastCheckedAt     string           `json:"last_checked_at,omitempty"`
	IsActive          bool             `json:"is_active"`
}

type CredentialPoolView struct {
	ActiveCredentialID     string           `json:"active_credential_id,omitempty"`
	ActiveRegion           string           `json:"active_region"`
	PasswordStorageEnabled bool             `json:"password_storage_enabled"`
	Credentials            []CredentialView `json:"credentials"`
}

type CredentialSecretView struct {
	CredentialID      string           `json:"credential_id"`
	CredentialName    string           `json:"credential_name"`
	AccessKeyID       string           `json:"access_key_id"`
	SecretAccessKey   string           `json:"secret_access_key"`
	SessionToken      string           `json:"session_token,omitempty"`
	MaskedAccessKeyID string           `json:"masked_access_key_id"`
	DefaultRegion     string           `json:"default_region"`
	AccountID         string           `json:"account_id,omitempty"`
	ARN               string           `json:"arn,omitempty"`
	UserID            string           `json:"user_id,omitempty"`
	EC2Quota          *EC2QuotaSummary `json:"ec2_quota,omitempty"`
	EC2QuotaError     string           `json:"ec2_quota_error,omitempty"`
}

func (a *Addition) Normalize() {
	if a == nil {
		return
	}

	a.AccessKeyID = strings.TrimSpace(a.AccessKeyID)
	a.SecretAccessKey = strings.TrimSpace(a.SecretAccessKey)
	a.SessionToken = strings.TrimSpace(a.SessionToken)
	a.DefaultRegion = normalizeRegion(a.DefaultRegion)
	a.ActiveCredentialID = strings.TrimSpace(a.ActiveCredentialID)
	a.ActiveRegion = normalizeRegion(a.ActiveRegion)

	if len(a.Credentials) == 0 && a.AccessKeyID != "" && a.SecretAccessKey != "" {
		a.Credentials = []CredentialRecord{
			{
				ID:              uuid.NewString(),
				Name:            "Default Credential",
				Group:           "",
				AccessKeyID:     a.AccessKeyID,
				SecretAccessKey: a.SecretAccessKey,
				SessionToken:    a.SessionToken,
				DefaultRegion:   a.DefaultRegion,
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
		credential.AccessKeyID = strings.TrimSpace(credential.AccessKeyID)
		credential.SecretAccessKey = strings.TrimSpace(credential.SecretAccessKey)
		credential.SessionToken = strings.TrimSpace(credential.SessionToken)
		credential.DefaultRegion = normalizeRegion(credential.DefaultRegion)
		credential.AccountID = strings.TrimSpace(credential.AccountID)
		credential.ARN = strings.TrimSpace(credential.ARN)
		credential.UserID = strings.TrimSpace(credential.UserID)
		credential.EC2Quota = normalizeEC2QuotaSummary(credential.EC2Quota)
		credential.EC2QuotaError = strings.TrimSpace(credential.EC2QuotaError)
		credential.LastStatus = normalizeCredentialStatus(credential.LastStatus)
		credential.LastError = strings.TrimSpace(credential.LastError)
		credential.LastCheckedAt = strings.TrimSpace(credential.LastCheckedAt)
		credential.ResourceCredentials = normalizeResourceCredentials(credential.ResourceCredentials)

		if credential.AccessKeyID == "" || credential.SecretAccessKey == "" {
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

		key := credential.AccessKeyID + "|" + credential.DefaultRegion
		if existingIndex, exists := seenKeys[key]; exists {
			merged := normalized[existingIndex]
			if credential.Name != "" {
				merged.Name = credential.Name
			}
			if credential.Group != "" {
				merged.Group = credential.Group
			}
			if credential.SecretAccessKey != "" {
				merged.SecretAccessKey = credential.SecretAccessKey
			}
			if credential.SessionToken != "" {
				merged.SessionToken = credential.SessionToken
			}
			if credential.DefaultRegion != "" {
				merged.DefaultRegion = credential.DefaultRegion
			}
			if credential.AccountID != "" {
				merged.AccountID = credential.AccountID
			}
			if credential.ARN != "" {
				merged.ARN = credential.ARN
			}
			if credential.UserID != "" {
				merged.UserID = credential.UserID
			}
			if credential.LastCheckedAt != "" {
				merged.LastCheckedAt = credential.LastCheckedAt
				merged.LastStatus = credential.LastStatus
				merged.LastError = credential.LastError
				merged.EC2Quota = normalizeEC2QuotaSummary(credential.EC2Quota)
				merged.EC2QuotaError = credential.EC2QuotaError
			}
			merged.ResourceCredentials = mergeResourceCredentials(merged.ResourceCredentials, credential.ResourceCredentials)
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
		a.AccessKeyID = active.AccessKeyID
		a.SecretAccessKey = active.SecretAccessKey
		a.SessionToken = active.SessionToken
		a.DefaultRegion = active.DefaultRegion
		if a.ActiveRegion == "" {
			a.ActiveRegion = active.DefaultRegion
		}
	} else if len(a.Credentials) == 0 {
		a.AccessKeyID = ""
		a.SecretAccessKey = ""
		a.SessionToken = ""
	}

	a.ActiveRegion = normalizeRegion(a.ActiveRegion)
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
	a.AccessKeyID = credential.AccessKeyID
	a.SecretAccessKey = credential.SecretAccessKey
	a.SessionToken = credential.SessionToken
	a.DefaultRegion = credential.DefaultRegion
	a.ActiveRegion = normalizeRegion(credential.DefaultRegion)
	return true
}

func (a *Addition) SetActiveRegion(region string) {
	if a == nil {
		return
	}
	a.ActiveRegion = normalizeRegion(region)
}

func (a *Addition) UpsertCredentials(inputs []CredentialImport) int {
	if a == nil {
		return 0
	}
	a.Normalize()

	count := 0
	for _, input := range inputs {
		accessKeyID := strings.TrimSpace(input.AccessKeyID)
		secretAccessKey := strings.TrimSpace(input.SecretAccessKey)
		name := strings.TrimSpace(input.Name)
		group := normalizeCredentialGroup(input.Group)
		defaultRegion := normalizeRegion(input.DefaultRegion)
		inputID := strings.TrimSpace(input.ID)
		hasCredentialValue := accessKeyID != "" && secretAccessKey != ""
		if !hasCredentialValue && inputID == "" {
			continue
		}

		var matched *CredentialRecord
		for index := range a.Credentials {
			if inputID != "" && a.Credentials[index].ID == inputID {
				matched = &a.Credentials[index]
				break
			}
			if hasCredentialValue && a.Credentials[index].AccessKeyID == accessKeyID && a.Credentials[index].DefaultRegion == defaultRegion {
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
				matched.AccessKeyID = accessKeyID
				matched.SecretAccessKey = secretAccessKey
				matched.SessionToken = strings.TrimSpace(input.SessionToken)
				matched.DefaultRegion = defaultRegion
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
				AccessKeyID:     accessKeyID,
				SecretAccessKey: secretAccessKey,
				SessionToken:    strings.TrimSpace(input.SessionToken),
				DefaultRegion:   defaultRegion,
				LastStatus:      CredentialStatusUnknown,
			})
		}
		count++
	}

	a.Normalize()
	return count
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
	if len(a.Credentials) == 0 {
		a.AccessKeyID = ""
		a.SecretAccessKey = ""
		a.SessionToken = ""
	}
	a.Normalize()
	return true
}

func (a *Addition) toPoolView(includeQuota bool) CredentialPoolView {
	if a == nil {
		return CredentialPoolView{
			ActiveRegion: normalizeRegion(""),
			Credentials:  make([]CredentialView, 0),
		}
	}
	a.Normalize()

	view := CredentialPoolView{
		ActiveCredentialID:     a.ActiveCredentialID,
		ActiveRegion:           normalizeRegion(a.ActiveRegion),
		PasswordStorageEnabled: IsRootPasswordVaultEnabled(),
		Credentials:            make([]CredentialView, 0, len(a.Credentials)),
	}
	for _, credential := range a.Credentials {
		quota := normalizeEC2QuotaSummary(credential.EC2Quota)
		quotaError := credential.EC2QuotaError
		if !includeQuota {
			quota = nil
			quotaError = ""
		}
		view.Credentials = append(view.Credentials, CredentialView{
			ID:                credential.ID,
			Name:              credential.Name,
			Group:             credential.Group,
			MaskedAccessKeyID: maskAccessKeyID(credential.AccessKeyID),
			DefaultRegion:     normalizeRegion(credential.DefaultRegion),
			AccountID:         credential.AccountID,
			ARN:               credential.ARN,
			UserID:            credential.UserID,
			EC2Quota:          quota,
			EC2QuotaError:     quotaError,
			LastStatus:        normalizeCredentialStatus(credential.LastStatus),
			LastError:         credential.LastError,
			LastCheckedAt:     credential.LastCheckedAt,
			IsActive:          credential.ID == a.ActiveCredentialID,
		})
	}
	return view
}

func (a *Addition) ToPoolView() CredentialPoolView {
	return a.toPoolView(true)
}

func (a *Addition) ToPoolViewWithoutQuota() CredentialPoolView {
	return a.toPoolView(false)
}

func (c *CredentialRecord) SecretView() *CredentialSecretView {
	if c == nil {
		return nil
	}
	return &CredentialSecretView{
		CredentialID:      c.ID,
		CredentialName:    c.Name,
		AccessKeyID:       c.AccessKeyID,
		SecretAccessKey:   c.SecretAccessKey,
		SessionToken:      c.SessionToken,
		MaskedAccessKeyID: maskAccessKeyID(c.AccessKeyID),
		DefaultRegion:     normalizeRegion(c.DefaultRegion),
		AccountID:         c.AccountID,
		ARN:               c.ARN,
		UserID:            c.UserID,
		EC2Quota:          normalizeEC2QuotaSummary(c.EC2Quota),
		EC2QuotaError:     c.EC2QuotaError,
	}
}

func (c *CredentialRecord) SetCheckResult(checkedAt time.Time, identity *Identity, quota *EC2QuotaSummary, quotaErr error, err error) {
	if c == nil {
		return
	}

	c.LastCheckedAt = checkedAt.UTC().Format(time.RFC3339)
	if err != nil {
		c.LastStatus = CredentialStatusError
		c.LastError = err.Error()
		c.AccountID = ""
		c.ARN = ""
		c.UserID = ""
		c.EC2Quota = nil
		c.EC2QuotaError = ""
		return
	}

	c.LastStatus = CredentialStatusHealthy
	c.LastError = ""
	c.EC2Quota = normalizeEC2QuotaSummary(quota)
	if quotaErr != nil {
		c.EC2QuotaError = quotaErr.Error()
	} else {
		c.EC2QuotaError = ""
	}
	if identity != nil {
		c.AccountID = strings.TrimSpace(identity.AccountID)
		c.ARN = strings.TrimSpace(identity.ARN)
		c.UserID = strings.TrimSpace(identity.UserID)
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

func normalizeRegion(region string) string {
	region = strings.TrimSpace(region)
	if region == "" {
		return DefaultRegion
	}
	return region
}

func maskAccessKeyID(accessKeyID string) string {
	accessKeyID = strings.TrimSpace(accessKeyID)
	if accessKeyID == "" {
		return ""
	}
	if len(accessKeyID) <= 4 {
		return strings.Repeat("*", len(accessKeyID))
	}
	if len(accessKeyID) <= 10 {
		return accessKeyID[:2] + strings.Repeat("*", len(accessKeyID)-4) + accessKeyID[len(accessKeyID)-2:]
	}
	return accessKeyID[:4] + strings.Repeat("*", len(accessKeyID)-8) + accessKeyID[len(accessKeyID)-4:]
}

func normalizeEC2QuotaSummary(summary *EC2QuotaSummary) *EC2QuotaSummary {
	if summary == nil {
		return nil
	}

	if summary.MaxInstances == 0 &&
		summary.MaxStandardVCPUs == 0 &&
		summary.MaxElasticIPs == 0 &&
		summary.VPCMaxElasticIPs == 0 &&
		summary.VPCMaxSecurityGroupsPerInterface == 0 &&
		summary.InstanceStandardVCPUs == 0 &&
		summary.ReservedStandardVCPUs == 0 &&
		summary.RunningStandardVCPUs == 0 &&
		summary.RunningInstances == 0 &&
		summary.TotalInstances == 0 &&
		summary.AllocatedElasticIPs == 0 &&
		summary.AssociatedElasticIPs == 0 {
		return nil
	}

	normalized := *summary
	normalized.Region = normalizeRegion(summary.Region)
	return &normalized
}
