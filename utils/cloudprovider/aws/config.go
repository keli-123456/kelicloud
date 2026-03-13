package aws

import (
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
	ID              string           `json:"id"`
	Name            string           `json:"name"`
	AccessKeyID     string           `json:"access_key_id"`
	SecretAccessKey string           `json:"secret_access_key"`
	SessionToken    string           `json:"session_token,omitempty"`
	DefaultRegion   string           `json:"default_region,omitempty"`
	AccountID       string           `json:"account_id,omitempty"`
	ARN             string           `json:"arn,omitempty"`
	UserID          string           `json:"user_id,omitempty"`
	EC2Quota        *EC2QuotaSummary `json:"ec2_quota,omitempty"`
	EC2QuotaError   string           `json:"ec2_quota_error,omitempty"`
	LastStatus      string           `json:"last_status,omitempty"`
	LastError       string           `json:"last_error,omitempty"`
	LastCheckedAt   string           `json:"last_checked_at,omitempty"`
}

type CredentialImport struct {
	ID              string `json:"id,omitempty"`
	Name            string `json:"name"`
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
	SessionToken    string `json:"session_token,omitempty"`
	DefaultRegion   string `json:"default_region,omitempty"`
}

type CredentialView struct {
	ID                string           `json:"id"`
	Name              string           `json:"name"`
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
	ActiveCredentialID string           `json:"active_credential_id,omitempty"`
	ActiveRegion       string           `json:"active_region"`
	Credentials        []CredentialView `json:"credentials"`
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
		if accessKeyID == "" || secretAccessKey == "" {
			continue
		}
		name := strings.TrimSpace(input.Name)
		if name == "" {
			name = "Credential"
		}
		defaultRegion := normalizeRegion(input.DefaultRegion)
		inputID := strings.TrimSpace(input.ID)

		var matched *CredentialRecord
		for index := range a.Credentials {
			if inputID != "" && a.Credentials[index].ID == inputID {
				matched = &a.Credentials[index]
				break
			}
			if a.Credentials[index].AccessKeyID == accessKeyID && a.Credentials[index].DefaultRegion == defaultRegion {
				matched = &a.Credentials[index]
				break
			}
		}

		if matched != nil {
			matched.Name = name
			matched.AccessKeyID = accessKeyID
			matched.SecretAccessKey = secretAccessKey
			matched.SessionToken = strings.TrimSpace(input.SessionToken)
			matched.DefaultRegion = defaultRegion
		} else {
			a.Credentials = append(a.Credentials, CredentialRecord{
				ID:              uuid.NewString(),
				Name:            name,
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
			ActiveRegion: normalizeRegion(""),
			Credentials:  make([]CredentialView, 0),
		}
	}
	a.Normalize()

	view := CredentialPoolView{
		ActiveCredentialID: a.ActiveCredentialID,
		ActiveRegion:       normalizeRegion(a.ActiveRegion),
		Credentials:        make([]CredentialView, 0, len(a.Credentials)),
	}
	for _, credential := range a.Credentials {
		view.Credentials = append(view.Credentials, CredentialView{
			ID:                credential.ID,
			Name:              credential.Name,
			MaskedAccessKeyID: maskAccessKeyID(credential.AccessKeyID),
			DefaultRegion:     normalizeRegion(credential.DefaultRegion),
			AccountID:         credential.AccountID,
			ARN:               credential.ARN,
			UserID:            credential.UserID,
			EC2Quota:          normalizeEC2QuotaSummary(credential.EC2Quota),
			EC2QuotaError:     credential.EC2QuotaError,
			LastStatus:        normalizeCredentialStatus(credential.LastStatus),
			LastError:         credential.LastError,
			LastCheckedAt:     credential.LastCheckedAt,
			IsActive:          credential.ID == a.ActiveCredentialID,
		})
	}
	return view
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
		summary.MaxElasticIPs == 0 &&
		summary.VPCMaxElasticIPs == 0 &&
		summary.VPCMaxSecurityGroupsPerInterface == 0 {
		return nil
	}

	normalized := *summary
	normalized.Region = normalizeRegion(summary.Region)
	return &normalized
}
