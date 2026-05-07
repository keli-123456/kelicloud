package vultr

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	TokenStatusUnknown = "unknown"
	TokenStatusHealthy = "healthy"
	TokenStatusError   = "error"

	missingInstanceCredentialRetention = 24 * time.Hour
)

type Addition struct {
	Token         string        `json:"token,omitempty"`
	ActiveTokenID string        `json:"active_token_id,omitempty"`
	Tokens        []TokenRecord `json:"tokens,omitempty"`
}

type TokenRecord struct {
	ID                  string                     `json:"id"`
	Name                string                     `json:"name"`
	Group               string                     `json:"group,omitempty"`
	Token               string                     `json:"token"`
	AccountName         string                     `json:"account_name,omitempty"`
	AccountEmail        string                     `json:"account_email,omitempty"`
	AccountBalance      float64                    `json:"account_balance,omitempty"`
	PendingCharges      float64                    `json:"pending_charges,omitempty"`
	LastStatus          string                     `json:"last_status,omitempty"`
	LastError           string                     `json:"last_error,omitempty"`
	LastCheckedAt       string                     `json:"last_checked_at,omitempty"`
	InstanceCredentials []InstanceCredentialRecord `json:"instance_credentials,omitempty"`
}

type TokenImport struct {
	ID    string `json:"id,omitempty"`
	Name  string `json:"name"`
	Group string `json:"group,omitempty"`
	Token string `json:"token"`
}

type TokenView struct {
	ID             string  `json:"id"`
	Name           string  `json:"name"`
	Group          string  `json:"group,omitempty"`
	MaskedToken    string  `json:"masked_token"`
	AccountName    string  `json:"account_name,omitempty"`
	AccountEmail   string  `json:"account_email,omitempty"`
	AccountBalance float64 `json:"account_balance,omitempty"`
	PendingCharges float64 `json:"pending_charges,omitempty"`
	LastStatus     string  `json:"last_status"`
	LastError      string  `json:"last_error,omitempty"`
	LastCheckedAt  string  `json:"last_checked_at,omitempty"`
	IsActive       bool    `json:"is_active"`
}

type TokenPoolView struct {
	ActiveTokenID          string      `json:"active_token_id,omitempty"`
	PasswordStorageEnabled bool        `json:"password_storage_enabled"`
	Tokens                 []TokenView `json:"tokens"`
}

type TokenSecretView struct {
	TokenID      string `json:"token_id"`
	TokenName    string `json:"token_name"`
	Token        string `json:"token"`
	MaskedToken  string `json:"masked_token"`
	AccountName  string `json:"account_name,omitempty"`
	AccountEmail string `json:"account_email,omitempty"`
}

type InstanceCredentialRecord struct {
	InstanceID         string `json:"instance_id"`
	InstanceLabel      string `json:"instance_label,omitempty"`
	Username           string `json:"username,omitempty"`
	PasswordMode       string `json:"password_mode,omitempty"`
	PasswordCiphertext string `json:"password_ciphertext,omitempty"`
	UpdatedAt          string `json:"updated_at,omitempty"`
}

type InstancePasswordView struct {
	InstanceID    string `json:"instance_id"`
	InstanceLabel string `json:"instance_label,omitempty"`
	Username      string `json:"username,omitempty"`
	PasswordMode  string `json:"password_mode,omitempty"`
	RootPassword  string `json:"root_password"`
	UpdatedAt     string `json:"updated_at,omitempty"`
}

func (a *Addition) Normalize() {
	if a == nil {
		return
	}

	a.Token = strings.TrimSpace(a.Token)
	a.ActiveTokenID = strings.TrimSpace(a.ActiveTokenID)

	if len(a.Tokens) == 0 && a.Token != "" {
		a.Tokens = []TokenRecord{{
			ID:         uuid.NewString(),
			Name:       "Default Token",
			Token:      a.Token,
			LastStatus: TokenStatusUnknown,
		}}
	}

	normalized := make([]TokenRecord, 0, len(a.Tokens))
	seenIDs := make(map[string]struct{}, len(a.Tokens))
	seenTokens := make(map[string]int, len(a.Tokens))

	for _, token := range a.Tokens {
		token.ID = strings.TrimSpace(token.ID)
		token.Name = strings.TrimSpace(token.Name)
		token.Group = normalizeTokenGroup(token.Group)
		token.Token = strings.TrimSpace(token.Token)
		token.AccountName = strings.TrimSpace(token.AccountName)
		token.AccountEmail = strings.TrimSpace(token.AccountEmail)
		token.LastStatus = normalizeTokenStatus(token.LastStatus)
		token.LastError = strings.TrimSpace(token.LastError)
		token.LastCheckedAt = strings.TrimSpace(token.LastCheckedAt)
		token.InstanceCredentials = normalizeInstanceCredentials(token.InstanceCredentials)

		if token.Token == "" {
			continue
		}
		if token.ID == "" {
			token.ID = uuid.NewString()
		}
		if token.Name == "" {
			token.Name = "Token"
		}
		if _, exists := seenIDs[token.ID]; exists {
			token.ID = uuid.NewString()
		}
		seenIDs[token.ID] = struct{}{}

		if existingIndex, exists := seenTokens[token.Token]; exists {
			merged := normalized[existingIndex]
			if token.Name != "" {
				merged.Name = token.Name
			}
			if token.Group != "" {
				merged.Group = token.Group
			}
			if token.AccountName != "" {
				merged.AccountName = token.AccountName
			}
			if token.AccountEmail != "" {
				merged.AccountEmail = token.AccountEmail
			}
			if token.LastCheckedAt != "" {
				merged.LastCheckedAt = token.LastCheckedAt
				merged.LastStatus = token.LastStatus
				merged.LastError = token.LastError
				merged.AccountBalance = token.AccountBalance
				merged.PendingCharges = token.PendingCharges
			}
			merged.InstanceCredentials = mergeInstanceCredentials(merged.InstanceCredentials, token.InstanceCredentials)
			normalized[existingIndex] = merged
			if a.ActiveTokenID == token.ID {
				a.ActiveTokenID = merged.ID
			}
			continue
		}

		seenTokens[token.Token] = len(normalized)
		normalized = append(normalized, token)
	}

	a.Tokens = normalized
	if a.ActiveTokenID == "" && len(a.Tokens) > 0 {
		a.ActiveTokenID = a.Tokens[0].ID
	}
	if a.ActiveTokenID != "" {
		if active := a.FindToken(a.ActiveTokenID); active != nil {
			a.Token = active.Token
		} else if len(a.Tokens) > 0 {
			a.ActiveTokenID = a.Tokens[0].ID
			a.Token = a.Tokens[0].Token
		} else {
			a.ActiveTokenID = ""
			a.Token = ""
		}
	} else if len(a.Tokens) == 0 {
		a.Token = ""
	}
}

func (a *Addition) FindToken(id string) *TokenRecord {
	if a == nil {
		return nil
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return nil
	}
	for index := range a.Tokens {
		if a.Tokens[index].ID == id {
			return &a.Tokens[index]
		}
	}
	return nil
}

func (a *Addition) ActiveToken() *TokenRecord {
	if a == nil {
		return nil
	}
	a.Normalize()
	return a.FindToken(a.ActiveTokenID)
}

func (a *Addition) SetActiveToken(id string) bool {
	if a == nil {
		return false
	}
	a.Normalize()
	token := a.FindToken(id)
	if token == nil {
		return false
	}
	a.ActiveTokenID = token.ID
	a.Token = token.Token
	return true
}

func (a *Addition) UpsertTokens(inputs []TokenImport) int {
	if a == nil {
		return 0
	}
	a.Normalize()

	count := 0
	for _, input := range inputs {
		tokenValue := strings.TrimSpace(input.Token)
		name := strings.TrimSpace(input.Name)
		group := normalizeTokenGroup(input.Group)
		inputID := strings.TrimSpace(input.ID)
		if tokenValue == "" && inputID == "" {
			continue
		}

		var matched *TokenRecord
		for index := range a.Tokens {
			if inputID != "" && a.Tokens[index].ID == inputID {
				matched = &a.Tokens[index]
				break
			}
			if tokenValue != "" && a.Tokens[index].Token == tokenValue {
				matched = &a.Tokens[index]
				break
			}
		}

		if matched != nil {
			if name != "" {
				matched.Name = name
			}
			matched.Group = group
			if tokenValue != "" {
				matched.Token = tokenValue
			}
		} else {
			if tokenValue == "" {
				continue
			}
			if name == "" {
				name = nextGeneratedTokenName(a.Tokens)
			}
			a.Tokens = append(a.Tokens, TokenRecord{
				ID:         uuid.NewString(),
				Name:       name,
				Group:      group,
				Token:      tokenValue,
				LastStatus: TokenStatusUnknown,
			})
		}
		count++
	}

	a.Normalize()
	return count
}

func (a *Addition) RemoveToken(id string) bool {
	if a == nil {
		return false
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return false
	}

	next := make([]TokenRecord, 0, len(a.Tokens))
	removed := false
	for _, token := range a.Tokens {
		if token.ID == id {
			removed = true
			continue
		}
		next = append(next, token)
	}
	if !removed {
		return false
	}

	a.Tokens = next
	if a.ActiveTokenID == id {
		a.ActiveTokenID = ""
	}
	if len(a.Tokens) == 0 {
		a.Token = ""
	}
	a.Normalize()
	return true
}

func (a *Addition) MergePersistentStateFrom(previous *Addition) {
	if a == nil || previous == nil {
		return
	}

	a.Normalize()
	previous.Normalize()
	for index := range a.Tokens {
		previousToken := previous.findTokenForPersistentStateMerge(a.Tokens[index].ID, a.Tokens[index].Token)
		if previousToken == nil {
			continue
		}
		a.Tokens[index].InstanceCredentials = mergeInstanceCredentials(
			previousToken.InstanceCredentials,
			a.Tokens[index].InstanceCredentials,
		)
	}
	a.Normalize()
}

func (a *Addition) ToPoolView() TokenPoolView {
	if a == nil {
		return TokenPoolView{Tokens: make([]TokenView, 0)}
	}
	a.Normalize()

	view := TokenPoolView{
		ActiveTokenID:          a.ActiveTokenID,
		PasswordStorageEnabled: IsRootPasswordVaultEnabled(),
		Tokens:                 make([]TokenView, 0, len(a.Tokens)),
	}
	for _, token := range a.Tokens {
		view.Tokens = append(view.Tokens, TokenView{
			ID:             token.ID,
			Name:           token.Name,
			Group:          token.Group,
			MaskedToken:    maskToken(token.Token),
			AccountName:    token.AccountName,
			AccountEmail:   token.AccountEmail,
			AccountBalance: token.AccountBalance,
			PendingCharges: token.PendingCharges,
			LastStatus:     normalizeTokenStatus(token.LastStatus),
			LastError:      token.LastError,
			LastCheckedAt:  token.LastCheckedAt,
			IsActive:       token.ID == a.ActiveTokenID,
		})
	}
	return view
}

func (t *TokenRecord) TokenSecretView() *TokenSecretView {
	if t == nil {
		return nil
	}
	return &TokenSecretView{
		TokenID:      t.ID,
		TokenName:    t.Name,
		Token:        t.Token,
		MaskedToken:  maskToken(t.Token),
		AccountName:  t.AccountName,
		AccountEmail: t.AccountEmail,
	}
}

func (t *TokenRecord) SetCheckResult(checkedAt time.Time, account *Account, err error) {
	if t == nil {
		return
	}
	t.LastCheckedAt = checkedAt.UTC().Format(time.RFC3339)
	if err != nil {
		t.LastStatus = TokenStatusError
		t.LastError = err.Error()
		t.AccountName = ""
		t.AccountEmail = ""
		t.AccountBalance = 0
		t.PendingCharges = 0
		return
	}
	t.LastStatus = TokenStatusHealthy
	t.LastError = ""
	if account != nil {
		t.AccountName = strings.TrimSpace(account.Name)
		t.AccountEmail = strings.TrimSpace(account.Email)
		t.AccountBalance = account.Balance
		t.PendingCharges = account.PendingCharges
	}
}

func (a *Addition) FindTokenWithSavedInstancePassword(instanceID string) *TokenRecord {
	if a == nil || strings.TrimSpace(instanceID) == "" {
		return nil
	}
	a.Normalize()
	for index := range a.Tokens {
		if a.Tokens[index].HasSavedInstancePassword(instanceID) {
			return &a.Tokens[index]
		}
	}
	return nil
}

func (a *Addition) HasSavedInstancePassword(instanceID string) bool {
	return a.FindTokenWithSavedInstancePassword(instanceID) != nil
}

func (a *Addition) SavedInstancePasswordUpdatedAt(instanceID string) string {
	token := a.FindTokenWithSavedInstancePassword(instanceID)
	if token == nil {
		return ""
	}
	return token.SavedInstancePasswordUpdatedAt(instanceID)
}

func (a *Addition) RevealInstancePassword(instanceID string) (*InstancePasswordView, error) {
	token := a.FindTokenWithSavedInstancePassword(instanceID)
	if token == nil {
		return nil, ErrSavedRootPasswordNotFound
	}
	return token.RevealInstancePassword(instanceID)
}

func (t *TokenRecord) HasSavedInstancePassword(instanceID string) bool {
	if t == nil || strings.TrimSpace(instanceID) == "" {
		return false
	}
	return t.findInstanceCredential(instanceID) != nil
}

func (t *TokenRecord) SavedInstancePasswordUpdatedAt(instanceID string) string {
	credential := t.findInstanceCredential(instanceID)
	if credential == nil {
		return ""
	}
	return credential.UpdatedAt
}

func (t *TokenRecord) SaveInstancePassword(instanceID, instanceLabel, passwordMode, rootPassword string, updatedAt time.Time) error {
	if t == nil || strings.TrimSpace(instanceID) == "" {
		return ErrSavedRootPasswordNotFound
	}
	rootPassword = strings.TrimSpace(rootPassword)
	if rootPassword == "" {
		return ErrSavedRootPasswordNotFound
	}

	ciphertext, err := encryptRootPassword(rootPassword)
	if err != nil {
		return err
	}

	record := InstanceCredentialRecord{
		InstanceID:         strings.TrimSpace(instanceID),
		InstanceLabel:      strings.TrimSpace(instanceLabel),
		Username:           "root",
		PasswordMode:       strings.TrimSpace(strings.ToLower(passwordMode)),
		PasswordCiphertext: ciphertext,
		UpdatedAt:          updatedAt.UTC().Format(time.RFC3339),
	}
	if existing := t.findInstanceCredential(record.InstanceID); existing != nil {
		*existing = record
	} else {
		t.InstanceCredentials = append(t.InstanceCredentials, record)
	}
	t.InstanceCredentials = normalizeInstanceCredentials(t.InstanceCredentials)
	return nil
}

func (t *TokenRecord) RevealInstancePassword(instanceID string) (*InstancePasswordView, error) {
	credential := t.findInstanceCredential(instanceID)
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

	return &InstancePasswordView{
		InstanceID:    credential.InstanceID,
		InstanceLabel: credential.InstanceLabel,
		Username:      username,
		PasswordMode:  credential.PasswordMode,
		RootPassword:  rootPassword,
		UpdatedAt:     credential.UpdatedAt,
	}, nil
}

func (t *TokenRecord) RemoveSavedInstancePassword(instanceID string) bool {
	if t == nil || strings.TrimSpace(instanceID) == "" || len(t.InstanceCredentials) == 0 {
		return false
	}

	next := make([]InstanceCredentialRecord, 0, len(t.InstanceCredentials))
	removed := false
	for _, credential := range t.InstanceCredentials {
		if credential.InstanceID == instanceID {
			removed = true
			continue
		}
		next = append(next, credential)
	}
	if !removed {
		return false
	}

	t.InstanceCredentials = next
	return true
}

func (t *TokenRecord) SyncInstanceCredentialLabel(instanceID, instanceLabel string) bool {
	credential := t.findInstanceCredential(instanceID)
	if credential == nil {
		return false
	}

	instanceLabel = strings.TrimSpace(instanceLabel)
	if credential.InstanceLabel == instanceLabel {
		return false
	}

	credential.InstanceLabel = instanceLabel
	return true
}

func (t *TokenRecord) PruneInstanceCredentials(validInstanceIDs map[string]struct{}) bool {
	if t == nil || len(t.InstanceCredentials) == 0 {
		return false
	}

	cutoff := time.Now().UTC().Add(-missingInstanceCredentialRetention)
	next := make([]InstanceCredentialRecord, 0, len(t.InstanceCredentials))
	changed := false
	for _, credential := range t.InstanceCredentials {
		if _, exists := validInstanceIDs[credential.InstanceID]; exists {
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
		t.InstanceCredentials = next
	}
	return changed
}

func (a *Addition) findTokenForPersistentStateMerge(id, tokenValue string) *TokenRecord {
	if a == nil {
		return nil
	}
	id = strings.TrimSpace(id)
	if id != "" {
		if token := a.FindToken(id); token != nil {
			return token
		}
	}
	tokenValue = strings.TrimSpace(tokenValue)
	if tokenValue == "" {
		return nil
	}
	for index := range a.Tokens {
		if a.Tokens[index].Token == tokenValue {
			return &a.Tokens[index]
		}
	}
	return nil
}

func nextGeneratedTokenName(tokens []TokenRecord) string {
	used := make(map[string]struct{}, len(tokens))
	for _, token := range tokens {
		name := strings.TrimSpace(token.Name)
		if name != "" {
			used[name] = struct{}{}
		}
	}
	for index := 1; ; index++ {
		candidate := fmt.Sprintf("Token %d", index)
		if _, exists := used[candidate]; !exists {
			return candidate
		}
	}
}

func normalizeTokenGroup(group string) string {
	group = strings.TrimSpace(group)
	if len(group) > 100 {
		group = group[:100]
	}
	return group
}

func normalizeTokenStatus(status string) string {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case TokenStatusHealthy:
		return TokenStatusHealthy
	case TokenStatusError:
		return TokenStatusError
	default:
		return TokenStatusUnknown
	}
}

func maskToken(token string) string {
	token = strings.TrimSpace(token)
	if token == "" {
		return ""
	}
	if len(token) <= 4 {
		return strings.Repeat("*", len(token))
	}
	if len(token) <= 10 {
		return token[:2] + strings.Repeat("*", len(token)-4) + token[len(token)-2:]
	}
	return token[:4] + strings.Repeat("*", len(token)-8) + token[len(token)-4:]
}

func (t *TokenRecord) findInstanceCredential(instanceID string) *InstanceCredentialRecord {
	if t == nil {
		return nil
	}
	instanceID = strings.TrimSpace(instanceID)
	if instanceID == "" {
		return nil
	}
	for index := range t.InstanceCredentials {
		if t.InstanceCredentials[index].InstanceID == instanceID {
			return &t.InstanceCredentials[index]
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
		if record.InstanceID == "" {
			continue
		}
		record.InstanceLabel = strings.TrimSpace(record.InstanceLabel)
		record.Username = strings.TrimSpace(record.Username)
		record.PasswordMode = strings.TrimSpace(strings.ToLower(record.PasswordMode))
		record.PasswordCiphertext = strings.TrimSpace(record.PasswordCiphertext)
		record.UpdatedAt = strings.TrimSpace(record.UpdatedAt)

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

func profileAvailabilityError(account *Account) error {
	if account == nil {
		return errors.New("vultr account response is empty")
	}
	return nil
}
