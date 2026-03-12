package digitalocean

import (
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	TokenStatusUnknown = "unknown"
	TokenStatusHealthy = "healthy"
	TokenStatusError   = "error"
)

type Addition struct {
	Token         string        `json:"token,omitempty"`
	ActiveTokenID string        `json:"active_token_id,omitempty"`
	Tokens        []TokenRecord `json:"tokens,omitempty"`
}

type TokenRecord struct {
	ID                       string                    `json:"id"`
	Name                     string                    `json:"name"`
	Token                    string                    `json:"token"`
	AccountEmail             string                    `json:"account_email,omitempty"`
	AccountUUID              string                    `json:"account_uuid,omitempty"`
	DropletLimit             int                       `json:"droplet_limit,omitempty"`
	LastStatus               string                    `json:"last_status,omitempty"`
	LastError                string                    `json:"last_error,omitempty"`
	LastCheckedAt            string                    `json:"last_checked_at,omitempty"`
	ManagedSSHKeyID          int                       `json:"managed_ssh_key_id,omitempty"`
	ManagedSSHKeyName        string                    `json:"managed_ssh_key_name,omitempty"`
	ManagedSSHKeyFingerprint string                    `json:"managed_ssh_key_fingerprint,omitempty"`
	ManagedSSHPrivateKey     string                    `json:"managed_ssh_private_key,omitempty"`
	ManagedSSHPublicKey      string                    `json:"managed_ssh_public_key,omitempty"`
	DropletCredentials       []DropletCredentialRecord `json:"droplet_credentials,omitempty"`
}

type TokenImport struct {
	ID    string `json:"id,omitempty"`
	Name  string `json:"name"`
	Token string `json:"token"`
}

type TokenView struct {
	ID                       string `json:"id"`
	Name                     string `json:"name"`
	MaskedToken              string `json:"masked_token"`
	AccountEmail             string `json:"account_email,omitempty"`
	AccountUUID              string `json:"account_uuid,omitempty"`
	DropletLimit             int    `json:"droplet_limit,omitempty"`
	LastStatus               string `json:"last_status"`
	LastError                string `json:"last_error,omitempty"`
	LastCheckedAt            string `json:"last_checked_at,omitempty"`
	ManagedSSHKeyName        string `json:"managed_ssh_key_name,omitempty"`
	ManagedSSHKeyFingerprint string `json:"managed_ssh_key_fingerprint,omitempty"`
	ManagedSSHKeyReady       bool   `json:"managed_ssh_key_ready"`
	IsActive                 bool   `json:"is_active"`
}

type TokenPoolView struct {
	ActiveTokenID          string      `json:"active_token_id,omitempty"`
	PasswordStorageEnabled bool        `json:"password_storage_enabled"`
	Tokens                 []TokenView `json:"tokens"`
}

type ManagedSSHKeyMaterialView struct {
	TokenID     string `json:"token_id"`
	TokenName   string `json:"token_name"`
	KeyID       int    `json:"key_id"`
	Name        string `json:"name"`
	Fingerprint string `json:"fingerprint,omitempty"`
	PublicKey   string `json:"public_key"`
	PrivateKey  string `json:"private_key"`
}

type TokenSecretView struct {
	TokenID      string `json:"token_id"`
	TokenName    string `json:"token_name"`
	Token        string `json:"token"`
	MaskedToken  string `json:"masked_token"`
	AccountEmail string `json:"account_email,omitempty"`
}

type DropletCredentialRecord struct {
	DropletID          int    `json:"droplet_id"`
	DropletName        string `json:"droplet_name,omitempty"`
	Username           string `json:"username,omitempty"`
	PasswordMode       string `json:"password_mode,omitempty"`
	PasswordCiphertext string `json:"password_ciphertext,omitempty"`
	UpdatedAt          string `json:"updated_at,omitempty"`
}

type DropletPasswordView struct {
	DropletID    int    `json:"droplet_id"`
	DropletName  string `json:"droplet_name,omitempty"`
	Username     string `json:"username,omitempty"`
	PasswordMode string `json:"password_mode,omitempty"`
	RootPassword string `json:"root_password"`
	UpdatedAt    string `json:"updated_at,omitempty"`
}

func (a *Addition) Normalize() {
	if a == nil {
		return
	}

	a.Token = strings.TrimSpace(a.Token)
	a.ActiveTokenID = strings.TrimSpace(a.ActiveTokenID)

	if len(a.Tokens) == 0 && a.Token != "" {
		a.Tokens = []TokenRecord{
			{
				ID:         uuid.NewString(),
				Name:       "Default Token",
				Token:      a.Token,
				LastStatus: TokenStatusUnknown,
			},
		}
	}

	normalized := make([]TokenRecord, 0, len(a.Tokens))
	seenIDs := make(map[string]struct{}, len(a.Tokens))
	seenTokens := make(map[string]int, len(a.Tokens))

	for _, token := range a.Tokens {
		token.ID = strings.TrimSpace(token.ID)
		token.Name = strings.TrimSpace(token.Name)
		token.Token = strings.TrimSpace(token.Token)
		token.AccountEmail = strings.TrimSpace(token.AccountEmail)
		token.AccountUUID = strings.TrimSpace(token.AccountUUID)
		token.LastError = strings.TrimSpace(token.LastError)
		token.LastCheckedAt = strings.TrimSpace(token.LastCheckedAt)
		token.ManagedSSHKeyName = strings.TrimSpace(token.ManagedSSHKeyName)
		token.ManagedSSHKeyFingerprint = strings.TrimSpace(token.ManagedSSHKeyFingerprint)
		token.ManagedSSHPrivateKey = strings.TrimSpace(token.ManagedSSHPrivateKey)
		token.ManagedSSHPublicKey = strings.TrimSpace(token.ManagedSSHPublicKey)
		token.DropletCredentials = normalizeDropletCredentials(token.DropletCredentials)
		token.LastStatus = normalizeTokenStatus(token.LastStatus)

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
			if token.AccountEmail != "" {
				merged.AccountEmail = token.AccountEmail
			}
			if token.AccountUUID != "" {
				merged.AccountUUID = token.AccountUUID
			}
			if token.DropletLimit > 0 {
				merged.DropletLimit = token.DropletLimit
			}
			if token.LastCheckedAt != "" {
				merged.LastCheckedAt = token.LastCheckedAt
				merged.LastStatus = token.LastStatus
				merged.LastError = token.LastError
			}
			if token.ManagedSSHKeyID > 0 {
				merged.ManagedSSHKeyID = token.ManagedSSHKeyID
			}
			if token.ManagedSSHKeyName != "" {
				merged.ManagedSSHKeyName = token.ManagedSSHKeyName
			}
			if token.ManagedSSHKeyFingerprint != "" {
				merged.ManagedSSHKeyFingerprint = token.ManagedSSHKeyFingerprint
			}
			if token.ManagedSSHPrivateKey != "" {
				merged.ManagedSSHPrivateKey = token.ManagedSSHPrivateKey
			}
			if token.ManagedSSHPublicKey != "" {
				merged.ManagedSSHPublicKey = token.ManagedSSHPublicKey
			}
			merged.DropletCredentials = mergeDropletCredentials(merged.DropletCredentials, token.DropletCredentials)
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
		if tokenValue == "" {
			continue
		}
		name := strings.TrimSpace(input.Name)
		if name == "" {
			name = "Token"
		}
		inputID := strings.TrimSpace(input.ID)

		var matched *TokenRecord
		for index := range a.Tokens {
			if inputID != "" && a.Tokens[index].ID == inputID {
				matched = &a.Tokens[index]
				break
			}
			if a.Tokens[index].Token == tokenValue {
				matched = &a.Tokens[index]
				break
			}
		}

		if matched != nil {
			matched.Name = name
			matched.Token = tokenValue
		} else {
			a.Tokens = append(a.Tokens, TokenRecord{
				ID:         uuid.NewString(),
				Name:       name,
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
	a.Normalize()
	return true
}

func (a *Addition) ToPoolView() TokenPoolView {
	if a == nil {
		return TokenPoolView{
			Tokens: make([]TokenView, 0),
		}
	}
	a.Normalize()

	view := TokenPoolView{
		ActiveTokenID:          a.ActiveTokenID,
		PasswordStorageEnabled: IsDropletPasswordVaultEnabled(),
		Tokens:                 make([]TokenView, 0, len(a.Tokens)),
	}
	for _, token := range a.Tokens {
		view.Tokens = append(view.Tokens, TokenView{
			ID:                       token.ID,
			Name:                     token.Name,
			MaskedToken:              maskToken(token.Token),
			AccountEmail:             token.AccountEmail,
			AccountUUID:              token.AccountUUID,
			DropletLimit:             token.DropletLimit,
			LastStatus:               normalizeTokenStatus(token.LastStatus),
			LastError:                token.LastError,
			LastCheckedAt:            token.LastCheckedAt,
			ManagedSSHKeyName:        token.ManagedSSHKeyName,
			ManagedSSHKeyFingerprint: token.ManagedSSHKeyFingerprint,
			ManagedSSHKeyReady:       token.HasManagedSSHKeyMaterial(),
			IsActive:                 token.ID == a.ActiveTokenID,
		})
	}
	return view
}

func (t *TokenRecord) HasManagedSSHKeyMaterial() bool {
	if t == nil {
		return false
	}
	return t.ManagedSSHKeyID > 0 && t.ManagedSSHPrivateKey != "" && t.ManagedSSHPublicKey != ""
}

func (t *TokenRecord) ManagedSSHKeyMaterialView() *ManagedSSHKeyMaterialView {
	if t == nil || !t.HasManagedSSHKeyMaterial() {
		return nil
	}
	return &ManagedSSHKeyMaterialView{
		TokenID:     t.ID,
		TokenName:   t.Name,
		KeyID:       t.ManagedSSHKeyID,
		Name:        t.ManagedSSHKeyName,
		Fingerprint: t.ManagedSSHKeyFingerprint,
		PublicKey:   t.ManagedSSHPublicKey,
		PrivateKey:  t.ManagedSSHPrivateKey,
	}
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
		AccountEmail: t.AccountEmail,
	}
}

func (t *TokenRecord) HasSavedDropletPassword(dropletID int) bool {
	if t == nil || dropletID <= 0 {
		return false
	}
	return t.findDropletCredential(dropletID) != nil
}

func (t *TokenRecord) SavedDropletPasswordUpdatedAt(dropletID int) string {
	credential := t.findDropletCredential(dropletID)
	if credential == nil {
		return ""
	}
	return credential.UpdatedAt
}

func (t *TokenRecord) SaveDropletPassword(dropletID int, dropletName, passwordMode, rootPassword string, updatedAt time.Time) error {
	if t == nil {
		return ErrSavedDropletPasswordNotFound
	}
	if dropletID <= 0 {
		return ErrSavedDropletPasswordNotFound
	}
	rootPassword = strings.TrimSpace(rootPassword)
	if rootPassword == "" {
		return ErrSavedDropletPasswordNotFound
	}

	ciphertext, err := encryptDropletPassword(rootPassword)
	if err != nil {
		return err
	}

	record := DropletCredentialRecord{
		DropletID:          dropletID,
		DropletName:        strings.TrimSpace(dropletName),
		Username:           "root",
		PasswordMode:       strings.TrimSpace(strings.ToLower(passwordMode)),
		PasswordCiphertext: ciphertext,
		UpdatedAt:          updatedAt.UTC().Format(time.RFC3339),
	}

	if existing := t.findDropletCredential(dropletID); existing != nil {
		*existing = record
	} else {
		t.DropletCredentials = append(t.DropletCredentials, record)
	}
	t.DropletCredentials = normalizeDropletCredentials(t.DropletCredentials)
	return nil
}

func (t *TokenRecord) RevealDropletPassword(dropletID int) (*DropletPasswordView, error) {
	credential := t.findDropletCredential(dropletID)
	if credential == nil {
		return nil, ErrSavedDropletPasswordNotFound
	}

	rootPassword, err := decryptDropletPassword(credential.PasswordCiphertext)
	if err != nil {
		return nil, err
	}

	username := credential.Username
	if username == "" {
		username = "root"
	}

	return &DropletPasswordView{
		DropletID:    credential.DropletID,
		DropletName:  credential.DropletName,
		Username:     username,
		PasswordMode: credential.PasswordMode,
		RootPassword: rootPassword,
		UpdatedAt:    credential.UpdatedAt,
	}, nil
}

func (t *TokenRecord) RemoveSavedDropletPassword(dropletID int) bool {
	if t == nil || dropletID <= 0 || len(t.DropletCredentials) == 0 {
		return false
	}

	next := make([]DropletCredentialRecord, 0, len(t.DropletCredentials))
	removed := false
	for _, credential := range t.DropletCredentials {
		if credential.DropletID == dropletID {
			removed = true
			continue
		}
		next = append(next, credential)
	}
	if !removed {
		return false
	}

	t.DropletCredentials = next
	return true
}

func (t *TokenRecord) SyncDropletCredentialName(dropletID int, dropletName string) bool {
	credential := t.findDropletCredential(dropletID)
	if credential == nil {
		return false
	}

	dropletName = strings.TrimSpace(dropletName)
	if credential.DropletName == dropletName {
		return false
	}

	credential.DropletName = dropletName
	return true
}

func (t *TokenRecord) PruneDropletCredentials(validDropletIDs map[int]struct{}) bool {
	if t == nil || len(t.DropletCredentials) == 0 {
		return false
	}

	next := make([]DropletCredentialRecord, 0, len(t.DropletCredentials))
	changed := false
	for _, credential := range t.DropletCredentials {
		if _, exists := validDropletIDs[credential.DropletID]; exists {
			next = append(next, credential)
			continue
		}
		changed = true
	}

	if changed {
		t.DropletCredentials = next
	}
	return changed
}

func (t *TokenRecord) SetCheckResult(checkedAt time.Time, account *Account, err error) {
	if t == nil {
		return
	}

	t.LastCheckedAt = checkedAt.UTC().Format(time.RFC3339)
	if err != nil {
		t.LastStatus = TokenStatusError
		t.LastError = err.Error()
		t.AccountEmail = ""
		t.AccountUUID = ""
		t.DropletLimit = 0
		return
	}

	t.LastStatus = TokenStatusHealthy
	t.LastError = ""
	if account != nil {
		t.AccountEmail = strings.TrimSpace(account.Email)
		t.AccountUUID = strings.TrimSpace(account.UUID)
		t.DropletLimit = account.DropletLimit
	}
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

func (t *TokenRecord) findDropletCredential(dropletID int) *DropletCredentialRecord {
	if t == nil || dropletID <= 0 {
		return nil
	}
	for index := range t.DropletCredentials {
		if t.DropletCredentials[index].DropletID == dropletID {
			return &t.DropletCredentials[index]
		}
	}
	return nil
}

func mergeDropletCredentials(left, right []DropletCredentialRecord) []DropletCredentialRecord {
	if len(left) == 0 {
		return normalizeDropletCredentials(right)
	}
	if len(right) == 0 {
		return normalizeDropletCredentials(left)
	}

	merged := append(append(make([]DropletCredentialRecord, 0, len(left)+len(right)), left...), right...)
	return normalizeDropletCredentials(merged)
}

func normalizeDropletCredentials(records []DropletCredentialRecord) []DropletCredentialRecord {
	if len(records) == 0 {
		return nil
	}

	normalized := make([]DropletCredentialRecord, 0, len(records))
	seen := make(map[int]int, len(records))
	for _, record := range records {
		record.DropletName = strings.TrimSpace(record.DropletName)
		record.Username = strings.TrimSpace(record.Username)
		record.PasswordMode = strings.TrimSpace(strings.ToLower(record.PasswordMode))
		record.PasswordCiphertext = strings.TrimSpace(record.PasswordCiphertext)
		record.UpdatedAt = strings.TrimSpace(record.UpdatedAt)
		if record.DropletID <= 0 || record.PasswordCiphertext == "" {
			continue
		}
		if record.Username == "" {
			record.Username = "root"
		}

		if existingIndex, exists := seen[record.DropletID]; exists {
			normalized[existingIndex] = record
			continue
		}

		seen[record.DropletID] = len(normalized)
		normalized = append(normalized, record)
	}

	if len(normalized) == 0 {
		return nil
	}
	return normalized
}
