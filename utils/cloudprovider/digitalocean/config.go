package digitalocean

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

	missingDropletCredentialRetention = 24 * time.Hour
)

type Addition struct {
	Token                    string                       `json:"token,omitempty"`
	ActiveTokenID            string                       `json:"active_token_id,omitempty"`
	ManagedSSHKeyName        string                       `json:"managed_ssh_key_name,omitempty"`
	ManagedSSHKeyFingerprint string                       `json:"managed_ssh_key_fingerprint,omitempty"`
	ManagedSSHPrivateKey     string                       `json:"managed_ssh_private_key,omitempty"`
	ManagedSSHPublicKey      string                       `json:"managed_ssh_public_key,omitempty"`
	ManagedSSHAccounts       []ManagedSSHKeyAccountRecord `json:"managed_ssh_accounts,omitempty"`
	Tokens                   []TokenRecord                `json:"tokens,omitempty"`
}

type ManagedSSHKeyAccountRecord struct {
	AccountUUID  string `json:"account_uuid,omitempty"`
	AccountEmail string `json:"account_email,omitempty"`
	KeyID        int    `json:"key_id,omitempty"`
	KeyName      string `json:"key_name,omitempty"`
	Fingerprint  string `json:"fingerprint,omitempty"`
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
	a.ManagedSSHKeyName = strings.TrimSpace(a.ManagedSSHKeyName)
	a.ManagedSSHKeyFingerprint = strings.TrimSpace(a.ManagedSSHKeyFingerprint)
	a.ManagedSSHPrivateKey = strings.TrimSpace(a.ManagedSSHPrivateKey)
	a.ManagedSSHPublicKey = strings.TrimSpace(a.ManagedSSHPublicKey)
	a.ManagedSSHAccounts = normalizeManagedSSHKeyAccounts(a.ManagedSSHAccounts)

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

	if !a.HasManagedSSHKeyMaterial() {
		for _, token := range a.Tokens {
			if strings.TrimSpace(token.ManagedSSHPrivateKey) == "" || strings.TrimSpace(token.ManagedSSHPublicKey) == "" {
				continue
			}

			a.ManagedSSHPrivateKey = strings.TrimSpace(token.ManagedSSHPrivateKey)
			a.ManagedSSHPublicKey = strings.TrimSpace(token.ManagedSSHPublicKey)
			a.ManagedSSHKeyName = firstNonEmpty(
				a.ManagedSSHKeyName,
				strings.TrimSpace(token.ManagedSSHKeyName),
				ManagedSSHKeyName(nil),
			)
			a.ManagedSSHKeyFingerprint = firstNonEmpty(
				a.ManagedSSHKeyFingerprint,
				strings.TrimSpace(token.ManagedSSHKeyFingerprint),
			)
			break
		}
	}
	if a.HasManagedSSHKeyMaterial() && a.ManagedSSHKeyName == "" {
		a.ManagedSSHKeyName = ManagedSSHKeyName(nil)
	}
	if a.HasManagedSSHKeyMaterial() {
		for _, token := range a.Tokens {
			if token.ManagedSSHKeyID <= 0 {
				continue
			}
			if token.AccountUUID == "" && token.AccountEmail == "" {
				continue
			}
			if token.ManagedSSHPublicKey != "" && strings.TrimSpace(token.ManagedSSHPublicKey) != a.ManagedSSHPublicKey {
				continue
			}
			a.upsertManagedSSHKeyAccountRecord(ManagedSSHKeyAccountRecord{
				AccountUUID:  token.AccountUUID,
				AccountEmail: token.AccountEmail,
				KeyID:        token.ManagedSSHKeyID,
				KeyName:      firstNonEmpty(token.ManagedSSHKeyName, a.ManagedSSHKeyName),
				Fingerprint:  firstNonEmpty(token.ManagedSSHKeyFingerprint, a.ManagedSSHKeyFingerprint),
			})
			if a.ManagedSSHKeyFingerprint == "" && token.ManagedSSHKeyFingerprint != "" {
				a.ManagedSSHKeyFingerprint = strings.TrimSpace(token.ManagedSSHKeyFingerprint)
			}
		}
		a.ManagedSSHAccounts = normalizeManagedSSHKeyAccounts(a.ManagedSSHAccounts)
	}

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

func (a *Addition) MergePersistentStateFrom(previous *Addition) {
	if a == nil || previous == nil {
		return
	}

	a.Normalize()
	previous.Normalize()

	a.ManagedSSHKeyName = firstNonEmpty(a.ManagedSSHKeyName, previous.ManagedSSHKeyName)
	a.ManagedSSHKeyFingerprint = firstNonEmpty(a.ManagedSSHKeyFingerprint, previous.ManagedSSHKeyFingerprint)
	a.ManagedSSHPrivateKey = firstNonEmpty(a.ManagedSSHPrivateKey, previous.ManagedSSHPrivateKey)
	a.ManagedSSHPublicKey = firstNonEmpty(a.ManagedSSHPublicKey, previous.ManagedSSHPublicKey)
	a.ManagedSSHAccounts = normalizeManagedSSHKeyAccounts(
		append(
			append(
				make([]ManagedSSHKeyAccountRecord, 0, len(previous.ManagedSSHAccounts)+len(a.ManagedSSHAccounts)),
				previous.ManagedSSHAccounts...,
			),
			a.ManagedSSHAccounts...,
		),
	)

	for index := range a.Tokens {
		previousToken := previous.findTokenForPersistentStateMerge(a.Tokens[index].ID, a.Tokens[index].Token)
		if previousToken == nil {
			continue
		}

		if a.Tokens[index].ManagedSSHKeyID <= 0 {
			a.Tokens[index].ManagedSSHKeyID = previousToken.ManagedSSHKeyID
		}
		a.Tokens[index].ManagedSSHKeyName = firstNonEmpty(
			a.Tokens[index].ManagedSSHKeyName,
			previousToken.ManagedSSHKeyName,
		)
		a.Tokens[index].ManagedSSHKeyFingerprint = firstNonEmpty(
			a.Tokens[index].ManagedSSHKeyFingerprint,
			previousToken.ManagedSSHKeyFingerprint,
		)
		a.Tokens[index].ManagedSSHPrivateKey = firstNonEmpty(
			a.Tokens[index].ManagedSSHPrivateKey,
			previousToken.ManagedSSHPrivateKey,
		)
		a.Tokens[index].ManagedSSHPublicKey = firstNonEmpty(
			a.Tokens[index].ManagedSSHPublicKey,
			previousToken.ManagedSSHPublicKey,
		)
		a.Tokens[index].DropletCredentials = mergeDropletCredentials(
			previousToken.DropletCredentials,
			a.Tokens[index].DropletCredentials,
		)
	}

	a.Normalize()
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
			if name != "" {
				matched.Name = name
			}
			matched.Token = tokenValue
		} else {
			if name == "" {
				name = nextGeneratedTokenName(a.Tokens)
			}
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
		material := a.ManagedSSHKeyMaterialViewForToken(&token)
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
			ManagedSSHKeyName:        managedSSHKeyNameFromMaterial(material),
			ManagedSSHKeyFingerprint: managedSSHKeyFingerprintFromMaterial(material),
			ManagedSSHKeyReady:       material != nil,
			IsActive:                 token.ID == a.ActiveTokenID,
		})
	}
	return view
}

func (a *Addition) HasManagedSSHKeyMaterial() bool {
	if a == nil {
		return false
	}
	return strings.TrimSpace(a.ManagedSSHPrivateKey) != "" && strings.TrimSpace(a.ManagedSSHPublicKey) != ""
}

func (a *Addition) ManagedSSHKeyMaterialViewForToken(token *TokenRecord) *ManagedSSHKeyMaterialView {
	if a != nil && a.HasManagedSSHKeyMaterial() {
		view := &ManagedSSHKeyMaterialView{
			Name:        firstNonEmpty(a.ManagedSSHKeyName, ManagedSSHKeyName(nil)),
			Fingerprint: a.ManagedSSHKeyFingerprint,
			PublicKey:   a.ManagedSSHPublicKey,
			PrivateKey:  a.ManagedSSHPrivateKey,
		}
		if token != nil {
			view.TokenID = token.ID
			view.TokenName = token.Name
			if account := a.FindManagedSSHKeyAccount(token.AccountUUID, token.AccountEmail); account != nil {
				view.KeyID = account.KeyID
				view.Name = firstNonEmpty(account.KeyName, view.Name)
				view.Fingerprint = firstNonEmpty(account.Fingerprint, view.Fingerprint)
			}
		}
		return view
	}
	if token != nil && token.HasManagedSSHKeyMaterial() {
		return token.ManagedSSHKeyMaterialView()
	}
	return nil
}

func (a *Addition) FindManagedSSHKeyAccount(accountUUID, accountEmail string) *ManagedSSHKeyAccountRecord {
	if a == nil || len(a.ManagedSSHAccounts) == 0 {
		return nil
	}

	lookupKey := managedSSHKeyAccountLookupKey(accountUUID, accountEmail)
	if lookupKey == "" {
		return nil
	}

	for index := range a.ManagedSSHAccounts {
		if managedSSHKeyAccountLookupKey(a.ManagedSSHAccounts[index].AccountUUID, a.ManagedSSHAccounts[index].AccountEmail) == lookupKey {
			return &a.ManagedSSHAccounts[index]
		}
	}
	return nil
}

func (a *Addition) UpsertManagedSSHKeyAccount(accountUUID, accountEmail string, sshKey *SSHKey) bool {
	if a == nil || sshKey == nil || sshKey.ID <= 0 {
		return false
	}
	return a.upsertManagedSSHKeyAccountRecord(ManagedSSHKeyAccountRecord{
		AccountUUID:  accountUUID,
		AccountEmail: accountEmail,
		KeyID:        sshKey.ID,
		KeyName:      sshKey.Name,
		Fingerprint:  sshKey.Fingerprint,
	})
}

func (a *Addition) upsertManagedSSHKeyAccountRecord(record ManagedSSHKeyAccountRecord) bool {
	if a == nil {
		return false
	}

	record, ok := normalizeManagedSSHKeyAccountRecord(record)
	if !ok {
		return false
	}

	if existing := a.FindManagedSSHKeyAccount(record.AccountUUID, record.AccountEmail); existing != nil {
		changed := false
		if existing.KeyID != record.KeyID {
			existing.KeyID = record.KeyID
			changed = true
		}
		if existing.KeyName != record.KeyName {
			existing.KeyName = record.KeyName
			changed = true
		}
		if existing.Fingerprint != record.Fingerprint {
			existing.Fingerprint = record.Fingerprint
			changed = true
		}
		if existing.AccountUUID != record.AccountUUID {
			existing.AccountUUID = record.AccountUUID
			changed = true
		}
		if existing.AccountEmail != record.AccountEmail {
			existing.AccountEmail = record.AccountEmail
			changed = true
		}
		return changed
	}

	a.ManagedSSHAccounts = append(a.ManagedSSHAccounts, record)
	return true
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

	cutoff := time.Now().UTC().Add(-missingDropletCredentialRetention)
	next := make([]DropletCredentialRecord, 0, len(t.DropletCredentials))
	changed := false
	for _, credential := range t.DropletCredentials {
		if _, exists := validDropletIDs[credential.DropletID]; exists {
			next = append(next, credential)
			continue
		}
		if shouldRetainMissingDropletCredential(credential, cutoff) {
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
	if err == nil {
		err = accountAvailabilityError(account)
	}
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

func accountAvailabilityError(account *Account) error {
	if account == nil {
		return nil
	}

	status := strings.ToLower(strings.TrimSpace(account.Status))
	message := strings.TrimSpace(account.StatusMessage)
	if status == "locked" {
		if message == "" {
			message = "digitalocean account is locked"
		}
		return errors.New(message)
	}

	lowerMessage := strings.ToLower(message)
	if strings.Contains(lowerMessage, "lock on the account") || strings.Contains(lowerMessage, "contact support") {
		return errors.New(message)
	}

	return nil
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

func normalizeManagedSSHKeyAccounts(records []ManagedSSHKeyAccountRecord) []ManagedSSHKeyAccountRecord {
	if len(records) == 0 {
		return nil
	}

	normalized := make([]ManagedSSHKeyAccountRecord, 0, len(records))
	seen := make(map[string]int, len(records))
	for _, record := range records {
		record, ok := normalizeManagedSSHKeyAccountRecord(record)
		if !ok {
			continue
		}

		key := managedSSHKeyAccountLookupKey(record.AccountUUID, record.AccountEmail)
		if existingIndex, exists := seen[key]; exists {
			merged := normalized[existingIndex]
			merged.KeyID = record.KeyID
			merged.KeyName = firstNonEmpty(record.KeyName, merged.KeyName)
			merged.Fingerprint = firstNonEmpty(record.Fingerprint, merged.Fingerprint)
			merged.AccountUUID = firstNonEmpty(record.AccountUUID, merged.AccountUUID)
			merged.AccountEmail = firstNonEmpty(record.AccountEmail, merged.AccountEmail)
			normalized[existingIndex] = merged
			continue
		}

		seen[key] = len(normalized)
		normalized = append(normalized, record)
	}

	return normalized
}

func normalizeManagedSSHKeyAccountRecord(record ManagedSSHKeyAccountRecord) (ManagedSSHKeyAccountRecord, bool) {
	record.AccountUUID = strings.TrimSpace(record.AccountUUID)
	record.AccountEmail = strings.ToLower(strings.TrimSpace(record.AccountEmail))
	record.KeyName = strings.TrimSpace(record.KeyName)
	record.Fingerprint = strings.TrimSpace(record.Fingerprint)
	if record.KeyID <= 0 {
		return ManagedSSHKeyAccountRecord{}, false
	}
	if record.AccountUUID == "" && record.AccountEmail == "" {
		return ManagedSSHKeyAccountRecord{}, false
	}
	return record, true
}

func managedSSHKeyAccountLookupKey(accountUUID, accountEmail string) string {
	accountUUID = strings.TrimSpace(accountUUID)
	if accountUUID != "" {
		return "uuid:" + accountUUID
	}

	accountEmail = strings.ToLower(strings.TrimSpace(accountEmail))
	if accountEmail != "" {
		return "email:" + accountEmail
	}

	return ""
}

func managedSSHKeyNameFromMaterial(material *ManagedSSHKeyMaterialView) string {
	if material == nil {
		return ""
	}
	return strings.TrimSpace(material.Name)
}

func managedSSHKeyFingerprintFromMaterial(material *ManagedSSHKeyMaterialView) string {
	if material == nil {
		return ""
	}
	return strings.TrimSpace(material.Fingerprint)
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

func shouldRetainMissingDropletCredential(record DropletCredentialRecord, cutoff time.Time) bool {
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
