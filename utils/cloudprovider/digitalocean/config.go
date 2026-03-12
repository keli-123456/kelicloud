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
	ID            string `json:"id"`
	Name          string `json:"name"`
	Token         string `json:"token"`
	AccountEmail  string `json:"account_email,omitempty"`
	AccountUUID   string `json:"account_uuid,omitempty"`
	DropletLimit  int    `json:"droplet_limit,omitempty"`
	LastStatus    string `json:"last_status,omitempty"`
	LastError     string `json:"last_error,omitempty"`
	LastCheckedAt string `json:"last_checked_at,omitempty"`
}

type TokenImport struct {
	ID    string `json:"id,omitempty"`
	Name  string `json:"name"`
	Token string `json:"token"`
}

type TokenView struct {
	ID            string `json:"id"`
	Name          string `json:"name"`
	MaskedToken   string `json:"masked_token"`
	AccountEmail  string `json:"account_email,omitempty"`
	AccountUUID   string `json:"account_uuid,omitempty"`
	DropletLimit  int    `json:"droplet_limit,omitempty"`
	LastStatus    string `json:"last_status"`
	LastError     string `json:"last_error,omitempty"`
	LastCheckedAt string `json:"last_checked_at,omitempty"`
	IsActive      bool   `json:"is_active"`
}

type TokenPoolView struct {
	ActiveTokenID string      `json:"active_token_id,omitempty"`
	Tokens        []TokenView `json:"tokens"`
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
		ActiveTokenID: a.ActiveTokenID,
		Tokens:        make([]TokenView, 0, len(a.Tokens)),
	}
	for _, token := range a.Tokens {
		view.Tokens = append(view.Tokens, TokenView{
			ID:            token.ID,
			Name:          token.Name,
			MaskedToken:   maskToken(token.Token),
			AccountEmail:  token.AccountEmail,
			AccountUUID:   token.AccountUUID,
			DropletLimit:  token.DropletLimit,
			LastStatus:    normalizeTokenStatus(token.LastStatus),
			LastError:     token.LastError,
			LastCheckedAt: token.LastCheckedAt,
			IsActive:      token.ID == a.ActiveTokenID,
		})
	}
	return view
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
