package linode

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTokenRecordSetCheckResultStoresProfileAndAccount(t *testing.T) {
	token := &TokenRecord{
		ID:    "token-1",
		Name:  "primary",
		Token: "linode-token",
	}

	token.SetCheckResult(time.Unix(1710000000, 0), &Profile{
		Username: "komari",
		Email:    "ops@example.com",
	}, &Account{
		Company: "Example Inc",
	}, nil)

	require.Equal(t, TokenStatusHealthy, token.LastStatus)
	require.Empty(t, token.LastError)
	require.Equal(t, "komari", token.ProfileUsername)
	require.Equal(t, "ops@example.com", token.ProfileEmail)
	require.Equal(t, "Example Inc", token.AccountCompany)
}

func TestTokenRecordSetCheckResultMarksRestrictedProfileAsError(t *testing.T) {
	token := &TokenRecord{
		ID:    "token-1",
		Name:  "primary",
		Token: "linode-token",
	}

	token.SetCheckResult(time.Unix(1710000000, 0), &Profile{
		Restricted: true,
	}, nil, nil)

	require.Equal(t, TokenStatusError, token.LastStatus)
	require.Equal(t, "linode profile is restricted", token.LastError)
}

func TestTokenRecordSetCheckResultClearsFieldsOnError(t *testing.T) {
	token := &TokenRecord{
		ID:              "token-1",
		Name:            "primary",
		Token:           "linode-token",
		ProfileUsername: "komari",
		ProfileEmail:    "ops@example.com",
		AccountCompany:  "Example Inc",
	}

	checkErr := errors.New("invalid token")
	token.SetCheckResult(time.Unix(1710000000, 0), nil, nil, checkErr)

	require.Equal(t, TokenStatusError, token.LastStatus)
	require.Equal(t, checkErr.Error(), token.LastError)
	require.Empty(t, token.ProfileUsername)
	require.Empty(t, token.ProfileEmail)
	require.Empty(t, token.AccountCompany)
}

func TestAdditionUpsertTokensGeneratesUniqueDefaultNames(t *testing.T) {
	addition := &Addition{
		Tokens: []TokenRecord{
			{
				ID:    "token-1",
				Name:  "Token 1",
				Token: "linode-token-1",
			},
		},
	}

	count := addition.UpsertTokens([]TokenImport{
		{Token: "linode-token-2"},
		{Token: "linode-token-3"},
	})

	require.Equal(t, 2, count)
	require.Len(t, addition.Tokens, 3)
	require.Equal(t, "Token 1", addition.Tokens[0].Name)
	require.Equal(t, "Token 2", addition.Tokens[1].Name)
	require.Equal(t, "Token 3", addition.Tokens[2].Name)
}
