package digitalocean

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAdditionNormalizeMigratesLegacyToken(t *testing.T) {
	addition := &Addition{
		Token: "dop_v1_legacy",
	}

	addition.Normalize()

	require.Len(t, addition.Tokens, 1)
	require.NotEmpty(t, addition.ActiveTokenID)
	require.Equal(t, addition.ActiveTokenID, addition.Tokens[0].ID)
	require.Equal(t, "dop_v1_legacy", addition.Tokens[0].Token)
	require.Equal(t, TokenStatusUnknown, addition.Tokens[0].LastStatus)
}

func TestAdditionUpsertTokensDeduplicatesByToken(t *testing.T) {
	addition := &Addition{
		Tokens: []TokenRecord{
			{
				ID:    "token-1",
				Name:  "old-name",
				Token: "dop_v1_same",
			},
		},
	}

	count := addition.UpsertTokens([]TokenImport{
		{Name: "new-name", Token: "dop_v1_same"},
		{Name: "another", Token: "dop_v1_other"},
	})

	require.Equal(t, 2, count)
	require.Len(t, addition.Tokens, 2)
	require.Equal(t, "new-name", addition.Tokens[0].Name)
	require.Equal(t, "dop_v1_same", addition.Tokens[0].Token)
	require.Equal(t, "another", addition.Tokens[1].Name)
	require.NotEmpty(t, addition.ActiveTokenID)
}

func TestTokenRecordSetCheckResult(t *testing.T) {
	token := &TokenRecord{
		ID:    "token-1",
		Name:  "primary",
		Token: "dop_v1_token",
	}

	token.SetCheckResult(time.Unix(1710000000, 0), &Account{
		UUID:         "account-uuid",
		Email:        "ops@example.com",
		DropletLimit: 15,
	}, nil)

	require.Equal(t, TokenStatusHealthy, token.LastStatus)
	require.Equal(t, "ops@example.com", token.AccountEmail)
	require.Equal(t, "account-uuid", token.AccountUUID)
	require.Equal(t, 15, token.DropletLimit)
	require.NotEmpty(t, token.LastCheckedAt)

	checkErr := errors.New("invalid token")
	token.SetCheckResult(time.Unix(1710000600, 0), nil, checkErr)

	require.Equal(t, TokenStatusError, token.LastStatus)
	require.Equal(t, checkErr.Error(), token.LastError)
	require.Empty(t, token.AccountEmail)
	require.Empty(t, token.AccountUUID)
	require.Zero(t, token.DropletLimit)
}
