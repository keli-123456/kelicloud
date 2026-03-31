package linode

import (
	"errors"
	"os"
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
		Balance: 12.34,
	}, nil)

	require.Equal(t, TokenStatusHealthy, token.LastStatus)
	require.Empty(t, token.LastError)
	require.Equal(t, "komari", token.ProfileUsername)
	require.Equal(t, "ops@example.com", token.ProfileEmail)
	require.Equal(t, "Example Inc", token.AccountCompany)
	require.Equal(t, 12.34, token.AccountBalance)
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
		AccountBalance:  12.34,
	}

	checkErr := errors.New("invalid token")
	token.SetCheckResult(time.Unix(1710000000, 0), nil, nil, checkErr)

	require.Equal(t, TokenStatusError, token.LastStatus)
	require.Equal(t, checkErr.Error(), token.LastError)
	require.Empty(t, token.ProfileUsername)
	require.Empty(t, token.ProfileEmail)
	require.Empty(t, token.AccountCompany)
	require.Zero(t, token.AccountBalance)
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

func TestAdditionRemoveTokenClearsLegacyTokenWhenLastEntryDeleted(t *testing.T) {
	addition := &Addition{
		Token: "linode-token-legacy",
	}

	addition.Normalize()
	require.Len(t, addition.Tokens, 1)

	tokenID := addition.Tokens[0].ID
	require.True(t, addition.RemoveToken(tokenID))
	require.Empty(t, addition.Tokens)
	require.Empty(t, addition.ActiveTokenID)
	require.Empty(t, addition.Token)

	addition.Normalize()
	require.Empty(t, addition.Tokens)
}

func TestTokenRecordSaveInstancePasswordAutoCreatesVaultSecret(t *testing.T) {
	t.Chdir(t.TempDir())

	token := &TokenRecord{
		ID:    "token-1",
		Name:  "primary",
		Token: "linode-token",
	}

	err := token.SaveInstancePassword(2001, "web-01", "custom", "Secret!123", time.Unix(1710000000, 0))
	require.NoError(t, err)

	passwordView, revealErr := token.RevealInstancePassword(2001)
	require.NoError(t, revealErr)
	require.Equal(t, "Secret!123", passwordView.RootPassword)

	_, statErr := os.Stat("./data/cloud_secret.key")
	require.NoError(t, statErr)
}

func TestTokenRecordPruneInstanceCredentialsKeepsRecentMissingCredential(t *testing.T) {
	t.Setenv(RootPasswordVaultKeyEnv, "komari-test-secret")

	token := &TokenRecord{
		ID:    "token-1",
		Name:  "primary",
		Token: "linode-token",
	}

	err := token.SaveInstancePassword(2001, "web-01", "custom", "Secret!123", time.Now().UTC())
	require.NoError(t, err)

	changed := token.PruneInstanceCredentials(map[int]struct{}{})
	require.False(t, changed)
	require.True(t, token.HasSavedInstancePassword(2001))
}

func TestTokenRecordPruneInstanceCredentialsDropsStaleMissingCredential(t *testing.T) {
	t.Setenv(RootPasswordVaultKeyEnv, "komari-test-secret")

	token := &TokenRecord{
		ID:    "token-1",
		Name:  "primary",
		Token: "linode-token",
	}

	err := token.SaveInstancePassword(2001, "web-01", "custom", "Secret!123", time.Now().UTC().Add(-25*time.Hour))
	require.NoError(t, err)

	changed := token.PruneInstanceCredentials(map[int]struct{}{})
	require.True(t, changed)
	require.False(t, token.HasSavedInstancePassword(2001))
}

func TestAdditionMergePersistentStateFromPreservesSavedInstancePassword(t *testing.T) {
	t.Setenv(RootPasswordVaultKeyEnv, "komari-test-secret")

	previous := &Addition{
		Tokens: []TokenRecord{
			{
				ID:    "token-1",
				Name:  "primary",
				Token: "linode-token",
			},
		},
	}
	require.NoError(t, previous.Tokens[0].SaveInstancePassword(2001, "web-01", "custom", "Secret!123", time.Unix(1710000000, 0)))

	current := &Addition{
		ActiveTokenID: "token-1",
		Tokens: []TokenRecord{
			{
				ID:    "token-1",
				Name:  "primary",
				Token: "linode-token",
			},
		},
	}

	current.MergePersistentStateFrom(previous)

	require.True(t, current.Tokens[0].HasSavedInstancePassword(2001))
	passwordView, err := current.Tokens[0].RevealInstancePassword(2001)
	require.NoError(t, err)
	require.Equal(t, "Secret!123", passwordView.RootPassword)
}

func TestAdditionRevealInstancePasswordAcrossTokens(t *testing.T) {
	t.Setenv(RootPasswordVaultKeyEnv, "komari-test-secret")

	addition := &Addition{
		ActiveTokenID: "token-1",
		Tokens: []TokenRecord{
			{
				ID:    "token-1",
				Name:  "active",
				Token: "linode-active",
			},
			{
				ID:    "token-2",
				Name:  "secondary",
				Token: "linode-secondary",
			},
		},
	}
	require.NoError(t, addition.Tokens[1].SaveInstancePassword(2001, "web-01", "custom", "Secret!123", time.Unix(1710000000, 0)))

	require.True(t, addition.HasSavedInstancePassword(2001))
	require.NotEmpty(t, addition.SavedInstancePasswordUpdatedAt(2001))

	passwordView, err := addition.RevealInstancePassword(2001)
	require.NoError(t, err)
	require.Equal(t, "Secret!123", passwordView.RootPassword)
}
