package digitalocean

import (
	"errors"
	"os"
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

func TestAdditionUpsertTokensGeneratesUniqueDefaultNames(t *testing.T) {
	addition := &Addition{
		Tokens: []TokenRecord{
			{
				ID:    "token-1",
				Name:  "Token 1",
				Token: "dop_v1_existing",
			},
		},
	}

	count := addition.UpsertTokens([]TokenImport{
		{Token: "dop_v1_second"},
		{Token: "dop_v1_third"},
	})

	require.Equal(t, 2, count)
	require.Len(t, addition.Tokens, 3)
	require.Equal(t, "Token 1", addition.Tokens[0].Name)
	require.Equal(t, "Token 2", addition.Tokens[1].Name)
	require.Equal(t, "Token 3", addition.Tokens[2].Name)
}

func TestAdditionNormalizeMigratesLegacyManagedSSHKeyToSharedMaterial(t *testing.T) {
	addition := &Addition{
		Tokens: []TokenRecord{
			{
				ID:                       "token-1",
				Name:                     "primary",
				Token:                    "dop_v1_token",
				AccountUUID:              "account-1",
				AccountEmail:             "ops@example.com",
				ManagedSSHKeyID:          101,
				ManagedSSHKeyName:        "komari-token-1",
				ManagedSSHKeyFingerprint: "fp-1",
				ManagedSSHPrivateKey:     "private-key",
				ManagedSSHPublicKey:      "ssh-ed25519 AAAATEST",
			},
		},
	}

	addition.Normalize()

	require.True(t, addition.HasManagedSSHKeyMaterial())
	require.Equal(t, "private-key", addition.ManagedSSHPrivateKey)
	require.Equal(t, "ssh-ed25519 AAAATEST", addition.ManagedSSHPublicKey)

	material := addition.ManagedSSHKeyMaterialViewForToken(&addition.Tokens[0])
	require.NotNil(t, material)
	require.Equal(t, 101, material.KeyID)
	require.Equal(t, "komari-token-1", material.Name)
	require.Equal(t, "fp-1", material.Fingerprint)
	require.Equal(t, "private-key", material.PrivateKey)
	require.Equal(t, "ssh-ed25519 AAAATEST", material.PublicKey)
}

func TestAdditionToPoolViewMarksSharedManagedSSHKeyReadyForAllTokens(t *testing.T) {
	addition := &Addition{
		ManagedSSHKeyName:    "komari-managed",
		ManagedSSHPrivateKey: "private-key",
		ManagedSSHPublicKey:  "ssh-ed25519 AAAATEST",
		Tokens: []TokenRecord{
			{
				ID:           "token-1",
				Name:         "primary",
				Token:        "dop_v1_token_1",
				AccountUUID:  "account-1",
				AccountEmail: "ops@example.com",
			},
			{
				ID:           "token-2",
				Name:         "secondary",
				Token:        "dop_v1_token_2",
				AccountUUID:  "account-2",
				AccountEmail: "ops2@example.com",
			},
		},
	}

	view := addition.ToPoolView()

	require.Len(t, view.Tokens, 2)
	require.True(t, view.Tokens[0].ManagedSSHKeyReady)
	require.True(t, view.Tokens[1].ManagedSSHKeyReady)
	require.Equal(t, "komari-managed", view.Tokens[0].ManagedSSHKeyName)
	require.Equal(t, "komari-managed", view.Tokens[1].ManagedSSHKeyName)
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

func TestTokenRecordSetCheckResultMarksLockedAccountAsError(t *testing.T) {
	token := &TokenRecord{
		ID:    "token-1",
		Name:  "primary",
		Token: "dop_v1_token",
	}

	token.SetCheckResult(time.Unix(1710000000, 0), &Account{
		StatusMessage: "There is currently a lock on the account, please log in to the control panel and contact support.",
	}, nil)

	require.Equal(t, TokenStatusError, token.LastStatus)
	require.Equal(t, "There is currently a lock on the account, please log in to the control panel and contact support.", token.LastError)
}

func TestGenerateManagedSSHKeyPair(t *testing.T) {
	material, err := GenerateManagedSSHKeyPair("komari-test")

	require.NoError(t, err)
	require.Equal(t, "komari-test", material.Name)
	require.Contains(t, material.PublicKey, "ssh-ed25519 ")
	require.Contains(t, material.PrivateKey, "BEGIN PRIVATE KEY")
}

func TestBuildRootPasswordUserData(t *testing.T) {
	userData, err := BuildRootPasswordUserData("Secret!123", "echo ready")

	require.NoError(t, err)
	require.Contains(t, userData, "root:Secret!123")
	require.Contains(t, userData, "PasswordAuthentication yes")
	require.Contains(t, userData, "echo ready")

	_, err = BuildRootPasswordUserData("Secret!123", "#cloud-config\nusers: []")
	require.Error(t, err)
}

func TestDropletPasswordVaultRoundTrip(t *testing.T) {
	t.Setenv(DropletPasswordVaultKeyEnv, "komari-test-secret")

	ciphertext, err := encryptDropletPassword("Secret!123")
	require.NoError(t, err)
	require.NotEmpty(t, ciphertext)

	plaintext, err := decryptDropletPassword(ciphertext)
	require.NoError(t, err)
	require.Equal(t, "Secret!123", plaintext)
}

func TestTokenRecordSaveAndRevealDropletPassword(t *testing.T) {
	t.Setenv(DropletPasswordVaultKeyEnv, "komari-test-secret")

	token := &TokenRecord{
		ID:    "token-1",
		Name:  "primary",
		Token: "dop_v1_token",
	}

	err := token.SaveDropletPassword(1001, "web-01", "custom", "Secret!123", time.Unix(1710000000, 0))
	require.NoError(t, err)
	require.True(t, token.HasSavedDropletPassword(1001))
	require.NotEmpty(t, token.SavedDropletPasswordUpdatedAt(1001))

	passwordView, err := token.RevealDropletPassword(1001)
	require.NoError(t, err)
	require.Equal(t, 1001, passwordView.DropletID)
	require.Equal(t, "web-01", passwordView.DropletName)
	require.Equal(t, "root", passwordView.Username)
	require.Equal(t, "custom", passwordView.PasswordMode)
	require.Equal(t, "Secret!123", passwordView.RootPassword)
}

func TestTokenRecordSaveDropletPasswordAutoCreatesVaultSecret(t *testing.T) {
	t.Chdir(t.TempDir())

	token := &TokenRecord{
		ID:    "token-1",
		Name:  "primary",
		Token: "dop_v1_token",
	}

	err := token.SaveDropletPassword(1001, "web-01", "custom", "Secret!123", time.Unix(1710000000, 0))
	require.NoError(t, err)

	passwordView, revealErr := token.RevealDropletPassword(1001)
	require.NoError(t, revealErr)
	require.Equal(t, "Secret!123", passwordView.RootPassword)

	_, statErr := os.Stat("./data/cloud_secret.key")
	require.NoError(t, statErr)
}
