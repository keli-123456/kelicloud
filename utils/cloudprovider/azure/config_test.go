package azure

import (
	"testing"
	"time"
)

func TestAdditionUpsertCredentialsUpdatesGroupByIDWithoutSecrets(t *testing.T) {
	addition := &Addition{
		Credentials: []CredentialRecord{
			{
				ID:              "cred-1",
				Name:            "Primary",
				Group:           "old",
				TenantID:        "tenant",
				ClientID:        "client",
				ClientSecret:    "secret",
				SubscriptionID:  "subscription",
				DefaultLocation: "eastus",
			},
		},
		ActiveCredentialID: "cred-1",
	}

	updated := addition.UpsertCredentials([]CredentialImport{
		{
			ID:              "cred-1",
			Name:            "Primary",
			Group:           "new",
			DefaultLocation: "westus",
		},
	})

	if updated != 1 {
		t.Fatalf("expected one credential update, got %d", updated)
	}
	credential := addition.FindCredential("cred-1")
	if credential == nil {
		t.Fatal("expected credential to remain available")
	}
	if credential.Group != "new" {
		t.Fatalf("expected group to update, got %q", credential.Group)
	}
	if credential.ClientSecret != "secret" {
		t.Fatalf("expected secret to be preserved, got %q", credential.ClientSecret)
	}
	if credential.DefaultLocation != "westus" {
		t.Fatalf("expected default location to update, got %q", credential.DefaultLocation)
	}
}

func TestAdditionUpsertCredentialsAcceptsAzureCLIServicePrincipalJSON(t *testing.T) {
	addition := &Addition{}

	updated := addition.UpsertCredentials([]CredentialImport{
		{
			AppID:       "00000000-0000-4000-8000-000000000001",
			DisplayName: "azure-cli-test",
			Password:    "client-secret",
			Tenant:      "00000000-0000-4000-8000-000000000002",
		},
	})

	if updated != 1 {
		t.Fatalf("expected one credential import, got %d", updated)
	}
	if len(addition.Credentials) != 1 {
		t.Fatalf("expected one credential, got %d", len(addition.Credentials))
	}
	credential := addition.Credentials[0]
	if credential.Name != "azure-cli-test" {
		t.Fatalf("expected displayName to become credential name, got %q", credential.Name)
	}
	if credential.TenantID != "00000000-0000-4000-8000-000000000002" {
		t.Fatalf("expected tenant from Azure CLI JSON, got %q", credential.TenantID)
	}
	if credential.ClientID != "00000000-0000-4000-8000-000000000001" {
		t.Fatalf("expected appId to become client id, got %q", credential.ClientID)
	}
	if credential.SubscriptionID != "" {
		t.Fatalf("expected subscription id to be optional, got %q", credential.SubscriptionID)
	}
}

func TestCredentialCheckResultStoresDiscoveredSubscriptionID(t *testing.T) {
	credential := &CredentialRecord{
		TenantID:     "tenant",
		ClientID:     "client",
		ClientSecret: "secret",
	}

	credential.SetCheckResult(testTime(), &Subscription{
		SubscriptionID: "subscription",
		DisplayName:    "Production",
		State:          "Enabled",
	}, nil)

	if credential.SubscriptionID != "subscription" {
		t.Fatalf("expected discovered subscription id to be stored, got %q", credential.SubscriptionID)
	}
	if credential.SubscriptionDisplayName != "Production" {
		t.Fatalf("expected subscription display name, got %q", credential.SubscriptionDisplayName)
	}
	if credential.LastStatus != CredentialStatusHealthy {
		t.Fatalf("expected healthy status, got %q", credential.LastStatus)
	}
}

func TestAdditionToPoolViewIncludesCredentialGroup(t *testing.T) {
	addition := &Addition{
		Credentials: []CredentialRecord{
			{
				ID:             "cred-1",
				Name:           "Primary",
				Group:          "prod",
				TenantID:       "tenant",
				ClientID:       "client",
				ClientSecret:   "secret",
				SubscriptionID: "subscription",
			},
		},
		ActiveCredentialID: "cred-1",
	}

	view := addition.ToPoolView()
	if len(view.Credentials) != 1 {
		t.Fatalf("expected one credential, got %d", len(view.Credentials))
	}
	if view.Credentials[0].Group != "prod" {
		t.Fatalf("expected group in pool view, got %q", view.Credentials[0].Group)
	}
}

func testTime() time.Time {
	return time.Date(2026, 5, 9, 0, 0, 0, 0, time.UTC)
}
