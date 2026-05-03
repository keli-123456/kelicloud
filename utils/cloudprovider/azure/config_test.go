package azure

import "testing"

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
