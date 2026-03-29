package clientddns

import (
	"errors"
	"testing"

	"github.com/komari-monitor/komari/database/models"
	"gorm.io/gorm"
)

func TestLoadClientForBindingFallsBackToOwnerlessClient(t *testing.T) {
	originalScoped := getClientByUUIDForUserFunc
	originalUnscoped := getClientByUUIDFunc
	t.Cleanup(func() {
		getClientByUUIDForUserFunc = originalScoped
		getClientByUUIDFunc = originalUnscoped
	})

	getClientByUUIDForUserFunc = func(uuid, userUUID string) (models.Client, error) {
		return models.Client{}, gorm.ErrRecordNotFound
	}
	getClientByUUIDFunc = func(uuid string) (models.Client, error) {
		return models.Client{UUID: uuid, UserID: "", IPv4: "1.1.1.1"}, nil
	}

	client, err := loadClientForBinding("node-a", "user-a")
	if err != nil {
		t.Fatalf("loadClientForBinding returned error: %v", err)
	}
	if client.UUID != "node-a" {
		t.Fatalf("expected ownerless fallback client, got %+v", client)
	}
}

func TestLoadClientForBindingRejectsDifferentOwner(t *testing.T) {
	originalScoped := getClientByUUIDForUserFunc
	originalUnscoped := getClientByUUIDFunc
	t.Cleanup(func() {
		getClientByUUIDForUserFunc = originalScoped
		getClientByUUIDFunc = originalUnscoped
	})

	getClientByUUIDForUserFunc = func(uuid, userUUID string) (models.Client, error) {
		return models.Client{}, gorm.ErrRecordNotFound
	}
	getClientByUUIDFunc = func(uuid string) (models.Client, error) {
		return models.Client{UUID: uuid, UserID: "user-b"}, nil
	}

	if _, err := loadClientForBinding("node-a", "user-a"); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("expected record not found error when client belongs to another user, got %v", err)
	}
}

func TestLoadClientForBindingPropagatesScopedLookupError(t *testing.T) {
	originalScoped := getClientByUUIDForUserFunc
	originalUnscoped := getClientByUUIDFunc
	t.Cleanup(func() {
		getClientByUUIDForUserFunc = originalScoped
		getClientByUUIDFunc = originalUnscoped
	})

	getClientByUUIDForUserFunc = func(uuid, userUUID string) (models.Client, error) {
		return models.Client{}, errors.New("db down")
	}
	getClientByUUIDFunc = func(uuid string) (models.Client, error) {
		t.Fatalf("unexpected fallback lookup for %s", uuid)
		return models.Client{}, nil
	}

	if _, err := loadClientForBinding("node-a", "user-a"); err == nil || err.Error() != "db down" {
		t.Fatalf("expected scoped lookup error to be returned, got %v", err)
	}
}
