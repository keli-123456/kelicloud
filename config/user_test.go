package config

import (
	"testing"
	"time"
)

func TestUserScopedConfigIsolation(t *testing.T) {
	setupConfigTestDB(t)

	if err := Set(AllowCorsKey, true); err != nil {
		t.Fatalf("failed to seed global allow_cors: %v", err)
	}
	if err := SetForUser("user-a", AutoDiscoveryKeyKey, "key-a-123456"); err != nil {
		t.Fatalf("failed to set user A auto discovery key: %v", err)
	}
	if err := SetForUser("user-b", AutoDiscoveryKeyKey, "key-b-123456"); err != nil {
		t.Fatalf("failed to set user B auto discovery key: %v", err)
	}
	if err := SetForUser("user-a", ScriptDomainKey, "panel-a.example.com"); err != nil {
		t.Fatalf("failed to set user A script domain: %v", err)
	}
	if err := SetForUser("user-a", BaseScriptsURLKey, "https://github.com/example/agent-fork"); err != nil {
		t.Fatalf("failed to set user A base scripts url: %v", err)
	}
	if err := SetForUser("user-a", SendIpAddrToGuestKey, true); err != nil {
		t.Fatalf("failed to set user A send ip to guest: %v", err)
	}

	userAKey, err := GetAsForUser[string]("user-a", AutoDiscoveryKeyKey)
	if err != nil {
		t.Fatalf("failed to read user A auto discovery key: %v", err)
	}
	if userAKey != "key-a-123456" {
		t.Fatalf("expected user A key, got %q", userAKey)
	}

	userBKey, err := GetAsForUser[string]("user-b", AutoDiscoveryKeyKey)
	if err != nil {
		t.Fatalf("failed to read user B auto discovery key: %v", err)
	}
	if userBKey != "key-b-123456" {
		t.Fatalf("expected user B key, got %q", userBKey)
	}

	allowCors, err := GetAsForUser[bool]("user-a", AllowCorsKey)
	if err != nil {
		t.Fatalf("failed to read user-scoped global key: %v", err)
	}
	if !allowCors {
		t.Fatal("expected global allow_cors to remain visible through user read")
	}

	allUserA, err := GetAllForUser("user-a")
	if err != nil {
		t.Fatalf("failed to get user A config snapshot: %v", err)
	}
	if allUserA[AutoDiscoveryKeyKey] != "key-a-123456" {
		t.Fatalf("expected user A snapshot to use user key, got %#v", allUserA[AutoDiscoveryKeyKey])
	}
	if allUserA[ScriptDomainKey] != "panel-a.example.com" {
		t.Fatalf("expected user A snapshot to use user script domain, got %#v", allUserA[ScriptDomainKey])
	}
	if allUserA[BaseScriptsURLKey] != "https://github.com/example/agent-fork" {
		t.Fatalf("expected user A snapshot to use user base scripts url, got %#v", allUserA[BaseScriptsURLKey])
	}
	if allUserA[SendIpAddrToGuestKey] != true {
		t.Fatalf("expected user A snapshot to use user send ip flag, got %#v", allUserA[SendIpAddrToGuestKey])
	}
}

func TestFindUserUUIDByConfigValue(t *testing.T) {
	setupConfigTestDB(t)

	if err := SetForUser("user-a", AutoDiscoveryKeyKey, "key-a-123456"); err != nil {
		t.Fatalf("failed to set user auto discovery key: %v", err)
	}

	userUUID, err := FindUserUUIDByConfigValue(AutoDiscoveryKeyKey, "key-a-123456")
	if err != nil {
		t.Fatalf("failed to resolve user by auto discovery key: %v", err)
	}
	if userUUID != "user-a" {
		t.Fatalf("expected user-a, got %q", userUUID)
	}
}

func TestEnsureAutoDiscoveryKeyForUser(t *testing.T) {
	setupConfigTestDB(t)

	key, err := EnsureAutoDiscoveryKeyForUser("user-a")
	if err != nil {
		t.Fatalf("failed to ensure auto discovery key: %v", err)
	}
	if len(key) < 12 {
		t.Fatalf("expected generated key to be long enough, got %q", key)
	}

	reloaded, err := GetAsForUser[string]("user-a", AutoDiscoveryKeyKey)
	if err != nil {
		t.Fatalf("failed to reload auto discovery key: %v", err)
	}
	if reloaded != key {
		t.Fatalf("expected persisted key %q, got %q", key, reloaded)
	}
}

func TestResolveValidTempShareUserUUID(t *testing.T) {
	setupConfigTestDB(t)

	now := time.Unix(1_700_000_000, 0)
	if err := SetForUser("user-a", TempShareTokenKey, "temp-token-123456"); err != nil {
		t.Fatalf("failed to set user temp share token: %v", err)
	}
	if err := SetForUser("user-a", TempShareTokenExpireAtKey, now.Add(time.Hour).Unix()); err != nil {
		t.Fatalf("failed to set user temp share expiry: %v", err)
	}

	userUUID, ok, err := ResolveValidTempShareUserUUID("temp-token-123456", now)
	if err != nil {
		t.Fatalf("failed to resolve temp share token: %v", err)
	}
	if !ok || userUUID != "user-a" {
		t.Fatalf("expected valid token for user-a, got ok=%v user=%q", ok, userUUID)
	}

	userUUID, ok, err = ResolveValidTempShareUserUUID("temp-token-123456", now.Add(2*time.Hour))
	if err != nil {
		t.Fatalf("failed to resolve expired temp share token: %v", err)
	}
	if ok || userUUID != "" {
		t.Fatalf("expected expired token to be rejected, got ok=%v user=%q", ok, userUUID)
	}
}

func TestUserPolicyDefaultsAndPersistence(t *testing.T) {
	setupConfigTestDB(t)

	policy, err := GetUserPolicy("user-a")
	if err != nil {
		t.Fatalf("failed to load default user policy: %v", err)
	}
	if policy.ServerQuota != 0 {
		t.Fatalf("expected unlimited quota by default, got %d", policy.ServerQuota)
	}
	if len(policy.AllowedFeatures) != 0 {
		t.Fatalf("expected default features to remain implicit, got %+v", policy.AllowedFeatures)
	}
	allowed, err := IsUserFeatureAllowed("user-a", UserFeatureClients)
	if err != nil {
		t.Fatalf("failed to check default client feature: %v", err)
	}
	if !allowed {
		t.Fatal("expected standard features to remain allowed by default")
	}
	allowed, err = IsUserFeatureAllowed("user-a", UserFeatureCNConnectivity)
	if err != nil {
		t.Fatalf("failed to check delegated cn connectivity feature: %v", err)
	}
	if allowed {
		t.Fatal("expected delegated cn connectivity feature to be disabled by default")
	}

	quota := 3
	features := []string{UserFeatureCloud, UserFeatureClients, "clients", "CLOUD"}
	if err := SetUserPolicy("user-a", &quota, &features); err != nil {
		t.Fatalf("failed to set user policy: %v", err)
	}

	policy, err = GetUserPolicy("user-a")
	if err != nil {
		t.Fatalf("failed to reload user policy: %v", err)
	}
	if policy.ServerQuota != 3 {
		t.Fatalf("expected server quota 3, got %d", policy.ServerQuota)
	}
	if len(policy.AllowedFeatures) != 2 || policy.AllowedFeatures[0] != UserFeatureClients || policy.AllowedFeatures[1] != UserFeatureCloud {
		t.Fatalf("unexpected normalized features: %+v", policy.AllowedFeatures)
	}

	allowed, err = IsUserFeatureAllowed("user-a", UserFeatureCloud)
	if err != nil {
		t.Fatalf("failed to check allowed feature: %v", err)
	}
	if !allowed {
		t.Fatal("expected cloud feature to remain allowed")
	}
	allowed, err = IsUserFeatureAllowed("user-a", UserFeatureLogs)
	if err != nil {
		t.Fatalf("failed to check denied feature: %v", err)
	}
	if allowed {
		t.Fatal("expected logs feature to be denied when not listed")
	}
}

func TestSetUserPolicyRejectsInvalidFeature(t *testing.T) {
	setupConfigTestDB(t)

	features := []string{"invalid-feature"}
	err := SetUserPolicy("user-a", nil, &features)
	if err == nil {
		t.Fatal("expected invalid feature to be rejected")
	}
}

func TestGetUserPolicyRequiresScopedUserKey(t *testing.T) {
	setupConfigTestDB(t)

	_, err := FindUserUUIDByConfigValue(SitenameKey, "Komari")
	if err == nil || err.Error() != "config key is not user scoped" {
		t.Fatalf("expected non-user-scoped key lookup to fail, got %v", err)
	}
}
