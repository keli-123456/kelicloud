package aws

import (
	"strings"
	"testing"
)

func TestBuildRootPasswordUserData(t *testing.T) {
	userData, err := BuildRootPasswordUserData("Secret!123", "echo ready")
	if err != nil {
		t.Fatalf("expected root password user data, got error: %v", err)
	}
	if !strings.Contains(userData, "root:Secret!123") {
		t.Fatalf("expected generated script to include root password command, got %q", userData)
	}
	if !strings.Contains(userData, "echo ready") {
		t.Fatalf("expected generated script to include extra user data, got %q", userData)
	}

	_, err = BuildRootPasswordUserData("Secret!123", "#cloud-config\nusers: []")
	if err == nil {
		t.Fatal("expected #cloud-config user_data to be rejected in root password mode")
	}
}

func TestGenerateRandomPassword(t *testing.T) {
	password, err := GenerateRandomPassword(20)
	if err != nil {
		t.Fatalf("expected generated password, got error: %v", err)
	}
	if len(password) != 20 {
		t.Fatalf("expected generated password length 20, got %d", len(password))
	}
}
