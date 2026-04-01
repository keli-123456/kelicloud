package cloudshare

import (
	"errors"
	"testing"
	"time"

	"github.com/komari-monitor/komari/database/models"
)

func TestNormalizeAccessPolicy(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    string
		wantErr error
	}{
		{name: "default", input: "", want: AccessPolicyPublic},
		{name: "public", input: "public", want: AccessPolicyPublic},
		{name: "single_use", input: "single_use", want: AccessPolicySingleUse},
		{name: "invalid", input: "private", wantErr: ErrInvalidAccessPolicy},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NormalizeAccessPolicy(tt.input)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("expected error %v, got %v", tt.wantErr, err)
			}
			if got != tt.want {
				t.Fatalf("expected policy %q, got %q", tt.want, got)
			}
		})
	}
}

func TestValidatePublicAccess(t *testing.T) {
	now := time.Date(2026, time.April, 1, 10, 0, 0, 0, time.UTC)
	expiredAt := now.Add(-time.Minute)
	consumedAt := now.Add(-2 * time.Minute)

	tests := []struct {
		name    string
		share   *models.CloudInstanceShare
		wantErr error
	}{
		{name: "nil", share: nil, wantErr: ErrInstanceNotFound},
		{name: "active", share: &models.CloudInstanceShare{ShareToken: "token-a"}, wantErr: nil},
		{name: "expired", share: &models.CloudInstanceShare{ShareToken: "token-a", ExpiresAt: &expiredAt}, wantErr: ErrShareExpired},
		{name: "consumed", share: &models.CloudInstanceShare{ShareToken: "token-a", ConsumedAt: &consumedAt}, wantErr: ErrShareConsumed},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidatePublicAccess(tt.share, now)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("expected error %v, got %v", tt.wantErr, err)
			}
		})
	}
}

func TestShareStatus(t *testing.T) {
	now := time.Date(2026, time.April, 1, 10, 0, 0, 0, time.UTC)
	expiredAt := now.Add(-time.Minute)
	consumedAt := now.Add(-2 * time.Minute)

	tests := []struct {
		name  string
		share *models.CloudInstanceShare
		want  string
	}{
		{name: "not_shared_nil", share: nil, want: ShareStatusNotShared},
		{name: "not_shared_no_token", share: &models.CloudInstanceShare{}, want: ShareStatusNotShared},
		{name: "active", share: &models.CloudInstanceShare{ShareToken: "token-a"}, want: ShareStatusActive},
		{name: "expired", share: &models.CloudInstanceShare{ShareToken: "token-a", ExpiresAt: &expiredAt}, want: ShareStatusExpired},
		{name: "consumed", share: &models.CloudInstanceShare{ShareToken: "token-a", ConsumedAt: &consumedAt}, want: ShareStatusConsumed},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ShareStatus(tt.share, now)
			if got != tt.want {
				t.Fatalf("expected status %q, got %q", tt.want, got)
			}
		})
	}
}
