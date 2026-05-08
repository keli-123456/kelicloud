package failover

import (
	"errors"
	"strings"
	"time"

	"github.com/komari-monitor/komari/database/models"
)

const (
	ShareAccessPolicyPublic    = "public"
	ShareAccessPolicySingleUse = "single_use"

	ShareStatusNotShared = "not_shared"
	ShareStatusActive    = "active"
	ShareStatusExpired   = "expired"
	ShareStatusConsumed  = "consumed"
)

var (
	ErrShareInvalidAccessPolicy = errors.New("invalid failover share access policy")
	ErrShareNotFound            = errors.New("failover share not found")
	ErrShareExpired             = errors.New("failover share has expired")
	ErrShareConsumed            = errors.New("failover share has already been used")
)

func NormalizeShareAccessPolicy(policy string) (string, error) {
	policy = strings.ToLower(strings.TrimSpace(policy))
	if policy == "" {
		return ShareAccessPolicyPublic, nil
	}
	switch policy {
	case ShareAccessPolicyPublic, ShareAccessPolicySingleUse:
		return policy, nil
	default:
		return "", ErrShareInvalidAccessPolicy
	}
}

func IsShareExpired(share *models.FailoverShare, now time.Time) bool {
	if share == nil || share.ExpiresAt == nil {
		return false
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	return !share.ExpiresAt.After(now.UTC())
}

func IsShareConsumed(share *models.FailoverShare) bool {
	return share != nil && share.ConsumedAt != nil && !share.ConsumedAt.IsZero()
}

func ShareStatus(share *models.FailoverShare, now time.Time) string {
	if share == nil || strings.TrimSpace(share.ShareToken) == "" {
		return ShareStatusNotShared
	}
	if IsShareConsumed(share) {
		return ShareStatusConsumed
	}
	if IsShareExpired(share, now) {
		return ShareStatusExpired
	}
	return ShareStatusActive
}

func ValidateSharePublicAccess(share *models.FailoverShare, now time.Time) error {
	if share == nil {
		return ErrShareNotFound
	}
	if IsShareConsumed(share) {
		return ErrShareConsumed
	}
	if IsShareExpired(share, now) {
		return ErrShareExpired
	}
	return nil
}

func ShouldConsumeShare(share *models.FailoverShare) bool {
	if share == nil {
		return false
	}
	policy, err := NormalizeShareAccessPolicy(share.AccessPolicy)
	return err == nil && policy == ShareAccessPolicySingleUse
}
