package config

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"gorm.io/gorm"
)

const (
	UserServerQuotaKey     = "server_quota"
	UserAllowedFeaturesKey = "allowed_features"
	UserPlanNameKey        = "plan_name"
	UserPlanExpiresAtKey   = "plan_expires_at"
	UserPlanNoteKey        = "plan_note"
	UserAccountDisabledKey = "account_disabled"
)

const (
	UserAccessStatusActive   = "active"
	UserAccessStatusDisabled = "disabled"
	UserAccessStatusExpired  = "expired"
)

const (
	UserFeatureClients           = "clients"
	UserFeatureRecords           = "records"
	UserFeatureTasks             = "tasks"
	UserFeaturePing              = "ping"
	UserFeatureNotifications     = "notifications"
	UserFeatureCloud             = "cloud"
	UserFeatureCloudDigitalOcean = "cloud_digitalocean"
	UserFeatureCloudLinode       = "cloud_linode"
	UserFeatureCloudVultr        = "cloud_vultr"
	UserFeatureCloudAzure        = "cloud_azure"
	UserFeatureCloudAWS          = "cloud_aws"
	UserFeatureCloudDNS          = "cloud_dns"
	UserFeatureCloudFailover     = "cloud_failover"
	UserFeatureClipboard         = "clipboard"
	UserFeatureLogs              = "logs"
	UserFeatureCNConnectivity    = "cn_connectivity"
)

var userFeatureDefaultAllowSet = map[string]struct{}{
	UserFeatureClients:           {},
	UserFeatureRecords:           {},
	UserFeatureTasks:             {},
	UserFeaturePing:              {},
	UserFeatureNotifications:     {},
	UserFeatureCloudDigitalOcean: {},
	UserFeatureCloudLinode:       {},
	UserFeatureCloudVultr:        {},
	UserFeatureCloudAzure:        {},
	UserFeatureCloudAWS:          {},
	UserFeatureCloudDNS:          {},
	UserFeatureCloudFailover:     {},
	UserFeatureClipboard:         {},
	UserFeatureLogs:              {},
}

var userFeatureExplicitGrantSet = map[string]struct{}{
	UserFeatureCNConnectivity: {},
}

var userFeatureSet = map[string]struct{}{
	UserFeatureClients:           {},
	UserFeatureRecords:           {},
	UserFeatureTasks:             {},
	UserFeaturePing:              {},
	UserFeatureNotifications:     {},
	UserFeatureCloud:             {},
	UserFeatureCloudDigitalOcean: {},
	UserFeatureCloudLinode:       {},
	UserFeatureCloudVultr:        {},
	UserFeatureCloudAzure:        {},
	UserFeatureCloudAWS:          {},
	UserFeatureCloudDNS:          {},
	UserFeatureCloudFailover:     {},
	UserFeatureClipboard:         {},
	UserFeatureLogs:              {},
	UserFeatureCNConnectivity:    {},
}

var userVisibleFeatureSet = map[string]struct{}{
	UserFeatureClients:           {},
	UserFeatureRecords:           {},
	UserFeatureTasks:             {},
	UserFeaturePing:              {},
	UserFeatureNotifications:     {},
	UserFeatureCloudDigitalOcean: {},
	UserFeatureCloudLinode:       {},
	UserFeatureCloudVultr:        {},
	UserFeatureCloudAzure:        {},
	UserFeatureCloudAWS:          {},
	UserFeatureCloudDNS:          {},
	UserFeatureCloudFailover:     {},
	UserFeatureClipboard:         {},
	UserFeatureLogs:              {},
	UserFeatureCNConnectivity:    {},
}

var legacyFeatureAliases = map[string][]string{
	UserFeatureCloud: {
		UserFeatureCloudDigitalOcean,
		UserFeatureCloudLinode,
		UserFeatureCloudVultr,
		UserFeatureCloudAzure,
		UserFeatureCloudAWS,
		UserFeatureCloudDNS,
		UserFeatureCloudFailover,
	},
}

type UserPolicy struct {
	ServerQuota     int      `json:"server_quota"`
	AllowedFeatures []string `json:"allowed_features,omitempty"`
	PlanName        string   `json:"plan_name,omitempty"`
	PlanExpiresAt   string   `json:"plan_expires_at,omitempty"`
	PlanNote        string   `json:"plan_note,omitempty"`
	AccountDisabled bool     `json:"account_disabled,omitempty"`
	AccessStatus    string   `json:"access_status"`
}

func UserAvailableFeatures() []string {
	features := make([]string, 0, len(userVisibleFeatureSet))
	for feature := range userVisibleFeatureSet {
		features = append(features, feature)
	}
	sort.Strings(features)
	return features
}

func IsSupportedUserFeature(feature string) bool {
	_, ok := userFeatureSet[strings.ToLower(strings.TrimSpace(feature))]
	return ok
}

func IsExplicitGrantUserFeature(feature string) bool {
	_, ok := userFeatureExplicitGrantSet[strings.ToLower(strings.TrimSpace(feature))]
	return ok
}

func NormalizeAllowedFeatures(features []string) []string {
	normalized := make([]string, 0, len(features))
	seen := make(map[string]struct{}, len(features))
	for _, feature := range features {
		value := strings.ToLower(strings.TrimSpace(feature))
		expanded, ok := legacyFeatureAliases[value]
		if ok {
			for _, alias := range expanded {
				if _, exists := seen[alias]; exists {
					continue
				}
				seen[alias] = struct{}{}
				normalized = append(normalized, alias)
			}
			continue
		}
		if value == "" {
			continue
		}
		if _, ok := userFeatureSet[value]; !ok {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		normalized = append(normalized, value)
	}
	sort.Strings(normalized)
	return normalized
}

func ValidateAllowedFeatures(features []string) error {
	for _, feature := range features {
		value := strings.ToLower(strings.TrimSpace(feature))
		if value == "" {
			continue
		}
		if _, ok := userFeatureSet[value]; !ok {
			return fmt.Errorf("invalid user feature: %s", feature)
		}
	}
	return nil
}

func NormalizeUserPlanExpiresAt(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", nil
	}
	if parsed, err := time.Parse("2006-01-02", value); err == nil {
		return parsed.Format("2006-01-02"), nil
	}
	if parsed, err := time.Parse(time.RFC3339, value); err == nil {
		return parsed.UTC().Format(time.RFC3339), nil
	}
	return "", fmt.Errorf("plan expiration must use YYYY-MM-DD or RFC3339 format")
}

func normalizeUserPlanText(value, field string, maxRunes int) (string, error) {
	value = strings.TrimSpace(value)
	if len([]rune(value)) > maxRunes {
		return "", fmt.Errorf("%s must be at most %d characters", field, maxRunes)
	}
	return value, nil
}

func userPlanExpirationTime(value string) (time.Time, bool, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, false, nil
	}
	if parsed, err := time.ParseInLocation("2006-01-02", value, time.Local); err == nil {
		return parsed.AddDate(0, 0, 1), true, nil
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return time.Time{}, false, err
	}
	return parsed, true, nil
}

func UserPolicyAccessStatus(policy UserPolicy, now time.Time) string {
	if policy.AccountDisabled {
		return UserAccessStatusDisabled
	}
	expiresAt, ok, err := userPlanExpirationTime(policy.PlanExpiresAt)
	if err == nil && ok && !now.Before(expiresAt) {
		return UserAccessStatusExpired
	}
	return UserAccessStatusActive
}

func IsUserAccessActive(userUUID string, now time.Time) (bool, string, error) {
	policy, err := GetUserPolicy(userUUID)
	if err != nil {
		return false, "", err
	}
	status := UserPolicyAccessStatus(policy, now)
	return status == UserAccessStatusActive, status, nil
}

func GetUserPolicy(userUUID string) (UserPolicy, error) {
	policy := UserPolicy{}

	quota, err := GetAsForUser[int](userUUID, UserServerQuotaKey)
	switch {
	case err == nil:
		if quota > 0 {
			policy.ServerQuota = quota
		}
	case errors.Is(err, gorm.ErrRecordNotFound):
	default:
		return policy, err
	}

	features, err := GetAsForUser[[]string](userUUID, UserAllowedFeaturesKey)
	switch {
	case err == nil:
		policy.AllowedFeatures = NormalizeAllowedFeatures(features)
	case errors.Is(err, gorm.ErrRecordNotFound):
	default:
		return policy, err
	}

	planName, err := GetAsForUser[string](userUUID, UserPlanNameKey)
	switch {
	case err == nil:
		policy.PlanName = strings.TrimSpace(planName)
	case errors.Is(err, gorm.ErrRecordNotFound):
	default:
		return policy, err
	}

	planExpiresAt, err := GetAsForUser[string](userUUID, UserPlanExpiresAtKey)
	switch {
	case err == nil:
		policy.PlanExpiresAt = strings.TrimSpace(planExpiresAt)
	case errors.Is(err, gorm.ErrRecordNotFound):
	default:
		return policy, err
	}

	planNote, err := GetAsForUser[string](userUUID, UserPlanNoteKey)
	switch {
	case err == nil:
		policy.PlanNote = strings.TrimSpace(planNote)
	case errors.Is(err, gorm.ErrRecordNotFound):
	default:
		return policy, err
	}

	accountDisabled, err := GetAsForUser[bool](userUUID, UserAccountDisabledKey)
	switch {
	case err == nil:
		policy.AccountDisabled = accountDisabled
	case errors.Is(err, gorm.ErrRecordNotFound):
	default:
		return policy, err
	}

	policy.AccessStatus = UserPolicyAccessStatus(policy, time.Now())
	return policy, nil
}

func SetUserPolicy(userUUID string, serverQuota *int, allowedFeatures *[]string) error {
	userUUID = strings.TrimSpace(userUUID)
	if userUUID == "" {
		return gorm.ErrRecordNotFound
	}

	values := make(map[string]any)
	if serverQuota != nil {
		if *serverQuota < 0 {
			return fmt.Errorf("server quota must be greater than or equal to 0")
		}
		values[UserServerQuotaKey] = *serverQuota
	}
	if allowedFeatures != nil {
		if err := ValidateAllowedFeatures(*allowedFeatures); err != nil {
			return err
		}
		values[UserAllowedFeaturesKey] = NormalizeAllowedFeatures(*allowedFeatures)
	}

	if len(values) == 0 {
		return nil
	}
	return SetManyForUser(userUUID, values)
}

func SetUserCommercialPolicy(userUUID string, planName, planExpiresAt, planNote *string, accountDisabled *bool) error {
	userUUID = strings.TrimSpace(userUUID)
	if userUUID == "" {
		return gorm.ErrRecordNotFound
	}

	values := make(map[string]any)
	if planName != nil {
		normalized, err := normalizeUserPlanText(*planName, "plan name", 64)
		if err != nil {
			return err
		}
		values[UserPlanNameKey] = normalized
	}
	if planExpiresAt != nil {
		normalized, err := NormalizeUserPlanExpiresAt(*planExpiresAt)
		if err != nil {
			return err
		}
		values[UserPlanExpiresAtKey] = normalized
	}
	if planNote != nil {
		normalized, err := normalizeUserPlanText(*planNote, "plan note", 512)
		if err != nil {
			return err
		}
		values[UserPlanNoteKey] = normalized
	}
	if accountDisabled != nil {
		values[UserAccountDisabledKey] = *accountDisabled
	}

	if len(values) == 0 {
		return nil
	}
	return SetManyForUser(userUUID, values)
}

func GetUserServerQuota(userUUID string) (int, error) {
	policy, err := GetUserPolicy(userUUID)
	if err != nil {
		return 0, err
	}
	return policy.ServerQuota, nil
}

func IsUserFeatureAllowed(userUUID, feature string) (bool, error) {
	value := strings.ToLower(strings.TrimSpace(feature))
	if value == "" {
		return false, fmt.Errorf("feature is required")
	}
	if _, ok := userFeatureSet[value]; !ok {
		return false, fmt.Errorf("invalid user feature: %s", feature)
	}

	policy, err := GetUserPolicy(userUUID)
	if err != nil {
		return false, err
	}
	if len(policy.AllowedFeatures) == 0 {
		if value == UserFeatureCloud {
			for _, alias := range legacyFeatureAliases[UserFeatureCloud] {
				if _, ok := userFeatureDefaultAllowSet[alias]; ok {
					return true, nil
				}
			}
		}
		if IsExplicitGrantUserFeature(value) {
			return false, nil
		}
		_, ok := userFeatureDefaultAllowSet[value]
		return ok, nil
	}
	if value == UserFeatureCloud {
		for _, allowed := range policy.AllowedFeatures {
			for _, alias := range legacyFeatureAliases[UserFeatureCloud] {
				if allowed == alias {
					return true, nil
				}
			}
		}
		return false, nil
	}
	for _, allowed := range policy.AllowedFeatures {
		if allowed == value {
			return true, nil
		}
	}
	return false, nil
}
