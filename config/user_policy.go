package config

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"gorm.io/gorm"
)

const (
	UserServerQuotaKey     = "server_quota"
	UserAllowedFeaturesKey = "allowed_features"
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
		UserFeatureCloudAzure,
		UserFeatureCloudAWS,
		UserFeatureCloudDNS,
		UserFeatureCloudFailover,
	},
}

type UserPolicy struct {
	ServerQuota     int      `json:"server_quota"`
	AllowedFeatures []string `json:"allowed_features,omitempty"`
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
