package failoverv2

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	awscloud "github.com/komari-monitor/komari/utils/cloudprovider/aws"
	"github.com/komari-monitor/komari/utils/cloudprovider/linode"
)

type cloudflareDNSConfig struct {
	APIToken string `json:"api_token"`
	ZoneID   string `json:"zone_id"`
	ZoneName string `json:"zone_name"`
	Proxied  bool   `json:"proxied"`
}

var loadCloudflareDNSConfigFunc = loadCloudflareDNSConfig

func loadCloudflareDNSConfig(userUUID, entryID string) (*cloudflareDNSConfig, error) {
	entry, err := loadGenericProviderEntry(userUUID, "cloudflare", entryID)
	if err != nil {
		return nil, err
	}

	configValue, err := decodeGenericEntryConfig[cloudflareDNSConfig](entry)
	if err != nil {
		return nil, fmt.Errorf("cloudflare dns config is invalid: %w", err)
	}
	if strings.TrimSpace(configValue.APIToken) == "" {
		return nil, errors.New("cloudflare api_token is required")
	}
	return configValue, nil
}

func loadLinodeAddition(userUUID string) (*linode.Addition, error) {
	raw, err := loadProviderAddition(userUUID, "linode")
	if err != nil {
		return nil, fmt.Errorf("Linode provider is not configured")
	}

	addition := &linode.Addition{}
	if raw == "" {
		raw = "{}"
	}
	if err := decodeJSON(raw, addition); err != nil {
		return nil, fmt.Errorf("Linode configuration is invalid: %w", err)
	}
	addition.Normalize()
	return addition, nil
}

func loadLinodeToken(userUUID, entryID string) (*linode.Addition, *linode.TokenRecord, error) {
	addition, err := loadLinodeAddition(userUUID)
	if err != nil {
		return nil, nil, err
	}

	entryID = strings.TrimSpace(entryID)
	if entryID == "" || entryID == activeProviderEntryID {
		token := addition.ActiveToken()
		if token == nil {
			return nil, nil, errors.New("Linode token is not configured")
		}
		return addition, token, nil
	}

	token := addition.FindToken(entryID)
	if token == nil {
		return nil, nil, fmt.Errorf("Linode token not found: %s", entryID)
	}
	return addition, token, nil
}

func saveLinodeAddition(userUUID string, addition *linode.Addition) error {
	if addition == nil {
		addition = &linode.Addition{}
	}
	addition.Normalize()
	payload, err := encodeJSON(addition)
	if err != nil {
		return err
	}
	return saveProviderAddition(userUUID, "linode", payload)
}

func reloadLinodeAdditionTokenState(userUUID string, token *linode.TokenRecord) (*linode.Addition, *linode.TokenRecord, error) {
	if token == nil {
		return nil, nil, errors.New("Linode token is not configured")
	}

	addition, err := loadLinodeAddition(userUUID)
	if err != nil {
		return nil, nil, err
	}

	latestToken := addition.FindToken(token.ID)
	if latestToken == nil {
		tokenValue := strings.TrimSpace(token.Token)
		if tokenValue != "" {
			for index := range addition.Tokens {
				if strings.TrimSpace(addition.Tokens[index].Token) == tokenValue {
					latestToken = &addition.Tokens[index]
					break
				}
			}
		}
	}
	if latestToken == nil {
		return nil, nil, fmt.Errorf("Linode token not found: %s", strings.TrimSpace(token.ID))
	}

	return addition, latestToken, nil
}

func persistLinodeRootPassword(userUUID string, addition *linode.Addition, token *linode.TokenRecord, instanceID int, instanceLabel, passwordMode, rootPassword string) error {
	if addition == nil || token == nil || instanceID <= 0 || strings.TrimSpace(rootPassword) == "" {
		return nil
	}

	latestAddition, latestToken, err := reloadLinodeAdditionTokenState(userUUID, token)
	if err != nil {
		log.Printf("failoverv2: failed to reload Linode token state for instance %d: %v", instanceID, err)
		return err
	}
	if err := latestToken.SaveInstancePassword(instanceID, instanceLabel, passwordMode, rootPassword, time.Now()); err != nil {
		log.Printf("failoverv2: failed to save Linode root password for instance %d: %v", instanceID, err)
		return err
	}
	if err := saveLinodeAddition(userUUID, latestAddition); err != nil {
		latestToken.RemoveSavedInstancePassword(instanceID)
		log.Printf("failoverv2: failed to persist Linode root password for instance %d: %v", instanceID, err)
		return err
	}
	return nil
}

func removeSavedLinodeRootPassword(userUUID string, addition *linode.Addition, token *linode.TokenRecord, instanceID int) {
	if addition == nil || token == nil || instanceID <= 0 {
		return
	}

	targetAddition := addition
	targetToken := token
	if latestAddition, latestToken, err := reloadLinodeAdditionTokenState(userUUID, token); err == nil {
		targetAddition = latestAddition
		targetToken = latestToken
	} else {
		log.Printf("failoverv2: failed to reload Linode token state for instance %d cleanup, falling back to in-memory state: %v", instanceID, err)
	}

	if !targetToken.RemoveSavedInstancePassword(instanceID) {
		return
	}
	if err := saveLinodeAddition(userUUID, targetAddition); err != nil {
		log.Printf("failoverv2: failed to remove saved Linode root password for instance %d: %v", instanceID, err)
	}
}

func loadAWSAddition(userUUID string) (*awscloud.Addition, error) {
	raw, err := loadProviderAddition(userUUID, "aws")
	if err != nil {
		return nil, fmt.Errorf("AWS provider is not configured")
	}

	addition := &awscloud.Addition{}
	if raw == "" {
		raw = "{}"
	}
	if err := decodeJSON(raw, addition); err != nil {
		return nil, fmt.Errorf("AWS configuration is invalid: %w", err)
	}
	addition.Normalize()
	return addition, nil
}

func loadAWSCredential(userUUID, entryID string) (*awscloud.Addition, *awscloud.CredentialRecord, error) {
	addition, err := loadAWSAddition(userUUID)
	if err != nil {
		return nil, nil, err
	}

	entryID = strings.TrimSpace(entryID)
	if entryID == "" || entryID == activeProviderEntryID {
		credential := addition.ActiveCredential()
		if credential == nil {
			return nil, nil, errors.New("AWS credential is not configured")
		}
		return addition, credential, nil
	}

	credential := addition.FindCredential(entryID)
	if credential == nil {
		return nil, nil, fmt.Errorf("AWS credential not found: %s", entryID)
	}
	return addition, credential, nil
}

func saveAWSAddition(userUUID string, addition *awscloud.Addition) error {
	if addition == nil {
		addition = &awscloud.Addition{}
	}
	addition.Normalize()
	payload, err := encodeJSON(addition)
	if err != nil {
		return err
	}
	return saveProviderAddition(userUUID, "aws", payload)
}

func reloadAWSAdditionCredentialState(userUUID string, credential *awscloud.CredentialRecord) (*awscloud.Addition, *awscloud.CredentialRecord, error) {
	if credential == nil {
		return nil, nil, errors.New("AWS credential is not configured")
	}

	addition, err := loadAWSAddition(userUUID)
	if err != nil {
		return nil, nil, err
	}

	latestCredential := addition.FindCredential(credential.ID)
	if latestCredential == nil {
		accessKeyID := strings.TrimSpace(credential.AccessKeyID)
		defaultRegion := strings.TrimSpace(credential.DefaultRegion)
		if accessKeyID != "" {
			for index := range addition.Credentials {
				if strings.TrimSpace(addition.Credentials[index].AccessKeyID) == accessKeyID &&
					strings.TrimSpace(addition.Credentials[index].DefaultRegion) == defaultRegion {
					latestCredential = &addition.Credentials[index]
					break
				}
			}
		}
	}
	if latestCredential == nil {
		entryID := strings.TrimSpace(credential.ID)
		if entryID == "" {
			return nil, nil, errors.New("AWS credential is not configured")
		}
		return nil, nil, fmt.Errorf("AWS credential not found: %s", entryID)
	}

	return addition, latestCredential, nil
}

func buildAWSResourceCredentialID(region, resourceID string) string {
	resourceID = strings.TrimSpace(resourceID)
	if resourceID == "" {
		return ""
	}

	region = strings.TrimSpace(strings.ToLower(region))
	if region == "" {
		return resourceID
	}
	return region + "::" + resourceID
}

func persistAWSRootPassword(
	userUUID string,
	addition *awscloud.Addition,
	credential *awscloud.CredentialRecord,
	resourceType string,
	region string,
	resourceID string,
	resourceName string,
	passwordMode string,
	rootPassword string,
) error {
	resourceType = strings.TrimSpace(resourceType)
	credentialResourceID := buildAWSResourceCredentialID(region, resourceID)
	rootPassword = strings.TrimSpace(rootPassword)
	if addition == nil || credential == nil || resourceType == "" || credentialResourceID == "" || rootPassword == "" {
		return nil
	}

	latestAddition, latestCredential, err := reloadAWSAdditionCredentialState(userUUID, credential)
	if err != nil {
		log.Printf("failoverv2: failed to reload AWS credential state for %s %s: %v", resourceType, strings.TrimSpace(resourceID), err)
		return err
	}
	if err := latestCredential.SaveResourcePassword(resourceType, credentialResourceID, resourceName, passwordMode, rootPassword, time.Now()); err != nil {
		log.Printf("failoverv2: failed to save AWS root password for %s %s: %v", resourceType, strings.TrimSpace(resourceID), err)
		return err
	}
	if err := saveAWSAddition(userUUID, latestAddition); err != nil {
		latestCredential.RemoveSavedResourcePassword(resourceType, credentialResourceID)
		log.Printf("failoverv2: failed to persist AWS root password for %s %s: %v", resourceType, strings.TrimSpace(resourceID), err)
		return err
	}
	return nil
}

func removeSavedAWSRootPassword(
	userUUID string,
	addition *awscloud.Addition,
	credential *awscloud.CredentialRecord,
	resourceType string,
	region string,
	resourceID string,
) {
	resourceType = strings.TrimSpace(resourceType)
	credentialResourceID := buildAWSResourceCredentialID(region, resourceID)
	if addition == nil || credential == nil || resourceType == "" || credentialResourceID == "" {
		return
	}

	targetAddition := addition
	targetCredential := credential
	if latestAddition, latestCredential, err := reloadAWSAdditionCredentialState(userUUID, credential); err == nil {
		targetAddition = latestAddition
		targetCredential = latestCredential
	} else {
		log.Printf("failoverv2: failed to reload AWS credential state for %s %s cleanup, falling back to in-memory state: %v", resourceType, strings.TrimSpace(resourceID), err)
	}

	if !targetCredential.RemoveSavedResourcePassword(resourceType, credentialResourceID) {
		return
	}
	if err := saveAWSAddition(userUUID, targetAddition); err != nil {
		log.Printf("failoverv2: failed to remove saved AWS root password for %s %s: %v", resourceType, strings.TrimSpace(resourceID), err)
	}
}

func normalizeAWSRootPasswordMode(mode string) string {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "none":
		return "none"
	case "custom":
		return "custom"
	case "random":
		return "random"
	default:
		return ""
	}
}

func resolveAWSRootPasswordUserData(mode, rootPassword, userData string) (string, string, error) {
	userData = strings.TrimSpace(userData)
	switch normalizeAWSRootPasswordMode(mode) {
	case "", "none":
		return userData, "", nil
	case "custom":
		rootPassword = strings.TrimSpace(rootPassword)
		if rootPassword == "" {
			return "", "", errors.New("root password is required when custom password mode is selected")
		}
		resolvedUserData, err := awscloud.BuildRootPasswordUserData(rootPassword, userData)
		return resolvedUserData, "", err
	case "random":
		generatedPassword, err := awscloud.GenerateRandomPassword(20)
		if err != nil {
			return "", "", err
		}
		resolvedUserData, err := awscloud.BuildRootPasswordUserData(generatedPassword, userData)
		return resolvedUserData, generatedPassword, err
	default:
		return userData, "", nil
	}
}

func resolveAWSFailoverRegion(region string, addition *awscloud.Addition, credential *awscloud.CredentialRecord) string {
	region = strings.TrimSpace(region)
	if region != "" {
		return region
	}
	if addition != nil {
		if activeRegion := strings.TrimSpace(addition.ActiveRegion); activeRegion != "" {
			return activeRegion
		}
	}
	if credential != nil {
		if defaultRegion := strings.TrimSpace(credential.DefaultRegion); defaultRegion != "" {
			return defaultRegion
		}
	}
	return awscloud.DefaultRegion
}

func buildAWSIPv6RefreshUserData(userData string) (string, error) {
	return mergeShellUserData(userData, buildAWSIPv6RefreshSnippet(), true, "AWS IPv6 refresh")
}

func buildAWSIPv6RefreshSnippet() string {
	var builder strings.Builder
	builder.WriteString("# Komari AWS IPv6 refresh\n")
	builder.WriteString("KOMARI_IPV6_IFACE=\"$(ip -o route show to default 2>/dev/null | awk '{print $5; exit}')\"\n")
	builder.WriteString("if [ -z \"$KOMARI_IPV6_IFACE\" ]; then\n")
	builder.WriteString("  KOMARI_IPV6_IFACE=\"$(ip -o link show 2>/dev/null | awk -F': ' '$2 != \\\"lo\\\" {print $2; exit}')\"\n")
	builder.WriteString("fi\n")
	builder.WriteString("if [ -n \"$KOMARI_IPV6_IFACE\" ]; then\n")
	builder.WriteString("  for _komari_ipv6_attempt in 1 2 3 4 5; do\n")
	builder.WriteString("    if ip -6 addr show dev \"$KOMARI_IPV6_IFACE\" scope global 2>/dev/null | grep -q 'inet6 '; then\n")
	builder.WriteString("      break\n")
	builder.WriteString("    fi\n")
	builder.WriteString("    networkctl reconfigure \"$KOMARI_IPV6_IFACE\" >/dev/null 2>&1 || true\n")
	builder.WriteString("    if command -v dhclient >/dev/null 2>&1; then\n")
	builder.WriteString("      dhclient -6 -v \"$KOMARI_IPV6_IFACE\" >/dev/null 2>&1 || true\n")
	builder.WriteString("    fi\n")
	builder.WriteString("    netplan apply >/dev/null 2>&1 || true\n")
	builder.WriteString("    systemctl restart systemd-networkd >/dev/null 2>&1 || true\n")
	builder.WriteString("    systemctl restart NetworkManager >/dev/null 2>&1 || true\n")
	builder.WriteString("    sleep 5\n")
	builder.WriteString("  done\n")
	builder.WriteString("fi\n")
	return builder.String()
}

func decodeJSON[T any](raw string, target *T) error {
	return json.Unmarshal([]byte(raw), target)
}

func encodeJSON(value any) (string, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return string(payload), nil
}
