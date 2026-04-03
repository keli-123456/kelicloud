package failover

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/komari-monitor/komari/config"
	"github.com/komari-monitor/komari/database"
	"github.com/komari-monitor/komari/database/models"
	awscloud "github.com/komari-monitor/komari/utils/cloudprovider/aws"
	"github.com/komari-monitor/komari/utils/cloudprovider/digitalocean"
	"github.com/komari-monitor/komari/utils/cloudprovider/linode"
)

const (
	activeProviderEntryID            = "active"
	defaultDNSProviderEntryID        = "default"
	defaultAgentInstallScriptBaseURL = "https://raw.githubusercontent.com/keli-123456/kelicloud-agent/refs/heads/main"
	cloudflareProviderName           = "cloudflare"
	aliyunProviderName               = "aliyun"
)

type providerEntryNotFoundError struct {
	Provider string
	EntryID  string
}

func (e *providerEntryNotFoundError) Error() string {
	if e == nil {
		return "provider entry not found"
	}

	entryID := strings.TrimSpace(e.EntryID)
	switch strings.ToLower(strings.TrimSpace(e.Provider)) {
	case "aws":
		return fmt.Sprintf("AWS credential not found: %s", entryID)
	case "linode":
		return fmt.Sprintf("Linode token not found: %s", entryID)
	default:
		return fmt.Sprintf("DigitalOcean token not found: %s", entryID)
	}
}

func newProviderEntryNotFoundError(provider, entryID string) error {
	return &providerEntryNotFoundError{
		Provider: strings.ToLower(strings.TrimSpace(provider)),
		EntryID:  strings.TrimSpace(entryID),
	}
}

func isProviderEntryNotFoundError(err error) bool {
	var target *providerEntryNotFoundError
	return errors.As(err, &target)
}

type genericProviderEntry struct {
	ID     string                 `json:"id"`
	Name   string                 `json:"name"`
	Values map[string]interface{} `json:"values"`
}

type cloudflareConfig struct {
	APIToken string `json:"api_token"`
	ZoneID   string `json:"zone_id"`
	ZoneName string `json:"zone_name"`
	Proxied  bool   `json:"proxied"`
}

type aliyunDNSConfig struct {
	AccessKeyID     string `json:"access_key_id"`
	AccessKeySecret string `json:"access_key_secret"`
	RegionID        string `json:"region_id"`
	DomainName      string `json:"domain_name"`
}

func loadProviderAddition(userUUID, providerName string) (string, error) {
	providerConfig, err := database.GetCloudProviderConfigByUserAndName(userUUID, providerName)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(providerConfig.Addition), nil
}

func saveProviderAddition(userUUID, providerName, addition string) error {
	return database.SaveCloudProviderConfigForUser(&models.CloudProvider{
		UserID:   strings.TrimSpace(userUUID),
		Name:     strings.TrimSpace(providerName),
		Addition: addition,
	})
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
	if err := json.Unmarshal([]byte(raw), addition); err != nil {
		return nil, fmt.Errorf("AWS configuration is invalid: %w", err)
	}
	addition.Normalize()
	return addition, nil
}

func normalizeProviderEntryGroup(group string) string {
	group = strings.TrimSpace(group)
	if len(group) > 100 {
		group = group[:100]
	}
	return group
}

func loadAWSCredential(userUUID, entryID string) (*awscloud.Addition, *awscloud.CredentialRecord, error) {
	return loadAWSCredentialSelection(userUUID, entryID, "")
}

func loadAWSCredentialSelection(userUUID, entryID, entryGroup string) (*awscloud.Addition, *awscloud.CredentialRecord, error) {
	addition, err := loadAWSAddition(userUUID)
	if err != nil {
		return nil, nil, err
	}

	entryID = strings.TrimSpace(entryID)
	entryGroup = normalizeProviderEntryGroup(entryGroup)
	if entryGroup != "" {
		if entryID != "" && entryID != activeProviderEntryID {
			credential := addition.FindCredential(entryID)
			if credential == nil {
				return nil, nil, newProviderEntryNotFoundError("aws", entryID)
			}
			if normalizeProviderEntryGroup(credential.Group) != entryGroup {
				return nil, nil, fmt.Errorf("AWS credential %s is not in group %s", entryID, entryGroup)
			}
			return addition, credential, nil
		}

		if credential := addition.ActiveCredential(); credential != nil && normalizeProviderEntryGroup(credential.Group) == entryGroup {
			return addition, credential, nil
		}
		for index := range addition.Credentials {
			if normalizeProviderEntryGroup(addition.Credentials[index].Group) == entryGroup {
				return addition, &addition.Credentials[index], nil
			}
		}
		return nil, nil, fmt.Errorf("AWS credential group not found: %s", entryGroup)
	}

	if entryID == "" || entryID == activeProviderEntryID {
		credential := addition.ActiveCredential()
		if credential == nil {
			return nil, nil, errors.New("AWS credential is not configured")
		}
		return addition, credential, nil
	}

	credential := addition.FindCredential(entryID)
	if credential == nil {
		return nil, nil, newProviderEntryNotFoundError("aws", entryID)
	}
	return addition, credential, nil
}

func loadDigitalOceanAddition(userUUID string) (*digitalocean.Addition, error) {
	raw, err := loadProviderAddition(userUUID, "digitalocean")
	if err != nil {
		return nil, fmt.Errorf("DigitalOcean provider is not configured")
	}

	addition := &digitalocean.Addition{}
	if raw == "" {
		raw = "{}"
	}
	if err := json.Unmarshal([]byte(raw), addition); err != nil {
		return nil, fmt.Errorf("DigitalOcean configuration is invalid: %w", err)
	}
	addition.Normalize()
	return addition, nil
}

func loadDigitalOceanToken(userUUID, entryID string) (*digitalocean.Addition, *digitalocean.TokenRecord, error) {
	return loadDigitalOceanTokenSelection(userUUID, entryID, "")
}

func loadDigitalOceanTokenSelection(userUUID, entryID, entryGroup string) (*digitalocean.Addition, *digitalocean.TokenRecord, error) {
	addition, err := loadDigitalOceanAddition(userUUID)
	if err != nil {
		return nil, nil, err
	}

	entryID = strings.TrimSpace(entryID)
	entryGroup = normalizeProviderEntryGroup(entryGroup)
	if entryGroup != "" {
		if entryID != "" && entryID != activeProviderEntryID {
			token := addition.FindToken(entryID)
			if token == nil {
				return nil, nil, newProviderEntryNotFoundError("digitalocean", entryID)
			}
			if normalizeProviderEntryGroup(token.Group) != entryGroup {
				return nil, nil, fmt.Errorf("DigitalOcean token %s is not in group %s", entryID, entryGroup)
			}
			return addition, token, nil
		}

		if token := addition.ActiveToken(); token != nil && normalizeProviderEntryGroup(token.Group) == entryGroup {
			return addition, token, nil
		}
		for index := range addition.Tokens {
			if normalizeProviderEntryGroup(addition.Tokens[index].Group) == entryGroup {
				return addition, &addition.Tokens[index], nil
			}
		}
		return nil, nil, fmt.Errorf("DigitalOcean token group not found: %s", entryGroup)
	}

	if entryID == "" || entryID == activeProviderEntryID {
		token := addition.ActiveToken()
		if token == nil {
			return nil, nil, errors.New("DigitalOcean token is not configured")
		}
		return addition, token, nil
	}

	token := addition.FindToken(entryID)
	if token == nil {
		return nil, nil, newProviderEntryNotFoundError("digitalocean", entryID)
	}
	return addition, token, nil
}

func saveDigitalOceanAddition(userUUID string, addition *digitalocean.Addition) error {
	if addition == nil {
		addition = &digitalocean.Addition{}
	}
	addition.Normalize()

	payload, err := json.Marshal(addition)
	if err != nil {
		return err
	}
	return saveProviderAddition(userUUID, "digitalocean", string(payload))
}

func reloadDigitalOceanAdditionTokenState(userUUID string, token *digitalocean.TokenRecord) (*digitalocean.Addition, *digitalocean.TokenRecord, error) {
	if token == nil {
		return nil, nil, errors.New("DigitalOcean token is not configured")
	}

	addition, err := loadDigitalOceanAddition(userUUID)
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
		return nil, nil, newProviderEntryNotFoundError("digitalocean", token.ID)
	}

	return addition, latestToken, nil
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
	if err := json.Unmarshal([]byte(raw), addition); err != nil {
		return nil, fmt.Errorf("Linode configuration is invalid: %w", err)
	}
	addition.Normalize()
	return addition, nil
}

func loadLinodeToken(userUUID, entryID string) (*linode.Addition, *linode.TokenRecord, error) {
	return loadLinodeTokenSelection(userUUID, entryID, "")
}

func loadLinodeTokenSelection(userUUID, entryID, entryGroup string) (*linode.Addition, *linode.TokenRecord, error) {
	addition, err := loadLinodeAddition(userUUID)
	if err != nil {
		return nil, nil, err
	}

	entryID = strings.TrimSpace(entryID)
	entryGroup = normalizeProviderEntryGroup(entryGroup)
	if entryGroup != "" {
		if entryID != "" && entryID != activeProviderEntryID {
			token := addition.FindToken(entryID)
			if token == nil {
				return nil, nil, newProviderEntryNotFoundError("linode", entryID)
			}
			if normalizeProviderEntryGroup(token.Group) != entryGroup {
				return nil, nil, fmt.Errorf("Linode token %s is not in group %s", entryID, entryGroup)
			}
			return addition, token, nil
		}

		if token := addition.ActiveToken(); token != nil && normalizeProviderEntryGroup(token.Group) == entryGroup {
			return addition, token, nil
		}
		for index := range addition.Tokens {
			if normalizeProviderEntryGroup(addition.Tokens[index].Group) == entryGroup {
				return addition, &addition.Tokens[index], nil
			}
		}
		return nil, nil, fmt.Errorf("Linode token group not found: %s", entryGroup)
	}

	if entryID == "" || entryID == activeProviderEntryID {
		token := addition.ActiveToken()
		if token == nil {
			return nil, nil, errors.New("Linode token is not configured")
		}
		return addition, token, nil
	}

	token := addition.FindToken(entryID)
	if token == nil {
		return nil, nil, newProviderEntryNotFoundError("linode", entryID)
	}
	return addition, token, nil
}

func saveLinodeAddition(userUUID string, addition *linode.Addition) error {
	if addition == nil {
		addition = &linode.Addition{}
	}
	addition.Normalize()

	payload, err := json.Marshal(addition)
	if err != nil {
		return err
	}
	return saveProviderAddition(userUUID, "linode", string(payload))
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
		return nil, nil, newProviderEntryNotFoundError("linode", token.ID)
	}

	return addition, latestToken, nil
}

func loadGenericProviderEntry(userUUID, providerName, entryID string) (*genericProviderEntry, error) {
	raw, err := loadProviderAddition(userUUID, providerName)
	if err != nil {
		return nil, fmt.Errorf("%s provider is not configured", providerName)
	}
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "{}" || raw == "null" {
		return nil, fmt.Errorf("%s provider is not configured", providerName)
	}

	var object map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &object); err == nil {
		if rawEntries, ok := object["entries"]; ok {
			var entries []genericProviderEntry
			if err := json.Unmarshal(rawEntries, &entries); err != nil {
				return nil, fmt.Errorf("%s provider configuration is invalid: %w", providerName, err)
			}
			for _, entry := range entries {
				normalized := normalizeGenericProviderEntry(entry)
				if normalized.ID == entryID {
					return &normalized, nil
				}
			}
			return nil, fmt.Errorf("%s provider entry not found: %s", providerName, entryID)
		}
	}

	if strings.TrimSpace(entryID) != "" && strings.TrimSpace(entryID) != defaultDNSProviderEntryID {
		return nil, fmt.Errorf("%s provider entry not found: %s", providerName, entryID)
	}

	var values map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &values); err != nil {
		return nil, fmt.Errorf("%s provider configuration is invalid: %w", providerName, err)
	}
	entry := normalizeGenericProviderEntry(genericProviderEntry{
		ID:     defaultDNSProviderEntryID,
		Name:   "Default",
		Values: values,
	})
	return &entry, nil
}

func normalizeGenericProviderEntry(entry genericProviderEntry) genericProviderEntry {
	entry.ID = strings.TrimSpace(entry.ID)
	entry.Name = strings.TrimSpace(entry.Name)
	if entry.ID == "" {
		entry.ID = defaultDNSProviderEntryID
	}
	if entry.Name == "" {
		entry.Name = "Default"
	}
	if entry.Values == nil {
		entry.Values = map[string]interface{}{}
	}
	return entry
}

func decodeGenericEntryConfig[T any](entry *genericProviderEntry) (*T, error) {
	if entry == nil {
		return nil, errors.New("provider entry is required")
	}
	payload, err := json.Marshal(entry.Values)
	if err != nil {
		return nil, err
	}
	var value T
	if err := json.Unmarshal(payload, &value); err != nil {
		return nil, err
	}
	return &value, nil
}

type autoConnectOptions struct {
	UserUUID          string
	UserData          string
	Provider          string
	CredentialName    string
	PoolGroup         string
	Group             string
	WrapInShellScript bool
}

func buildAutoConnectUserData(opts autoConnectOptions) (string, string, error) {
	userData := strings.TrimSpace(opts.UserData)
	origin, err := resolveAutoConnectOriginForUser(opts.UserUUID)
	if err != nil {
		return "", "", err
	}

	autoDiscoveryKey, err := config.EnsureAutoDiscoveryKeyForUser(opts.UserUUID)
	if err != nil {
		return "", "", fmt.Errorf("failed to load auto discovery key: %w", err)
	}
	autoDiscoveryKey = strings.TrimSpace(autoDiscoveryKey)
	if len(autoDiscoveryKey) < 12 {
		return "", "", errors.New("auto connect requires a configured Auto Discovery Key")
	}

	group := normalizeAutoConnectGroup(opts.Group)
	if group == "" {
		group = normalizeAutoConnectGroup(defaultAutoConnectGroup(opts.Provider, opts.PoolGroup, opts.CredentialName))
	}
	if group == "" {
		return "", "", errors.New("auto connect group cannot be empty")
	}

	installScriptURL, err := resolveAgentInstallScriptURLForUser(opts.UserUUID, "install.sh")
	if err != nil {
		return "", "", err
	}

	scopedKey := buildScopedAutoDiscoveryKey(autoDiscoveryKey, group)
	snippet := buildInstallSnippet(installScriptURL, origin, scopedKey)
	merged, err := mergeAutoConnectUserData(userData, snippet, opts.WrapInShellScript)
	if err != nil {
		return "", "", err
	}
	return merged, group, nil
}

func buildAWSIPv6RefreshUserData(userData string) (string, error) {
	return mergeShellUserData(userData, buildAWSIPv6RefreshSnippet(), true, "AWS IPv6 refresh")
}

func buildAWSIPv6RefreshSnippet() string {
	var builder strings.Builder
	builder.WriteString("# Komari AWS IPv6 refresh\n")
	builder.WriteString("KOMARI_IPV6_IFACE=\"$(ip -o route show to default 2>/dev/null | awk '{print $5; exit}')\"\n")
	builder.WriteString("if [ -z \"$KOMARI_IPV6_IFACE\" ]; then\n")
	builder.WriteString("  KOMARI_IPV6_IFACE=\"$(ip -o link show 2>/dev/null | awk -F': ' '$2 != \"lo\" {print $2; exit}')\"\n")
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

func resolveAutoConnectOriginForUser(userUUID string) (string, error) {
	scriptDomain, err := config.GetAsForUser[string](userUUID, config.ScriptDomainKey, "")
	if err != nil {
		return "", fmt.Errorf("failed to load script domain: %w", err)
	}
	scriptDomain = strings.TrimSpace(scriptDomain)
	if scriptDomain == "" {
		return "", errors.New("script_domain is required for automatic failover auto-connect; set Settings -> Site -> Agent connection address because failover tasks run without a browser request context")
	}
	if strings.Contains(scriptDomain, "://") {
		return strings.TrimRight(scriptDomain, "/"), nil
	}
	return "https://" + strings.TrimRight(scriptDomain, "/"), nil
}

func resolveAgentInstallScriptURLForUser(userUUID, scriptFile string) (string, error) {
	baseScriptsURL, err := config.GetAsForUser[string](userUUID, config.BaseScriptsURLKey, "")
	if err != nil {
		return "", fmt.Errorf("failed to load base scripts url: %w", err)
	}
	return buildAgentInstallScriptURL(baseScriptsURL, scriptFile), nil
}

func buildAgentInstallScriptURL(baseScriptsURL, scriptFile string) string {
	base := normalizeAgentInstallScriptBaseURL(baseScriptsURL)
	scriptFile = strings.TrimLeft(strings.TrimSpace(scriptFile), "/")
	if scriptFile == "" {
		scriptFile = "install.sh"
	}
	return strings.TrimRight(base, "/") + "/" + scriptFile
}

func normalizeAgentInstallScriptBaseURL(raw string) string {
	base := strings.TrimSpace(raw)
	if base == "" {
		return defaultAgentInstallScriptBaseURL
	}

	if !strings.Contains(base, "://") && strings.HasPrefix(base, "github.com/") {
		base = "https://" + base
	}

	parsed, err := url.Parse(base)
	if err == nil && parsed.Host != "" {
		switch strings.ToLower(parsed.Host) {
		case "github.com", "www.github.com":
			if normalized := normalizeGitHubRepositoryURLToRawBase(parsed); normalized != "" {
				return normalized
			}
		case "raw.githubusercontent.com":
			if normalized := normalizeRawGitHubURLToBase(parsed); normalized != "" {
				return normalized
			}
		}
	}

	if strings.HasSuffix(base, "/install.sh") || strings.HasSuffix(base, "/install.ps1") {
		if idx := strings.LastIndex(base, "/"); idx > 0 {
			base = base[:idx]
		}
	}

	if !strings.Contains(base, "://") {
		base = "https://" + base
	}
	return strings.TrimRight(base, "/")
}

func normalizeGitHubRepositoryURLToRawBase(parsed *url.URL) string {
	parts := splitURLPath(parsed.Path)
	if len(parts) < 2 {
		return ""
	}

	owner := parts[0]
	repo := parts[1]
	branch := "main"
	subpath := ""
	if len(parts) >= 4 && (parts[2] == "tree" || parts[2] == "blob") {
		branch = parts[3]
		if len(parts) > 4 {
			subpath = strings.Join(parts[4:], "/")
		}
	}

	if strings.HasSuffix(subpath, "install.sh") || strings.HasSuffix(subpath, "install.ps1") {
		if idx := strings.LastIndex(subpath, "/"); idx >= 0 {
			subpath = subpath[:idx]
		} else {
			subpath = ""
		}
	}

	base := fmt.Sprintf("https://raw.githubusercontent.com/%s/%s/refs/heads/%s", owner, repo, branch)
	if subpath != "" {
		base += "/" + strings.Trim(subpath, "/")
	}
	return base
}

func normalizeRawGitHubURLToBase(parsed *url.URL) string {
	parts := splitURLPath(parsed.Path)
	if len(parts) < 3 {
		return ""
	}
	baseParts := parts
	if strings.HasSuffix(baseParts[len(baseParts)-1], "install.sh") || strings.HasSuffix(baseParts[len(baseParts)-1], "install.ps1") {
		baseParts = baseParts[:len(baseParts)-1]
	}
	return "https://raw.githubusercontent.com/" + strings.Join(baseParts, "/")
}

func splitURLPath(path string) []string {
	trimmed := strings.Trim(path, "/")
	if trimmed == "" {
		return nil
	}
	return strings.Split(trimmed, "/")
}

func normalizeAutoConnectGroup(group string) string {
	group = strings.Join(strings.Fields(strings.TrimSpace(group)), " ")
	if len(group) > 100 {
		group = group[:100]
	}
	return strings.TrimSpace(group)
}

func defaultAutoConnectGroup(provider, poolGroup, credentialName string) string {
	provider = strings.ToLower(strings.TrimSpace(provider))
	if provider == "" {
		provider = "cloud"
	}
	poolGroup = normalizeAutoConnectGroup(poolGroup)
	if poolGroup != "" {
		return provider + "/" + poolGroup
	}
	credentialName = normalizeAutoConnectGroup(credentialName)
	if credentialName == "" {
		credentialName = "default"
	}
	return provider + "/" + credentialName
}

func buildScopedAutoDiscoveryKey(baseKey, group string) string {
	baseKey = strings.TrimSpace(baseKey)
	group = normalizeAutoConnectGroup(group)
	if group == "" {
		return baseKey
	}
	return baseKey + "::group-b64=" + base64.RawURLEncoding.EncodeToString([]byte(group))
}

func buildInstallSnippet(installScriptURL, endpoint, scopedAutoDiscoveryKey string) string {
	var builder strings.Builder
	builder.WriteString("# Komari auto-connect\n")
	builder.WriteString("KOMARI_INSTALL_URL=")
	builder.WriteString(shellSingleQuote(strings.TrimSpace(installScriptURL)))
	builder.WriteString("\n")
	builder.WriteString("KOMARI_ENDPOINT=")
	builder.WriteString(shellSingleQuote(strings.TrimSpace(endpoint)))
	builder.WriteString("\n")
	builder.WriteString("KOMARI_AUTO_DISCOVERY=")
	builder.WriteString(shellSingleQuote(strings.TrimSpace(scopedAutoDiscoveryKey)))
	builder.WriteString("\n")
	builder.WriteString("KOMARI_INSTALL_SCRIPT=\"$(mktemp)\"\n")
	builder.WriteString("trap 'rm -f \"$KOMARI_INSTALL_SCRIPT\"' EXIT\n")
	builder.WriteString("if command -v wget >/dev/null 2>&1; then\n")
	builder.WriteString("  wget -qO \"$KOMARI_INSTALL_SCRIPT\" \"$KOMARI_INSTALL_URL\"\n")
	builder.WriteString("elif command -v curl >/dev/null 2>&1; then\n")
	builder.WriteString("  curl -fsSL \"$KOMARI_INSTALL_URL\" -o \"$KOMARI_INSTALL_SCRIPT\"\n")
	builder.WriteString("else\n")
	builder.WriteString("  echo 'wget or curl is required to install komari-agent' >&2\n")
	builder.WriteString("  exit 1\n")
	builder.WriteString("fi\n")
	builder.WriteString("if command -v sudo >/dev/null 2>&1; then\n")
	builder.WriteString("  sudo bash \"$KOMARI_INSTALL_SCRIPT\" -e \"$KOMARI_ENDPOINT\" --auto-discovery \"$KOMARI_AUTO_DISCOVERY\"\n")
	builder.WriteString("else\n")
	builder.WriteString("  bash \"$KOMARI_INSTALL_SCRIPT\" -e \"$KOMARI_ENDPOINT\" --auto-discovery \"$KOMARI_AUTO_DISCOVERY\"\n")
	builder.WriteString("fi\n")
	builder.WriteString("rm -f \"$KOMARI_INSTALL_SCRIPT\"\n")
	builder.WriteString("trap - EXIT\n")
	return builder.String()
}

func mergeAutoConnectUserData(userData, installSnippet string, wrapInShellScript bool) (string, error) {
	return mergeShellUserData(userData, installSnippet, wrapInShellScript, "auto connect")
}

func mergeShellUserData(userData, snippet string, wrapInShellScript bool, purpose string) (string, error) {
	userData = strings.TrimSpace(userData)
	if strings.HasPrefix(userData, "#cloud-config") {
		purpose = strings.TrimSpace(purpose)
		if purpose == "" {
			purpose = "this feature"
		}
		return "", fmt.Errorf("%s cannot be combined with #cloud-config user_data; use shell commands instead", purpose)
	}
	snippet = strings.TrimSpace(snippet)
	if snippet == "" {
		return userData, nil
	}

	if userData == "" {
		if !wrapInShellScript {
			return snippet + "\n", nil
		}
		var builder strings.Builder
		builder.WriteString("#!/bin/bash\n")
		builder.WriteString("set -eu\n\n")
		builder.WriteString(snippet)
		builder.WriteString("\n")
		return builder.String(), nil
	}

	var builder strings.Builder
	builder.WriteString(userData)
	if !strings.HasSuffix(userData, "\n") {
		builder.WriteString("\n")
	}
	builder.WriteString("\n")
	builder.WriteString(snippet)
	if !strings.HasSuffix(snippet, "\n") {
		builder.WriteString("\n")
	}
	return builder.String(), nil
}

func shellSingleQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", `'"'"'`) + "'"
}
