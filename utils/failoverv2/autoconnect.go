package failoverv2

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/url"
	"strings"
	"time"

	"github.com/komari-monitor/komari/config"
	"github.com/komari-monitor/komari/database"
	"github.com/komari-monitor/komari/database/models"
	"github.com/komari-monitor/komari/utils/cloudprovider/digitalocean"
)

const (
	activeProviderEntryID            = "active"
	defaultAgentInstallScriptBaseURL = "https://raw.githubusercontent.com/keli-123456/kelicloud-agent/refs/heads/main"
)

type autoConnectOptions struct {
	UserUUID          string
	UserData          string
	Group             string
	WrapInShellScript bool
}

func saveProviderAddition(userUUID, providerName, addition string) error {
	return database.SaveCloudProviderConfigForUser(&models.CloudProvider{
		UserID:   strings.TrimSpace(userUUID),
		Name:     strings.TrimSpace(providerName),
		Addition: addition,
	})
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
				return nil, nil, fmt.Errorf("DigitalOcean token not found: %s", entryID)
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
		return nil, nil, fmt.Errorf("DigitalOcean token not found: %s", entryID)
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
		return nil, nil, fmt.Errorf("DigitalOcean token not found: %s", strings.TrimSpace(token.ID))
	}

	return addition, latestToken, nil
}

func persistDigitalOceanRootPassword(userUUID string, addition *digitalocean.Addition, token *digitalocean.TokenRecord, dropletID int, dropletName, passwordMode, rootPassword string) error {
	if addition == nil || token == nil || dropletID <= 0 || strings.TrimSpace(rootPassword) == "" {
		return nil
	}

	latestAddition, latestToken, err := reloadDigitalOceanAdditionTokenState(userUUID, token)
	if err != nil {
		log.Printf("failoverv2: failed to reload DigitalOcean token state for droplet %d: %v", dropletID, err)
		return err
	}
	if err := latestToken.SaveDropletPassword(dropletID, dropletName, passwordMode, rootPassword, time.Now()); err != nil {
		log.Printf("failoverv2: failed to save DigitalOcean root password for droplet %d: %v", dropletID, err)
		return err
	}
	if err := saveDigitalOceanAddition(userUUID, latestAddition); err != nil {
		latestToken.RemoveSavedDropletPassword(dropletID)
		log.Printf("failoverv2: failed to persist DigitalOcean root password for droplet %d: %v", dropletID, err)
		return err
	}
	return nil
}

func removeSavedDigitalOceanRootPassword(userUUID string, addition *digitalocean.Addition, token *digitalocean.TokenRecord, dropletID int) {
	if addition == nil || token == nil || dropletID <= 0 {
		return
	}

	targetAddition := addition
	targetToken := token
	if latestAddition, latestToken, err := reloadDigitalOceanAdditionTokenState(userUUID, token); err == nil {
		targetAddition = latestAddition
		targetToken = latestToken
	} else {
		log.Printf("failoverv2: failed to reload DigitalOcean token state for droplet %d cleanup, falling back to in-memory state: %v", dropletID, err)
	}

	if !targetToken.RemoveSavedDropletPassword(dropletID) {
		return
	}
	if err := saveDigitalOceanAddition(userUUID, targetAddition); err != nil {
		log.Printf("failoverv2: failed to remove saved DigitalOcean root password for droplet %d: %v", dropletID, err)
	}
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

func resolveAutoConnectOriginForUser(userUUID string) (string, error) {
	scriptDomain, err := config.GetAsForUser[string](userUUID, config.ScriptDomainKey, "")
	if err != nil {
		return "", fmt.Errorf("failed to load script domain: %w", err)
	}
	scriptDomain = strings.TrimSpace(scriptDomain)
	if scriptDomain == "" {
		return "", errors.New("script_domain is required for V2 auto-connect; set Settings -> Site -> Agent connection address")
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
