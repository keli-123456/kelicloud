package admin

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/komari-monitor/komari/config"
	webutils "github.com/komari-monitor/komari/utils"
)

type autoConnectUserDataOptions struct {
	Enabled           bool
	Group             string
	Provider          string
	CredentialName    string
	WrapInShellScript bool
}

func prepareCloudAutoConnectUserData(c *gin.Context, userData string, opts autoConnectUserDataOptions) (string, string, error) {
	userData = strings.TrimSpace(userData)
	if !opts.Enabled {
		return userData, "", nil
	}

	autoDiscoveryKey, err := config.GetAs[string](config.AutoDiscoveryKeyKey, "")
	if err != nil {
		return "", "", fmt.Errorf("failed to load auto discovery key: %w", err)
	}
	autoDiscoveryKey = strings.TrimSpace(autoDiscoveryKey)
	if len(autoDiscoveryKey) < 12 {
		return "", "", errors.New("auto connect requires a configured Auto Discovery Key")
	}

	endpoint, err := resolveCloudAutoConnectEndpoint(c)
	if err != nil {
		return "", "", err
	}

	group := normalizeCloudAutoConnectGroup(opts.Group)
	if group == "" {
		group = normalizeCloudAutoConnectGroup(defaultCloudAutoConnectGroup(opts.Provider, opts.CredentialName))
	}
	if group == "" {
		return "", "", errors.New("auto connect group cannot be empty")
	}

	scopedAutoDiscoveryKey := buildScopedAutoDiscoveryKey(autoDiscoveryKey, group)
	installSnippet := buildCloudAutoConnectInstallSnippet(endpoint, scopedAutoDiscoveryKey)
	mergedUserData, err := mergeCloudAutoConnectUserData(userData, installSnippet, opts.WrapInShellScript)
	if err != nil {
		return "", "", err
	}

	return mergedUserData, group, nil
}

func resolveCloudAutoConnectEndpoint(c *gin.Context) (string, error) {
	scriptDomain, err := config.GetAs[string](config.ScriptDomainKey, "")
	if err != nil {
		return "", fmt.Errorf("failed to load script domain: %w", err)
	}

	return resolveCloudAutoConnectOrigin(
		scriptDomain,
		webutils.GetScheme(c),
		resolveCloudAutoConnectRequestHost(c),
	)
}

func resolveCloudAutoConnectOrigin(scriptDomain, requestScheme, requestHost string) (string, error) {
	scriptDomain = strings.TrimSpace(scriptDomain)
	if scriptDomain != "" {
		return normalizeCloudAutoConnectOrigin(scriptDomain)
	}

	requestHost = strings.TrimSpace(requestHost)
	if requestHost == "" {
		return "", errors.New("failed to resolve panel host for auto connect")
	}

	scheme := strings.TrimSpace(requestScheme)
	if scheme == "" {
		scheme = "http"
	}

	return fmt.Sprintf("%s://%s", scheme, requestHost), nil
}

func normalizeCloudAutoConnectOrigin(origin string) (string, error) {
	origin = strings.TrimSpace(origin)
	if origin == "" {
		return "", errors.New("auto connect origin is empty")
	}

	origin = strings.TrimRight(origin, "/")
	if strings.Contains(origin, "://") {
		return origin, nil
	}
	return "http://" + origin, nil
}

func resolveCloudAutoConnectRequestHost(c *gin.Context) string {
	for _, rawHost := range []string{
		c.Request.Header.Get("X-Forwarded-Host"),
		c.Request.Host,
	} {
		rawHost = strings.TrimSpace(rawHost)
		if rawHost == "" {
			continue
		}

		if strings.Contains(rawHost, ",") {
			rawHost = strings.TrimSpace(strings.Split(rawHost, ",")[0])
		}
		if rawHost != "" {
			return rawHost
		}
	}

	return ""
}

func defaultCloudAutoConnectGroup(provider, credentialName string) string {
	provider = strings.ToLower(strings.TrimSpace(provider))
	if provider == "" {
		provider = "cloud"
	}

	credentialName = normalizeCloudAutoConnectGroup(credentialName)
	if credentialName == "" {
		credentialName = "default"
	}

	return provider + "/" + credentialName
}

func normalizeCloudAutoConnectGroup(group string) string {
	group = strings.Join(strings.Fields(strings.TrimSpace(group)), " ")
	if len(group) > 100 {
		group = group[:100]
	}
	return strings.TrimSpace(group)
}

func buildScopedAutoDiscoveryKey(baseKey, group string) string {
	baseKey = strings.TrimSpace(baseKey)
	group = normalizeCloudAutoConnectGroup(group)
	if group == "" {
		return baseKey
	}
	return baseKey + "::group-b64=" + base64.RawURLEncoding.EncodeToString([]byte(group))
}

func buildCloudAutoConnectInstallSnippet(endpoint, scopedAutoDiscoveryKey string) string {
	installScriptURL, err := resolveAgentInstallScriptURL("install.sh")
	if err != nil {
		installScriptURL = buildAgentInstallScriptURL("", "install.sh")
	}

	var builder strings.Builder
	builder.WriteString("# Komari auto-connect\n")
	builder.WriteString("KOMARI_INSTALL_URL=")
	builder.WriteString(shellSingleQuote(installScriptURL))
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

func mergeCloudAutoConnectUserData(userData, installSnippet string, wrapInShellScript bool) (string, error) {
	userData = strings.TrimSpace(userData)
	if strings.HasPrefix(userData, "#cloud-config") {
		return "", errors.New("auto connect cannot be combined with #cloud-config user_data; use shell commands instead")
	}

	installSnippet = strings.TrimSpace(installSnippet)
	if installSnippet == "" {
		return userData, nil
	}

	if userData == "" {
		if !wrapInShellScript {
			return installSnippet + "\n", nil
		}

		var builder strings.Builder
		builder.WriteString("#!/bin/bash\n")
		builder.WriteString("set -eu\n\n")
		builder.WriteString(installSnippet)
		builder.WriteString("\n")
		return builder.String(), nil
	}

	var builder strings.Builder
	builder.WriteString(userData)
	if !strings.HasSuffix(userData, "\n") {
		builder.WriteString("\n")
	}
	builder.WriteString("\n")
	builder.WriteString(installSnippet)
	if !strings.HasSuffix(installSnippet, "\n") {
		builder.WriteString("\n")
	}
	return builder.String(), nil
}

func shellSingleQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", `'"'"'`) + "'"
}
