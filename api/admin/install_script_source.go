package admin

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/komari-monitor/komari/config"
)

const defaultAgentInstallScriptBaseURL = "https://raw.githubusercontent.com/komari-monitor/komari-agent/refs/heads/main"

func resolveAgentInstallScriptURL(scriptFile string) (string, error) {
	baseScriptsURL, err := config.GetAs[string](config.BaseScriptsURLKey, "")
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
