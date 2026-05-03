package ws

import (
	"net/http"
	"net/url"
	"os"
	"strings"
)

const (
	wsDisableOriginEnv   = "KOMARI_WS_DISABLE_ORIGIN"
	wsAllowedOriginsEnv  = "KOMARI_WS_ALLOWED_ORIGINS"
	corsAllowedOriginsEnv = "KOMARI_CORS_ALLOWED_ORIGINS"
)

func CheckOrigin(r *http.Request) bool {
	// 显式关闭校验
	if strings.EqualFold(os.Getenv(wsDisableOriginEnv), "true") {
		return true
	}
	origin := normalizedOrigin(r.Header.Get("Origin"))
	if origin == "" {
		return false
	}
	if isSameHostOrigin(r, origin) {
		return true
	}
	for _, allowed := range configuredAllowedOrigins() {
		if allowed == "*" || allowed == origin {
			return true
		}
	}
	return false
}

func configuredAllowedOrigins() []string {
	var origins []string
	for _, envKey := range []string{corsAllowedOriginsEnv, wsAllowedOriginsEnv} {
		for _, part := range strings.Split(os.Getenv(envKey), ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			if part == "*" {
				origins = append(origins, part)
				continue
			}
			if normalized := normalizedOrigin(part); normalized != "" {
				origins = append(origins, normalized)
			}
		}
	}
	return origins
}

func isSameHostOrigin(r *http.Request, origin string) bool {
	parsed, err := url.Parse(origin)
	if err != nil || parsed.Host == "" {
		return false
	}
	return strings.EqualFold(parsed.Host, r.Host)
}

func normalizedOrigin(origin string) string {
	parsed, err := url.Parse(strings.TrimSpace(origin))
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return ""
	}
	scheme := strings.ToLower(parsed.Scheme)
	if scheme != "http" && scheme != "https" {
		return ""
	}
	return scheme + "://" + strings.ToLower(parsed.Host)
}
