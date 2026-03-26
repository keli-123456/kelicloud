package outboundproxy

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

type ProbeResult struct {
	Mode       string `json:"mode"`
	IPv4       string `json:"ipv4,omitempty"`
	IPv6       string `json:"ipv6,omitempty"`
	IPv4URL    string `json:"ipv4_url,omitempty"`
	IPv6URL    string `json:"ipv6_url,omitempty"`
	ProxyError string `json:"proxy_error,omitempty"`
}

var probeTargetsIPv4 = []string{
	"https://api.ipify.org?format=json",
	"https://ifconfig.me/ip",
}

var probeTargetsIPv6 = []string{
	"https://api64.ipify.org?format=json",
}

var fetchExitIPsFunc = fetchExitIPs

func Probe(ctx context.Context) (*ProbeResult, error) {
	settings, err := LoadSettings()
	if err != nil {
		return nil, err
	}
	return ProbeWithSettings(ctx, settings)
}

func ProbeWithSettings(ctx context.Context, settings *Settings) (*ProbeResult, error) {
	if settings != nil {
		cloned := *settings
		settings = &cloned
		settings.Protocol = strings.TrimSpace(settings.Protocol)
		settings.Host = strings.TrimSpace(settings.Host)
		settings.Username = strings.TrimSpace(settings.Username)
		settings.normalizeCredentialLine()
	}

	if settings == nil || !settings.Enabled {
		ipv4, ipv4URL, ipv6, ipv6URL, err := fetchExitIPsFunc(ctx, &http.Client{
			Timeout:   10 * time.Second,
			Transport: newBaseTransport(),
		})
		if err != nil {
			return nil, err
		}
		return &ProbeResult{
			Mode:    "direct",
			IPv4:    ipv4,
			IPv4URL: ipv4URL,
			IPv6:    ipv6,
			IPv6URL: ipv6URL,
		}, nil
	}

	proxyTransport, err := newProxyTransport(settings)
	if err != nil {
		return nil, err
	}

	ipv4, ipv4URL, ipv6, ipv6URL, proxyErr := fetchExitIPsFunc(ctx, &http.Client{
		Timeout:   10 * time.Second,
		Transport: proxyTransport,
	})
	if proxyErr == nil {
		return &ProbeResult{
			Mode:    "proxy",
			IPv4:    ipv4,
			IPv4URL: ipv4URL,
			IPv6:    ipv6,
			IPv6URL: ipv6URL,
		}, nil
	}

	directIPv4, directIPv4URL, directIPv6, directIPv6URL, directErr := fetchExitIPsFunc(ctx, &http.Client{
		Timeout:   10 * time.Second,
		Transport: newBaseTransport(),
	})
	if directErr != nil {
		return nil, fmt.Errorf("proxy probe failed: %w; direct probe failed: %w", proxyErr, directErr)
	}

	return &ProbeResult{
		Mode:       "direct_fallback",
		IPv4:       directIPv4,
		IPv4URL:    directIPv4URL,
		IPv6:       directIPv6,
		IPv6URL:    directIPv6URL,
		ProxyError: proxyErr.Error(),
	}, nil
}

func fetchExitIPs(ctx context.Context, client *http.Client) (string, string, string, string, error) {
	ipv4, ipv4URL, ipv4Err := fetchExitIPFromTargets(ctx, client, probeTargetsIPv4)
	ipv6, ipv6URL, ipv6Err := fetchExitIPFromTargets(ctx, client, probeTargetsIPv6)

	if ipv4Err != nil && ipv6Err != nil {
		return "", "", "", "", fmt.Errorf("ipv4 probe failed: %w; ipv6 probe failed: %w", ipv4Err, ipv6Err)
	}

	return ipv4, ipv4URL, ipv6, ipv6URL, nil
}

func fetchExitIPFromTargets(ctx context.Context, client *http.Client, targets []string) (string, string, error) {
	var lastErr error
	for _, target := range targets {
		exitIP, err := fetchExitIPFromTarget(ctx, client, target)
		if err == nil {
			return exitIP, target, nil
		}
		lastErr = err
	}
	if lastErr != nil {
		return "", "", fmt.Errorf("all probe targets failed: %w", lastErr)
	}
	return "", "", fmt.Errorf("all probe targets failed")
}

func fetchExitIPFromTarget(ctx context.Context, client *http.Client, target string) (string, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return "", err
	}
	request.Header.Set("Accept", "application/json, text/plain;q=0.9, */*;q=0.8")

	response, err := client.Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()

	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return "", fmt.Errorf("probe target returned %s", response.Status)
	}

	body, err := io.ReadAll(io.LimitReader(response.Body, 4096))
	if err != nil {
		return "", err
	}

	if strings.Contains(target, "format=json") {
		var payload struct {
			IP string `json:"ip"`
		}
		if err := json.Unmarshal(body, &payload); err == nil {
			if parsed := strings.TrimSpace(payload.IP); net.ParseIP(parsed) != nil {
				return parsed, nil
			}
		}
	}

	parsed := strings.TrimSpace(string(body))
	if net.ParseIP(parsed) == nil {
		return "", fmt.Errorf("probe target did not return a valid IP")
	}
	return parsed, nil
}
