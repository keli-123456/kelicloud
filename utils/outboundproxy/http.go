package outboundproxy

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/komari-monitor/komari/config"
	xproxy "golang.org/x/net/proxy"
)

type Settings struct {
	Enabled  bool
	Protocol string
	Host     string
	Port     int
	Username string
	Password string
}

func LoadSettings() (*Settings, error) {
	legacy, err := config.GetManyAs[config.Legacy]()
	if err != nil {
		return nil, err
	}

	settings := &Settings{
		Enabled:  legacy.OutboundProxyEnabled,
		Protocol: strings.TrimSpace(legacy.OutboundProxyProtocol),
		Host:     strings.TrimSpace(legacy.OutboundProxyHost),
		Port:     legacy.OutboundProxyPort,
		Username: strings.TrimSpace(legacy.OutboundProxyUsername),
		Password: legacy.OutboundProxyPassword,
	}
	settings.normalizeCredentialLine()
	return settings, nil
}

func NewHTTPClient(timeout time.Duration) *http.Client {
	directTransport := newBaseTransport()
	applyAttemptTimeout(directTransport, timeout)
	transport, err := NewTransport()
	if err != nil {
		slog.Warn("failed to build outbound proxy transport", "error", err)
		return &http.Client{
			Transport: directTransport,
		}
	}
	if transport == nil {
		return &http.Client{
			Transport: directTransport,
		}
	}
	applyAttemptTimeout(transport, timeout)
	return &http.Client{
		Transport: &fallbackRoundTripper{
			proxied: transport,
			direct:  directTransport,
		},
	}
}

func applyAttemptTimeout(transport *http.Transport, timeout time.Duration) {
	if transport == nil || timeout <= 0 {
		return
	}
	transport.ResponseHeaderTimeout = timeout
}

func NewTransport() (*http.Transport, error) {
	settings, err := LoadSettings()
	if err != nil {
		return nil, err
	}
	if settings == nil || !settings.Enabled {
		return nil, nil
	}

	return newProxyTransport(settings)
}

type fallbackRoundTripper struct {
	proxied http.RoundTripper
	direct  http.RoundTripper
}

func (r *fallbackRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := r.proxied.RoundTrip(request)
	if !shouldFallback(response, err) {
		return response, err
	}

	retryRequest, retryErr := cloneRequestForRetry(request)
	if retryErr != nil {
		return response, err
	}

	if response != nil && response.Body != nil {
		response.Body.Close()
	}

	if err != nil {
		slog.Warn("outbound proxy request failed, falling back to direct connection", "method", request.Method, "host", request.URL.Host, "error", err)
	} else if response != nil {
		slog.Warn("outbound proxy rejected request, falling back to direct connection", "method", request.Method, "host", request.URL.Host, "status", response.StatusCode)
	}

	return r.direct.RoundTrip(retryRequest)
}

func shouldFallback(response *http.Response, err error) bool {
	if err != nil {
		return true
	}
	return response != nil && response.StatusCode == http.StatusProxyAuthRequired
}

func cloneRequestForRetry(request *http.Request) (*http.Request, error) {
	retryRequest := request.Clone(request.Context())
	if request.Body == nil || request.Body == http.NoBody {
		return retryRequest, nil
	}
	if request.GetBody == nil {
		return nil, errors.New("request body cannot be replayed")
	}

	body, err := request.GetBody()
	if err != nil {
		return nil, err
	}
	retryRequest.Body = body
	return retryRequest, nil
}

func newProxyTransport(settings *Settings) (*http.Transport, error) {
	if settings == nil || !settings.Enabled {
		return nil, nil
	}

	scheme := normalizeProtocol(settings.Protocol)
	if scheme == "" {
		return nil, fmt.Errorf("outbound proxy protocol is invalid")
	}

	if scheme == "socks5" {
		return newSocks5Transport(settings)
	}

	proxyURL, err := settings.ProxyURL()
	if err != nil {
		return nil, err
	}

	transport := newBaseTransport()
	transport.Proxy = http.ProxyURL(proxyURL)
	transport.DisableKeepAlives = true
	transport.ForceAttemptHTTP2 = false
	transport.MaxIdleConns = 0
	transport.MaxIdleConnsPerHost = 0
	transport.MaxConnsPerHost = 0
	return transport, nil
}

func newSocks5Transport(settings *Settings) (*http.Transport, error) {
	host := normalizeHost(settings.Host)
	if host == "" {
		return nil, fmt.Errorf("outbound proxy host is empty")
	}
	if settings.Port <= 0 || settings.Port > 65535 {
		return nil, fmt.Errorf("outbound proxy port is invalid")
	}

	var auth *xproxy.Auth
	if settings.Username != "" || settings.Password != "" {
		auth = &xproxy.Auth{
			User:     settings.Username,
			Password: settings.Password,
		}
	}

	forwardDialer := &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}

	socksDialer, err := xproxy.SOCKS5(
		"tcp",
		net.JoinHostPort(host, strconv.Itoa(settings.Port)),
		auth,
		forwardDialer,
	)
	if err != nil {
		return nil, err
	}

	contextDialer, ok := socksDialer.(xproxy.ContextDialer)
	if !ok {
		return nil, fmt.Errorf("socks5 dialer does not support contexts")
	}

	transport := newBaseTransport()
	transport.Proxy = nil
	transport.DialContext = contextDialer.DialContext
	transport.DialTLSContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		rawConn, err := contextDialer.DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}

		host, _, splitErr := net.SplitHostPort(addr)
		if splitErr != nil {
			rawConn.Close()
			return nil, splitErr
		}

		tlsConfig := transport.TLSClientConfig
		if tlsConfig != nil {
			tlsConfig = tlsConfig.Clone()
		} else {
			tlsConfig = &tls.Config{}
		}
		if tlsConfig.ServerName == "" {
			tlsConfig.ServerName = host
		}
		tlsConfig.NextProtos = []string{"http/1.1"}

		tlsConn := tls.Client(rawConn, tlsConfig)
		if deadline, ok := ctx.Deadline(); ok {
			if err := tlsConn.SetDeadline(deadline); err != nil {
				rawConn.Close()
				return nil, err
			}
			defer tlsConn.SetDeadline(time.Time{})
		}
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			rawConn.Close()
			return nil, err
		}
		return tlsConn, nil
	}
	transport.DisableKeepAlives = true
	transport.ForceAttemptHTTP2 = false
	transport.MaxIdleConns = 0
	transport.MaxIdleConnsPerHost = 0
	transport.MaxConnsPerHost = 0
	return transport, nil
}

func newBaseTransport() *http.Transport {
	return http.DefaultTransport.(*http.Transport).Clone()
}

func (s *Settings) ProxyURL() (*url.URL, error) {
	if s == nil || !s.Enabled {
		return nil, nil
	}

	scheme := normalizeProtocol(s.Protocol)
	if scheme == "" {
		return nil, fmt.Errorf("outbound proxy protocol is invalid")
	}

	host := normalizeHost(s.Host)
	if host == "" {
		return nil, fmt.Errorf("outbound proxy host is empty")
	}
	if s.Port <= 0 || s.Port > 65535 {
		return nil, fmt.Errorf("outbound proxy port is invalid")
	}

	proxyURL := &url.URL{
		Scheme: scheme,
		Host:   net.JoinHostPort(host, strconv.Itoa(s.Port)),
	}
	if s.Username != "" {
		if s.Password != "" {
			proxyURL.User = url.UserPassword(s.Username, s.Password)
		} else {
			proxyURL.User = url.User(s.Username)
		}
	}
	return proxyURL, nil
}

func normalizeProtocol(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "http", "https", "socks5":
		return strings.ToLower(strings.TrimSpace(value))
	default:
		return ""
	}
}

func normalizeHost(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if strings.Contains(value, "://") {
		if parsed, err := url.Parse(value); err == nil {
			if host := strings.TrimSpace(parsed.Hostname()); host != "" {
				return host
			}
		}
	}
	return strings.Trim(value, "[]")
}

func (s *Settings) normalizeCredentialLine() {
	if s == nil {
		return
	}
	host, port, username, password, ok := parseCredentialLine(s.Host)
	if !ok {
		return
	}

	s.Host = host
	s.Port = port
	if strings.TrimSpace(s.Username) == "" {
		s.Username = username
	}
	if s.Password == "" {
		s.Password = password
	}
}

func parseCredentialLine(value string) (host string, port int, username string, password string, ok bool) {
	value = strings.TrimSpace(value)
	if value == "" || strings.Contains(value, "://") {
		return "", 0, "", "", false
	}

	parts := strings.Split(value, ":")
	if len(parts) < 4 {
		return "", 0, "", "", false
	}

	parsedPort, err := strconv.Atoi(strings.TrimSpace(parts[1]))
	if err != nil || parsedPort <= 0 || parsedPort > 65535 {
		return "", 0, "", "", false
	}

	host = strings.TrimSpace(parts[0])
	username = strings.TrimSpace(parts[2])
	password = strings.Join(parts[3:], ":")
	if host == "" || username == "" || password == "" {
		return "", 0, "", "", false
	}

	return host, parsedPort, username, password, true
}
