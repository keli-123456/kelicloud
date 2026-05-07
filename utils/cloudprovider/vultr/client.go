package vultr

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/komari-monitor/komari/utils/outboundproxy"
)

const defaultBaseURL = "https://api.vultr.com"

type APIError struct {
	StatusCode int    `json:"-"`
	ErrorText  string `json:"error"`
}

func (e *APIError) Error() string {
	message := strings.TrimSpace(e.ErrorText)
	if message == "" {
		message = http.StatusText(e.StatusCode)
	}
	if message == "" {
		message = "Vultr API request failed"
	}
	return message
}

type Client struct {
	token      string
	baseURL    string
	httpClient *http.Client
}

func NewClient(addition *Addition) (*Client, error) {
	if addition == nil {
		return nil, errors.New("vultr configuration is missing")
	}
	addition.Normalize()
	active := addition.ActiveToken()
	if active != nil {
		return newClient(active.Token, defaultBaseURL)
	}
	return newClient(addition.Token, defaultBaseURL)
}

func NewClientFromToken(token string) (*Client, error) {
	return newClient(token, defaultBaseURL)
}

func newClient(token, baseURL string) (*Client, error) {
	token = strings.TrimSpace(token)
	if token == "" {
		return nil, errors.New("vultr token is empty")
	}

	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if baseURL == "" {
		baseURL = defaultBaseURL
	}

	return &Client{
		token:      token,
		baseURL:    baseURL,
		httpClient: outboundproxy.NewHTTPClient(20 * time.Second),
	}, nil
}

type Account struct {
	Name              string   `json:"name"`
	Email             string   `json:"email"`
	ACLs              []string `json:"acls"`
	Balance           float64  `json:"balance"`
	PendingCharges    float64  `json:"pending_charges"`
	LastPaymentDate   string   `json:"last_payment_date"`
	LastPaymentAmount float64  `json:"last_payment_amount"`
}

type Region struct {
	ID        string   `json:"id"`
	City      string   `json:"city"`
	Country   string   `json:"country"`
	Continent string   `json:"continent"`
	Options   []string `json:"options"`
}

type Plan struct {
	ID          string   `json:"id"`
	VCPUCount   int      `json:"vcpu_count"`
	RAM         int      `json:"ram"`
	Disk        int      `json:"disk"`
	DiskCount   int      `json:"disk_count"`
	Bandwidth   float64  `json:"bandwidth"`
	MonthlyCost float64  `json:"monthly_cost"`
	Type        string   `json:"type"`
	Locations   []string `json:"locations"`
	GPUVramGB   int      `json:"gpu_vram_gb,omitempty"`
	GPUType     string   `json:"gpu_type,omitempty"`
}

type OS struct {
	ID     int    `json:"id"`
	Name   string `json:"name"`
	Arch   string `json:"arch"`
	Family string `json:"family"`
}

type SSHKey struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	SSHKey      string `json:"ssh_key"`
	DateCreated string `json:"date_created"`
}

type Instance struct {
	ID               string   `json:"id"`
	OS               string   `json:"os"`
	RAM              int      `json:"ram"`
	Disk             int      `json:"disk"`
	MainIP           string   `json:"main_ip"`
	VCPUCount        int      `json:"vcpu_count"`
	Region           string   `json:"region"`
	Plan             string   `json:"plan"`
	DateCreated      string   `json:"date_created"`
	Status           string   `json:"status"`
	AllowedBandwidth float64  `json:"allowed_bandwidth"`
	NetmaskV4        string   `json:"netmask_v4"`
	GatewayV4        string   `json:"gateway_v4"`
	PowerStatus      string   `json:"power_status"`
	ServerStatus     string   `json:"server_status"`
	V6Network        string   `json:"v6_network"`
	V6MainIP         string   `json:"v6_main_ip"`
	V6NetworkSize    int      `json:"v6_network_size"`
	Label            string   `json:"label"`
	InternalIP       string   `json:"internal_ip"`
	Hostname         string   `json:"hostname"`
	OSID             int      `json:"os_id"`
	AppID            int      `json:"app_id"`
	ImageID          string   `json:"image_id"`
	SnapshotID       string   `json:"snapshot_id"`
	FirewallGroupID  string   `json:"firewall_group_id"`
	Features         []string `json:"features"`
	DefaultPassword  string   `json:"default_password,omitempty"`
	Tags             []string `json:"tags"`
	UserScheme       string   `json:"user_scheme"`
	PendingCharges   float64  `json:"pending_charges"`
}

type CreateInstanceRequest struct {
	Region            string   `json:"region" binding:"required"`
	Plan              string   `json:"plan" binding:"required"`
	OSID              int      `json:"os_id,omitempty"`
	Label             string   `json:"label,omitempty"`
	Hostname          string   `json:"hostname,omitempty"`
	SSHKeyIDs         []string `json:"sshkey_id,omitempty"`
	EnableIPv6        bool     `json:"enable_ipv6"`
	Backups           string   `json:"backups,omitempty"`
	DDOSProtection    bool     `json:"ddos_protection"`
	ActivationEmail   bool     `json:"activation_email"`
	UserData          string   `json:"user_data,omitempty"`
	Tags              []string `json:"tags,omitempty"`
	FirewallGroupID   string   `json:"firewall_group_id,omitempty"`
	ReservedIPv4      string   `json:"reserved_ipv4,omitempty"`
	DisablePublicIPv4 bool     `json:"disable_public_ipv4,omitempty"`
}

func (c *Client) GetAccount(ctx context.Context) (*Account, error) {
	return getObject[Account](ctx, c, "/v2/account", nil, "account")
}

func (c *Client) ListRegions(ctx context.Context) ([]Region, error) {
	query := url.Values{"per_page": {"500"}}
	return getPaginated[Region](ctx, c, "/v2/regions", query, "regions")
}

func (c *Client) ListPlans(ctx context.Context) ([]Plan, error) {
	query := url.Values{"per_page": {"500"}}
	return getPaginated[Plan](ctx, c, "/v2/plans", query, "plans")
}

func (c *Client) ListOS(ctx context.Context) ([]OS, error) {
	query := url.Values{"per_page": {"500"}}
	return getPaginated[OS](ctx, c, "/v2/os", query, "os")
}

func (c *Client) ListSSHKeys(ctx context.Context) ([]SSHKey, error) {
	query := url.Values{"per_page": {"500"}}
	return getPaginated[SSHKey](ctx, c, "/v2/ssh-keys", query, "ssh_keys")
}

func (c *Client) ListInstances(ctx context.Context) ([]Instance, error) {
	query := url.Values{"per_page": {"500"}}
	return getPaginated[Instance](ctx, c, "/v2/instances", query, "instances")
}

func (c *Client) GetInstance(ctx context.Context, instanceID string) (*Instance, error) {
	return getObject[Instance](ctx, c, "/v2/instances/"+url.PathEscape(strings.TrimSpace(instanceID)), nil, "instance")
}

func (c *Client) CreateInstance(ctx context.Context, request CreateInstanceRequest) (*Instance, error) {
	return postObject[Instance](ctx, c, "/v2/instances", request, "instance")
}

func (c *Client) DeleteInstance(ctx context.Context, instanceID string) error {
	return c.doEmpty(ctx, http.MethodDelete, "/v2/instances/"+url.PathEscape(strings.TrimSpace(instanceID)), nil, nil)
}

func (c *Client) StartInstance(ctx context.Context, instanceID string) error {
	return c.doEmpty(ctx, http.MethodPost, "/v2/instances/start", nil, map[string]any{
		"instance_ids": []string{strings.TrimSpace(instanceID)},
	})
}

func (c *Client) HaltInstance(ctx context.Context, instanceID string) error {
	return c.doEmpty(ctx, http.MethodPost, "/v2/instances/halt", nil, map[string]any{
		"instance_ids": []string{strings.TrimSpace(instanceID)},
	})
}

func (c *Client) RebootInstance(ctx context.Context, instanceID string) error {
	return c.doEmpty(ctx, http.MethodPost, "/v2/instances/reboot", nil, map[string]any{
		"instance_ids": []string{strings.TrimSpace(instanceID)},
	})
}

func EncodeUserData(userData string) string {
	userData = strings.TrimSpace(userData)
	if userData == "" {
		return ""
	}
	return base64.StdEncoding.EncodeToString([]byte(userData))
}

func getPaginated[T any](ctx context.Context, client *Client, endpoint string, query url.Values, root string) ([]T, error) {
	var cursor string
	var items []T
	for {
		pageQuery := cloneValues(query)
		if cursor != "" {
			pageQuery.Set("cursor", cursor)
		}

		body, err := client.do(ctx, http.MethodGet, endpoint, pageQuery, nil)
		if err != nil {
			return nil, err
		}

		pageItems, nextCursor, err := decodePaginatedItems[T](body, root)
		if err != nil {
			return nil, err
		}
		items = append(items, pageItems...)
		if strings.TrimSpace(nextCursor) == "" {
			break
		}
		cursor = nextCursor
	}
	return items, nil
}

func getObject[T any](ctx context.Context, client *Client, endpoint string, query url.Values, root string) (*T, error) {
	body, err := client.do(ctx, http.MethodGet, endpoint, query, nil)
	if err != nil {
		return nil, err
	}
	return decodeRoot[T](body, root)
}

func postObject[T any](ctx context.Context, client *Client, endpoint string, payload any, root string) (*T, error) {
	body, err := client.do(ctx, http.MethodPost, endpoint, nil, payload)
	if err != nil {
		return nil, err
	}
	return decodeRoot[T](body, root)
}

func (c *Client) doEmpty(ctx context.Context, method, endpoint string, query url.Values, payload any) error {
	_, err := c.do(ctx, method, endpoint, query, payload)
	return err
}

func (c *Client) do(ctx context.Context, method, endpoint string, query url.Values, payload any) ([]byte, error) {
	targetURL, err := c.buildURL(endpoint, query)
	if err != nil {
		return nil, err
	}

	var bodyReader io.Reader
	if payload != nil {
		bodyBytes, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		bodyReader = bytes.NewReader(bodyBytes)
	}

	request, err := http.NewRequestWithContext(ctx, method, targetURL, bodyReader)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Authorization", "Bearer "+c.token)
	request.Header.Set("Accept", "application/json")
	request.Header.Set("User-Agent", "komari-cloud-vultr")
	if payload != nil {
		request.Header.Set("Content-Type", "application/json")
	}

	response, err := c.httpClient.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	if response.StatusCode >= http.StatusBadRequest {
		return nil, decodeAPIError(response.StatusCode, body)
	}
	if response.StatusCode == http.StatusNoContent || len(body) == 0 {
		return nil, nil
	}
	return body, nil
}

func (c *Client) buildURL(endpoint string, query url.Values) (string, error) {
	var parsed *url.URL
	var err error
	if strings.HasPrefix(endpoint, "http://") || strings.HasPrefix(endpoint, "https://") {
		parsed, err = url.Parse(endpoint)
		if err != nil {
			return "", err
		}
	} else {
		baseURL, err := url.Parse(c.baseURL)
		if err != nil {
			return "", err
		}
		relativeURL, err := url.Parse(endpoint)
		if err != nil {
			return "", err
		}
		parsed = baseURL.ResolveReference(relativeURL)
	}

	if query != nil {
		values := parsed.Query()
		for key, entries := range query {
			for _, entry := range entries {
				values.Add(key, entry)
			}
		}
		parsed.RawQuery = values.Encode()
	}
	return parsed.String(), nil
}

func decodePaginatedItems[T any](body []byte, root string) ([]T, string, error) {
	if len(body) == 0 {
		return nil, "", nil
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, "", err
	}

	var items []T
	if payload, exists := raw[root]; exists {
		if err := json.Unmarshal(payload, &items); err != nil {
			return nil, "", err
		}
	}

	var meta struct {
		Links struct {
			Next string `json:"next"`
		} `json:"links"`
	}
	if payload, exists := raw["meta"]; exists {
		_ = json.Unmarshal(payload, &meta)
	}

	return items, meta.Links.Next, nil
}

func decodeRoot[T any](body []byte, root string) (*T, error) {
	if len(body) == 0 {
		return nil, fmt.Errorf("response missing %s payload", root)
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, err
	}

	payload, exists := raw[root]
	if !exists {
		return nil, fmt.Errorf("response missing %s payload", root)
	}

	var value T
	if err := json.Unmarshal(payload, &value); err != nil {
		return nil, err
	}
	return &value, nil
}

func decodeAPIError(statusCode int, body []byte) error {
	if len(body) == 0 {
		return &APIError{StatusCode: statusCode, ErrorText: http.StatusText(statusCode)}
	}

	var apiErr APIError
	if err := json.Unmarshal(body, &apiErr); err == nil && strings.TrimSpace(apiErr.ErrorText) != "" {
		apiErr.StatusCode = statusCode
		return &apiErr
	}

	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err == nil {
		if message, ok := raw["message"].(string); ok && strings.TrimSpace(message) != "" {
			return &APIError{StatusCode: statusCode, ErrorText: message}
		}
	}

	return &APIError{StatusCode: statusCode, ErrorText: strings.TrimSpace(string(body))}
}

func cloneValues(values url.Values) url.Values {
	if values == nil {
		return nil
	}
	clone := make(url.Values, len(values))
	for key, entries := range values {
		clone[key] = append([]string(nil), entries...)
	}
	return clone
}
