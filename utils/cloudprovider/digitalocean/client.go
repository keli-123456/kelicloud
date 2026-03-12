package digitalocean

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const defaultBaseURL = "https://api.digitalocean.com"

type APIError struct {
	StatusCode int    `json:"-"`
	ID         string `json:"id"`
	Message    string `json:"message"`
	RequestID  string `json:"request_id,omitempty"`
}

func (e *APIError) Error() string {
	message := strings.TrimSpace(e.Message)
	if message == "" {
		message = http.StatusText(e.StatusCode)
	}
	if strings.TrimSpace(e.RequestID) == "" {
		return message
	}
	return fmt.Sprintf("%s (request_id: %s)", message, e.RequestID)
}

type Client struct {
	token      string
	baseURL    string
	httpClient *http.Client
}

func NewClient(addition *Addition) (*Client, error) {
	if addition == nil {
		return nil, errors.New("digitalocean configuration is missing")
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
		return nil, errors.New("digitalocean token is empty")
	}

	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if baseURL == "" {
		baseURL = defaultBaseURL
	}

	return &Client{
		token:   token,
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 20 * time.Second,
		},
	}, nil
}

type Account struct {
	UUID          string `json:"uuid"`
	Email         string `json:"email"`
	EmailVerified bool   `json:"email_verified"`
	DropletLimit  int    `json:"droplet_limit"`
	Status        string `json:"status"`
	StatusMessage string `json:"status_message"`
}

type Region struct {
	Name      string   `json:"name"`
	Slug      string   `json:"slug"`
	Available bool     `json:"available"`
	Features  []string `json:"features"`
	Sizes     []string `json:"sizes"`
}

type Size struct {
	Slug         string   `json:"slug"`
	Memory       int      `json:"memory"`
	Vcpus        int      `json:"vcpus"`
	Disk         int      `json:"disk"`
	Transfer     float64  `json:"transfer"`
	PriceMonthly float64  `json:"price_monthly"`
	PriceHourly  float64  `json:"price_hourly"`
	Available    bool     `json:"available"`
	Regions      []string `json:"regions"`
	Description  string   `json:"description"`
}

type Image struct {
	ID           int      `json:"id"`
	Name         string   `json:"name"`
	Type         string   `json:"type"`
	Distribution string   `json:"distribution"`
	Slug         string   `json:"slug"`
	Public       bool     `json:"public"`
	Regions      []string `json:"regions"`
	MinDiskSize  int      `json:"min_disk_size"`
	Description  string   `json:"description"`
}

type SSHKey struct {
	ID          int    `json:"id"`
	Name        string `json:"name"`
	Fingerprint string `json:"fingerprint"`
	PublicKey   string `json:"public_key"`
}

type CreateSSHKeyRequest struct {
	Name      string `json:"name" binding:"required"`
	PublicKey string `json:"public_key" binding:"required"`
}

type Networks struct {
	V4 []NetworkV4 `json:"v4"`
	V6 []NetworkV6 `json:"v6"`
}

type NetworkV4 struct {
	IPAddress string `json:"ip_address"`
	Netmask   string `json:"netmask"`
	Gateway   string `json:"gateway"`
	Type      string `json:"type"`
}

type NetworkV6 struct {
	IPAddress string `json:"ip_address"`
	Netmask   int    `json:"netmask"`
	Gateway   string `json:"gateway"`
	Type      string `json:"type"`
}

type Droplet struct {
	ID          int      `json:"id"`
	Name        string   `json:"name"`
	Memory      int      `json:"memory"`
	Vcpus       int      `json:"vcpus"`
	Disk        int      `json:"disk"`
	Locked      bool     `json:"locked"`
	Status      string   `json:"status"`
	CreatedAt   string   `json:"created_at"`
	Features    []string `json:"features"`
	BackupIDs   []int    `json:"backup_ids"`
	SnapshotIDs []int    `json:"snapshot_ids"`
	SizeSlug    string   `json:"size_slug"`
	VolumeIDs   []string `json:"volume_ids"`
	VPCUUID     string   `json:"vpc_uuid"`
	Tags        []string `json:"tags"`
	Image       Image    `json:"image"`
	Region      Region   `json:"region"`
	Size        Size     `json:"size"`
	Networks    Networks `json:"networks"`
}

type Action struct {
	ID           int    `json:"id"`
	Status       string `json:"status"`
	Type         string `json:"type"`
	StartedAt    string `json:"started_at"`
	CompletedAt  string `json:"completed_at"`
	ResourceID   int    `json:"resource_id"`
	ResourceType string `json:"resource_type"`
}

type CreateDropletRequest struct {
	Name       string   `json:"name" binding:"required"`
	Region     string   `json:"region" binding:"required"`
	Size       string   `json:"size" binding:"required"`
	Image      string   `json:"image" binding:"required"`
	SSHKeys    []int    `json:"ssh_keys,omitempty"`
	Backups    bool     `json:"backups"`
	IPv6       bool     `json:"ipv6"`
	Monitoring bool     `json:"monitoring"`
	Tags       []string `json:"tags,omitempty"`
	UserData   string   `json:"user_data,omitempty"`
	VPCUUID    string   `json:"vpc_uuid,omitempty"`
}

type DropletActionRequest struct {
	Type string `json:"type" binding:"required"`
}

func (c *Client) GetAccount(ctx context.Context) (*Account, error) {
	return getObject[Account](ctx, c, "/v2/account", nil, "account")
}

func (c *Client) ListRegions(ctx context.Context) ([]Region, error) {
	query := url.Values{"per_page": {"200"}}
	return getPaginated[Region](ctx, c, "/v2/regions", query, "regions")
}

func (c *Client) ListSizes(ctx context.Context) ([]Size, error) {
	query := url.Values{"per_page": {"200"}}
	return getPaginated[Size](ctx, c, "/v2/sizes", query, "sizes")
}

func (c *Client) ListImages(ctx context.Context, imageType string) ([]Image, error) {
	query := url.Values{"per_page": {"200"}}
	if imageType != "" {
		query.Set("type", imageType)
	}
	return getPaginated[Image](ctx, c, "/v2/images", query, "images")
}

func (c *Client) ListSSHKeys(ctx context.Context) ([]SSHKey, error) {
	query := url.Values{"per_page": {"200"}}
	return getPaginated[SSHKey](ctx, c, "/v2/account/keys", query, "ssh_keys")
}

func (c *Client) CreateSSHKey(ctx context.Context, request CreateSSHKeyRequest) (*SSHKey, error) {
	return postObject[SSHKey](ctx, c, "/v2/account/keys", request, "ssh_key")
}

func (c *Client) DeleteSSHKey(ctx context.Context, keyID int) error {
	return c.doEmpty(ctx, http.MethodDelete, fmt.Sprintf("/v2/account/keys/%d", keyID), nil)
}

func (c *Client) ListDroplets(ctx context.Context) ([]Droplet, error) {
	query := url.Values{"per_page": {"200"}}
	return getPaginated[Droplet](ctx, c, "/v2/droplets", query, "droplets")
}

func (c *Client) CreateDroplet(ctx context.Context, request CreateDropletRequest) (*Droplet, error) {
	return postObject[Droplet](ctx, c, "/v2/droplets", request, "droplet")
}

func (c *Client) DeleteDroplet(ctx context.Context, dropletID int) error {
	return c.doEmpty(ctx, http.MethodDelete, fmt.Sprintf("/v2/droplets/%d", dropletID), nil)
}

func (c *Client) PostDropletAction(ctx context.Context, dropletID int, request DropletActionRequest) (*Action, error) {
	return postObject[Action](ctx, c, fmt.Sprintf("/v2/droplets/%d/actions", dropletID), request, "action")
}

func getPaginated[T any](ctx context.Context, client *Client, endpoint string, query url.Values, root string) ([]T, error) {
	nextURL, err := client.buildURL(endpoint, query)
	if err != nil {
		return nil, err
	}

	var items []T
	for nextURL != "" {
		body, err := client.do(ctx, http.MethodGet, nextURL, nil, nil)
		if err != nil {
			return nil, err
		}

		pageItems, pageNext, err := decodePaginatedItems[T](body, root)
		if err != nil {
			return nil, err
		}
		items = append(items, pageItems...)
		nextURL = pageNext
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

func (c *Client) doEmpty(ctx context.Context, method, endpoint string, query url.Values) error {
	_, err := c.do(ctx, method, endpoint, query, nil)
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
	request.Header.Set("User-Agent", "komari-cloud-digitalocean")
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
			Pages struct {
				Next string `json:"next"`
			} `json:"pages"`
		} `json:"links"`
	}
	if err := json.Unmarshal(body, &meta); err != nil {
		return nil, "", err
	}

	return items, meta.Links.Pages.Next, nil
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
		return &APIError{
			StatusCode: statusCode,
			Message:    http.StatusText(statusCode),
		}
	}

	var apiErr APIError
	if err := json.Unmarshal(body, &apiErr); err == nil && strings.TrimSpace(apiErr.Message) != "" {
		apiErr.StatusCode = statusCode
		return &apiErr
	}

	return &APIError{
		StatusCode: statusCode,
		Message:    strings.TrimSpace(string(body)),
	}
}
