package linode

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

const defaultBaseURL = "https://api.linode.com"

type APIError struct {
	StatusCode int             `json:"-"`
	Errors     []APIErrorEntry `json:"errors"`
}

type APIErrorEntry struct {
	Field  string `json:"field"`
	Reason string `json:"reason"`
}

func (e *APIError) Error() string {
	if len(e.Errors) == 0 {
		if e.StatusCode == 0 {
			return "Linode API request failed"
		}
		return http.StatusText(e.StatusCode)
	}

	parts := make([]string, 0, len(e.Errors))
	for _, entry := range e.Errors {
		reason := strings.TrimSpace(entry.Reason)
		field := strings.TrimSpace(entry.Field)
		if field == "" {
			if reason != "" {
				parts = append(parts, reason)
			}
			continue
		}
		if reason == "" {
			parts = append(parts, field)
			continue
		}
		parts = append(parts, field+": "+reason)
	}
	if len(parts) == 0 {
		return http.StatusText(e.StatusCode)
	}
	return strings.Join(parts, "; ")
}

type Client struct {
	token      string
	baseURL    string
	httpClient *http.Client
}

func NewClient(addition *Addition) (*Client, error) {
	if addition == nil {
		return nil, errors.New("linode configuration is missing")
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
		return nil, errors.New("linode token is empty")
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

type Profile struct {
	UID        int    `json:"uid"`
	Username   string `json:"username"`
	Email      string `json:"email"`
	Restricted bool   `json:"restricted"`
}

type Account struct {
	Company string  `json:"company"`
	Email   string  `json:"email"`
	Balance float64 `json:"balance"`
}

type Region struct {
	ID           string   `json:"id"`
	Label        string   `json:"label"`
	Country      string   `json:"country"`
	Capabilities []string `json:"capabilities"`
}

type Price struct {
	Hourly  float64 `json:"hourly"`
	Monthly float64 `json:"monthly"`
}

type Type struct {
	ID       string  `json:"id"`
	Label    string  `json:"label"`
	Memory   int     `json:"memory"`
	Disk     int     `json:"disk"`
	VCPUs    int     `json:"vcpus"`
	Transfer float64 `json:"transfer"`
	Price    Price   `json:"price"`
	Addons   struct {
		Backups Price `json:"backups"`
	} `json:"addons"`
	NetworkOut int    `json:"network_out"`
	Class      string `json:"class"`
}

type Image struct {
	ID          string `json:"id"`
	Label       string `json:"label"`
	Description string `json:"description"`
	Vendor      string `json:"vendor"`
	Deprecated  bool   `json:"deprecated"`
	IsPublic    bool   `json:"is_public"`
	Created     string `json:"created"`
}

type SSHKey struct {
	Label   string `json:"label"`
	SSHKey  string `json:"ssh_key"`
	Created string `json:"created"`
}

type InstanceSpecs struct {
	Disk     int     `json:"disk"`
	Memory   int     `json:"memory"`
	VCPUs    int     `json:"vcpus"`
	Transfer float64 `json:"transfer"`
}

type Instance struct {
	ID      int           `json:"id"`
	Label   string        `json:"label"`
	Group   string        `json:"group"`
	Status  string        `json:"status"`
	Region  string        `json:"region"`
	Type    string        `json:"type"`
	Image   string        `json:"image"`
	IPv4    []string      `json:"ipv4"`
	IPv6    string        `json:"ipv6"`
	Specs   InstanceSpecs `json:"specs"`
	Tags    []string      `json:"tags"`
	Created string        `json:"created"`
}

type CreateInstanceRequest struct {
	Label          string   `json:"label" binding:"required"`
	Region         string   `json:"region" binding:"required"`
	Type           string   `json:"type" binding:"required"`
	Image          string   `json:"image" binding:"required"`
	RootPass       string   `json:"root_pass" binding:"required"`
	AuthorizedKeys []string `json:"authorized_keys,omitempty"`
	BackupsEnabled bool     `json:"backups_enabled"`
	Booted         bool     `json:"booted"`
	Tags           []string `json:"tags,omitempty"`
	Metadata       *struct {
		UserData string `json:"user_data,omitempty"`
	} `json:"metadata,omitempty"`
}

func (c *Client) GetProfile(ctx context.Context) (*Profile, error) {
	return getObject[Profile](ctx, c, "/v4/profile", nil)
}

func (c *Client) GetAccount(ctx context.Context) (*Account, error) {
	return getObject[Account](ctx, c, "/v4/account", nil)
}

func (c *Client) RedeemPromoCode(ctx context.Context, promoCode string) error {
	return c.doEmpty(ctx, http.MethodPost, "/v4/account/promo-codes", nil, map[string]any{
		"promo_code": strings.TrimSpace(promoCode),
	})
}

func (c *Client) ListRegions(ctx context.Context) ([]Region, error) {
	query := url.Values{"page_size": {"200"}}
	return getPaginated[Region](ctx, c, "/v4/regions", query)
}

func (c *Client) ListTypes(ctx context.Context) ([]Type, error) {
	query := url.Values{"page_size": {"500"}}
	return getPaginated[Type](ctx, c, "/v4/linode/types", query)
}

func (c *Client) ListImages(ctx context.Context) ([]Image, error) {
	query := url.Values{"page_size": {"500"}}
	return getPaginated[Image](ctx, c, "/v4/images", query)
}

func (c *Client) ListSSHKeys(ctx context.Context) ([]SSHKey, error) {
	query := url.Values{"page_size": {"500"}}
	return getPaginated[SSHKey](ctx, c, "/v4/profile/sshkeys", query)
}

func (c *Client) ListInstances(ctx context.Context) ([]Instance, error) {
	query := url.Values{"page_size": {"500"}}
	return getPaginated[Instance](ctx, c, "/v4/linode/instances", query)
}

func (c *Client) CreateInstance(ctx context.Context, request CreateInstanceRequest) (*Instance, error) {
	return postObject[Instance](ctx, c, "/v4/linode/instances", request)
}

func (c *Client) DeleteInstance(ctx context.Context, instanceID int) error {
	return c.doEmpty(ctx, http.MethodDelete, fmt.Sprintf("/v4/linode/instances/%d", instanceID), nil, nil)
}

func (c *Client) BootInstance(ctx context.Context, instanceID int) error {
	return c.doEmpty(ctx, http.MethodPost, fmt.Sprintf("/v4/linode/instances/%d/boot", instanceID), nil, map[string]any{})
}

func (c *Client) ShutdownInstance(ctx context.Context, instanceID int) error {
	return c.doEmpty(ctx, http.MethodPost, fmt.Sprintf("/v4/linode/instances/%d/shutdown", instanceID), nil, map[string]any{})
}

func (c *Client) RebootInstance(ctx context.Context, instanceID int) error {
	return c.doEmpty(ctx, http.MethodPost, fmt.Sprintf("/v4/linode/instances/%d/reboot", instanceID), nil, map[string]any{})
}

func getPaginated[T any](ctx context.Context, client *Client, endpoint string, query url.Values) ([]T, error) {
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

		pageItems, currentPage, totalPages, err := decodePaginatedItems[T](body)
		if err != nil {
			return nil, err
		}
		items = append(items, pageItems...)
		if currentPage <= 0 || totalPages <= currentPage {
			nextURL = ""
			continue
		}

		parsed, err := url.Parse(nextURL)
		if err != nil {
			nextURL = ""
			continue
		}
		values := parsed.Query()
		values.Set("page", fmt.Sprintf("%d", currentPage+1))
		parsed.RawQuery = values.Encode()
		nextURL = parsed.String()
	}

	return items, nil
}

func getObject[T any](ctx context.Context, client *Client, endpoint string, query url.Values) (*T, error) {
	body, err := client.do(ctx, http.MethodGet, endpoint, query, nil)
	if err != nil {
		return nil, err
	}

	var value T
	if err := json.Unmarshal(body, &value); err != nil {
		return nil, err
	}
	return &value, nil
}

func postObject[T any](ctx context.Context, client *Client, endpoint string, payload any) (*T, error) {
	body, err := client.do(ctx, http.MethodPost, endpoint, nil, payload)
	if err != nil {
		return nil, err
	}

	var value T
	if err := json.Unmarshal(body, &value); err != nil {
		return nil, err
	}
	return &value, nil
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
	request.Header.Set("User-Agent", "komari-cloud-linode")
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

func decodePaginatedItems[T any](body []byte) ([]T, int, int, error) {
	if len(body) == 0 {
		return nil, 0, 0, nil
	}

	var raw struct {
		Data    []T `json:"data"`
		Page    int `json:"page"`
		Pages   int `json:"pages"`
		Results int `json:"results"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, 0, 0, err
	}

	return raw.Data, raw.Page, raw.Pages, nil
}

func decodeAPIError(statusCode int, body []byte) error {
	if len(body) == 0 {
		return &APIError{StatusCode: statusCode}
	}

	var apiErr APIError
	if err := json.Unmarshal(body, &apiErr); err == nil && len(apiErr.Errors) > 0 {
		apiErr.StatusCode = statusCode
		return &apiErr
	}

	return &APIError{
		StatusCode: statusCode,
		Errors: []APIErrorEntry{
			{
				Reason: strings.TrimSpace(string(body)),
			},
		},
	}
}

func EncodeUserData(userData string) string {
	userData = strings.TrimSpace(userData)
	if userData == "" {
		return ""
	}
	return base64.StdEncoding.EncodeToString([]byte(userData))
}
