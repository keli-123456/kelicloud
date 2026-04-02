package digitalocean

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

func newTestClient(t *testing.T, transport http.RoundTripper) *Client {
	t.Helper()

	client, err := newClient("token", "https://api.digitalocean.test")
	require.NoError(t, err)
	client.httpClient = &http.Client{Transport: transport}
	return client
}

func jsonResponse(statusCode int, body string) *http.Response {
	return &http.Response{
		StatusCode: statusCode,
		Header: http.Header{
			"Content-Type": []string{"application/json"},
		},
		Body: io.NopCloser(strings.NewReader(body)),
	}
}

func TestListDropletsFollowsPagination(t *testing.T) {
	client := newTestClient(t, roundTripFunc(func(request *http.Request) (*http.Response, error) {
		require.Equal(t, "/v2/droplets", request.URL.Path)

		switch request.URL.Query().Get("page") {
		case "2":
			return jsonResponse(http.StatusOK, `{
				"droplets": [{"id": 2, "name": "second"}],
				"links": {"pages": {}}
			}`), nil
		default:
			return jsonResponse(http.StatusOK, `{
				"droplets": [{"id": 1, "name": "first"}],
				"links": {"pages": {"next": "https://api.digitalocean.test/v2/droplets?page=2"}}
			}`), nil
		}
	}))

	droplets, err := client.ListDroplets(context.Background())
	require.NoError(t, err)
	require.Len(t, droplets, 2)
	require.Equal(t, 1, droplets[0].ID)
	require.Equal(t, 2, droplets[1].ID)
}

func TestGetAccountReturnsAPIError(t *testing.T) {
	client := newTestClient(t, roundTripFunc(func(request *http.Request) (*http.Response, error) {
		require.Equal(t, "/v2/account", request.URL.Path)
		return jsonResponse(http.StatusUnauthorized, `{
			"id": "unauthorized",
			"message": "invalid token",
			"request_id": "req-123"
		}`), nil
	}))

	_, err := client.GetAccount(context.Background())
	require.Error(t, err)

	var apiErr *APIError
	require.True(t, errors.As(err, &apiErr))
	require.Equal(t, http.StatusUnauthorized, apiErr.StatusCode)
	require.Equal(t, "invalid token", apiErr.Message)
	require.Equal(t, "req-123", apiErr.RequestID)
}
