package digitalocean

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestListDropletsFollowsPagination(t *testing.T) {
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Query().Get("page") {
		case "2":
			require.Equal(t, "/v2/droplets", r.URL.Path)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"droplets": []map[string]any{
					{"id": 2, "name": "second"},
				},
				"links": map[string]any{
					"pages": map[string]any{},
				},
			})
		default:
			require.Equal(t, "/v2/droplets", r.URL.Path)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"droplets": []map[string]any{
					{"id": 1, "name": "first"},
				},
				"links": map[string]any{
					"pages": map[string]any{
						"next": server.URL + "/v2/droplets?page=2",
					},
				},
			})
		}
	}))
	defer server.Close()

	client, err := newClient("token", server.URL)
	require.NoError(t, err)

	droplets, err := client.ListDroplets(context.Background())
	require.NoError(t, err)
	require.Len(t, droplets, 2)
	require.Equal(t, 1, droplets[0].ID)
	require.Equal(t, 2, droplets[1].ID)
}

func TestGetAccountReturnsAPIError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id":         "unauthorized",
			"message":    "invalid token",
			"request_id": "req-123",
		})
	}))
	defer server.Close()

	client, err := newClient("token", server.URL)
	require.NoError(t, err)

	_, err = client.GetAccount(context.Background())
	require.Error(t, err)

	var apiErr *APIError
	require.True(t, errors.As(err, &apiErr))
	require.Equal(t, http.StatusUnauthorized, apiErr.StatusCode)
	require.Equal(t, "invalid token", apiErr.Message)
	require.Equal(t, "req-123", apiErr.RequestID)
}
