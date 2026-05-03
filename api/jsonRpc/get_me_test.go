package jsonRpc

import (
	"context"
	"testing"
	"time"

	"github.com/komari-monitor/komari/database/models"
	"github.com/komari-monitor/komari/utils/rpc"
)

func TestGetMeClientReturnsClientUUIDFromToken(t *testing.T) {
	db := configureJSONRPCTestDB()
	now := models.FromTime(time.Now())

	user := models.User{
		UUID:      "getme-user",
		Username:  "client-owner",
		Passwd:    "hashed",
		CreatedAt: now,
		UpdatedAt: now,
	}
	client := models.Client{
		UUID:      "getme-client-uuid",
		Token:     "getme-client-token",
		UserID:    user.UUID,
		Name:      "Client Node",
		CreatedAt: now,
		UpdatedAt: now,
	}

	for _, item := range []any{&user, &client} {
		if err := db.Create(item).Error; err != nil {
			t.Fatalf("failed to seed %T: %v", item, err)
		}
	}

	ctx := rpc.NewContextWithMeta(context.Background(), &rpc.ContextMeta{
		Permission:  "client",
		ClientToken: client.Token,
	})

	result, rpcErr := getMe(ctx, rpc.NewRequest("1", "common:getMe", map[string]any{}))
	if rpcErr != nil {
		t.Fatalf("expected getMe client response, got rpc error: %+v", rpcErr)
	}

	var resp struct {
		LoggedIn bool   `json:"logged_in"`
		SSOId    string `json:"sso_id"`
		SSOType  string `json:"sso_type"`
		Username string `json:"username"`
		UUID     string `json:"uuid"`
	}
	decodeRPCResult(t, result, &resp)

	if !resp.LoggedIn {
		t.Fatalf("expected client to be logged in, got %+v", resp)
	}
	if resp.SSOId != "client" || resp.SSOType != "client" || resp.Username != "client" {
		t.Fatalf("expected client identity fields, got %+v", resp)
	}
	if resp.UUID != client.UUID {
		t.Fatalf("expected client uuid %q, got %q", client.UUID, resp.UUID)
	}
}
