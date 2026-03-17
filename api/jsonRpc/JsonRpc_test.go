package jsonRpc

import (
	"context"
	"testing"

	"github.com/komari-monitor/komari/utils/rpc"
)

func TestOnInternalRequestRejectsGuest(t *testing.T) {
	resp := OnInternalRequest(context.Background(), "guest", "common:getNodes", nil)
	if resp == nil || resp.Error == nil {
		t.Fatal("expected permission error for guest request")
	}
	if resp.Error.Code != rpc.PermissionDenied {
		t.Fatalf("expected permission denied, got %d", resp.Error.Code)
	}
}
