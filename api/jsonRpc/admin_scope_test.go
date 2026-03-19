package jsonRpc

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	apiroot "github.com/komari-monitor/komari/api"
	"github.com/komari-monitor/komari/cmd/flags"
	"github.com/komari-monitor/komari/common"
	"github.com/komari-monitor/komari/database/accounts"
	"github.com/komari-monitor/komari/database/dbcore"
	"github.com/komari-monitor/komari/database/models"
	"github.com/komari-monitor/komari/utils/rpc"
	"github.com/komari-monitor/komari/ws"
)

func TestPlatformAdminCanReadOtherUsersNodeData(t *testing.T) {
	flags.DatabaseType = "sqlite"
	flags.DatabaseFile = filepath.Join(t.TempDir(), "komari-jsonrpc.db")

	db := dbcore.GetDBInstance()
	now := models.FromTime(time.Now())

	adminUser := models.User{
		UUID:      "jsonrpc-admin",
		Username:  "admin",
		Passwd:    "hashed",
		Role:      accounts.RoleAdmin,
		CreatedAt: now,
		UpdatedAt: now,
	}
	memberUser := models.User{
		UUID:      "jsonrpc-member",
		Username:  "member",
		Passwd:    "hashed",
		Role:      accounts.RoleUser,
		CreatedAt: now,
		UpdatedAt: now,
	}
	adminClient := models.Client{
		UUID:      "jsonrpc-admin-client",
		Token:     "jsonrpc-admin-token",
		UserID:    adminUser.UUID,
		Name:      "Admin Node",
		CreatedAt: now,
		UpdatedAt: now,
	}
	memberClient := models.Client{
		UUID:      "jsonrpc-member-client",
		Token:     "jsonrpc-member-token",
		UserID:    memberUser.UUID,
		Name:      "Member Node",
		CreatedAt: now,
		UpdatedAt: now,
	}
	memberRecord := models.Record{
		Client:       memberClient.UUID,
		Time:         models.FromTime(time.Now().Add(-1 * time.Minute)),
		Cpu:          42,
		Ram:          512,
		RamTotal:     1024,
		Swap:         128,
		SwapTotal:    2048,
		Load:         1.5,
		Disk:         256,
		DiskTotal:    4096,
		NetIn:        10,
		NetOut:       20,
		NetTotalUp:   100,
		NetTotalDown: 200,
		Process:      12,
		Connections:  4,
	}

	for _, item := range []any{&adminUser, &memberUser, &adminClient, &memberClient, &memberRecord} {
		if err := db.Create(item).Error; err != nil {
			t.Fatalf("failed to seed %T: %v", item, err)
		}
	}

	memberReport := common.Report{
		UUID: memberClient.UUID,
		CPU: common.CPUReport{
			Usage: 38,
		},
		Ram: common.RamReport{
			Used:  512,
			Total: 1024,
		},
		Swap: common.RamReport{
			Used:  128,
			Total: 2048,
		},
		Load: common.LoadReport{
			Load1:  1.2,
			Load5:  1.0,
			Load15: 0.9,
		},
		Disk: common.DiskReport{
			Used:  256,
			Total: 4096,
		},
		Network: common.NetworkReport{
			Up:        10,
			Down:      20,
			TotalUp:   100,
			TotalDown: 200,
		},
		Connections: common.ConnectionsReport{
			TCP: 3,
			UDP: 1,
		},
		Process:   12,
		Uptime:    3600,
		UpdatedAt: time.Now(),
	}

	apiroot.Records.Set(memberClient.UUID, []common.Report{memberReport}, time.Minute)
	defer apiroot.Records.Delete(memberClient.UUID)

	ws.SetLatestReport(memberClient.UUID, &memberReport)
	defer ws.DeleteLatestReport(memberClient.UUID)

	meta := &rpc.ContextMeta{
		Permission: "admin",
		User:       &adminUser,
		UserUUID:   adminUser.UUID,
	}
	ctx := rpc.NewContextWithMeta(context.Background(), meta)

	latestResult, rpcErr := getNodesLatestStatus(ctx, rpc.NewRequest("1", "common:getNodesLatestStatus", map[string]any{}))
	if rpcErr != nil {
		t.Fatalf("expected latest status response, got rpc error: %+v", rpcErr)
	}
	var latest map[string]map[string]any
	decodeRPCResult(t, latestResult, &latest)
	if _, ok := latest[memberClient.UUID]; !ok {
		t.Fatalf("expected admin to see member latest status, got keys=%v", keysOfMap(latest))
	}

	recentResult, rpcErr := getNodeRecentStatus(ctx, rpc.NewRequest("1", "common:getNodeRecentStatus", map[string]any{
		"uuid": memberClient.UUID,
	}))
	if rpcErr != nil {
		t.Fatalf("expected recent status response, got rpc error: %+v", rpcErr)
	}
	var recent struct {
		Count   int              `json:"count"`
		Records []map[string]any `json:"records"`
	}
	decodeRPCResult(t, recentResult, &recent)
	if recent.Count == 0 || len(recent.Records) == 0 {
		t.Fatalf("expected admin to see member recent status, got %+v", recent)
	}

	recordsResult, rpcErr := getRecords(ctx, rpc.NewRequest("1", "common:getRecords", map[string]any{
		"type":  "load",
		"uuid":  memberClient.UUID,
		"hours": 1,
	}))
	if rpcErr != nil {
		t.Fatalf("expected records response, got rpc error: %+v", rpcErr)
	}
	var records struct {
		Count   int                         `json:"count"`
		Records map[string][]map[string]any `json:"records"`
	}
	decodeRPCResult(t, recordsResult, &records)
	if records.Count == 0 || len(records.Records[memberClient.UUID]) == 0 {
		t.Fatalf("expected admin to see member history records, got %+v", records)
	}
}

func decodeRPCResult(t *testing.T, value any, target any) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("failed to marshal rpc result: %v", err)
	}
	if err := json.Unmarshal(data, target); err != nil {
		t.Fatalf("failed to decode rpc result: %v", err)
	}
}

func keysOfMap[K comparable, V any](input map[K]V) []K {
	keys := make([]K, 0, len(input))
	for key := range input {
		keys = append(keys, key)
	}
	return keys
}
