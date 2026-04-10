package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/komari-monitor/komari/common"
	"github.com/komari-monitor/komari/database/accounts"
	"github.com/komari-monitor/komari/database/clients"
)

func TestGetClientRecentRecordsReturnsExplicitTimeDTO(t *testing.T) {
	gin.SetMode(gin.TestMode)
	configureAPITestDB()

	user, err := accounts.CreateAccount("recent-user", "password")
	if err != nil {
		t.Fatalf("failed to create account: %v", err)
	}
	t.Cleanup(func() {
		accounts.DeleteAccountByUsername("recent-user")
		accounts.DeleteAllSessions()
	})

	clientUUID, _, err := clients.CreateClientWithNameForUser(user.UUID, "recent-node")
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	recordTime := time.Now().UTC().Truncate(time.Second)
	Records.Set(clientUUID, []common.Report{
		{
			UUID:      clientUUID,
			UpdatedAt: recordTime,
			Uptime:    3600,
			CPU: common.CPUReport{
				Usage: 32,
			},
			Ram: common.RamReport{
				Used:  512,
				Total: 1024,
			},
			Swap: common.RamReport{
				Used:  64,
				Total: 2048,
			},
			Load: common.LoadReport{
				Load1: 0.5,
			},
			Disk: common.DiskReport{
				Used:  123,
				Total: 456,
			},
			Network: common.NetworkReport{
				Up:   10,
				Down: 20,
			},
			Connections: common.ConnectionsReport{
				TCP: 3,
				UDP: 1,
			},
		},
	}, time.Minute)
	t.Cleanup(func() {
		Records.Delete(clientUUID)
	})

	sessionToken, err := accounts.CreateSession(user.UUID, 2592000, "test_user_agent", "127.0.0.1", "password")
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}

	router := gin.New()
	router.GET("/recent/:uuid", GetClientRecentRecords)

	req := httptest.NewRequest(http.MethodGet, "/recent/"+clientUUID, nil)
	req.AddCookie(&http.Cookie{
		Name:  "session_token",
		Value: sessionToken,
	})
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200, got %d", w.Code)
	}

	var resp struct {
		Data []map[string]any `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if len(resp.Data) != 1 {
		t.Fatalf("expected one record, got %d", len(resp.Data))
	}

	item := resp.Data[0]
	if got := item["time"]; got != recordTime.Format(time.RFC3339) {
		t.Fatalf("expected time %q, got %#v", recordTime.Format(time.RFC3339), got)
	}
	if _, ok := item["updated_at"]; ok {
		t.Fatalf("expected updated_at to be removed from recent DTO, got %#v", item["updated_at"])
	}
}
