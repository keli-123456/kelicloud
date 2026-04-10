package client

import (
	"fmt"
	"testing"
	"time"

	"github.com/komari-monitor/komari/api"
	"github.com/komari-monitor/komari/common"
	"github.com/komari-monitor/komari/database/models"
)

func TestSaveClientReportUpdatesLatestOnline(t *testing.T) {
	db := configureClientTestDB(t)

	clientUUID := fmt.Sprintf("client-report-%d", time.Now().UnixNano())
	initialLatestOnline := models.FromTime(time.Now().Add(-2 * time.Hour))
	clientRecord := models.Client{
		UUID:         clientUUID,
		Token:        "token-report",
		Name:         "Report Client",
		LatestOnline: initialLatestOnline,
		CreatedAt:    initialLatestOnline,
		UpdatedAt:    initialLatestOnline,
	}
	if err := db.Create(&clientRecord).Error; err != nil {
		t.Fatalf("failed to seed client: %v", err)
	}
	api.Records.Delete(clientUUID)

	reportTime := time.Now().UTC()
	report := common.Report{
		CPU:         common.CPUReport{Usage: 12.5},
		Ram:         common.RamReport{Total: 2048, Used: 1024},
		Swap:        common.RamReport{Total: 1024, Used: 0},
		Load:        common.LoadReport{Load1: 0.5, Load5: 0.4, Load15: 0.3},
		Disk:        common.DiskReport{Total: 4096, Used: 1024},
		Network:     common.NetworkReport{Up: 128, Down: 256, TotalUp: 1024, TotalDown: 2048},
		Connections: common.ConnectionsReport{TCP: 10, UDP: 2},
		Process:     42,
		UpdatedAt:   reportTime,
	}

	if err := SaveClientReport(clientUUID, report); err != nil {
		t.Fatalf("SaveClientReport returned error: %v", err)
	}

	var updated models.Client
	if err := db.Where("uuid = ?", clientUUID).First(&updated).Error; err != nil {
		t.Fatalf("failed to reload client: %v", err)
	}
	gotLatestOnline := updated.LatestOnline.ToTime()
	if !gotLatestOnline.After(initialLatestOnline.ToTime()) {
		t.Fatalf("expected latest_online to advance beyond %v, got %v", initialLatestOnline.ToTime(), gotLatestOnline)
	}
	if diff := gotLatestOnline.Sub(reportTime); diff < -time.Second || diff > time.Second {
		t.Fatalf("expected latest_online near %v, got %v", reportTime, gotLatestOnline)
	}
}
