package ws

import (
	"testing"

	"github.com/komari-monitor/komari/common"
)

func TestDeleteLatestReportIfOfflineDeletesReport(t *testing.T) {
	const uuid = "ws-delete-report-offline"
	const connID int64 = 101

	t.Cleanup(func() {
		SetPresence(uuid, connID, false)
		DeleteConnectedClients(uuid)
		DeleteLatestReport(uuid)
	})

	SetPresence(uuid, connID, false)
	DeleteConnectedClients(uuid)
	DeleteLatestReport(uuid)

	SetLatestReport(uuid, &common.Report{})
	DeleteLatestReportIfOffline(uuid)

	if _, exists := GetLatestReport()[uuid]; exists {
		t.Fatalf("expected latest report for %s to be deleted when offline", uuid)
	}
}

func TestDeleteLatestReportIfOfflineKeepsConnectedClientReport(t *testing.T) {
	const uuid = "ws-keep-report-connected"
	const connID int64 = 202

	t.Cleanup(func() {
		SetPresence(uuid, connID, false)
		DeleteConnectedClients(uuid)
		DeleteLatestReport(uuid)
	})

	SetPresence(uuid, connID, false)
	DeleteConnectedClients(uuid)
	DeleteLatestReport(uuid)

	SetLatestReport(uuid, &common.Report{})
	SetConnectedClients(uuid, nil)
	DeleteLatestReportIfOffline(uuid)

	if _, exists := GetLatestReport()[uuid]; !exists {
		t.Fatalf("expected latest report for %s to remain while connection is active", uuid)
	}
}

func TestDeleteLatestReportIfOfflineKeepsPresenceLeaseReport(t *testing.T) {
	const uuid = "ws-keep-report-presence"
	const connID int64 = 303

	t.Cleanup(func() {
		SetPresence(uuid, connID, false)
		DeleteConnectedClients(uuid)
		DeleteLatestReport(uuid)
	})

	SetPresence(uuid, connID, false)
	DeleteConnectedClients(uuid)
	DeleteLatestReport(uuid)

	SetLatestReport(uuid, &common.Report{})
	SetPresence(uuid, connID, true)
	DeleteLatestReportIfOffline(uuid)

	if _, exists := GetLatestReport()[uuid]; !exists {
		t.Fatalf("expected latest report for %s to remain while presence lease is active", uuid)
	}

	SetPresence(uuid, connID, false)
	DeleteLatestReportIfOffline(uuid)

	if _, exists := GetLatestReport()[uuid]; exists {
		t.Fatalf("expected latest report for %s to be deleted after presence lease expires", uuid)
	}
}
