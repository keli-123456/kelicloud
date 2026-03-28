package offlinecleanup

import (
	"errors"
	"testing"
	"time"

	"github.com/komari-monitor/komari/database/models"
)

func TestNormalizeDailyCleanupTime(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{name: "default empty", input: "", want: DefaultDailyCleanupTime},
		{name: "trimmed", input: " 08:30 ", want: "08:30"},
		{name: "midnight", input: "00:00", want: "00:00"},
		{name: "invalid hour", input: "24:00", wantErr: true},
		{name: "invalid minute", input: "12:60", wantErr: true},
		{name: "invalid shape", input: "8:30", wantErr: true},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := NormalizeDailyCleanupTime(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for %q", tc.input)
				}
				return
			}
			if err != nil {
				t.Fatalf("NormalizeDailyCleanupTime(%q) returned error: %v", tc.input, err)
			}
			if got != tc.want {
				t.Fatalf("NormalizeDailyCleanupTime(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

func TestShouldRun(t *testing.T) {
	t.Parallel()

	location := time.FixedZone("UTC+8", 8*60*60)
	now := time.Date(2026, 3, 28, 3, 30, 0, 0, location)

	cases := []struct {
		name      string
		runAt     string
		lastRunAt string
		want      bool
		wantErr   bool
	}{
		{name: "not reached yet", runAt: "04:00", want: false},
		{name: "due with no prior run", runAt: "03:00", want: true},
		{name: "already ran today", runAt: "03:00", lastRunAt: "2026-03-28T03:05:00+08:00", want: false},
		{name: "ran yesterday", runAt: "03:00", lastRunAt: "2026-03-27T03:05:00+08:00", want: true},
		{name: "invalid schedule", runAt: "3pm", wantErr: true},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := ShouldRun(now, tc.runAt, tc.lastRunAt)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for %q", tc.runAt)
				}
				return
			}
			if err != nil {
				t.Fatalf("ShouldRun returned error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("ShouldRun(%q, %q) = %v, want %v", tc.runAt, tc.lastRunAt, got, tc.want)
			}
		})
	}
}

func TestCleanupOfflineClients(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 3, 28, 12, 0, 0, 0, time.UTC)
	allClients := []models.Client{
		{UUID: "node-b", Name: "Beta", LatestOnline: models.FromTime(now.Add(-48 * time.Hour))},
		{UUID: "node-a", Name: "Alpha", LatestOnline: models.FromTime(now.Add(-30 * time.Hour))},
		{UUID: "node-c", Name: "Gamma", LatestOnline: models.FromTime(now.Add(-72 * time.Hour))},
		{UUID: "node-d", Name: "Delta", LatestOnline: models.FromTime(now.Add(-2 * time.Hour))},
	}
	onlineSet := map[string]struct{}{
		"node-c": {},
	}

	deleted := make([]string, 0)
	result := cleanupOfflineClients(allClients, onlineSet, 24, now, func(client models.Client) error {
		deleted = append(deleted, client.UUID)
		if client.UUID == "node-b" {
			return errors.New("delete failed")
		}
		return nil
	})

	if len(result.Deleted) != 1 || result.Deleted[0] != "node-a" {
		t.Fatalf("unexpected deleted list: %#v", result.Deleted)
	}
	if len(result.Failed) != 1 || result.Failed["node-b"] != "delete failed" {
		t.Fatalf("unexpected failed map: %#v", result.Failed)
	}

	if len(deleted) != 2 || deleted[0] != "node-a" || deleted[1] != "node-b" {
		t.Fatalf("expected cleanup to process offline nodes in name order, got %#v", deleted)
	}
}

func TestEffectiveOfflineReferenceTimeFallsBackToUpdatedAt(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 3, 28, 12, 0, 0, 0, time.UTC)
	client := models.Client{
		UUID:      "legacy-node",
		UpdatedAt: models.FromTime(now.Add(-36 * time.Hour)),
		CreatedAt: models.FromTime(now.Add(-72 * time.Hour)),
	}

	got := effectiveOfflineReferenceTime(client)
	if !got.Equal(now.Add(-36 * time.Hour)) {
		t.Fatalf("expected fallback to updated_at, got %v", got)
	}
}
