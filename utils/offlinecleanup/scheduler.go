package offlinecleanup

import (
	"fmt"
	"log"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/komari-monitor/komari/config"
	"github.com/komari-monitor/komari/database/auditlog"
	"github.com/komari-monitor/komari/database/clients"
	"github.com/komari-monitor/komari/database/models"
	"github.com/komari-monitor/komari/ws"
)

const DefaultDailyCleanupTime = "03:00"

type Settings struct {
	Enabled    bool
	RunAt      string
	GraceHours int
}

type CleanupResult struct {
	Deleted []string
	Failed  map[string]string
}

var cleanupInProgress atomic.Bool

func NormalizeDailyCleanupTime(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		value = DefaultDailyCleanupTime
	}

	if len(value) != len("15:04") || value[2] != ':' {
		return "", fmt.Errorf("offline cleanup time must use HH:MM format")
	}

	parsed, err := time.Parse("15:04", value)
	if err != nil {
		return "", fmt.Errorf("offline cleanup time must use HH:MM format")
	}

	return parsed.Format("15:04"), nil
}

func ShouldRun(now time.Time, runAt, lastRunAtRaw string) (bool, error) {
	normalizedRunAt, err := NormalizeDailyCleanupTime(runAt)
	if err != nil {
		return false, err
	}

	scheduledToday, err := scheduledTimeForDate(now, normalizedRunAt)
	if err != nil {
		return false, err
	}
	if now.Before(scheduledToday) {
		return false, nil
	}

	lastRunAt, ok := parseLastRunAt(lastRunAtRaw, now.Location())
	if !ok {
		return true, nil
	}

	return lastRunAt.Before(scheduledToday), nil
}

func RunScheduledWork() {
	settings, lastRunAtRaw, err := loadSettings()
	if err != nil {
		log.Printf("offline cleanup: failed to load settings: %v", err)
		return
	}
	if !settings.Enabled {
		return
	}

	now := time.Now()
	shouldRun, err := ShouldRun(now, settings.RunAt, lastRunAtRaw)
	if err != nil {
		log.Printf("offline cleanup: invalid schedule %q: %v", settings.RunAt, err)
		return
	}
	if !shouldRun {
		return
	}
	if !cleanupInProgress.CompareAndSwap(false, true) {
		return
	}
	defer cleanupInProgress.Store(false)

	result, err := CleanupOfflineNodes(settings.GraceHours)
	if err != nil {
		log.Printf("offline cleanup: scheduled run failed: %v", err)
		return
	}

	if err := config.Set(config.OfflineCleanupLastRunAtKey, now.Format(time.RFC3339)); err != nil {
		log.Printf("offline cleanup: failed to persist last run timestamp: %v", err)
	}

	if len(result.Deleted) == 0 && len(result.Failed) == 0 {
		return
	}

	message := fmt.Sprintf(
		"scheduled offline cleanup completed: deleted=%d failed=%d",
		len(result.Deleted),
		len(result.Failed),
	)
	if len(result.Failed) > 0 {
		auditlog.EventLog("warn", message)
		log.Printf("offline cleanup: %s", message)
		return
	}

	auditlog.EventLog("info", message)
	log.Printf("offline cleanup: %s", message)
}

func CleanupOfflineNodes(graceHours int) (CleanupResult, error) {
	allClients, err := clients.GetAllClientBasicInfo()
	if err != nil {
		return CleanupResult{}, err
	}

	onlineSet := make(map[string]struct{})
	for _, uuid := range ws.GetAllOnlineUUIDs() {
		onlineSet[uuid] = struct{}{}
	}

	return cleanupOfflineClients(allClients, onlineSet, graceHours, time.Now(), func(client models.Client) error {
		if err := clients.DeleteClient(client.UUID); err != nil {
			return err
		}
		ws.DeleteConnectedClients(client.UUID)
		ws.DeleteLatestReport(client.UUID)
		return nil
	}), nil
}

func cleanupOfflineClients(allClients []models.Client, onlineSet map[string]struct{}, graceHours int, now time.Time, deleteFn func(models.Client) error) CleanupResult {
	if graceHours < 1 {
		graceHours = 1
	}
	cutoff := now.Add(-time.Duration(graceHours) * time.Hour)

	offlineClients := make([]models.Client, 0)
	for _, client := range allClients {
		if client.UUID == "" {
			continue
		}
		if _, online := onlineSet[client.UUID]; online {
			continue
		}
		if lastOnlineAt := effectiveOfflineReferenceTime(client); !lastOnlineAt.IsZero() && lastOnlineAt.After(cutoff) {
			continue
		}
		offlineClients = append(offlineClients, client)
	}

	sort.SliceStable(offlineClients, func(i, j int) bool {
		leftName := strings.TrimSpace(offlineClients[i].Name)
		rightName := strings.TrimSpace(offlineClients[j].Name)
		if leftName == rightName {
			return offlineClients[i].UUID < offlineClients[j].UUID
		}
		if leftName == "" {
			return false
		}
		if rightName == "" {
			return true
		}
		return leftName < rightName
	})

	result := CleanupResult{
		Deleted: make([]string, 0, len(offlineClients)),
		Failed:  make(map[string]string),
	}
	for _, client := range offlineClients {
		if err := deleteFn(client); err != nil {
			result.Failed[client.UUID] = err.Error()
			continue
		}
		result.Deleted = append(result.Deleted, client.UUID)
	}
	if len(result.Failed) == 0 {
		result.Failed = nil
	}
	return result
}

func loadSettings() (Settings, string, error) {
	enabled, err := config.GetAs[bool](config.OfflineCleanupEnabledKey, false)
	if err != nil {
		return Settings{}, "", err
	}
	runAt, err := config.GetAs[string](config.OfflineCleanupTimeKey, DefaultDailyCleanupTime)
	if err != nil {
		return Settings{}, "", err
	}
	lastRunAt, err := config.GetAs[string](config.OfflineCleanupLastRunAtKey, "")
	if err != nil {
		return Settings{}, "", err
	}
	graceHours, err := config.GetAs[int](config.OfflineCleanupGraceHoursKey, 24)
	if err != nil {
		return Settings{}, "", err
	}
	if graceHours < 1 {
		graceHours = 1
	}

	normalizedRunAt, err := NormalizeDailyCleanupTime(runAt)
	if err != nil {
		return Settings{}, "", err
	}

	return Settings{
		Enabled:    enabled,
		RunAt:      normalizedRunAt,
		GraceHours: graceHours,
	}, strings.TrimSpace(lastRunAt), nil
}

func effectiveOfflineReferenceTime(client models.Client) time.Time {
	for _, candidate := range []time.Time{
		client.LatestOnline.ToTime(),
		client.CreatedAt.ToTime(),
	} {
		if !candidate.IsZero() {
			return candidate
		}
	}
	return time.Time{}
}

func scheduledTimeForDate(now time.Time, runAt string) (time.Time, error) {
	parsed, err := time.Parse("15:04", runAt)
	if err != nil {
		return time.Time{}, err
	}

	return time.Date(
		now.Year(),
		now.Month(),
		now.Day(),
		parsed.Hour(),
		parsed.Minute(),
		0,
		0,
		now.Location(),
	), nil
}

func parseLastRunAt(value string, location *time.Location) (time.Time, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, false
	}

	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return time.Time{}, false
	}
	if location != nil {
		parsed = parsed.In(location)
	}
	return parsed, true
}
