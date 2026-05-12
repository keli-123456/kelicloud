package failoverv2

import (
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/komari-monitor/komari/common"
	"github.com/komari-monitor/komari/config"
	failoverv2db "github.com/komari-monitor/komari/database/failoverv2"
	"github.com/komari-monitor/komari/database/models"
	"github.com/komari-monitor/komari/ws"
)

const (
	scheduledDefaultFailureThreshold          = 2
	scheduledDefaultStaleAfterSeconds         = 300
	scheduledDefaultCheckIntervalSec          = models.FailoverV2DefaultCheckIntervalSeconds
	scheduledExecutionLogRetentionDefaultDays = 30
	scheduledExecutionLogCleanupRunInterval   = 24 * time.Hour
	scheduledExecutionLogCleanupBatchLimit    = 1000
	scheduledExecutionLogCleanupMaxBatches    = 20
	scheduledAutomaticExecutionConcurrency    = 4
)

type scheduledTriggerCandidate struct {
	member *models.FailoverV2Member
	report *common.Report
	reason string
}

var scheduledAutomaticExecutionSlots = make(chan struct{}, scheduledAutomaticExecutionConcurrency)

func RunScheduledWork() error {
	now := time.Now()
	if err := runScheduledExecutionLogCleanup(now); err != nil {
		log.Printf("failoverv2: scheduled execution log cleanup failed: %v", err)
	}

	enabled, err := config.GetAs[bool](config.FailoverV2SchedulerEnabledKey, false)
	if err != nil {
		return err
	}
	if !enabled {
		return nil
	}

	if err := runPendingFailoverV2CleanupRetries(); err != nil {
		log.Printf("failoverv2: pending cleanup retry failed: %v", err)
	}

	services, err := failoverv2db.ListScheduledCheckCandidateServices(now)
	if err != nil {
		return err
	}

	latestReports := ws.GetLatestReport()
	for index := range services {
		service := &services[index]
		if !shouldRunServiceScheduledCheck(service, now) {
			continue
		}
		checkedAt := models.FromTime(now)

		triggers := evaluateServiceHealth(service, latestReports, now)
		if len(triggers) == 0 {
			status, message := summarizeServiceHealth(service)
			fields := scheduledChangedServiceHealthFields(service, status, message, checkedAt)
			if err := failoverv2db.UpdateServiceFieldsForUser(service.UserID, service.ID, fields); err != nil {
				log.Printf("failoverv2: failed to update service %d health: %v", service.ID, err)
			} else {
				service.LastStatus = status
				service.LastMessage = message
				service.LastCheckedAt = &checkedAt
			}
			continue
		}

		for _, trigger := range triggers {
			triggerMember := trigger.member
			triggerReport := trigger.report
			triggerReason := trigger.reason
			startAction := "automatic failover"
			if memberUsesExistingClient(triggerMember) {
				startAction = "automatic protection"
			}
			startMessage := fmt.Sprintf(
				"%s started for member %s: %s",
				startAction,
				memberDisplayLabel(triggerMember),
				strings.TrimSpace(triggerReason),
			)
			var execution *models.FailoverV2Execution
			releaseExecutionSlot, acquired := tryAcquireScheduledAutomaticExecutionSlot()
			if !acquired {
				log.Printf(
					"failoverv2: delayed automatic action for service %d member %d because automatic execution concurrency limit is reached",
					service.ID,
					triggerMember.ID,
				)
				continue
			}
			if memberUsesExistingClient(triggerMember) {
				execution, err = queueMemberDetachExecution(
					service.UserID,
					service,
					triggerMember,
					"automatic existing_client protection",
					buildTriggerSnapshot(triggerReport),
					startMessage,
					releaseExecutionSlot,
				)
			} else {
				execution, err = queueMemberProvisioningFailoverExecution(
					service.UserID,
					service,
					triggerMember,
					triggerReason,
					buildTriggerSnapshot(triggerReport),
					startMessage,
					releaseExecutionSlot,
				)
			}
			if err != nil {
				message := fmt.Sprintf(
					"failed to queue automatic action for member %s: %v",
					memberDisplayLabel(triggerMember),
					err,
				)
				log.Printf("failoverv2: %s", message)
				if updateErr := failoverv2db.UpdateServiceFieldsForUser(service.UserID, service.ID, map[string]interface{}{
					"last_status":  models.FailoverV2ServiceStatusFailed,
					"last_message": message,
				}); updateErr != nil {
					log.Printf("failoverv2: failed to update service %d queue error: %v", service.ID, updateErr)
				}
				continue
			}
			notifyAutomaticFailoverTriggered(service, triggerMember, execution, triggerReason)
		}

		if err := failoverv2db.UpdateServiceFieldsForUser(service.UserID, service.ID, map[string]interface{}{
			"last_checked_at": checkedAt,
		}); err != nil {
			log.Printf("failoverv2: failed to update service %d last_checked_at: %v", service.ID, err)
		} else {
			service.LastCheckedAt = &checkedAt
		}
	}

	return nil
}

func tryAcquireScheduledAutomaticExecutionSlot() (func(), bool) {
	select {
	case scheduledAutomaticExecutionSlots <- struct{}{}:
		var once sync.Once
		return func() {
			once.Do(func() {
				<-scheduledAutomaticExecutionSlots
			})
		}, true
	default:
		return nil, false
	}
}

func runScheduledExecutionLogCleanup(now time.Time) error {
	if now.IsZero() {
		now = time.Now()
	}

	retentionDays, err := config.GetAs[int](config.FailoverV2ExecutionLogRetentionDaysKey, scheduledExecutionLogRetentionDefaultDays)
	if err != nil {
		return err
	}
	if retentionDays <= 0 {
		return nil
	}

	lastRunAtRaw, err := config.GetAs[string](config.FailoverV2ExecutionLogCleanupLastRunAtKey, "")
	if err != nil {
		return err
	}
	if !shouldRunScheduledExecutionLogCleanup(now, lastRunAtRaw) {
		return nil
	}

	cutoff := now.Add(-time.Duration(retentionDays) * 24 * time.Hour)
	var totalDeleted int64
	for batch := 0; batch < scheduledExecutionLogCleanupMaxBatches; batch++ {
		deleted, deleteErr := failoverv2db.DeleteTerminalExecutionsStartedBefore(cutoff, scheduledExecutionLogCleanupBatchLimit)
		if deleteErr != nil {
			return deleteErr
		}
		totalDeleted += deleted
		if deleted == 0 {
			break
		}
	}

	if err := config.Set(config.FailoverV2ExecutionLogCleanupLastRunAtKey, now.Format(time.RFC3339)); err != nil {
		log.Printf("failoverv2: failed to persist execution log cleanup last run: %v", err)
	}
	if totalDeleted > 0 {
		log.Printf(
			"failoverv2: scheduled execution log cleanup deleted=%d retention_days=%d cutoff=%s",
			totalDeleted,
			retentionDays,
			cutoff.Format(time.RFC3339),
		)
	}
	return nil
}

func shouldRunScheduledExecutionLogCleanup(now time.Time, lastRunAtRaw string) bool {
	if now.IsZero() {
		now = time.Now()
	}
	lastRunAtRaw = strings.TrimSpace(lastRunAtRaw)
	if lastRunAtRaw == "" {
		return true
	}
	lastRunAt, err := time.Parse(time.RFC3339, lastRunAtRaw)
	if err != nil {
		return true
	}
	return !now.Before(lastRunAt.Add(scheduledExecutionLogCleanupRunInterval))
}

func normalizedServiceCheckIntervalSeconds(service *models.FailoverV2Service) int {
	if service == nil {
		return scheduledDefaultCheckIntervalSec
	}
	if service.CheckIntervalSeconds < models.FailoverV2MinCheckIntervalSeconds {
		return scheduledDefaultCheckIntervalSec
	}
	return service.CheckIntervalSeconds
}

func shouldRunServiceScheduledCheck(service *models.FailoverV2Service, now time.Time) bool {
	if service == nil {
		return false
	}
	if now.IsZero() {
		now = time.Now()
	}
	if service.LastCheckedAt == nil {
		return true
	}
	lastChecked := service.LastCheckedAt.ToTime()
	if lastChecked.IsZero() {
		return true
	}
	if hasExpiredCooldownMember(service, lastChecked, now) {
		return true
	}
	nextCheckAt := lastChecked.Add(time.Duration(normalizedServiceCheckIntervalSeconds(service)) * time.Second)
	return !nextCheckAt.After(now)
}

func hasExpiredCooldownMember(service *models.FailoverV2Service, lastChecked, now time.Time) bool {
	if service == nil {
		return false
	}
	if now.IsZero() {
		now = time.Now()
	}

	for index := range service.Members {
		member := &service.Members[index]
		if !member.Enabled {
			continue
		}

		if nextMessageCooldown, ok := parseCooldownUntilFromMessage(member.LastMessage); ok {
			if !nextMessageCooldown.After(now) {
				if lastChecked.IsZero() || lastChecked.Before(nextMessageCooldown) {
					return true
				}
			}
			if strings.TrimSpace(member.LastStatus) == models.FailoverV2MemberStatusCooldown {
				continue
			}
		}

		if member.CooldownSeconds <= 0 || member.LastTriggeredAt == nil {
			if strings.TrimSpace(member.LastStatus) == models.FailoverV2MemberStatusCooldown {
				// Legacy/stale cooldown rows should be rechecked immediately.
				return true
			}
			continue
		}
		nextRun := member.LastTriggeredAt.ToTime().Add(time.Duration(member.CooldownSeconds) * time.Second)
		if nextRun.IsZero() || nextRun.After(now) {
			continue
		}
		if strings.TrimSpace(member.LastStatus) == models.FailoverV2MemberStatusCooldown {
			// If the UI is still showing cooldown but the window has expired, force an immediate recheck.
			return true
		}
		// Re-check once when cooldown expires, even if the regular interval has not elapsed yet.
		if lastChecked.IsZero() || lastChecked.Before(nextRun) {
			return true
		}
	}

	return false
}

func evaluateServiceHealth(service *models.FailoverV2Service, latestReports map[string]*common.Report, now time.Time) []scheduledTriggerCandidate {
	if service == nil {
		return nil
	}

	var triggers []scheduledTriggerCandidate

	for index := range service.Members {
		member := &service.Members[index]
		if !member.Enabled {
			continue
		}

		report := latestReports[strings.TrimSpace(member.WatchClientUUID)]
		shouldTrigger, fields, reason := evaluateMemberHealth(member, report, now)
		if len(fields) > 0 {
			changedFields := scheduledChangedMemberFields(member, fields)
			applyScheduledMemberFields(member, fields)
			if len(changedFields) > 0 {
				if err := failoverv2db.UpdateMemberFieldsForUser(service.UserID, service.ID, member.ID, changedFields); err != nil {
					log.Printf("failoverv2: failed to update member %d health: %v", member.ID, err)
				}
			}
		}
		if shouldTrigger {
			triggers = append(triggers, scheduledTriggerCandidate{
				member: member,
				report: report,
				reason: reason,
			})
		}
	}

	return triggers
}

func evaluateMemberHealth(member *models.FailoverV2Member, report *common.Report, now time.Time) (bool, map[string]interface{}, string) {
	fields := map[string]interface{}{}
	if member == nil {
		return false, fields, ""
	}

	var cooldownUntil time.Time
	cooldownActive := false
	if member.CooldownSeconds > 0 && member.LastTriggeredAt != nil {
		cooldownUntil = member.LastTriggeredAt.ToTime().Add(time.Duration(member.CooldownSeconds) * time.Second)
		cooldownActive = cooldownUntil.After(now)
	}

	if memberUsesExistingClient(member) && memberHasAllLinesDetached(member) {
		fields["last_status"] = models.FailoverV2MemberStatusFailed
		fields["last_message"] = "all dns lines are already detached"
		fields["trigger_failure_count"] = 0
		return false, fields, ""
	}

	if memberUsesExistingClient(member) && strings.TrimSpace(member.WatchClientUUID) == "" {
		fields["last_status"] = models.FailoverV2MemberStatusUnknown
		fields["last_message"] = "existing_client member requires watch_client_uuid"
		fields["trigger_failure_count"] = 0
		return false, fields, ""
	}

	if report == nil || report.CNConnectivity == nil {
		shouldTrigger, updatedFields, reason := evaluateMissingMemberHealth(member, fields, "cn_connectivity report is unavailable")
		if shouldTrigger && cooldownActive {
			updatedFields["last_status"] = models.FailoverV2MemberStatusTriggered
			updatedFields["last_message"] = appendCooldownUntilMessage(stringMapValue(updatedFields, "last_message"), cooldownUntil)
		}
		return shouldTrigger, updatedFields, reason
	}

	staleAfter := member.StaleAfterSeconds
	if staleAfter <= 0 {
		staleAfter = scheduledDefaultStaleAfterSeconds
	}

	reportTime := report.UpdatedAt
	if report.CNConnectivity.CheckedAt.After(reportTime) {
		reportTime = report.CNConnectivity.CheckedAt
	}
	if reportTime.IsZero() || now.Sub(reportTime) > time.Duration(staleAfter)*time.Second {
		shouldTrigger, updatedFields, reason := evaluateMissingMemberHealth(member, fields, "latest report is stale")
		if shouldTrigger && cooldownActive {
			updatedFields["last_status"] = models.FailoverV2MemberStatusTriggered
			updatedFields["last_message"] = appendCooldownUntilMessage(stringMapValue(updatedFields, "last_message"), cooldownUntil)
		}
		return shouldTrigger, updatedFields, reason
	}

	fields["trigger_failure_count"] = 0

	threshold := member.FailureThreshold
	if threshold <= 0 {
		threshold = scheduledDefaultFailureThreshold
	}
	if report.CNConnectivity.Status == "blocked_suspected" && report.CNConnectivity.ConsecutiveFailures >= threshold {
		fields["last_status"] = models.FailoverV2MemberStatusTriggered
		fields["last_message"] = fmt.Sprintf("cn_connectivity blocked_suspected (%d failures)", report.CNConnectivity.ConsecutiveFailures)
		if cooldownActive {
			fields["last_message"] = appendCooldownUntilMessage(stringMapValue(fields, "last_message"), cooldownUntil)
			return true, fields, stringMapValue(fields, "last_message")
		}
		return true, fields, fields["last_message"].(string)
	}

	fields["last_status"] = models.FailoverV2MemberStatusHealthy
	fields["last_message"] = strings.TrimSpace(report.CNConnectivity.Status)
	return false, fields, ""
}

func scheduledChangedMemberFields(member *models.FailoverV2Member, fields map[string]interface{}) map[string]interface{} {
	if member == nil || len(fields) == 0 {
		return fields
	}
	changed := map[string]interface{}{}
	for key, value := range fields {
		switch key {
		case "last_status":
			if strings.TrimSpace(stringMapValue(fields, key)) != strings.TrimSpace(member.LastStatus) {
				changed[key] = value
			}
		case "last_message":
			if strings.TrimSpace(stringMapValue(fields, key)) != strings.TrimSpace(member.LastMessage) {
				changed[key] = value
			}
		case "trigger_failure_count":
			if intMapValue(fields, key) != member.TriggerFailureCount {
				changed[key] = value
			}
		default:
			changed[key] = value
		}
	}
	return changed
}

func scheduledChangedServiceHealthFields(service *models.FailoverV2Service, status, message string, checkedAt models.LocalTime) map[string]interface{} {
	fields := map[string]interface{}{
		"last_checked_at": checkedAt,
	}
	if service == nil {
		fields["last_status"] = status
		fields["last_message"] = message
		return fields
	}
	if strings.TrimSpace(status) != strings.TrimSpace(service.LastStatus) {
		fields["last_status"] = status
	}
	if strings.TrimSpace(message) != strings.TrimSpace(service.LastMessage) {
		fields["last_message"] = message
	}
	return fields
}

func appendCooldownUntilMessage(base string, cooldownUntil time.Time) string {
	base = strings.TrimSpace(base)
	if cooldownUntil.IsZero() {
		return base
	}

	cooldownText := "cooldown until " + cooldownUntil.UTC().Format(time.RFC3339)
	if base == "" {
		return cooldownText
	}
	if strings.Contains(strings.ToLower(base), "cooldown until") {
		return base
	}
	return base + "; " + cooldownText
}

func parseCooldownUntilFromMessage(message string) (time.Time, bool) {
	message = strings.TrimSpace(message)
	lowered := strings.ToLower(message)
	marker := "cooldown until "
	idx := strings.Index(lowered, marker)
	if idx < 0 {
		return time.Time{}, false
	}

	suffix := strings.TrimSpace(message[idx+len(marker):])
	if suffix == "" {
		return time.Time{}, false
	}
	if semicolon := strings.Index(suffix, ";"); semicolon >= 0 {
		suffix = strings.TrimSpace(suffix[:semicolon])
	}
	if suffix == "" {
		return time.Time{}, false
	}
	cooldownUntil, err := time.Parse(time.RFC3339, suffix)
	if err != nil {
		return time.Time{}, false
	}
	return cooldownUntil, true
}

func evaluateMissingMemberHealth(member *models.FailoverV2Member, fields map[string]interface{}, baseMessage string) (bool, map[string]interface{}, string) {
	if fields == nil {
		fields = map[string]interface{}{}
	}

	threshold := member.FailureThreshold
	if threshold <= 0 {
		threshold = scheduledDefaultFailureThreshold
	}
	failures := member.TriggerFailureCount + 1
	fields["trigger_failure_count"] = failures
	fields["last_message"] = fmt.Sprintf("%s (%d/%d)", strings.TrimSpace(baseMessage), failures, threshold)
	if failures >= threshold {
		fields["last_status"] = models.FailoverV2MemberStatusTriggered
		return true, fields, stringMapValue(fields, "last_message")
	}
	fields["last_status"] = models.FailoverV2MemberStatusUnknown
	return false, fields, ""
}

func applyScheduledMemberFields(member *models.FailoverV2Member, fields map[string]interface{}) {
	if member == nil {
		return
	}
	if status := stringMapValue(fields, "last_status"); status != "" {
		member.LastStatus = status
	}
	if message := stringMapValue(fields, "last_message"); message != "" {
		member.LastMessage = message
	}
	if _, ok := fields["trigger_failure_count"]; ok {
		member.TriggerFailureCount = intMapValue(fields, "trigger_failure_count")
	}
}

func summarizeServiceHealth(service *models.FailoverV2Service) (string, string) {
	if service == nil {
		return models.FailoverV2ServiceStatusUnknown, "service is unavailable"
	}

	enabledCount := 0
	healthyCount := 0
	var firstCooldown string
	var firstUnknown string
	var firstFailure string

	for index := range service.Members {
		member := &service.Members[index]
		if !member.Enabled {
			continue
		}
		enabledCount++
		message := firstNonEmpty(
			strings.TrimSpace(member.LastMessage),
			strings.TrimSpace(member.LastStatus),
			memberDisplayLabel(member),
		)
		switch strings.TrimSpace(member.LastStatus) {
		case models.FailoverV2MemberStatusHealthy:
			healthyCount++
		case models.FailoverV2MemberStatusTriggered, models.FailoverV2MemberStatusFailed:
			if firstFailure == "" {
				firstFailure = fmt.Sprintf("member %s: %s", memberDisplayLabel(member), message)
			}
		case models.FailoverV2MemberStatusCooldown:
			if firstCooldown == "" {
				firstCooldown = fmt.Sprintf("member %s: %s", memberDisplayLabel(member), message)
			}
		default:
			if firstUnknown == "" {
				firstUnknown = fmt.Sprintf("member %s: %s", memberDisplayLabel(member), message)
			}
		}
	}

	if enabledCount == 0 {
		return models.FailoverV2ServiceStatusUnknown, "no enabled members configured"
	}
	if firstFailure != "" {
		return models.FailoverV2ServiceStatusFailed, firstFailure
	}
	if healthyCount == enabledCount {
		return models.FailoverV2ServiceStatusHealthy, fmt.Sprintf("%d/%d members healthy", healthyCount, enabledCount)
	}
	if firstCooldown != "" {
		return models.FailoverV2ServiceStatusUnknown, firstCooldown
	}
	if firstUnknown != "" {
		return models.FailoverV2ServiceStatusUnknown, firstUnknown
	}
	return models.FailoverV2ServiceStatusUnknown, "service health is unknown"
}

func buildTriggerSnapshot(report *common.Report) string {
	if report == nil || report.CNConnectivity == nil {
		return "null"
	}
	return string(marshalJSON(map[string]interface{}{
		"status":               report.CNConnectivity.Status,
		"target":               report.CNConnectivity.Target,
		"latency":              report.CNConnectivity.Latency,
		"message":              report.CNConnectivity.Message,
		"checked_at":           report.CNConnectivity.CheckedAt,
		"consecutive_failures": report.CNConnectivity.ConsecutiveFailures,
		"report_updated_at":    report.UpdatedAt,
	}))
}
