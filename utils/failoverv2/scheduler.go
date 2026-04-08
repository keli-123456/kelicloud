package failoverv2

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/komari-monitor/komari/common"
	"github.com/komari-monitor/komari/config"
	failoverv2db "github.com/komari-monitor/komari/database/failoverv2"
	"github.com/komari-monitor/komari/database/models"
	"github.com/komari-monitor/komari/ws"
)

const (
	scheduledDefaultFailureThreshold  = 2
	scheduledDefaultStaleAfterSeconds = 300
)

func RunScheduledWork() error {
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

	services, err := failoverv2db.ListEnabledServices()
	if err != nil {
		return err
	}

	latestReports := ws.GetLatestReport()
	now := time.Now()
	for index := range services {
		service := &services[index]

		active, err := failoverv2db.HasActiveExecutionForService(service.UserID, service.ID)
		if err != nil {
			log.Printf("failoverv2: failed to query active execution for service %d: %v", service.ID, err)
			continue
		}
		if active {
			continue
		}

		triggerMember, triggerReport, triggerReason := evaluateServiceHealth(service, latestReports, now)
		if triggerMember == nil {
			status, message := summarizeServiceHealth(service)
			if err := failoverv2db.UpdateServiceFieldsForUser(service.UserID, service.ID, map[string]interface{}{
				"last_status":  status,
				"last_message": message,
			}); err != nil {
				log.Printf("failoverv2: failed to update service %d health: %v", service.ID, err)
			}
			continue
		}

		startMessage := fmt.Sprintf(
			"automatic failover started for member %s: %s",
			memberDisplayLabel(triggerMember),
			strings.TrimSpace(triggerReason),
		)
		execution, err := queueMemberFailoverExecution(
			service.UserID,
			service,
			triggerMember,
			triggerReason,
			buildTriggerSnapshot(triggerReport),
			startMessage,
		)
		if err != nil {
			message := fmt.Sprintf(
				"failed to queue automatic failover for member %s: %v",
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

	return nil
}

func evaluateServiceHealth(service *models.FailoverV2Service, latestReports map[string]*common.Report, now time.Time) (*models.FailoverV2Member, *common.Report, string) {
	if service == nil {
		return nil, nil, ""
	}

	var triggerMember *models.FailoverV2Member
	var triggerReport *common.Report
	var triggerReason string

	for index := range service.Members {
		member := &service.Members[index]
		if !member.Enabled {
			continue
		}

		report := latestReports[strings.TrimSpace(member.WatchClientUUID)]
		shouldTrigger, fields, reason := evaluateMemberHealth(member, report, now)
		if len(fields) > 0 {
			if err := failoverv2db.UpdateMemberFieldsForUser(service.UserID, service.ID, member.ID, fields); err != nil {
				log.Printf("failoverv2: failed to update member %d health: %v", member.ID, err)
			} else {
				applyScheduledMemberFields(member, fields)
			}
		}
		if shouldTrigger && triggerMember == nil {
			triggerMember = member
			triggerReport = report
			triggerReason = reason
		}
	}

	return triggerMember, triggerReport, triggerReason
}

func evaluateMemberHealth(member *models.FailoverV2Member, report *common.Report, now time.Time) (bool, map[string]interface{}, string) {
	fields := map[string]interface{}{}
	if member == nil {
		return false, fields, ""
	}

	if member.CooldownSeconds > 0 && member.LastTriggeredAt != nil {
		nextRun := member.LastTriggeredAt.ToTime().Add(time.Duration(member.CooldownSeconds) * time.Second)
		if nextRun.After(now) {
			fields["last_status"] = models.FailoverV2MemberStatusCooldown
			fields["last_message"] = "cooldown until " + nextRun.UTC().Format(time.RFC3339)
			return false, fields, ""
		}
	}

	if strings.TrimSpace(member.WatchClientUUID) == "" {
		fields["last_status"] = models.FailoverV2MemberStatusUnknown
		fields["last_message"] = "member is not initialized"
		fields["trigger_failure_count"] = 0
		return false, fields, ""
	}

	if report == nil || report.CNConnectivity == nil {
		return evaluateMissingMemberHealth(member, fields, "cn_connectivity report is unavailable")
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
		return evaluateMissingMemberHealth(member, fields, "latest report is stale")
	}

	fields["trigger_failure_count"] = 0

	threshold := member.FailureThreshold
	if threshold <= 0 {
		threshold = scheduledDefaultFailureThreshold
	}
	if report.CNConnectivity.Status == "blocked_suspected" && report.CNConnectivity.ConsecutiveFailures >= threshold {
		fields["last_status"] = models.FailoverV2MemberStatusTriggered
		fields["last_message"] = fmt.Sprintf("cn_connectivity blocked_suspected (%d failures)", report.CNConnectivity.ConsecutiveFailures)
		return true, fields, fields["last_message"].(string)
	}

	fields["last_status"] = models.FailoverV2MemberStatusHealthy
	fields["last_message"] = strings.TrimSpace(report.CNConnectivity.Status)
	return false, fields, ""
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
