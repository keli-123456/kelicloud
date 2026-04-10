package failoverv2

import (
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	clientdb "github.com/komari-monitor/komari/database/clients"
	failoverv2db "github.com/komari-monitor/komari/database/failoverv2"
	"github.com/komari-monitor/komari/database/models"
	"github.com/komari-monitor/komari/utils/messageSender"
)

const (
	failoverV2EventAutoTriggered       = "Failover V2 Auto Triggered"
	failoverV2EventExecutionFailed     = "Failover V2 Execution Failed"
	failoverV2EventPendingManualReview = "Failover V2 Pending Cleanup Manual Review"
	failoverV2EventActionCompleted     = "Failover V2 Action Completed"
)

var (
	failoverV2SendEventFunc          = messageSender.SendEvent
	failoverV2GetClientByUUIDForUser = clientdb.GetClientByUUIDForUser
)

func notifyAutomaticFailoverTriggered(service *models.FailoverV2Service, member *models.FailoverV2Member, execution *models.FailoverV2Execution, triggerReason string) {
	if service == nil || member == nil {
		return
	}

	lines := []string{
		formatNotificationServiceLine(service),
		formatNotificationMemberLine(member),
	}
	if memberUsesExistingClient(member) {
		lines = append(lines, fmt.Sprintf("Action: automatic dns detach queued for member %s", memberDisplayLabel(member)))
	} else {
		lines = append(lines, fmt.Sprintf("Action: automatic failover queued for member %s", memberDisplayLabel(member)))
	}
	if execution != nil && execution.ID > 0 {
		lines = append(lines, fmt.Sprintf("Execution: #%d", execution.ID))
	}
	if reason := strings.TrimSpace(triggerReason); reason != "" {
		lines = append(lines, "Reason: "+reason)
	}
	if currentAddress := strings.TrimSpace(member.CurrentAddress); currentAddress != "" {
		lines = append(lines, "Current address: "+currentAddress)
	}

	sendFailoverV2Notification(
		service.UserID,
		failoverV2EventAutoTriggered,
		"⚠️",
		notificationClientUUIDs(member, execution),
		lines...,
	)
}

func notifyExecutionFailed(userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member, execution *models.FailoverV2Execution, message string) {
	message = strings.TrimSpace(message)
	if message == "" || message == errExecutionStopped.Error() {
		return
	}

	lines := []string{
		formatNotificationServiceLine(service),
		formatNotificationMemberLine(member),
		"Status: failed",
	}
	if execution != nil && execution.ID > 0 {
		lines = append(lines, fmt.Sprintf("Execution: #%d", execution.ID))
		if triggerReason := strings.TrimSpace(execution.TriggerReason); triggerReason != "" {
			lines = append(lines, "Trigger: "+triggerReason)
		}
	}
	lines = append(lines, "Message: "+message)

	sendFailoverV2Notification(
		firstNonEmpty(strings.TrimSpace(userUUID), notificationUserID(service, member)),
		failoverV2EventExecutionFailed,
		"🚨",
		notificationClientUUIDs(member, execution),
		lines...,
	)
}

func notifyPendingCleanupManualReview(item *models.FailoverV2PendingCleanup, service *models.FailoverV2Service, member *models.FailoverV2Member, reason string) {
	if item == nil {
		return
	}

	lines := []string{
		formatNotificationServiceLine(service),
		formatNotificationMemberLine(member),
		fmt.Sprintf("Pending cleanup: %s", formatPendingCleanupNotificationLabel(item)),
		"Status: manual_review",
	}
	if item.ExecutionID > 0 {
		lines = append(lines, fmt.Sprintf("Execution: #%d", item.ExecutionID))
	}
	if message := firstNonEmpty(strings.TrimSpace(reason), strings.TrimSpace(item.LastError)); message != "" {
		lines = append(lines, "Reason: "+message)
	}

	sendFailoverV2Notification(
		notificationUserID(service, member, item),
		failoverV2EventPendingManualReview,
		"⚠️",
		notificationClientUUIDs(member, nil),
		lines...,
	)
}

func notifyExecutionActionCompleted(action string, service *models.FailoverV2Service, member *models.FailoverV2Member, execution *models.FailoverV2Execution, summary string) {
	action = strings.TrimSpace(action)
	if action == "" {
		return
	}

	lines := []string{
		formatNotificationServiceLine(service),
		formatNotificationMemberLine(member),
		"Action: " + action,
	}
	if execution != nil && execution.ID > 0 {
		lines = append(lines, fmt.Sprintf("Execution: #%d", execution.ID))
	}
	if execution != nil {
		if status := strings.TrimSpace(execution.Status); status != "" {
			lines = append(lines, "Status: "+status)
		}
		if summary == "" {
			summary = strings.TrimSpace(execution.ErrorMessage)
		}
	}
	if summary = strings.TrimSpace(summary); summary != "" {
		lines = append(lines, "Message: "+summary)
	}

	sendFailoverV2Notification(
		notificationUserID(service, member),
		failoverV2EventActionCompleted,
		"ℹ️",
		notificationClientUUIDs(member, execution),
		lines...,
	)
}

func notifyPendingCleanupActionCompleted(action string, item *models.FailoverV2PendingCleanup, service *models.FailoverV2Service, member *models.FailoverV2Member, summary string) {
	if item == nil {
		return
	}
	action = strings.TrimSpace(action)
	if action == "" {
		return
	}

	lines := []string{
		formatNotificationServiceLine(service),
		formatNotificationMemberLine(member),
		"Action: " + action,
		fmt.Sprintf("Pending cleanup: %s", formatPendingCleanupNotificationLabel(item)),
	}
	if item.ExecutionID > 0 {
		lines = append(lines, fmt.Sprintf("Execution: #%d", item.ExecutionID))
	}
	if status := strings.TrimSpace(item.Status); status != "" {
		lines = append(lines, "Status: "+status)
	}
	if summary = strings.TrimSpace(summary); summary == "" {
		summary = firstNonEmpty(strings.TrimSpace(item.LastError), strings.TrimSpace(item.CleanupLabel))
	}
	if summary != "" {
		lines = append(lines, "Message: "+summary)
	}

	sendFailoverV2Notification(
		notificationUserID(service, member, item),
		failoverV2EventActionCompleted,
		"ℹ️",
		notificationClientUUIDs(member, nil),
		lines...,
	)
}

func sendFailoverV2Notification(userUUID, eventName, emoji string, clientUUIDs []string, lines ...string) {
	userUUID = strings.TrimSpace(userUUID)
	eventName = strings.TrimSpace(eventName)
	if userUUID == "" || eventName == "" {
		return
	}

	message := strings.TrimSpace(strings.Join(filterNotificationLines(lines), "\n"))
	event := models.EventMessage{
		UserID:  userUUID,
		Event:   eventName,
		Clients: buildNotificationClients(userUUID, clientUUIDs...),
		Time:    time.Now(),
		Message: message,
		Emoji:   strings.TrimSpace(emoji),
	}
	if err := failoverV2SendEventFunc(event); err != nil && !errors.Is(err, messageSender.ErrUserNotificationTargetNotConfigured) {
		log.Printf("failoverv2: failed to send notification %q for user %s: %v", eventName, userUUID, err)
	}
}

func buildNotificationClients(userUUID string, clientUUIDs ...string) []models.Client {
	if len(clientUUIDs) == 0 {
		return nil
	}

	clients := make([]models.Client, 0, len(clientUUIDs))
	seen := make(map[string]struct{}, len(clientUUIDs))
	for _, clientUUID := range clientUUIDs {
		clientUUID = strings.TrimSpace(clientUUID)
		if clientUUID == "" {
			continue
		}
		if _, exists := seen[clientUUID]; exists {
			continue
		}
		seen[clientUUID] = struct{}{}

		client, err := failoverV2GetClientByUUIDForUser(clientUUID, userUUID)
		if err != nil {
			clients = append(clients, models.Client{
				UUID:   clientUUID,
				UserID: userUUID,
				Name:   clientUUID,
			})
			continue
		}
		if strings.TrimSpace(client.Name) == "" {
			client.Name = clientUUID
		}
		clients = append(clients, client)
	}
	return clients
}

func notificationClientUUIDs(member *models.FailoverV2Member, execution *models.FailoverV2Execution) []string {
	values := make([]string, 0, 2)
	if member != nil {
		values = append(values, strings.TrimSpace(member.WatchClientUUID))
	}
	if execution != nil {
		values = append(values, strings.TrimSpace(execution.NewClientUUID))
	}
	return values
}

func notificationUserID(values ...interface{}) string {
	for _, value := range values {
		switch typed := value.(type) {
		case *models.FailoverV2Service:
			if typed != nil && strings.TrimSpace(typed.UserID) != "" {
				return strings.TrimSpace(typed.UserID)
			}
		case *models.FailoverV2PendingCleanup:
			if typed != nil && strings.TrimSpace(typed.UserID) != "" {
				return strings.TrimSpace(typed.UserID)
			}
		}
	}
	return ""
}

func formatNotificationServiceLine(service *models.FailoverV2Service) string {
	if service == nil {
		return "Service: unknown"
	}
	label := firstNonEmpty(strings.TrimSpace(service.Name), fmt.Sprintf("#%d", service.ID))
	if service.ID > 0 {
		return fmt.Sprintf("Service: %s (#%d)", label, service.ID)
	}
	return "Service: " + label
}

func formatNotificationMemberLine(member *models.FailoverV2Member) string {
	if member == nil {
		return "Member: unknown"
	}
	label := memberDisplayLabel(member)
	if member.ID > 0 {
		return fmt.Sprintf("Member: %s (#%d)", label, member.ID)
	}
	return "Member: " + label
}

func formatPendingCleanupNotificationLabel(item *models.FailoverV2PendingCleanup) string {
	if item == nil {
		return "unknown"
	}
	if label := strings.TrimSpace(item.CleanupLabel); label != "" {
		return label
	}
	parts := []string{
		strings.TrimSpace(item.Provider),
		strings.TrimSpace(item.ResourceType),
		strings.TrimSpace(item.ResourceID),
	}
	return firstNonEmpty(strings.Join(filterNotificationLines(parts), " / "), fmt.Sprintf("#%d", item.ID))
}

func filterNotificationLines(lines []string) []string {
	filtered := make([]string, 0, len(lines))
	for _, line := range lines {
		if trimmed := strings.TrimSpace(line); trimmed != "" {
			filtered = append(filtered, trimmed)
		}
	}
	return filtered
}

func notifyPendingCleanupManualReviewByID(userUUID string, serviceID, cleanupID uint, reason string) {
	if strings.TrimSpace(userUUID) == "" || serviceID == 0 || cleanupID == 0 {
		return
	}

	service, err := failoverv2db.GetServiceByIDForUser(userUUID, serviceID)
	if err != nil {
		log.Printf("failoverv2: failed to load service %d for pending cleanup notification: %v", serviceID, err)
		return
	}
	item, err := failoverv2db.GetPendingCleanupByIDForUser(userUUID, serviceID, cleanupID)
	if err != nil {
		log.Printf("failoverv2: failed to load pending cleanup %d for notification: %v", cleanupID, err)
		return
	}
	var member *models.FailoverV2Member
	if item.MemberID > 0 {
		member, _ = findMemberOnService(service, item.MemberID)
	}
	notifyPendingCleanupManualReview(item, service, member, reason)
}
