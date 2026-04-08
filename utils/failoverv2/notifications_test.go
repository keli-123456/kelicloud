package failoverv2

import (
	"strings"
	"testing"

	"github.com/komari-monitor/komari/database/models"
)

func captureFailoverV2Notifications(t *testing.T) *[]models.EventMessage {
	t.Helper()

	events := make([]models.EventMessage, 0, 4)
	previousSend := failoverV2SendEventFunc
	previousGetClient := failoverV2GetClientByUUIDForUser
	failoverV2SendEventFunc = func(event models.EventMessage) error {
		events = append(events, event)
		return nil
	}
	failoverV2GetClientByUUIDForUser = func(clientUUID, userUUID string) (models.Client, error) {
		return models.Client{
			UUID:   clientUUID,
			UserID: userUUID,
			Name:   clientUUID,
		}, nil
	}
	t.Cleanup(func() {
		failoverV2SendEventFunc = previousSend
		failoverV2GetClientByUUIDForUser = previousGetClient
	})
	return &events
}

func requireFailoverV2Notification(t *testing.T, events []models.EventMessage, eventName, messageContains string) {
	t.Helper()

	for _, event := range events {
		if strings.TrimSpace(event.Event) != strings.TrimSpace(eventName) {
			continue
		}
		if messageContains == "" || strings.Contains(event.Message, messageContains) {
			return
		}
	}

	t.Fatalf("expected notification %q containing %q, got %#v", eventName, messageContains, events)
}
