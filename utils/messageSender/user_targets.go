package messageSender

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/komari-monitor/komari/config"
	"github.com/komari-monitor/komari/database"
	"github.com/komari-monitor/komari/database/models"
	"github.com/komari-monitor/komari/utils/messageSender/factory"
)

var ErrUserNotificationTargetNotConfigured = errors.New("notification target is not configured for this user")

type providerUserTargetField struct {
	ConfigKey   string
	AdditionKey string
	Required    bool
}

var providerUserTargetFields = map[string][]providerUserTargetField{
	"telegram": {
		{ConfigKey: config.NotificationTelegramChatIDKey, AdditionKey: "chat_id", Required: true},
		{ConfigKey: config.NotificationTelegramMessageThreadIDKey, AdditionKey: "message_thread_id"},
	},
	"bark": {
		{ConfigKey: config.NotificationBarkDeviceKeyKey, AdditionKey: "device_key", Required: true},
	},
	"webhook": {
		{ConfigKey: config.NotificationWebhookURLKey, AdditionKey: "url", Required: true},
	},
}

func resolveEventUserUUID(event models.EventMessage) string {
	if userUUID := strings.TrimSpace(event.UserID); userUUID != "" {
		return userUUID
	}

	candidate := ""
	for _, client := range event.Clients {
		userUUID := strings.TrimSpace(client.UserID)
		if userUUID == "" {
			continue
		}
		if candidate == "" {
			candidate = userUUID
			continue
		}
		if candidate != userUUID {
			return ""
		}
	}

	return candidate
}

func resolveProviderForUser(userUUID string) (factory.IMessageSender, func(), error) {
	provider := CurrentProvider()
	cleanup := func() {}

	userUUID = strings.TrimSpace(userUUID)
	if userUUID == "" {
		if provider == nil {
			return nil, cleanup, fmt.Errorf("message sender provider is not initialized")
		}
		return provider, cleanup, nil
	}

	providerName, _ := config.GetAs[string](config.NotificationMethodKey, "none")
	providerName = strings.TrimSpace(providerName)
	fields, ok := providerUserTargetFields[providerName]
	if !ok {
		if provider == nil {
			return nil, cleanup, fmt.Errorf("message sender provider is not initialized")
		}
		return provider, cleanup, nil
	}

	mergedAddition, err := buildProviderAdditionForUser(providerName, userUUID, fields)
	if err != nil {
		return nil, cleanup, err
	}

	userProvider, err := buildProvider(providerName, mergedAddition)
	if err != nil {
		return nil, cleanup, err
	}

	return userProvider, func() {
		_ = userProvider.Destroy()
	}, nil
}

func buildProviderAdditionForUser(providerName, userUUID string, fields []providerUserTargetField) (string, error) {
	senderConfig, err := database.GetMessageSenderConfigByName(providerName)
	if err != nil {
		return "", err
	}

	addition := map[string]any{}
	trimmedAddition := strings.TrimSpace(senderConfig.Addition)
	if trimmedAddition != "" && trimmedAddition != "{}" && trimmedAddition != "null" {
		if err := json.Unmarshal([]byte(trimmedAddition), &addition); err != nil {
			return "", err
		}
	}

	defaults := make(map[string]any, len(fields))
	for _, field := range fields {
		defaults[field.ConfigKey] = nil
	}
	userValues, err := config.GetManyForUser(userUUID, defaults)
	if err != nil {
		return "", err
	}

	missingRequired := make([]string, 0)
	for _, field := range fields {
		rawValue, exists := userValues[field.ConfigKey]
		value := strings.TrimSpace(fmt.Sprint(rawValue))
		if !exists || value == "" {
			if field.Required {
				missingRequired = append(missingRequired, field.ConfigKey)
			}
			continue
		}
		addition[field.AdditionKey] = value
	}

	if len(missingRequired) > 0 {
		return "", fmt.Errorf("%w: %s", ErrUserNotificationTargetNotConfigured, strings.Join(missingRequired, ", "))
	}

	payload, err := json.Marshal(addition)
	if err != nil {
		return "", err
	}
	return string(payload), nil
}
