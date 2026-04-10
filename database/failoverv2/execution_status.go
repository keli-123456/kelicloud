package failoverv2

import (
	"strings"

	"github.com/komari-monitor/komari/database/models"
)

var terminalFailoverV2ExecutionStatuses = []string{
	models.FailoverV2ExecutionStatusSuccess,
	models.FailoverV2ExecutionStatusFailed,
}

func statusInList(status string, candidates []string) bool {
	normalized := strings.TrimSpace(strings.ToLower(status))
	if normalized == "" {
		return false
	}
	for _, candidate := range candidates {
		if normalized == strings.TrimSpace(strings.ToLower(candidate)) {
			return true
		}
	}
	return false
}

func IsFailoverV2ExecutionTerminal(status string) bool {
	return statusInList(status, terminalFailoverV2ExecutionStatuses)
}

func IsFailoverV2ExecutionActive(status string, finishedAt *models.LocalTime) bool {
	if finishedAt != nil {
		return false
	}
	return !IsFailoverV2ExecutionTerminal(status)
}
