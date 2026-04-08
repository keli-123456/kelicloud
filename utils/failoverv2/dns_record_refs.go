package failoverv2

import (
	"encoding/json"
	"strconv"
	"strings"
)

func decodeMemberDNSRecordRefs(raw string) map[string]string {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "null" {
		return map[string]string{}
	}

	var payload map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return map[string]string{}
	}

	normalized := make(map[string]string, len(payload))
	for key, value := range payload {
		recordType := strings.ToUpper(strings.TrimSpace(key))
		if recordType == "" {
			continue
		}
		switch typedValue := value.(type) {
		case string:
			if trimmed := strings.TrimSpace(typedValue); trimmed != "" {
				normalized[recordType] = trimmed
			}
		case float64:
			normalized[recordType] = strconv.FormatInt(int64(typedValue), 10)
		}
	}
	return normalized
}

func encodeMemberDNSRecordRefs(recordRefs map[string]string) string {
	if len(recordRefs) == 0 {
		return "{}"
	}

	normalized := make(map[string]string, len(recordRefs))
	for recordType, recordID := range recordRefs {
		recordType = strings.ToUpper(strings.TrimSpace(recordType))
		recordID = strings.TrimSpace(recordID)
		if recordType == "" || recordID == "" {
			continue
		}
		normalized[recordType] = recordID
	}
	if len(normalized) == 0 {
		return "{}"
	}

	payload, err := json.Marshal(normalized)
	if err != nil {
		return "{}"
	}
	return string(payload)
}
