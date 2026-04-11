package failoverv2

import (
	"strings"

	"github.com/komari-monitor/komari/database/models"
)

func detachRecordBelongsToAnotherMember(
	service *models.FailoverV2Service,
	member *models.FailoverV2Member,
	recordType string,
	recordValue string,
	includeMember func(*models.FailoverV2Member) bool,
) bool {
	if service == nil || member == nil {
		return false
	}

	recordType = strings.ToUpper(strings.TrimSpace(recordType))
	switch recordType {
	case "A", "AAAA":
	default:
		return false
	}

	normalizedValue, err := normalizeDNSRecordIPValue("record_value", recordType, recordValue, recordType == "A")
	if err != nil || normalizedValue == "" {
		return false
	}

	for index := range service.Members {
		candidate := &service.Members[index]
		if member.ID != 0 && candidate.ID == member.ID {
			continue
		}
		if !candidate.Enabled {
			continue
		}
		if includeMember != nil && !includeMember(candidate) {
			continue
		}

		candidateAddress, ok := normalizeMemberAddressForRecordType(recordType, candidate.CurrentAddress)
		if !ok || candidateAddress == "" {
			continue
		}
		if !sameAddress(candidateAddress, normalizedValue) {
			continue
		}
		return true
	}

	return false
}
