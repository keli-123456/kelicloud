package failoverv2

import (
	"fmt"
	"strings"

	"github.com/komari-monitor/komari/database/models"
)

func ensureDetachRecordDoesNotBelongToAnotherMember(
	service *models.FailoverV2Service,
	member *models.FailoverV2Member,
	recordType string,
	recordValue string,
	includeMember func(*models.FailoverV2Member) bool,
) error {
	if service == nil || member == nil {
		return nil
	}

	recordType = strings.ToUpper(strings.TrimSpace(recordType))
	switch recordType {
	case "A", "AAAA":
	default:
		return nil
	}

	normalizedValue, err := normalizeDNSRecordIPValue("record_value", recordType, recordValue, recordType == "A")
	if err != nil || normalizedValue == "" {
		return nil
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

		label := strings.TrimSpace(candidate.Name)
		if label == "" {
			label = fmt.Sprintf("id:%d", candidate.ID)
		}
		return fmt.Errorf("dns %s record appears to belong to another member (%s)", recordType, label)
	}

	return nil
}
