package failoverv2

import (
	"sort"
	"strings"

	"github.com/komari-monitor/komari/database/models"
)

func buildServiceMemberExpectedRecordValues(
	service *models.FailoverV2Service,
	targetMember *models.FailoverV2Member,
	recordType string,
	targetIPv4 string,
	targetIPv6 string,
	includeMember func(*models.FailoverV2Member) bool,
) ([]string, error) {
	recordType = strings.ToUpper(strings.TrimSpace(recordType))
	targetValue := ""
	if targetMember != nil {
		value, err := selectAliyunRecordValue(recordType, targetIPv4, targetIPv6)
		if err != nil {
			return nil, err
		}
		targetValue = value
	}

	expectedSet := map[string]struct{}{}
	if targetValue != "" {
		expectedSet[targetValue] = struct{}{}
	}

	targetMemberID := uint(0)
	if targetMember != nil {
		targetMemberID = targetMember.ID
	}
	targetMatched := false

	if service != nil {
		for index := range service.Members {
			member := &service.Members[index]
			isTarget := targetMemberID != 0 && member.ID == targetMemberID
			if includeMember != nil && !includeMember(member) && !isTarget {
				continue
			}
			if !member.Enabled && !isTarget {
				continue
			}

			value := ""
			if isTarget {
				targetMatched = true
				value = targetValue
			} else {
				normalized, ok := normalizeMemberAddressForRecordType(recordType, member.CurrentAddress)
				if !ok {
					continue
				}
				value = normalized
			}
			if value != "" {
				expectedSet[value] = struct{}{}
			}
		}
	}

	if targetMember != nil && !targetMatched && targetValue != "" {
		expectedSet[targetValue] = struct{}{}
	}

	expected := make([]string, 0, len(expectedSet))
	for value := range expectedSet {
		expected = append(expected, value)
	}
	sort.Strings(expected)
	return expected, nil
}

func normalizeMemberAddressForRecordType(recordType, currentAddress string) (string, bool) {
	recordType = strings.ToUpper(strings.TrimSpace(recordType))
	switch recordType {
	case "A":
		value, err := normalizeDNSRecordIPValue("current_address", "A", currentAddress, true)
		return value, err == nil
	case "AAAA":
		value, err := normalizeDNSRecordIPValue("current_address", "AAAA", currentAddress, false)
		return value, err == nil
	default:
		return "", false
	}
}

func memberHasAliyunLine(member *models.FailoverV2Member, line string) bool {
	if member == nil {
		return false
	}
	line = canonicalAliyunLineValue(line)
	if strings.TrimSpace(line) == "" {
		return true
	}
	for _, memberLine := range effectiveMemberLines(member) {
		if sameAliyunRecordLine(memberLine.LineCode, line) {
			return true
		}
	}
	return false
}
