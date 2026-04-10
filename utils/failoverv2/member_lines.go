package failoverv2

import (
	"strings"

	"github.com/komari-monitor/komari/database/models"
)

func memberModeValue(member *models.FailoverV2Member) string {
	if member == nil {
		return models.FailoverV2MemberModeProviderTemplate
	}
	switch strings.TrimSpace(strings.ToLower(member.Mode)) {
	case models.FailoverV2MemberModeExistingClient:
		return models.FailoverV2MemberModeExistingClient
	case models.FailoverV2MemberModeProviderTemplate:
		return models.FailoverV2MemberModeProviderTemplate
	default:
		return models.FailoverV2MemberModeProviderTemplate
	}
}

func memberUsesExistingClient(member *models.FailoverV2Member) bool {
	return memberModeValue(member) == models.FailoverV2MemberModeExistingClient
}

func memberUsesProviderTemplate(member *models.FailoverV2Member) bool {
	return memberModeValue(member) == models.FailoverV2MemberModeProviderTemplate
}

func effectiveMemberLines(member *models.FailoverV2Member) []models.FailoverV2MemberLine {
	if member == nil {
		return nil
	}
	if len(member.Lines) > 0 {
		lines := make([]models.FailoverV2MemberLine, 0, len(member.Lines))
		for _, line := range member.Lines {
			lineCode := strings.TrimSpace(line.LineCode)
			if lineCode == "" {
				continue
			}
			lines = append(lines, models.FailoverV2MemberLine{
				ID:            line.ID,
				ServiceID:     line.ServiceID,
				MemberID:      line.MemberID,
				LineCode:      lineCode,
				DNSRecordRefs: firstNonEmpty(strings.TrimSpace(line.DNSRecordRefs), "{}"),
				CreatedAt:     line.CreatedAt,
				UpdatedAt:     line.UpdatedAt,
			})
		}
		if len(lines) > 0 {
			return lines
		}
	}

	lineCode := strings.TrimSpace(member.DNSLine)
	if lineCode == "" {
		return nil
	}
	return []models.FailoverV2MemberLine{
		{
			ServiceID:     member.ServiceID,
			MemberID:      member.ID,
			LineCode:      lineCode,
			DNSRecordRefs: firstNonEmpty(strings.TrimSpace(member.DNSRecordRefs), "{}"),
		},
	}
}

func memberLineCodes(member *models.FailoverV2Member) []string {
	lines := effectiveMemberLines(member)
	if len(lines) == 0 {
		return nil
	}

	result := make([]string, 0, len(lines))
	for _, line := range lines {
		if lineCode := strings.TrimSpace(line.LineCode); lineCode != "" {
			result = append(result, lineCode)
		}
	}
	return result
}

func firstMemberLineCode(member *models.FailoverV2Member) string {
	lines := memberLineCodes(member)
	if len(lines) == 0 {
		return strings.TrimSpace(member.DNSLine)
	}
	return strings.TrimSpace(lines[0])
}

func memberHasAllLinesDetached(member *models.FailoverV2Member) bool {
	lines := effectiveMemberLines(member)
	if len(lines) == 0 {
		return false
	}

	for _, line := range lines {
		if len(decodeMemberDNSRecordRefs(line.DNSRecordRefs)) > 0 {
			return false
		}
	}
	return true
}

func cloneMemberForLine(member *models.FailoverV2Member, line models.FailoverV2MemberLine) models.FailoverV2Member {
	cloned := models.FailoverV2Member{}
	if member != nil {
		cloned = *member
	}
	cloned.DNSLine = strings.TrimSpace(line.LineCode)
	cloned.DNSRecordRefs = firstNonEmpty(strings.TrimSpace(line.DNSRecordRefs), "{}")
	cloned.Lines = nil
	return cloned
}
