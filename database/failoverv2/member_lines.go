package failoverv2

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/komari-monitor/komari/database/models"
	"gorm.io/gorm"
)

func normalizeFailoverV2MemberModeValue(mode string) string {
	switch strings.TrimSpace(strings.ToLower(mode)) {
	case models.FailoverV2MemberModeExistingClient:
		return models.FailoverV2MemberModeExistingClient
	case models.FailoverV2MemberModeProviderTemplate:
		return models.FailoverV2MemberModeProviderTemplate
	default:
		return models.FailoverV2MemberModeProviderTemplate
	}
}

func normalizeFailoverV2MemberLineCode(lineCode string) string {
	return strings.TrimSpace(lineCode)
}

func normalizeFailoverV2MemberLineRecordRefs(raw string) string {
	raw = strings.TrimSpace(raw)
	switch raw {
	case "", "null":
		return "{}"
	default:
		return raw
	}
}

func cloneFailoverV2MemberLines(lines []models.FailoverV2MemberLine) []models.FailoverV2MemberLine {
	if len(lines) == 0 {
		return nil
	}

	cloned := make([]models.FailoverV2MemberLine, 0, len(lines))
	for _, line := range lines {
		cloned = append(cloned, models.FailoverV2MemberLine{
			ID:            line.ID,
			ServiceID:     line.ServiceID,
			MemberID:      line.MemberID,
			LineCode:      line.LineCode,
			DNSRecordRefs: line.DNSRecordRefs,
			CreatedAt:     line.CreatedAt,
			UpdatedAt:     line.UpdatedAt,
		})
	}
	return cloned
}

func effectiveFailoverV2MemberLines(member *models.FailoverV2Member) []models.FailoverV2MemberLine {
	if member == nil {
		return nil
	}

	if len(member.Lines) > 0 {
		return cloneFailoverV2MemberLines(member.Lines)
	}

	legacyLine := normalizeFailoverV2MemberLineCode(member.DNSLine)
	if legacyLine == "" {
		return nil
	}

	return []models.FailoverV2MemberLine{
		{
			ServiceID:     member.ServiceID,
			MemberID:      member.ID,
			LineCode:      legacyLine,
			DNSRecordRefs: normalizeFailoverV2MemberLineRecordRefs(member.DNSRecordRefs),
		},
	}
}

func normalizeFailoverV2MemberLines(
	serviceID uint,
	memberID uint,
	legacyLine string,
	legacyRecordRefs string,
	lines []models.FailoverV2MemberLine,
) []models.FailoverV2MemberLine {
	sourceLines := lines
	if len(sourceLines) == 0 {
		legacyLine = normalizeFailoverV2MemberLineCode(legacyLine)
		if legacyLine != "" {
			sourceLines = []models.FailoverV2MemberLine{
				{
					ServiceID:     serviceID,
					MemberID:      memberID,
					LineCode:      legacyLine,
					DNSRecordRefs: normalizeFailoverV2MemberLineRecordRefs(legacyRecordRefs),
				},
			}
		}
	}

	if len(sourceLines) == 0 {
		return nil
	}

	normalized := make([]models.FailoverV2MemberLine, 0, len(sourceLines))
	seen := make(map[string]struct{}, len(sourceLines))
	for _, line := range sourceLines {
		lineCode := normalizeFailoverV2MemberLineCode(line.LineCode)
		if lineCode == "" {
			continue
		}
		if _, exists := seen[lineCode]; exists {
			continue
		}
		seen[lineCode] = struct{}{}
		normalized = append(normalized, models.FailoverV2MemberLine{
			ID:            line.ID,
			ServiceID:     serviceID,
			MemberID:      memberID,
			LineCode:      lineCode,
			DNSRecordRefs: normalizeFailoverV2MemberLineRecordRefs(line.DNSRecordRefs),
			CreatedAt:     line.CreatedAt,
			UpdatedAt:     line.UpdatedAt,
		})
	}

	sort.SliceStable(normalized, func(i, j int) bool {
		return normalized[i].LineCode < normalized[j].LineCode
	})
	return normalized
}

func syncFailoverV2MemberLegacyLineFields(member *models.FailoverV2Member) {
	if member == nil {
		return
	}

	lines := effectiveFailoverV2MemberLines(member)
	if len(lines) == 0 {
		member.DNSLine = normalizeFailoverV2MemberLineCode(member.DNSLine)
		member.DNSRecordRefs = normalizeFailoverV2MemberLineRecordRefs(member.DNSRecordRefs)
		return
	}

	member.Lines = normalizeFailoverV2MemberLines(member.ServiceID, member.ID, "", "", lines)
	member.DNSLine = member.Lines[0].LineCode
	member.DNSRecordRefs = member.Lines[0].DNSRecordRefs
}

func replaceFailoverV2MemberLinesWithDB(
	tx *gorm.DB,
	serviceID uint,
	memberID uint,
	inputLines []models.FailoverV2MemberLine,
	existingLines []models.FailoverV2MemberLine,
) error {
	if tx == nil {
		return fmt.Errorf("database transaction is required")
	}
	if serviceID == 0 || memberID == 0 {
		return fmt.Errorf("service id and member id are required")
	}

	lines := normalizeFailoverV2MemberLines(serviceID, memberID, "", "", inputLines)
	existingByCode := make(map[string]models.FailoverV2MemberLine, len(existingLines))
	for _, line := range existingLines {
		lineCode := normalizeFailoverV2MemberLineCode(line.LineCode)
		if lineCode == "" {
			continue
		}
		line.LineCode = lineCode
		line.DNSRecordRefs = normalizeFailoverV2MemberLineRecordRefs(line.DNSRecordRefs)
		existingByCode[lineCode] = line
	}

	for index := range lines {
		line := &lines[index]
		if existing, ok := existingByCode[line.LineCode]; ok && (strings.TrimSpace(line.DNSRecordRefs) == "" || strings.TrimSpace(line.DNSRecordRefs) == "{}") {
			line.DNSRecordRefs = existing.DNSRecordRefs
		}
		line.ServiceID = serviceID
		line.MemberID = memberID
		line.DNSRecordRefs = normalizeFailoverV2MemberLineRecordRefs(line.DNSRecordRefs)
	}

	if err := tx.Where("service_id = ? AND member_id = ?", serviceID, memberID).
		Delete(&models.FailoverV2MemberLine{}).Error; err != nil {
		return err
	}

	if len(lines) == 0 {
		return nil
	}
	return tx.Create(&lines).Error
}

func encodeMemberLineRecordRefsJSON(recordRefs map[string]string) string {
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
