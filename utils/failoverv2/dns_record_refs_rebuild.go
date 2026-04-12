package failoverv2

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"strings"

	failoverv2db "github.com/komari-monitor/komari/database/failoverv2"
	"github.com/komari-monitor/komari/database/models"
)

func ShouldRebuildServiceMemberDNSRecordRefs(before, after *models.FailoverV2Service) bool {
	if before == nil || after == nil {
		return false
	}
	if strings.ToLower(strings.TrimSpace(before.DNSProvider)) != strings.ToLower(strings.TrimSpace(after.DNSProvider)) {
		return true
	}
	if strings.TrimSpace(before.DNSEntryID) != strings.TrimSpace(after.DNSEntryID) {
		return true
	}
	return canonicalServiceDNSPayload(before.DNSPayload) != canonicalServiceDNSPayload(after.DNSPayload)
}

func RebuildServiceMemberDNSRecordRefs(userUUID string, serviceID uint) error {
	service, err := failoverv2db.GetServiceByIDForUser(userUUID, serviceID)
	if err != nil {
		return err
	}
	return rebuildServiceMemberDNSRecordRefsForService(userUUID, service)
}

func rebuildServiceMemberDNSRecordRefsForService(userUUID string, service *models.FailoverV2Service) error {
	if service == nil {
		return errors.New("service is required")
	}
	if len(service.Members) == 0 {
		return nil
	}

	if err := clearServiceMemberDNSRecordRefs(userUUID, service); err != nil {
		return err
	}

	switch strings.ToLower(strings.TrimSpace(service.DNSProvider)) {
	case models.FailoverDNSProviderAliyun:
		if err := rebuildAliyunServiceMemberDNSRecordRefs(userUUID, service); err != nil {
			log.Printf("failoverv2: dns refs rebuild skipped for service %d (aliyun): %v", service.ID, err)
		}
	case models.FailoverDNSProviderCloudflare:
		if err := rebuildCloudflareServiceMemberDNSRecordRefs(userUUID, service); err != nil {
			log.Printf("failoverv2: dns refs rebuild skipped for service %d (cloudflare): %v", service.ID, err)
		}
	}
	return nil
}

func clearServiceMemberDNSRecordRefs(userUUID string, service *models.FailoverV2Service) error {
	for index := range service.Members {
		member := &service.Members[index]
		if err := failoverv2db.UpdateMemberFieldsAndLineRecordRefsForUser(
			userUUID,
			service.ID,
			member.ID,
			nil,
			buildEmptyMemberLineRecordRefs(member),
		); err != nil {
			return err
		}
	}
	return nil
}

func rebuildAliyunServiceMemberDNSRecordRefs(userUUID string, service *models.FailoverV2Service) error {
	configValue, err := loadAliyunDNSConfigFunc(userUUID, service.DNSEntryID)
	if err != nil {
		return err
	}
	if configValue == nil {
		return errors.New("aliyun dns config is required")
	}

	payload, err := parseAliyunMemberDNSPayload(service.DNSPayload)
	if err != nil {
		return err
	}
	domainName := strings.TrimSpace(payload.DomainName)
	if domainName == "" {
		domainName = strings.TrimSpace(configValue.DomainName)
	}
	if domainName == "" {
		return errors.New("aliyun domain_name is required")
	}
	rr, err := validateAliyunRR(domainName, payload.RR)
	if err != nil {
		return err
	}

	client := newAliyunDNSClientFunc(configValue)
	if client == nil {
		return errors.New("aliyun dns client is not configured")
	}
	existingRecords, err := client.listRecords(contextOrBackground(context.Background()), domainName)
	if err != nil {
		return err
	}

	managedTypes := managedAliyunRecordTypes(payload.RecordType)
	for index := range service.Members {
		member := &service.Members[index]
		lineRefs := buildAliyunMemberLineRecordRefs(member, existingRecords, rr, managedTypes)
		if err := failoverv2db.UpdateMemberFieldsAndLineRecordRefsForUser(userUUID, service.ID, member.ID, nil, lineRefs); err != nil {
			return err
		}
	}
	return nil
}

func buildAliyunMemberLineRecordRefs(member *models.FailoverV2Member, records []aliyunDNSRecord, rr string, managedTypes []string) map[string]map[string]string {
	lineRefs := buildEmptyMemberLineRecordRefs(member)
	currentAddress := normalizeIPAddress(member.CurrentAddress)
	if currentAddress == "" {
		return lineRefs
	}

	for _, line := range effectiveMemberLines(member) {
		rawLine := strings.TrimSpace(line.LineCode)
		if rawLine == "" {
			continue
		}
		canonicalLine := canonicalAliyunLineValue(rawLine)
		if canonicalLine == "" {
			continue
		}

		refs := map[string]string{}
		for _, managedType := range managedTypes {
			recordType := strings.ToUpper(strings.TrimSpace(managedType))
			if recordType == "" {
				continue
			}
			typedAddress, ok := normalizeMemberAddressForRecordType(recordType, currentAddress)
			if !ok {
				continue
			}
			record := findAliyunDNSRecordExactMatch(records, rr, recordType, canonicalLine, typedAddress)
			if recordID := strings.TrimSpace(record.RecordID); recordID != "" {
				refs[recordType] = recordID
			}
		}
		lineRefs[rawLine] = refs
	}

	return lineRefs
}

func rebuildCloudflareServiceMemberDNSRecordRefs(userUUID string, service *models.FailoverV2Service) error {
	configValue, err := loadCloudflareDNSConfigFunc(userUUID, service.DNSEntryID)
	if err != nil {
		return err
	}
	if configValue == nil {
		return errors.New("cloudflare dns config is required")
	}

	payload, err := parseCloudflareMemberDNSPayload(service.DNSPayload)
	if err != nil {
		return err
	}

	client := newCloudflareDNSClientFunc(configValue)
	if client == nil {
		return errors.New("cloudflare dns client is not configured")
	}

	zoneID := strings.TrimSpace(firstNonEmpty(payload.ZoneID, configValue.ZoneID))
	zoneName := normalizeServiceDNSDomainName(firstNonEmpty(payload.ZoneName, configValue.ZoneName))
	recordName := normalizeCloudflareRecordName(firstNonEmpty(strings.TrimSpace(payload.RecordName), zoneName), zoneName)
	if recordName == "" {
		return errors.New("cloudflare record_name is required")
	}
	if zoneID == "" {
		if zoneName == "" {
			return errors.New("cloudflare zone_id or zone_name is required")
		}
		resolvedZoneID, resolveErr := client.resolveZoneID(contextOrBackground(context.Background()), zoneName)
		if resolveErr != nil {
			return resolveErr
		}
		zoneID = resolvedZoneID
	}

	existingRecords, err := client.listRecords(contextOrBackground(context.Background()), zoneID)
	if err != nil {
		return err
	}
	managedTypes := managedAliyunRecordTypes(payload.RecordType)
	for index := range service.Members {
		member := &service.Members[index]
		lineRefs := buildCloudflareMemberLineRecordRefs(member, existingRecords, recordName, managedTypes)
		if err := failoverv2db.UpdateMemberFieldsAndLineRecordRefsForUser(userUUID, service.ID, member.ID, nil, lineRefs); err != nil {
			return err
		}
	}
	return nil
}

func buildCloudflareMemberLineRecordRefs(member *models.FailoverV2Member, records []cloudflareDNSRecord, recordName string, managedTypes []string) map[string]map[string]string {
	lineRefs := buildEmptyMemberLineRecordRefs(member)
	currentAddress := normalizeIPAddress(member.CurrentAddress)
	if currentAddress == "" {
		return lineRefs
	}

	refs := map[string]string{}
	for _, managedType := range managedTypes {
		recordType := strings.ToUpper(strings.TrimSpace(managedType))
		if recordType == "" {
			continue
		}
		typedAddress, ok := normalizeMemberAddressForRecordType(recordType, currentAddress)
		if !ok {
			continue
		}
		record := findCloudflareMatchingRecord(records, recordName, recordType, typedAddress)
		if recordID := strings.TrimSpace(record.ID); recordID != "" {
			refs[recordType] = recordID
		}
	}
	if len(refs) == 0 {
		return lineRefs
	}

	for lineCode := range lineRefs {
		lineRefs[lineCode] = cloneStringMap(refs)
	}
	return lineRefs
}

func buildEmptyMemberLineRecordRefs(member *models.FailoverV2Member) map[string]map[string]string {
	result := map[string]map[string]string{}
	for _, line := range effectiveMemberLines(member) {
		lineCode := strings.TrimSpace(line.LineCode)
		if lineCode == "" {
			continue
		}
		result[lineCode] = map[string]string{}
	}
	if len(result) == 0 && member != nil {
		if lineCode := strings.TrimSpace(member.DNSLine); lineCode != "" {
			result[lineCode] = map[string]string{}
		}
	}
	return result
}

func canonicalServiceDNSPayload(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || trimmed == "null" {
		return "{}"
	}
	var parsed interface{}
	if err := json.Unmarshal([]byte(trimmed), &parsed); err != nil {
		return trimmed
	}
	encoded, err := json.Marshal(parsed)
	if err != nil {
		return trimmed
	}
	return string(encoded)
}

func cloneStringMap(source map[string]string) map[string]string {
	if len(source) == 0 {
		return map[string]string{}
	}
	cloned := make(map[string]string, len(source))
	for key, value := range source {
		cloned[key] = value
	}
	return cloned
}
