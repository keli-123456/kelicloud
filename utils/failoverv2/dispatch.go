package failoverv2

import (
	"context"
	"fmt"
	"strings"

	"github.com/komari-monitor/komari/database/models"
)

type memberDNSResultLine struct {
	Line       string            `json:"line"`
	Result     interface{}       `json:"result,omitempty"`
	RecordRefs map[string]string `json:"record_refs,omitempty"`
}

type memberDNSVerificationLine struct {
	Line         string      `json:"line"`
	Success      bool        `json:"success"`
	Verification interface{} `json:"verification,omitempty"`
}

type multiLineMemberDNSResult struct {
	Provider string                `json:"provider"`
	Lines    []memberDNSResultLine `json:"lines,omitempty"`
}

type multiLineMemberDNSVerification struct {
	Provider string                      `json:"provider"`
	Success  bool                        `json:"success"`
	Lines    []memberDNSVerificationLine `json:"lines,omitempty"`
}

func applyMemberDNSDetach(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member) (interface{}, error) {
	if service == nil {
		return nil, fmt.Errorf("service is required")
	}
	lines := effectiveMemberLines(member)
	if len(lines) == 0 {
		return nil, fmt.Errorf("member has no dns lines configured")
	}

	result := &multiLineMemberDNSResult{
		Provider: strings.ToLower(strings.TrimSpace(service.DNSProvider)),
		Lines:    make([]memberDNSResultLine, 0, len(lines)),
	}
	for _, line := range lines {
		lineMember := cloneMemberForLine(member, line)
		lineResult, err := applyMemberDNSDetachSingleLine(ctx, userUUID, service, &lineMember)
		if err != nil {
			return nil, err
		}
		result.Lines = append(result.Lines, memberDNSResultLine{
			Line:       strings.TrimSpace(line.LineCode),
			Result:     lineResult,
			RecordRefs: extractMemberDNSRecordRefs(lineResult),
		})
	}
	return result, nil
}

func verifyMemberDNSDetached(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member) (interface{}, error) {
	if service == nil {
		return nil, fmt.Errorf("service is required")
	}
	lines := effectiveMemberLines(member)
	if len(lines) == 0 {
		return nil, fmt.Errorf("member has no dns lines configured")
	}

	verification := &multiLineMemberDNSVerification{
		Provider: strings.ToLower(strings.TrimSpace(service.DNSProvider)),
		Success:  true,
		Lines:    make([]memberDNSVerificationLine, 0, len(lines)),
	}
	for _, line := range lines {
		lineMember := cloneMemberForLine(member, line)
		lineVerification, err := verifyMemberDNSDetachedSingleLine(ctx, userUUID, service, &lineMember)
		if err != nil {
			return nil, err
		}
		lineSuccess := dnsVerificationSucceeded(lineVerification)
		if !lineSuccess {
			verification.Success = false
		}
		verification.Lines = append(verification.Lines, memberDNSVerificationLine{
			Line:         strings.TrimSpace(line.LineCode),
			Success:      lineSuccess,
			Verification: lineVerification,
		})
	}
	return verification, nil
}

func applyMemberDNSAttach(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member, ipv4, ipv6 string) (interface{}, error) {
	if service == nil {
		return nil, fmt.Errorf("service is required")
	}
	lines := effectiveMemberLines(member)
	if len(lines) == 0 {
		return nil, fmt.Errorf("member has no dns lines configured")
	}

	result := &multiLineMemberDNSResult{
		Provider: strings.ToLower(strings.TrimSpace(service.DNSProvider)),
		Lines:    make([]memberDNSResultLine, 0, len(lines)),
	}
	for _, line := range lines {
		lineMember := cloneMemberForLine(member, line)
		lineResult, err := applyMemberDNSAttachSingleLine(ctx, userUUID, service, &lineMember, ipv4, ipv6)
		if err != nil {
			return nil, err
		}
		result.Lines = append(result.Lines, memberDNSResultLine{
			Line:       strings.TrimSpace(line.LineCode),
			Result:     lineResult,
			RecordRefs: extractMemberDNSRecordRefs(lineResult),
		})
	}
	return result, nil
}

func verifyMemberDNSAttached(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member, ipv4, ipv6 string) (interface{}, error) {
	if service == nil {
		return nil, fmt.Errorf("service is required")
	}
	lines := effectiveMemberLines(member)
	if len(lines) == 0 {
		return nil, fmt.Errorf("member has no dns lines configured")
	}

	verification := &multiLineMemberDNSVerification{
		Provider: strings.ToLower(strings.TrimSpace(service.DNSProvider)),
		Success:  true,
		Lines:    make([]memberDNSVerificationLine, 0, len(lines)),
	}
	for _, line := range lines {
		lineMember := cloneMemberForLine(member, line)
		lineVerification, err := verifyMemberDNSAttachedSingleLine(ctx, userUUID, service, &lineMember, ipv4, ipv6)
		if err != nil {
			return nil, err
		}
		lineSuccess := dnsVerificationSucceeded(lineVerification)
		if !lineSuccess {
			verification.Success = false
		}
		verification.Lines = append(verification.Lines, memberDNSVerificationLine{
			Line:         strings.TrimSpace(line.LineCode),
			Success:      lineSuccess,
			Verification: lineVerification,
		})
	}
	return verification, nil
}

func applyMemberDNSDetachSingleLine(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member) (interface{}, error) {
	dnsLock, err := claimDNSProviderEntryRunLock(ctx, userUUID, service)
	if err != nil {
		return nil, err
	}
	if dnsLock != nil {
		defer dnsLock.release()
	}

	switch strings.ToLower(strings.TrimSpace(service.DNSProvider)) {
	case models.FailoverDNSProviderAliyun:
		return ApplyAliyunMemberDNSDetach(ctx, userUUID, service, member)
	case models.FailoverDNSProviderCloudflare:
		return ApplyCloudflareMemberDNSDetach(ctx, userUUID, service, member)
	default:
		return nil, fmt.Errorf("unsupported failover v2 dns provider: %s", service.DNSProvider)
	}
}

func verifyMemberDNSDetachedSingleLine(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member) (interface{}, error) {
	dnsLock, err := claimDNSProviderEntryRunLock(ctx, userUUID, service)
	if err != nil {
		return nil, err
	}
	if dnsLock != nil {
		defer dnsLock.release()
	}

	switch strings.ToLower(strings.TrimSpace(service.DNSProvider)) {
	case models.FailoverDNSProviderAliyun:
		return VerifyAliyunMemberDNSDetached(ctx, userUUID, service, member)
	case models.FailoverDNSProviderCloudflare:
		return VerifyCloudflareMemberDNSDetached(ctx, userUUID, service, member)
	default:
		return nil, fmt.Errorf("unsupported failover v2 dns provider: %s", service.DNSProvider)
	}
}

func applyMemberDNSAttachSingleLine(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member, ipv4, ipv6 string) (interface{}, error) {
	dnsLock, err := claimDNSProviderEntryRunLock(ctx, userUUID, service)
	if err != nil {
		return nil, err
	}
	if dnsLock != nil {
		defer dnsLock.release()
	}

	switch strings.ToLower(strings.TrimSpace(service.DNSProvider)) {
	case models.FailoverDNSProviderAliyun:
		return ApplyAliyunMemberDNSAttach(ctx, userUUID, service, member, ipv4, ipv6)
	case models.FailoverDNSProviderCloudflare:
		return ApplyCloudflareMemberDNSAttach(ctx, userUUID, service, member, ipv4, ipv6)
	default:
		return nil, fmt.Errorf("unsupported failover v2 dns provider: %s", service.DNSProvider)
	}
}

func verifyMemberDNSAttachedSingleLine(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member, ipv4, ipv6 string) (interface{}, error) {
	dnsLock, err := claimDNSProviderEntryRunLock(ctx, userUUID, service)
	if err != nil {
		return nil, err
	}
	if dnsLock != nil {
		defer dnsLock.release()
	}

	switch strings.ToLower(strings.TrimSpace(service.DNSProvider)) {
	case models.FailoverDNSProviderAliyun:
		return VerifyAliyunMemberDNSAttached(ctx, userUUID, service, member, ipv4, ipv6)
	case models.FailoverDNSProviderCloudflare:
		return VerifyCloudflareMemberDNSAttached(ctx, userUUID, service, member, ipv4, ipv6)
	default:
		return nil, fmt.Errorf("unsupported failover v2 dns provider: %s", service.DNSProvider)
	}
}

func provisionMember(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member) (*memberProvisionOutcome, error) {
	if member == nil {
		return nil, fmt.Errorf("member is required")
	}
	switch strings.ToLower(strings.TrimSpace(member.Provider)) {
	case "digitalocean":
		return provisionDigitalOceanMember(ctx, userUUID, service, member)
	case "linode":
		return provisionLinodeMember(ctx, userUUID, service, member)
	case "vultr":
		return provisionVultrMember(ctx, userUUID, service, member)
	case "aws":
		return provisionAWSMember(ctx, userUUID, service, member)
	case "azure":
		return provisionAzureMember(ctx, userUUID, service, member)
	default:
		return nil, fmt.Errorf("unsupported V2 member provider: %s", member.Provider)
	}
}

func dnsVerificationSucceeded(verification interface{}) bool {
	switch typed := verification.(type) {
	case *AliyunMemberDNSVerification:
		return typed != nil && typed.Success
	case *CloudflareMemberDNSVerification:
		return typed != nil && typed.Success
	case *multiLineMemberDNSVerification:
		return typed != nil && typed.Success
	default:
		return false
	}
}

func extractMemberDNSRecordRefs(result interface{}) map[string]string {
	switch typed := result.(type) {
	case *multiLineMemberDNSResult:
		if typed == nil || len(typed.Lines) == 0 {
			return map[string]string{}
		}
		return cloneRecordRefs(typed.Lines[0].RecordRefs)
	case *AliyunMemberDNSResult:
		if typed == nil {
			return map[string]string{}
		}
		return cloneRecordRefs(typed.RecordRefs)
	case *CloudflareMemberDNSResult:
		if typed == nil {
			return map[string]string{}
		}
		return cloneRecordRefs(typed.RecordRefs)
	default:
		return map[string]string{}
	}
}

func extractMemberLineRecordRefs(result interface{}) map[string]map[string]string {
	switch typed := result.(type) {
	case *multiLineMemberDNSResult:
		if typed == nil {
			return map[string]map[string]string{}
		}
		refsByLine := make(map[string]map[string]string, len(typed.Lines))
		for _, line := range typed.Lines {
			lineCode := strings.TrimSpace(line.Line)
			if lineCode == "" {
				continue
			}
			refsByLine[lineCode] = cloneRecordRefs(line.RecordRefs)
		}
		return refsByLine
	case *AliyunMemberDNSResult:
		if typed == nil {
			return map[string]map[string]string{}
		}
		return map[string]map[string]string{
			strings.TrimSpace(typed.Line): cloneRecordRefs(typed.RecordRefs),
		}
	case *CloudflareMemberDNSResult:
		if typed == nil {
			return map[string]map[string]string{}
		}
		return map[string]map[string]string{
			strings.TrimSpace(typed.Line): cloneRecordRefs(typed.RecordRefs),
		}
	default:
		return map[string]map[string]string{}
	}
}

func cloneRecordRefs(recordRefs map[string]string) map[string]string {
	if len(recordRefs) == 0 {
		return map[string]string{}
	}
	cloned := make(map[string]string, len(recordRefs))
	for recordType, recordID := range recordRefs {
		recordType = strings.ToUpper(strings.TrimSpace(recordType))
		recordID = strings.TrimSpace(recordID)
		if recordType == "" || recordID == "" {
			continue
		}
		cloned[recordType] = recordID
	}
	return cloned
}
