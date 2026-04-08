package failoverv2

import (
	"context"
	"fmt"
	"strings"

	"github.com/komari-monitor/komari/database/models"
)

func applyMemberDNSDetach(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member) (interface{}, error) {
	if service == nil {
		return nil, fmt.Errorf("service is required")
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

func verifyMemberDNSDetached(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member) (interface{}, error) {
	if service == nil {
		return nil, fmt.Errorf("service is required")
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

func applyMemberDNSAttach(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member, ipv4, ipv6 string) (interface{}, error) {
	if service == nil {
		return nil, fmt.Errorf("service is required")
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

func verifyMemberDNSAttached(ctx context.Context, userUUID string, service *models.FailoverV2Service, member *models.FailoverV2Member, ipv4, ipv6 string) (interface{}, error) {
	if service == nil {
		return nil, fmt.Errorf("service is required")
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
	case "aws":
		return provisionAWSMember(ctx, userUUID, service, member)
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
	default:
		return false
	}
}

func extractMemberDNSRecordRefs(result interface{}) map[string]string {
	switch typed := result.(type) {
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
