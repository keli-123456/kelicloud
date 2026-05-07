package failoverv2

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	awscloud "github.com/komari-monitor/komari/utils/cloudprovider/aws"
	azurecloud "github.com/komari-monitor/komari/utils/cloudprovider/azure"
	"github.com/komari-monitor/komari/utils/cloudprovider/digitalocean"
	linodecloud "github.com/komari-monitor/komari/utils/cloudprovider/linode"
	vultrcloud "github.com/komari-monitor/komari/utils/cloudprovider/vultr"
)

func isDigitalOceanNotFoundError(err error) bool {
	var apiErr *digitalocean.APIError
	return errors.As(err, &apiErr) && apiErr != nil && apiErr.StatusCode == 404
}

func isLinodeNotFoundError(err error) bool {
	var apiErr *linodecloud.APIError
	return errors.As(err, &apiErr) && apiErr != nil && apiErr.StatusCode == 404
}

func isVultrNotFoundError(err error) bool {
	var apiErr *vultrcloud.APIError
	return errors.As(err, &apiErr) && apiErr != nil && apiErr.StatusCode == 404
}

func isAWSResourceNotFoundError(service string, err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(strings.TrimSpace(err.Error()))
	if strings.TrimSpace(service) == "lightsail" {
		return strings.Contains(message, "lightsail instance not found")
	}
	return strings.Contains(message, "instance not found") || strings.Contains(message, "invalidinstanceid.notfound")
}

func isAzureNotFoundError(err error) bool {
	var apiErr *azurecloud.APIError
	if !errors.As(err, &apiErr) || apiErr == nil {
		return false
	}
	if apiErr.StatusCode == 404 {
		return true
	}
	code := strings.ToLower(strings.TrimSpace(apiErr.Code))
	return code == "resourcenotfound" || code == "notfound" || code == "resourcegroupnotfound"
}

func runAWSProvisionFollowUp(ctx context.Context, action func(context.Context) error) error {
	if action == nil {
		return nil
	}
	runCtx := contextOrBackground(ctx)
	if deadline, ok := runCtx.Deadline(); ok {
		followUpDeadline := deadline
		if shortened := time.Now().Add(2 * time.Minute); shortened.Before(followUpDeadline) {
			followUpDeadline = shortened
		}
		var cancel context.CancelFunc
		runCtx, cancel = context.WithDeadline(runCtx, followUpDeadline)
		defer cancel()
	}
	return action(runCtx)
}

func firstUsablePublicIPv6(values []string) string {
	for _, value := range values {
		normalized := normalizeIPAddress(value)
		if normalized == "" {
			continue
		}
		ip := net.ParseIP(normalized)
		if ip == nil || ip.To4() != nil || ip.IsLinkLocalUnicast() {
			continue
		}
		if strings.HasPrefix(strings.ToLower(normalized), "fe80:") {
			continue
		}
		return ip.String()
	}
	return ""
}

func requireUsableAWSIPv6(addresses []string) (string, error) {
	if ipv6 := firstUsablePublicIPv6(addresses); ipv6 != "" {
		return ipv6, nil
	}
	return "", errors.New("AWS did not expose a usable public IPv6 address on the replacement instance")
}

func lightsailProvisionWantsIPv6(ipAddressType string) bool {
	return strings.EqualFold(strings.TrimSpace(ipAddressType), "dualstack")
}

func waitForAWSEC2Instance(ctx context.Context, region string, credential *awscloud.CredentialRecord, instanceID string) (*awscloud.Instance, *awscloud.InstanceDetail, error) {
	deadline := time.Now().Add(5 * time.Minute)
	for time.Now().Before(deadline) {
		if err := waitContextOrDelay(ctx, 0); err != nil {
			return nil, nil, err
		}
		instance, err := awscloud.GetInstance(ctx, credential, region, instanceID)
		if err != nil {
			if normalizedErr := normalizeExecutionStopError(err); errors.Is(normalizedErr, errExecutionStopped) {
				return nil, nil, normalizedErr
			}
		}
		if err == nil && instance != nil && strings.TrimSpace(instance.PublicIP) != "" {
			detail, detailErr := awscloud.GetInstanceDetail(ctx, credential, region, instanceID)
			if detailErr != nil {
				if normalizedErr := normalizeExecutionStopError(detailErr); errors.Is(normalizedErr, errExecutionStopped) {
					return nil, nil, normalizedErr
				}
				return instance, nil, nil
			}
			return instance, detail, nil
		}
		if err := waitContextOrDelay(ctx, 5*time.Second); err != nil {
			return nil, nil, err
		}
	}
	instance, err := awscloud.GetInstance(ctx, credential, region, instanceID)
	if err != nil {
		return nil, nil, normalizeExecutionStopError(err)
	}
	detail, _ := awscloud.GetInstanceDetail(ctx, credential, region, instanceID)
	return instance, detail, nil
}

func waitForLightsailInstance(ctx context.Context, region string, credential *awscloud.CredentialRecord, instanceName string) (*awscloud.LightsailInstanceDetail, error) {
	deadline := time.Now().Add(5 * time.Minute)
	for time.Now().Before(deadline) {
		if err := waitContextOrDelay(ctx, 0); err != nil {
			return nil, err
		}
		detail, err := awscloud.GetLightsailInstanceDetail(ctx, credential, region, instanceName)
		if err != nil {
			if normalizedErr := normalizeExecutionStopError(err); errors.Is(normalizedErr, errExecutionStopped) {
				return nil, normalizedErr
			}
		}
		if err == nil && detail != nil && strings.TrimSpace(detail.Instance.PublicIP) != "" {
			return detail, nil
		}
		if err := waitContextOrDelay(ctx, 5*time.Second); err != nil {
			return nil, err
		}
	}
	detail, err := awscloud.GetLightsailInstanceDetail(ctx, credential, region, instanceName)
	return detail, normalizeExecutionStopError(err)
}

func waitForLinodeInstance(ctx context.Context, client *linodecloud.Client, instanceID int) (*linodecloud.Instance, error) {
	deadline := time.Now().Add(5 * time.Minute)
	ipv4ReadyAt := time.Time{}
	var latest *linodecloud.Instance
	const ipv6GraceAfterIPv4 = 90 * time.Second
	for time.Now().Before(deadline) {
		if err := waitContextOrDelay(ctx, 0); err != nil {
			return nil, err
		}
		instance, err := client.GetInstance(ctx, instanceID)
		if err != nil {
			if normalizedErr := normalizeExecutionStopError(err); errors.Is(normalizedErr, errExecutionStopped) {
				return nil, normalizedErr
			}
		}
		if err == nil && instance != nil {
			latest = instance
			hasIPv4 := strings.TrimSpace(firstString(instance.IPv4)) != ""
			hasIPv6 := strings.TrimSpace(instance.IPv6) != ""
			if hasIPv4 && hasIPv6 {
				return instance, nil
			}
			if hasIPv4 {
				if ipv4ReadyAt.IsZero() {
					ipv4ReadyAt = time.Now()
				} else if time.Since(ipv4ReadyAt) >= ipv6GraceAfterIPv4 {
					return instance, nil
				}
			}
		}
		if err := waitContextOrDelay(ctx, 5*time.Second); err != nil {
			return nil, err
		}
	}
	if latest != nil {
		return latest, nil
	}
	instance, err := client.GetInstance(ctx, instanceID)
	return instance, normalizeExecutionStopError(err)
}

func waitForVultrInstance(ctx context.Context, client *vultrcloud.Client, instanceID string) (*vultrcloud.Instance, error) {
	deadline := time.Now().Add(5 * time.Minute)
	ipv4ReadyAt := time.Time{}
	var latest *vultrcloud.Instance
	const ipv6GraceAfterIPv4 = 90 * time.Second
	for time.Now().Before(deadline) {
		if err := waitContextOrDelay(ctx, 0); err != nil {
			return nil, err
		}
		instance, err := client.GetInstance(ctx, instanceID)
		if err != nil {
			if normalizedErr := normalizeExecutionStopError(err); errors.Is(normalizedErr, errExecutionStopped) {
				return nil, normalizedErr
			}
		}
		if err == nil && instance != nil {
			latest = instance
			hasIPv4 := strings.TrimSpace(vultrPublicIPv4(instance)) != ""
			hasIPv6 := strings.TrimSpace(vultrPublicIPv6(instance)) != ""
			if hasIPv4 && hasIPv6 {
				return instance, nil
			}
			if hasIPv4 {
				if ipv4ReadyAt.IsZero() {
					ipv4ReadyAt = time.Now()
				} else if time.Since(ipv4ReadyAt) >= ipv6GraceAfterIPv4 {
					return instance, nil
				}
			}
		}
		if err := waitContextOrDelay(ctx, 5*time.Second); err != nil {
			return nil, err
		}
	}
	if latest != nil {
		return latest, nil
	}
	instance, err := client.GetInstance(ctx, instanceID)
	return instance, normalizeExecutionStopError(err)
}

func vultrPublicIPv4(instance *vultrcloud.Instance) string {
	if instance == nil {
		return ""
	}
	return normalizeIPAddress(instance.MainIP)
}

func vultrPublicIPv6(instance *vultrcloud.Instance) string {
	if instance == nil {
		return ""
	}
	return normalizeIPAddress(instance.V6MainIP)
}

func cleanupOnProvisionError(cleanup func(context.Context) error, label string, cause error) error {
	if cleanup == nil {
		return cause
	}
	if err := cleanup(context.Background()); err != nil {
		return fmt.Errorf("%w; automatic cleanup %s also failed: %v", cause, strings.TrimSpace(label), err)
	}
	return cause
}
