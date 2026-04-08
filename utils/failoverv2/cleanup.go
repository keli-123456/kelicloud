package failoverv2

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	failoverv2db "github.com/komari-monitor/komari/database/failoverv2"
	"github.com/komari-monitor/komari/database/models"
	awscloud "github.com/komari-monitor/komari/utils/cloudprovider/aws"
	"github.com/komari-monitor/komari/utils/cloudprovider/digitalocean"
	linodecloud "github.com/komari-monitor/komari/utils/cloudprovider/linode"
)

type oldInstanceCleanup struct {
	Ref     map[string]interface{}
	Label   string
	Cleanup func(context.Context) error
}

var (
	failoverV2ResolveOldInstanceCleanupFunc        = resolveCurrentOldInstanceCleanup
	failoverV2ResolveOldInstanceCleanupFromRefFunc = resolveCurrentOldInstanceCleanupFromRef
)

func (r *memberExecutionRunner) cleanupOldInstanceOnSuccess(outcome *memberProvisionOutcome) (string, map[string]interface{}, string) {
	deleteStrategy := strings.ToLower(strings.TrimSpace(r.service.DeleteStrategy))
	switch deleteStrategy {
	case "", models.FailoverDeleteStrategyKeep:
		return models.FailoverCleanupStatusSkipped, map[string]interface{}{
			"strategy":       models.FailoverDeleteStrategyKeep,
			"classification": "not_requested",
			"summary":        "old instance cleanup was not requested for this service",
		}, ""
	case models.FailoverDeleteStrategyDeleteAfterSuccess, models.FailoverDeleteStrategyDeleteAfterSuccessDelay:
	default:
		deleteStrategy = models.FailoverDeleteStrategyKeep
		return models.FailoverCleanupStatusSkipped, map[string]interface{}{
			"strategy":       deleteStrategy,
			"classification": "not_requested",
			"summary":        "old instance cleanup is not configured",
		}, ""
	}

	if err := r.checkStopped(); err != nil {
		result := map[string]interface{}{
			"strategy":       deleteStrategy,
			"classification": "cleanup_interrupted",
			"summary":        "old instance cleanup was skipped because the execution was stopped",
			"error":          err.Error(),
		}
		return models.FailoverCleanupStatusWarning, result, "old instance cleanup skipped after stop"
	}

	cleanup, err := failoverV2ResolveOldInstanceCleanupFunc(r.userUUID, r.member)
	if err != nil {
		result := map[string]interface{}{
			"strategy":       deleteStrategy,
			"classification": "cleanup_unavailable",
			"summary":        "old instance cleanup could not be prepared",
			"error":          err.Error(),
		}
		return models.FailoverCleanupStatusWarning, result, "old instance cleanup could not be prepared"
	}
	if cleanup == nil {
		result := map[string]interface{}{
			"strategy":       deleteStrategy,
			"classification": "cleanup_unavailable",
			"summary":        "old instance cleanup was requested but the current instance reference is unavailable",
		}
		return models.FailoverCleanupStatusWarning, result, "old instance cleanup requires manual review"
	}
	if sameManagedResource(cleanup.Ref, outcome.NewInstanceRef) {
		result := map[string]interface{}{
			"strategy":       deleteStrategy,
			"classification": "cleanup_skipped_same_instance",
			"summary":        "old instance cleanup was skipped because the saved old instance matches the replacement instance",
			"ref":            cleanup.Ref,
			"cleanup_label":  cleanup.Label,
		}
		return models.FailoverCleanupStatusWarning, result, "old instance cleanup skipped because it matches the replacement instance"
	}

	cleanupStep := r.startStep("cleanup_old", "Cleanup Old Instance", map[string]interface{}{
		"strategy": deleteStrategy,
		"label":    cleanup.Label,
		"ref":      cleanup.Ref,
	})
	r.updateActiveExecutionFields("mark execution cleaning_old", map[string]interface{}{
		"status": models.FailoverV2ExecutionStatusCleaningOld,
	})

	if deleteStrategy == models.FailoverDeleteStrategyDeleteAfterSuccessDelay && r.service.DeleteDelaySeconds > 0 {
		if err := waitContextOrDelay(r.ctx, time.Duration(r.service.DeleteDelaySeconds)*time.Second); err != nil {
			err = normalizeExecutionStopError(err)
			result := map[string]interface{}{
				"strategy":       deleteStrategy,
				"classification": "cleanup_interrupted",
				"summary":        "old instance cleanup was interrupted before the delayed delete could start",
				"ref":            cleanup.Ref,
				"cleanup_label":  cleanup.Label,
				"error":          err.Error(),
			}
			r.finishStep(cleanupStep, models.FailoverStepStatusSkipped, "old instance cleanup delayed delete was interrupted", result)
			return models.FailoverCleanupStatusWarning, result, "old instance cleanup delayed delete was interrupted"
		}
	}

	if err := cleanup.Cleanup(r.ctx); err != nil {
		err = normalizeExecutionStopError(err)
		result := map[string]interface{}{
			"strategy":       deleteStrategy,
			"classification": "cleanup_delete_failed",
			"summary":        "old instance cleanup failed; the old instance may still exist until V2 compensation cleanup succeeds",
			"ref":            cleanup.Ref,
			"cleanup_label":  cleanup.Label,
			"error":          err.Error(),
		}
		pendingCleanup, queueErr := queueFailoverV2PendingCleanup(r.userUUID, r.service.ID, r.member.ID, r.execution.ID, cleanup, err)
		if queueErr != nil {
			result["pending_cleanup_error"] = queueErr.Error()
		} else if pendingCleanup != nil {
			result["pending_cleanup_id"] = pendingCleanup.ID
		}
		r.finishStep(cleanupStep, models.FailoverStepStatusFailed, err.Error(), result)
		return models.FailoverCleanupStatusFailed, result, "old instance cleanup failed"
	}

	result := map[string]interface{}{
		"strategy":       deleteStrategy,
		"classification": "instance_deleted",
		"summary":        "old instance was deleted successfully",
		"ref":            cleanup.Ref,
		"cleanup_label":  cleanup.Label,
	}
	r.finishStep(cleanupStep, models.FailoverStepStatusSuccess, "old instance deleted", result)
	return models.FailoverCleanupStatusSuccess, result, ""
}

func resolveCurrentOldInstanceCleanup(userUUID string, member *models.FailoverV2Member) (*oldInstanceCleanup, error) {
	return resolveCurrentOldInstanceCleanupFromRef(userUUID, resolvedMemberCurrentInstanceRef(member))
}

func resolveCurrentOldInstanceCleanupFromRef(userUUID string, ref map[string]interface{}) (*oldInstanceCleanup, error) {
	if len(ref) == 0 {
		return nil, nil
	}

	provider := strings.ToLower(strings.TrimSpace(stringMapValue(ref, "provider")))
	entryID := strings.TrimSpace(providerEntryIDFromRef(ref))
	switch provider {
	case "digitalocean":
		dropletID := intMapValue(ref, "droplet_id")
		if dropletID <= 0 || entryID == "" {
			return nil, nil
		}
		addition, token, err := loadDigitalOceanToken(userUUID, entryID)
		if err != nil {
			return nil, err
		}
		client, err := digitalocean.NewClientFromToken(token.Token)
		if err != nil {
			return nil, err
		}
		resolvedRef := resolvedCurrentInstanceRef(ref, provider, entryID)
		return &oldInstanceCleanup{
			Ref:   resolvedRef,
			Label: fmt.Sprintf("delete digitalocean droplet %d", dropletID),
			Cleanup: func(ctx context.Context) error {
				if err := client.DeleteDroplet(contextOrBackground(ctx), dropletID); err != nil {
					if isDigitalOceanNotFoundError(err) {
						removeSavedDigitalOceanRootPassword(userUUID, addition, token, dropletID)
						return nil
					}
					return err
				}
				removeSavedDigitalOceanRootPassword(userUUID, addition, token, dropletID)
				return nil
			},
		}, nil
	case "linode":
		instanceID := intMapValue(ref, "instance_id")
		if instanceID <= 0 || entryID == "" {
			return nil, nil
		}
		addition, token, err := loadLinodeToken(userUUID, entryID)
		if err != nil {
			return nil, err
		}
		client, err := linodecloud.NewClientFromToken(token.Token)
		if err != nil {
			return nil, err
		}
		resolvedRef := resolvedCurrentInstanceRef(ref, provider, entryID)
		return &oldInstanceCleanup{
			Ref:   resolvedRef,
			Label: fmt.Sprintf("delete linode instance %d", instanceID),
			Cleanup: func(ctx context.Context) error {
				if err := client.DeleteInstance(contextOrBackground(ctx), instanceID); err != nil {
					if isLinodeNotFoundError(err) {
						removeSavedLinodeRootPassword(userUUID, addition, token, instanceID)
						return nil
					}
					return err
				}
				removeSavedLinodeRootPassword(userUUID, addition, token, instanceID)
				return nil
			},
		}, nil
	case "aws":
		addition, credential, err := loadAWSCredential(userUUID, entryID)
		if err != nil {
			return nil, err
		}
		service := normalizeAWSFailoverService(firstNonEmpty(stringMapValue(ref, "service"), "ec2"))
		region := resolveAWSFailoverRegion(stringMapValue(ref, "region"), addition, credential)
		resolvedRef := resolvedCurrentInstanceRef(ref, provider, entryID)
		switch service {
		case "lightsail":
			instanceName := strings.TrimSpace(stringMapValue(ref, "instance_name"))
			if instanceName == "" {
				return nil, nil
			}
			return &oldInstanceCleanup{
				Ref:   resolvedRef,
				Label: "delete aws lightsail instance " + instanceName,
				Cleanup: func(ctx context.Context) error {
					if err := awscloud.DeleteLightsailInstance(contextOrBackground(ctx), credential, region, instanceName); err != nil {
						if isAWSResourceNotFoundError("lightsail", err) {
							removeSavedAWSRootPassword(userUUID, addition, credential, "lightsail", region, instanceName)
							return nil
						}
						return err
					}
					removeSavedAWSRootPassword(userUUID, addition, credential, "lightsail", region, instanceName)
					return nil
				},
			}, nil
		default:
			instanceID := strings.TrimSpace(stringMapValue(ref, "instance_id"))
			if instanceID == "" {
				return nil, nil
			}
			return &oldInstanceCleanup{
				Ref:   resolvedRef,
				Label: "terminate aws ec2 instance " + instanceID,
				Cleanup: func(ctx context.Context) error {
					if err := awscloud.TerminateInstance(contextOrBackground(ctx), credential, region, instanceID); err != nil {
						if isAWSResourceNotFoundError("ec2", err) {
							removeSavedAWSRootPassword(userUUID, addition, credential, "ec2", region, instanceID)
							return nil
						}
						return err
					}
					removeSavedAWSRootPassword(userUUID, addition, credential, "ec2", region, instanceID)
					return nil
				},
			}, nil
		}
	default:
		return nil, nil
	}
}

func queueFailoverV2PendingCleanup(userUUID string, serviceID, memberID, executionID uint, cleanup *oldInstanceCleanup, cleanupErr error) (*models.FailoverV2PendingCleanup, error) {
	if cleanup == nil || cleanupErr == nil {
		return nil, nil
	}

	ref := cloneJSONMap(cleanup.Ref)
	provider, resourceType, resourceID, providerEntryID := pendingCleanupIdentityFromRef(ref)
	if provider == "" || resourceType == "" || resourceID == "" {
		return nil, nil
	}

	now := time.Now()
	lastAttemptedAt := models.FromTime(now)
	nextRetryAt := models.FromTime(now.Add(pendingFailoverV2CleanupRetryBackoff(provider, cleanupErr, 1)))
	item := &models.FailoverV2PendingCleanup{
		UserID:          strings.TrimSpace(userUUID),
		ServiceID:       serviceID,
		MemberID:        memberID,
		ExecutionID:     executionID,
		Provider:        provider,
		ProviderEntryID: providerEntryID,
		ResourceType:    resourceType,
		ResourceID:      resourceID,
		InstanceRef:     string(marshalJSON(ref)),
		CleanupLabel:    strings.TrimSpace(cleanup.Label),
		Status:          models.FailoverV2PendingCleanupStatusPending,
		AttemptCount:    1,
		LastError:       strings.TrimSpace(cleanupErr.Error()),
		LastAttemptedAt: &lastAttemptedAt,
		NextRetryAt:     &nextRetryAt,
	}
	return failoverv2db.CreateOrUpdatePendingCleanup(item)
}

func pendingCleanupIdentityFromRef(ref map[string]interface{}) (string, string, string, string) {
	if len(ref) == 0 {
		return "", "", "", ""
	}

	provider := strings.ToLower(strings.TrimSpace(stringMapValue(ref, "provider")))
	entryID := strings.TrimSpace(providerEntryIDFromRef(ref))
	switch provider {
	case "digitalocean":
		if dropletID := intMapValue(ref, "droplet_id"); dropletID > 0 {
			return provider, "droplet", strconv.Itoa(dropletID), entryID
		}
	case "linode":
		if instanceID := intMapValue(ref, "instance_id"); instanceID > 0 {
			return provider, "instance", strconv.Itoa(instanceID), entryID
		}
	case "aws":
		service := normalizeAWSFailoverService(firstNonEmpty(stringMapValue(ref, "service"), "ec2"))
		switch service {
		case "lightsail":
			if instanceName := strings.TrimSpace(stringMapValue(ref, "instance_name")); instanceName != "" {
				return provider, "lightsail_instance", instanceName, entryID
			}
		default:
			if instanceID := strings.TrimSpace(stringMapValue(ref, "instance_id")); instanceID != "" {
				return provider, "ec2_instance", instanceID, entryID
			}
		}
	}
	return "", "", "", ""
}

func resolvedMemberCurrentInstanceRef(member *models.FailoverV2Member) map[string]interface{} {
	if member == nil {
		return nil
	}
	ref := parseJSONMap(member.CurrentInstanceRef)
	if len(ref) == 0 {
		return nil
	}
	if strings.TrimSpace(stringMapValue(ref, "provider")) == "" && strings.TrimSpace(member.Provider) != "" {
		ref["provider"] = strings.TrimSpace(member.Provider)
	}
	if strings.TrimSpace(providerEntryIDFromRef(ref)) == "" && strings.TrimSpace(member.ProviderEntryID) != "" {
		ref["provider_entry_id"] = strings.TrimSpace(member.ProviderEntryID)
	}
	return ref
}

func resolvedCurrentInstanceRef(ref map[string]interface{}, provider, entryID string) map[string]interface{} {
	resolved := cloneJSONMap(ref)
	if len(resolved) == 0 {
		resolved = map[string]interface{}{}
	}
	resolved["provider"] = strings.TrimSpace(provider)
	resolved["provider_entry_id"] = strings.TrimSpace(entryID)
	return resolved
}

func sameManagedResource(left, right map[string]interface{}) bool {
	leftProvider, leftType, leftID, _ := pendingCleanupIdentityFromRef(left)
	rightProvider, rightType, rightID, _ := pendingCleanupIdentityFromRef(right)
	return leftProvider != "" && leftProvider == rightProvider && leftType == rightType && leftID == rightID
}

func parseJSONMap(raw string) map[string]interface{} {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	var decoded map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		return nil
	}
	if len(decoded) == 0 {
		return nil
	}
	return decoded
}

func cloneJSONMap(source map[string]interface{}) map[string]interface{} {
	if len(source) == 0 {
		return nil
	}
	cloned := make(map[string]interface{}, len(source))
	for key, value := range source {
		cloned[key] = value
	}
	return cloned
}

func providerEntryIDFromRef(ref map[string]interface{}) string {
	return firstNonEmpty(stringMapValue(ref, "provider_entry_id"), stringMapValue(ref, "entry_id"))
}

func stringMapValue(source map[string]interface{}, key string) string {
	if len(source) == 0 {
		return ""
	}
	value, ok := source[key]
	if !ok || value == nil {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case json.Number:
		return typed.String()
	default:
		return strings.TrimSpace(fmt.Sprintf("%v", typed))
	}
}

func intMapValue(source map[string]interface{}, key string) int {
	if len(source) == 0 {
		return 0
	}
	value, ok := source[key]
	if !ok || value == nil {
		return 0
	}
	switch typed := value.(type) {
	case int:
		return typed
	case int64:
		return int(typed)
	case float64:
		return int(typed)
	case json.Number:
		parsed, _ := typed.Int64()
		return int(parsed)
	case string:
		parsed, _ := strconv.Atoi(strings.TrimSpace(typed))
		return parsed
	default:
		return 0
	}
}
