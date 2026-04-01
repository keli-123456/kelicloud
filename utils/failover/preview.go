package failover

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/komari-monitor/komari/database/models"
	linodecloud "github.com/komari-monitor/komari/utils/cloudprovider/linode"
)

const (
	previewStatusSuccess = "success"
	previewStatusWarning = "warning"
	previewStatusError   = "error"
	previewStatusInfo    = "info"
)

type PreviewCheck struct {
	Key     string                 `json:"key"`
	Status  string                 `json:"status"`
	Title   string                 `json:"title"`
	Message string                 `json:"message"`
	Detail  map[string]interface{} `json:"detail,omitempty"`
}

type PreviewPlan struct {
	Index              int            `json:"index"`
	Name               string         `json:"name"`
	Provider           string         `json:"provider"`
	ActionType         string         `json:"action_type"`
	ProviderEntryID    string         `json:"provider_entry_id"`
	ProviderEntryGroup string         `json:"provider_entry_group"`
	Checks             []PreviewCheck `json:"checks"`
}

type PreviewResult struct {
	Success     bool           `json:"success"`
	GeneratedAt time.Time      `json:"generated_at"`
	Checks      []PreviewCheck `json:"checks"`
	Plans       []PreviewPlan  `json:"plans"`
}

func PreviewTask(userUUID string, task models.FailoverTask, plans []models.FailoverPlan) (*PreviewResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	result := &PreviewResult{
		Success:     true,
		GeneratedAt: time.Now().UTC(),
		Checks:      make([]PreviewCheck, 0, 2),
		Plans:       make([]PreviewPlan, 0, len(plans)),
	}

	enabledPlans := 0
	for index, plan := range plans {
		if !plan.Enabled {
			result.Plans = append(result.Plans, PreviewPlan{
				Index:              index + 1,
				Name:               strings.TrimSpace(plan.Name),
				Provider:           strings.TrimSpace(plan.Provider),
				ActionType:         strings.TrimSpace(plan.ActionType),
				ProviderEntryID:    strings.TrimSpace(plan.ProviderEntryID),
				ProviderEntryGroup: strings.TrimSpace(plan.ProviderEntryGroup),
				Checks: []PreviewCheck{
					newPreviewCheck(
						"plan_disabled",
						previewStatusInfo,
						"Plan disabled",
						"This plan is disabled and will be skipped during failover.",
						nil,
					),
				},
			})
			continue
		}

		enabledPlans++
		planPreview := previewPlan(ctx, userUUID, plan, index)
		if previewHasError(planPreview.Checks) {
			result.Success = false
		}
		result.Plans = append(result.Plans, planPreview)
	}

	if enabledPlans == 0 {
		result.Success = false
		result.Checks = append(result.Checks, newPreviewCheck(
			"enabled_plans",
			previewStatusError,
			"No enabled plans",
			"At least one enabled failover plan is required for preview.",
			nil,
		))
	}

	dnsCheck := previewDNS(ctx, userUUID, task)
	if dnsCheck.Key != "" {
		result.Checks = append(result.Checks, dnsCheck)
		if dnsCheck.Status == previewStatusError {
			result.Success = false
		}
	}

	return result, nil
}

func previewPlan(ctx context.Context, userUUID string, plan models.FailoverPlan, index int) PreviewPlan {
	preview := PreviewPlan{
		Index:              index + 1,
		Name:               strings.TrimSpace(plan.Name),
		Provider:           strings.TrimSpace(plan.Provider),
		ActionType:         strings.TrimSpace(plan.ActionType),
		ProviderEntryID:    strings.TrimSpace(plan.ProviderEntryID),
		ProviderEntryGroup: strings.TrimSpace(plan.ProviderEntryGroup),
		Checks:             make([]PreviewCheck, 0, 5),
	}

	candidates, err := listProviderPoolCandidates(userUUID, plan)
	if err != nil {
		preview.Checks = append(preview.Checks, newPreviewCheck(
			"provider_entry",
			previewStatusError,
			"Provider entry",
			err.Error(),
			nil,
		))
		return preview
	}
	candidates = sortProviderPoolCandidates(candidates)
	if len(candidates) == 0 {
		preview.Checks = append(preview.Checks, newPreviewCheck(
			"provider_entry",
			previewStatusError,
			"Provider entry",
			"No matching provider entry was found for this plan.",
			nil,
		))
		return preview
	}

	selectedCandidate := candidates[0]
	preview.ProviderEntryID = strings.TrimSpace(selectedCandidate.EntryID)
	preview.ProviderEntryGroup = firstNonEmpty(strings.TrimSpace(selectedCandidate.EntryGroup), preview.ProviderEntryGroup)
	preview.Checks = append(preview.Checks, newPreviewCheck(
		"provider_entry",
		previewStatusSuccess,
		"Provider entry",
		buildPreviewCandidateMessage(selectedCandidate, len(candidates)),
		map[string]interface{}{
			"entry_id":    strings.TrimSpace(selectedCandidate.EntryID),
			"entry_name":  strings.TrimSpace(selectedCandidate.EntryName),
			"entry_group": strings.TrimSpace(selectedCandidate.EntryGroup),
			"preferred":   selectedCandidate.Preferred,
			"active":      selectedCandidate.Active,
			"candidates":  buildPreviewCandidateDetails(candidates),
		},
	))

	if strings.TrimSpace(plan.ActionType) == models.FailoverActionProvisionInstance {
		preview.Checks = append(preview.Checks, previewCapacityCheck(ctx, userUUID, plan, selectedCandidate))
	}

	preview.Checks = append(preview.Checks, previewPlanCatalogCheck(userUUID, plan, selectedCandidate))
	preview.Checks = append(preview.Checks, previewAutoConnectCheck(ctx, userUUID, plan, selectedCandidate))

	if planHasScripts(plan) {
		scriptIDs := plan.EffectiveScriptClipboardIDs()
		preview.Checks = append(preview.Checks, newPreviewCheck(
			"scripts",
			previewStatusInfo,
			"Scripts",
			fmt.Sprintf("%d script(s) will run after the new agent connects.", len(scriptIDs)),
			map[string]interface{}{
				"script_clipboard_ids": scriptIDs,
			},
		))
	}

	return preview
}

func previewCapacityCheck(ctx context.Context, userUUID string, plan models.FailoverPlan, candidate providerPoolCandidate) PreviewCheck {
	snapshot, err := providerEntryCapacitySnapshotLoader(ctx, userUUID, plan, candidate)
	if err != nil {
		return newPreviewCheck(
			"capacity",
			previewStatusError,
			"Provider capacity",
			err.Error(),
			nil,
		)
	}

	state := failoverProviderEntryScheduler.stateFor(providerEntryStateKey(userUUID, plan.Provider, candidate.EntryID))
	state.mu.Lock()
	availability := state.buildAvailability(snapshot, plan)
	free, _ := availability["free"].(int)
	mode, _ := availability["mode"].(string)
	state.mu.Unlock()

	switch {
	case snapshot == nil:
		return newPreviewCheck(
			"capacity",
			previewStatusWarning,
			"Provider capacity",
			"Provider capacity could not be determined before execution.",
			availability,
		)
	case mode == providerEntryCapacityModeQuota && free <= 0:
		return newPreviewCheck(
			"capacity",
			previewStatusWarning,
			"Provider capacity",
			"Tracked provider quota is full right now. Failover may need cleanup or a later retry before creating a new instance.",
			availability,
		)
	case mode == providerEntryCapacityModeQuota:
		return newPreviewCheck(
			"capacity",
			previewStatusSuccess,
			"Provider capacity",
			fmt.Sprintf("Tracked provider quota currently has %d free slot(s).", free),
			availability,
		)
	default:
		return newPreviewCheck(
			"capacity",
			previewStatusSuccess,
			"Provider capacity",
			"Provider entry is reachable. Create operations will be serialized during failover.",
			availability,
		)
	}
}

func previewPlanCatalogCheck(userUUID string, plan models.FailoverPlan, candidate providerPoolCandidate) PreviewCheck {
	issues, warnings, detail, service, region, err := buildPreviewPlanCatalogCheck(userUUID, plan, candidate)
	if err != nil {
		return newPreviewCheck(
			"plan_catalog",
			previewStatusError,
			"Plan configuration",
			err.Error(),
			detail,
		)
	}

	if service != "" {
		detail["service"] = service
	}
	if region != "" {
		detail["region"] = region
	}

	switch {
	case len(issues) > 0:
		return newPreviewCheck(
			"plan_catalog",
			previewStatusError,
			"Plan configuration",
			strings.Join(issues, "; "),
			detail,
		)
	case len(warnings) > 0:
		return newPreviewCheck(
			"plan_catalog",
			previewStatusWarning,
			"Plan configuration",
			strings.Join(warnings, "; "),
			detail,
		)
	default:
		return newPreviewCheck(
			"plan_catalog",
			previewStatusSuccess,
			"Plan configuration",
			"Selected provider options were found in the current catalog.",
			detail,
		)
	}
}

func buildPreviewPlanCatalogCheck(userUUID string, plan models.FailoverPlan, candidate providerPoolCandidate) ([]string, []string, map[string]interface{}, string, string, error) {
	detail := map[string]interface{}{
		"provider":    strings.TrimSpace(plan.Provider),
		"action_type": strings.TrimSpace(plan.ActionType),
		"entry_id":    strings.TrimSpace(candidate.EntryID),
	}
	issues := make([]string, 0)
	warnings := make([]string, 0)

	switch strings.TrimSpace(plan.Provider) {
	case "aws":
		switch strings.TrimSpace(plan.ActionType) {
		case models.FailoverActionProvisionInstance:
			var payload awsProvisionPayload
			if err := json.Unmarshal([]byte(plan.Payload), &payload); err != nil {
				return nil, nil, detail, "", "", fmt.Errorf("invalid aws provision payload: %w", err)
			}
			service := strings.ToLower(strings.TrimSpace(payload.Service))
			if service == "" {
				service = "ec2"
			}
			region := strings.TrimSpace(payload.Region)
			if region == "" {
				issues = append(issues, "region is required")
				return issues, warnings, detail, service, region, nil
			}

			catalog, err := LoadPlanCatalog(userUUID, "aws", candidate.EntryID, "", plan.ActionType, service, region, "full")
			if err != nil {
				return nil, nil, detail, service, region, err
			}
			verifyPreviewCatalogOption(&issues, &warnings, "region", region, catalog.Regions)
			if service == "lightsail" {
				availabilityZone := strings.TrimSpace(payload.AvailabilityZone)
				blueprintID := strings.TrimSpace(payload.BlueprintID)
				bundleID := strings.TrimSpace(payload.BundleID)
				detail["availability_zone"] = availabilityZone
				detail["blueprint_id"] = blueprintID
				detail["bundle_id"] = bundleID
				if availabilityZone == "" {
					issues = append(issues, "availability_zone is required")
				}
				if blueprintID == "" {
					issues = append(issues, "blueprint_id is required")
				}
				if bundleID == "" {
					issues = append(issues, "bundle_id is required")
				}
				if len(issues) == 0 {
					verifyPreviewCatalogOption(&issues, &warnings, "availability_zone", availabilityZone, catalog.AvailabilityZones)
					verifyPreviewCatalogOption(&issues, &warnings, "blueprint_id", blueprintID, catalog.Blueprints)
					verifyPreviewCatalogOption(&issues, &warnings, "bundle_id", bundleID, catalog.Bundles)
				}
				return issues, warnings, detail, service, region, nil
			}

			imageID := strings.TrimSpace(payload.ImageID)
			instanceType := strings.TrimSpace(payload.InstanceType)
			detail["image_id"] = imageID
			detail["instance_type"] = instanceType
			if imageID == "" {
				issues = append(issues, "image_id is required")
			}
			if instanceType == "" {
				issues = append(issues, "instance_type is required")
			}
			if len(issues) == 0 {
				verifyPreviewCatalogOption(&issues, &warnings, "image_id", imageID, catalog.Images)
				verifyPreviewCatalogOption(&issues, &warnings, "instance_type", instanceType, catalog.InstanceTypes)
			}
			return issues, warnings, detail, service, region, nil
		case models.FailoverActionRebindPublicIP:
			var payload awsRebindPayload
			if err := json.Unmarshal([]byte(plan.Payload), &payload); err != nil {
				return nil, nil, detail, "", "", fmt.Errorf("invalid aws rebind payload: %w", err)
			}
			service := strings.ToLower(strings.TrimSpace(payload.Service))
			if service == "" {
				service = "ec2"
			}
			region := strings.TrimSpace(payload.Region)
			if region == "" {
				issues = append(issues, "region is required")
				return issues, warnings, detail, service, region, nil
			}

			catalog, err := LoadPlanCatalog(userUUID, "aws", candidate.EntryID, "", plan.ActionType, service, region, "full")
			if err != nil {
				return nil, nil, detail, service, region, err
			}
			verifyPreviewCatalogOption(&issues, &warnings, "region", region, catalog.Regions)
			if service == "lightsail" {
				instanceName := strings.TrimSpace(payload.InstanceName)
				detail["instance_name"] = instanceName
				if instanceName == "" {
					issues = append(issues, "instance_name is required")
				} else {
					verifyPreviewCatalogOption(&issues, &warnings, "instance_name", instanceName, catalog.Instances)
				}
				return issues, warnings, detail, service, region, nil
			}
			instanceID := strings.TrimSpace(payload.InstanceID)
			detail["instance_id"] = instanceID
			if instanceID == "" {
				issues = append(issues, "instance_id is required")
			} else {
				verifyPreviewCatalogOption(&issues, &warnings, "instance_id", instanceID, catalog.Instances)
			}
			return issues, warnings, detail, service, region, nil
		}
	case "digitalocean":
		var payload digitalOceanProvisionPayload
		if err := json.Unmarshal([]byte(plan.Payload), &payload); err != nil {
			return nil, nil, detail, "", "", fmt.Errorf("invalid digitalocean provision payload: %w", err)
		}
		region := strings.TrimSpace(payload.Region)
		size := strings.TrimSpace(payload.Size)
		image := strings.TrimSpace(payload.Image)
		detail["region"] = region
		detail["size"] = size
		detail["image"] = image
		if region == "" {
			issues = append(issues, "region is required")
		}
		if size == "" {
			issues = append(issues, "size is required")
		}
		if image == "" {
			issues = append(issues, "image is required")
		}
		if len(issues) > 0 {
			return issues, warnings, detail, "", region, nil
		}
		catalog, err := LoadPlanCatalog(userUUID, "digitalocean", candidate.EntryID, "", plan.ActionType, "", region, "full")
		if err != nil {
			return nil, nil, detail, "", region, err
		}
		verifyPreviewCatalogOption(&issues, &warnings, "region", region, catalog.Regions)
		verifyPreviewCatalogOption(&issues, &warnings, "size", size, catalog.Sizes)
		verifyPreviewCatalogOption(&issues, &warnings, "image", image, catalog.Images)
		return issues, warnings, detail, "", region, nil
	case "linode":
		var payload linodeProvisionPayload
		if err := json.Unmarshal([]byte(plan.Payload), &payload); err != nil {
			return nil, nil, detail, "", "", fmt.Errorf("invalid linode provision payload: %w", err)
		}
		region := strings.TrimSpace(payload.Region)
		planType := strings.TrimSpace(payload.Type)
		image := strings.TrimSpace(payload.Image)
		detail["region"] = region
		detail["type"] = planType
		detail["image"] = image
		if region == "" {
			issues = append(issues, "region is required")
		}
		if planType == "" {
			issues = append(issues, "type is required")
		}
		if image == "" {
			issues = append(issues, "image is required")
		}
		if len(issues) > 0 {
			return issues, warnings, detail, "", region, nil
		}
		catalog, err := LoadPlanCatalog(userUUID, "linode", candidate.EntryID, "", plan.ActionType, "", region, "full")
		if err != nil {
			return nil, nil, detail, "", region, err
		}
		verifyPreviewCatalogOption(&issues, &warnings, "region", region, catalog.Regions)
		verifyPreviewCatalogOption(&issues, &warnings, "type", planType, catalog.Types)
		verifyPreviewCatalogOption(&issues, &warnings, "image", image, catalog.Images)
		return issues, warnings, detail, "", region, nil
	}

	return nil, nil, detail, "", "", fmt.Errorf("unsupported preview provider: %s", plan.Provider)
}

func previewAutoConnectCheck(ctx context.Context, userUUID string, plan models.FailoverPlan, candidate providerPoolCandidate) PreviewCheck {
	if strings.TrimSpace(plan.ActionType) != models.FailoverActionProvisionInstance {
		return newPreviewCheck(
			"auto_connect",
			previewStatusInfo,
			"Auto-connect",
			"Auto-connect checks only apply to instance provisioning plans.",
			nil,
		)
	}

	if strings.TrimSpace(plan.AutoConnectGroup) == "" && !planHasScripts(plan) {
		return newPreviewCheck(
			"auto_connect",
			previewStatusWarning,
			"Auto-connect",
			"This plan does not request auto-connect. A newly created instance will not register back to the panel automatically.",
			nil,
		)
	}

	userData, wrapInShellScript, region, image, err := previewAutoConnectInputs(plan)
	if err != nil {
		return newPreviewCheck(
			"auto_connect",
			previewStatusError,
			"Auto-connect",
			err.Error(),
			nil,
		)
	}

	mergedUserData, autoConnectGroup, err := buildAutoConnectUserData(autoConnectOptions{
		UserUUID:          userUUID,
		UserData:          userData,
		Provider:          strings.TrimSpace(plan.Provider),
		CredentialName:    strings.TrimSpace(candidate.EntryName),
		PoolGroup:         resolveAutoConnectPoolGroup(plan, candidate.EntryGroup),
		Group:             plan.AutoConnectGroup,
		WrapInShellScript: wrapInShellScript,
	})
	if err != nil {
		return newPreviewCheck(
			"auto_connect",
			previewStatusError,
			"Auto-connect",
			err.Error(),
			nil,
		)
	}

	detail := map[string]interface{}{
		"auto_connect_group":      autoConnectGroup,
		"wrap_in_shell_script":    wrapInShellScript,
		"script_clipboard_ids":    plan.EffectiveScriptClipboardIDs(),
		"user_data_preview_bytes": len(strings.TrimSpace(mergedUserData)),
	}

	if strings.TrimSpace(plan.Provider) == "linode" && autoConnectGroup != "" {
		_, token, err := loadLinodeToken(userUUID, candidate.EntryID)
		if err != nil {
			return newPreviewCheck(
				"auto_connect",
				previewStatusError,
				"Auto-connect",
				err.Error(),
				detail,
			)
		}

		client, err := linodecloud.NewClientFromToken(token.Token)
		if err != nil {
			return newPreviewCheck(
				"auto_connect",
				previewStatusError,
				"Auto-connect",
				err.Error(),
				detail,
			)
		}

		checkCtx, cancel := context.WithTimeout(contextOrBackground(ctx), 20*time.Second)
		defer cancel()
		if err := linodecloud.ValidateAutoConnectSupport(checkCtx, client, region, image); err != nil {
			return newPreviewCheck(
				"auto_connect",
				previewStatusError,
				"Auto-connect",
				err.Error(),
				detail,
			)
		}
		detail["region"] = region
		detail["image"] = image
	}

	return newPreviewCheck(
		"auto_connect",
		previewStatusSuccess,
		"Auto-connect",
		fmt.Sprintf("Auto-connect prerequisites look good for group %s.", autoConnectGroup),
		detail,
	)
}

func previewAutoConnectInputs(plan models.FailoverPlan) (string, bool, string, string, error) {
	switch strings.TrimSpace(plan.Provider) {
	case "aws":
		if strings.TrimSpace(plan.ActionType) != models.FailoverActionProvisionInstance {
			return "", true, "", "", nil
		}
		var payload awsProvisionPayload
		if err := json.Unmarshal([]byte(plan.Payload), &payload); err != nil {
			return "", true, "", "", fmt.Errorf("invalid aws provision payload: %w", err)
		}
		service := strings.ToLower(strings.TrimSpace(payload.Service))
		if service == "" {
			service = "ec2"
		}
		if service == "lightsail" {
			return strings.TrimSpace(payload.UserData), true, strings.TrimSpace(payload.Region), "", nil
		}
		return strings.TrimSpace(payload.UserData), true, strings.TrimSpace(payload.Region), strings.TrimSpace(payload.ImageID), nil
	case "digitalocean":
		var payload digitalOceanProvisionPayload
		if err := json.Unmarshal([]byte(plan.Payload), &payload); err != nil {
			return "", false, "", "", fmt.Errorf("invalid digitalocean provision payload: %w", err)
		}
		return strings.TrimSpace(payload.UserData), false, strings.TrimSpace(payload.Region), strings.TrimSpace(payload.Image), nil
	case "linode":
		var payload linodeProvisionPayload
		if err := json.Unmarshal([]byte(plan.Payload), &payload); err != nil {
			return "", true, "", "", fmt.Errorf("invalid linode provision payload: %w", err)
		}
		return strings.TrimSpace(payload.UserData), true, strings.TrimSpace(payload.Region), strings.TrimSpace(payload.Image), nil
	default:
		return "", true, "", "", fmt.Errorf("unsupported preview provider: %s", plan.Provider)
	}
}

func previewDNS(ctx context.Context, userUUID string, task models.FailoverTask) PreviewCheck {
	if strings.TrimSpace(task.DNSProvider) == "" {
		return newPreviewCheck(
			"dns",
			previewStatusInfo,
			"DNS switching",
			"DNS switching is disabled for this task.",
			nil,
		)
	}

	detail := map[string]interface{}{
		"provider": strings.TrimSpace(task.DNSProvider),
		"entry_id": strings.TrimSpace(task.DNSEntryID),
	}

	zoneName := ""
	domainName := ""
	switch strings.TrimSpace(task.DNSProvider) {
	case cloudflareProviderName:
		var payload cloudflareDNSPayload
		if err := json.Unmarshal([]byte(task.DNSPayload), &payload); err == nil {
			zoneName = strings.TrimSpace(payload.ZoneName)
			detail["zone_name"] = zoneName
			detail["record_name"] = strings.TrimSpace(payload.RecordName)
		}
	case aliyunProviderName:
		var payload aliyunRecordPayload
		if err := json.Unmarshal([]byte(task.DNSPayload), &payload); err == nil {
			domainName = strings.TrimSpace(payload.DomainName)
			detail["domain_name"] = domainName
			detail["rr"] = strings.TrimSpace(payload.RR)
		}
	}

	catalog, err := LoadDNSCatalog(userUUID, task.DNSProvider, task.DNSEntryID, zoneName, domainName)
	if err != nil {
		return newPreviewCheck(
			"dns",
			previewStatusError,
			"DNS switching",
			err.Error(),
			detail,
		)
	}

	if catalog != nil {
		if detail["zone_name"] == "" && strings.TrimSpace(catalog.Defaults.ZoneName) != "" {
			detail["zone_name"] = strings.TrimSpace(catalog.Defaults.ZoneName)
		}
		if detail["domain_name"] == "" && strings.TrimSpace(catalog.Defaults.DomainName) != "" {
			detail["domain_name"] = strings.TrimSpace(catalog.Defaults.DomainName)
		}
	}
	detail["records"] = buildPreviewDNSRecords(task, catalog)

	return newPreviewCheck(
		"dns",
		previewStatusSuccess,
		"DNS switching",
		"DNS credential is reachable. Preview records below show what will be updated after a successful failover.",
		detail,
	)
}

func buildPreviewDNSRecords(task models.FailoverTask, catalog *DNSCatalog) []map[string]interface{} {
	recordType := "A"
	syncIPv6 := false
	placeholderIPv4 := "203.0.113.10"
	placeholderIPv6 := "2001:db8::10"

	switch strings.TrimSpace(task.DNSProvider) {
	case cloudflareProviderName:
		var payload cloudflareDNSPayload
		if err := json.Unmarshal([]byte(task.DNSPayload), &payload); err != nil {
			return nil
		}
		if normalized := strings.ToUpper(strings.TrimSpace(payload.RecordType)); normalized != "" {
			recordType = normalized
		}
		syncIPv6 = payload.SyncIPv6
		plan, err := buildDNSApplyPlan(recordType, syncIPv6, placeholderIPv4, placeholderIPv6)
		if err != nil {
			return nil
		}
		zoneName := strings.TrimSpace(payload.ZoneName)
		if zoneName == "" && catalog != nil {
			zoneName = strings.TrimSpace(catalog.Defaults.ZoneName)
		}
		recordName := strings.TrimSpace(payload.RecordName)
		if recordName == "" {
			recordName = "@"
		}
		name := normalizeCloudflareRecordName(recordName, zoneName)
		records := make([]map[string]interface{}, 0, len(plan.RecordTypes))
		for _, currentType := range plan.RecordTypes {
			value, _ := selectRecordValue(currentType, placeholderIPv4, placeholderIPv6)
			records = append(records, map[string]interface{}{
				"name":  firstNonEmpty(name, recordName),
				"type":  currentType,
				"value": previewDNSPlaceholderLabel(currentType, value),
			})
		}
		return records
	case aliyunProviderName:
		var payload aliyunRecordPayload
		if err := json.Unmarshal([]byte(task.DNSPayload), &payload); err != nil {
			return nil
		}
		if normalized := strings.ToUpper(strings.TrimSpace(payload.RecordType)); normalized != "" {
			recordType = normalized
		}
		syncIPv6 = payload.SyncIPv6
		plan, err := buildDNSApplyPlan(recordType, syncIPv6, placeholderIPv4, placeholderIPv6)
		if err != nil {
			return nil
		}
		domainName := strings.TrimSpace(payload.DomainName)
		if domainName == "" && catalog != nil {
			domainName = strings.TrimSpace(catalog.Defaults.DomainName)
		}
		rr := normalizeAliyunRR(domainName, payload.RR)
		lines := normalizeAliyunLines(payload.Line, payload.Lines)
		records := make([]map[string]interface{}, 0, len(plan.RecordTypes)*len(lines))
		for _, currentType := range plan.RecordTypes {
			value, _ := selectRecordValue(currentType, placeholderIPv4, placeholderIPv6)
			for _, line := range lines {
				records = append(records, map[string]interface{}{
					"domain": domainName,
					"rr":     rr,
					"line":   canonicalAliyunLineValue(line),
					"type":   currentType,
					"value":  previewDNSPlaceholderLabel(currentType, value),
				})
			}
		}
		return records
	default:
		return nil
	}
}

func previewDNSPlaceholderLabel(recordType, value string) string {
	switch strings.ToUpper(strings.TrimSpace(recordType)) {
	case "AAAA":
		return "<new IPv6> " + value
	default:
		return "<new IPv4> " + value
	}
}

func verifyPreviewCatalogOption(issues *[]string, warnings *[]string, field, value string, options []CatalogOption) {
	value = strings.TrimSpace(value)
	field = strings.TrimSpace(field)
	if value == "" || field == "" {
		return
	}
	if len(options) == 0 {
		*warnings = append(*warnings, fmt.Sprintf("%s could not be verified because the provider returned no %s catalog entries", value, field))
		return
	}
	for _, option := range options {
		if strings.EqualFold(strings.TrimSpace(option.Value), value) {
			return
		}
	}
	*issues = append(*issues, fmt.Sprintf("%s was not found in the current %s catalog", value, field))
}

func buildPreviewCandidateMessage(candidate providerPoolCandidate, total int) string {
	name := firstNonEmpty(strings.TrimSpace(candidate.EntryName), strings.TrimSpace(candidate.EntryID))
	parts := []string{fmt.Sprintf("Preview will use %s", name)}
	if group := strings.TrimSpace(candidate.EntryGroup); group != "" {
		parts = append(parts, fmt.Sprintf("from group %s", group))
	}
	if total > 1 {
		parts = append(parts, fmt.Sprintf("(%d matching entries available)", total))
	}
	return strings.Join(parts, " ")
}

func buildPreviewCandidateDetails(candidates []providerPoolCandidate) []map[string]interface{} {
	details := make([]map[string]interface{}, 0, len(candidates))
	for _, candidate := range candidates {
		details = append(details, map[string]interface{}{
			"entry_id":    strings.TrimSpace(candidate.EntryID),
			"entry_name":  strings.TrimSpace(candidate.EntryName),
			"entry_group": strings.TrimSpace(candidate.EntryGroup),
			"preferred":   candidate.Preferred,
			"active":      candidate.Active,
		})
	}
	return details
}

func newPreviewCheck(key, status, title, message string, detail map[string]interface{}) PreviewCheck {
	return PreviewCheck{
		Key:     strings.TrimSpace(key),
		Status:  strings.TrimSpace(status),
		Title:   strings.TrimSpace(title),
		Message: strings.TrimSpace(message),
		Detail:  detail,
	}
}

func previewHasError(checks []PreviewCheck) bool {
	for _, check := range checks {
		if strings.TrimSpace(check.Status) == previewStatusError {
			return true
		}
	}
	return false
}
