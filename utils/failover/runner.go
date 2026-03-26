package failover

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/komari-monitor/komari/common"
	"github.com/komari-monitor/komari/config"
	clientdb "github.com/komari-monitor/komari/database/clients"
	clipboarddb "github.com/komari-monitor/komari/database/clipboard"
	failoverdb "github.com/komari-monitor/komari/database/failover"
	"github.com/komari-monitor/komari/database/models"
	"github.com/komari-monitor/komari/database/tasks"
	"github.com/komari-monitor/komari/utils"
	awscloud "github.com/komari-monitor/komari/utils/cloudprovider/aws"
	"github.com/komari-monitor/komari/utils/cloudprovider/digitalocean"
	linodecloud "github.com/komari-monitor/komari/utils/cloudprovider/linode"
	"github.com/komari-monitor/komari/ws"
	"gorm.io/gorm"
)

var (
	runningTasksMu sync.Mutex
	runningTasks   = map[uint]struct{}{}
)

type executionRunner struct {
	task      models.FailoverTask
	execution *models.FailoverExecution
	startedAt time.Time
	stepSort  int
	attempts  []map[string]interface{}
	succeeded bool
}

type actionOutcome struct {
	IPv4             string
	IPv6             string
	TargetClientUUID string
	NewClientUUID    string
	AutoConnectGroup string
	NewInstanceRef   map[string]interface{}
	NewAddresses     map[string]interface{}
	OldInstanceRef   map[string]interface{}
	CleanupLabel     string
	Cleanup          func() error
	RollbackLabel    string
	Rollback         func() error
}

type currentInstanceCleanup struct {
	Ref       map[string]interface{}
	Addresses map[string]interface{}
	Label     string
	Cleanup   func() error
}

type blockedOutletError struct {
	ClientUUID string
	Status     string
	Message    string
}

func (e *blockedOutletError) Error() string {
	if e == nil {
		return "new outlet connectivity validation failed"
	}
	parts := make([]string, 0, 3)
	if strings.TrimSpace(e.ClientUUID) != "" {
		parts = append(parts, "client "+strings.TrimSpace(e.ClientUUID))
	}
	if strings.TrimSpace(e.Status) != "" {
		parts = append(parts, "status="+strings.TrimSpace(e.Status))
	}
	if strings.TrimSpace(e.Message) != "" {
		parts = append(parts, strings.TrimSpace(e.Message))
	}
	if len(parts) == 0 {
		return "new outlet connectivity validation failed"
	}
	return "new outlet connectivity validation failed: " + strings.Join(parts, ", ")
}

type planExecutionFailureDecision struct {
	Class          string
	RetrySameEntry bool
	Cooldown       time.Duration
}

const providerEntryProvisionRetryLimit = 2

type awsProvisionPayload struct {
	Service             string         `json:"service,omitempty"`
	Region              string         `json:"region,omitempty"`
	Name                string         `json:"name,omitempty"`
	ImageID             string         `json:"image_id,omitempty"`
	InstanceType        string         `json:"instance_type,omitempty"`
	KeyName             string         `json:"key_name,omitempty"`
	SubnetID            string         `json:"subnet_id,omitempty"`
	SecurityGroupIDs    []string       `json:"security_group_ids,omitempty"`
	UserData            string         `json:"user_data,omitempty"`
	AssignPublicIP      bool           `json:"assign_public_ip"`
	Tags                []awscloud.Tag `json:"tags,omitempty"`
	AvailabilityZone    string         `json:"availability_zone,omitempty"`
	BlueprintID         string         `json:"blueprint_id,omitempty"`
	BundleID            string         `json:"bundle_id,omitempty"`
	KeyPairName         string         `json:"key_pair_name,omitempty"`
	IPAddressType       string         `json:"ip_address_type,omitempty"`
	CleanupInstanceID   string         `json:"cleanup_instance_id,omitempty"`
	CleanupInstanceName string         `json:"cleanup_instance_name,omitempty"`
}

type digitalOceanProvisionPayload struct {
	Name             string   `json:"name,omitempty"`
	Region           string   `json:"region,omitempty"`
	Size             string   `json:"size,omitempty"`
	Image            string   `json:"image,omitempty"`
	Backups          bool     `json:"backups"`
	IPv6             bool     `json:"ipv6"`
	Monitoring       bool     `json:"monitoring"`
	Tags             []string `json:"tags,omitempty"`
	UserData         string   `json:"user_data,omitempty"`
	VPCUUID          string   `json:"vpc_uuid,omitempty"`
	RootPasswordMode string   `json:"root_password_mode,omitempty"`
	RootPassword     string   `json:"root_password,omitempty"`
	CleanupDropletID int      `json:"cleanup_droplet_id,omitempty"`
}

type linodeProvisionPayload struct {
	Label             string   `json:"label,omitempty"`
	Region            string   `json:"region,omitempty"`
	Type              string   `json:"type,omitempty"`
	Image             string   `json:"image,omitempty"`
	AuthorizedKeys    []string `json:"authorized_keys,omitempty"`
	BackupsEnabled    bool     `json:"backups_enabled"`
	Booted            bool     `json:"booted"`
	Tags              []string `json:"tags,omitempty"`
	UserData          string   `json:"user_data,omitempty"`
	RootPasswordMode  string   `json:"root_password_mode,omitempty"`
	RootPassword      string   `json:"root_password,omitempty"`
	CleanupInstanceID int      `json:"cleanup_instance_id,omitempty"`
}

type awsRebindPayload struct {
	Service      string `json:"service,omitempty"`
	Region       string `json:"region,omitempty"`
	InstanceID   string `json:"instance_id,omitempty"`
	PrivateIP    string `json:"private_ip,omitempty"`
	InstanceName string `json:"instance_name,omitempty"`
	StaticIPName string `json:"static_ip_name,omitempty"`
}

func RunScheduledWork() error {
	taskList, err := failoverdb.ListEnabledTasks()
	if err != nil {
		return err
	}

	latestReports := ws.GetLatestReport()
	now := time.Now()
	for _, task := range taskList {
		taskCopy := task
		report := latestReports[strings.TrimSpace(taskCopy.WatchClientUUID)]
		shouldTrigger, statusFields, reason := evaluateTaskHealth(&taskCopy, report, now)
		if len(statusFields) > 0 {
			if err := failoverdb.UpdateTaskFields(taskCopy.ID, statusFields); err != nil {
				log.Printf("failover: failed to update task %d status: %v", taskCopy.ID, err)
			}
		}
		if !shouldTrigger {
			continue
		}
		if _, err := queueExecution(&taskCopy, report, reason); err != nil {
			log.Printf("failover: failed to queue task %d: %v", taskCopy.ID, err)
		}
	}

	return nil
}

func RunTaskNowForUser(userUUID string, taskID uint) (*models.FailoverExecution, error) {
	task, err := failoverdb.GetTaskByIDForUser(userUUID, taskID)
	if err != nil {
		return nil, err
	}
	latestReports := ws.GetLatestReport()
	return queueExecution(task, latestReports[strings.TrimSpace(task.WatchClientUUID)], "manual run")
}

func evaluateTaskHealth(task *models.FailoverTask, report *common.Report, now time.Time) (bool, map[string]interface{}, string) {
	fields := map[string]interface{}{}
	if task == nil {
		return false, fields, ""
	}

	if task.CooldownSeconds > 0 && task.LastTriggeredAt != nil {
		nextRun := task.LastTriggeredAt.ToTime().Add(time.Duration(task.CooldownSeconds) * time.Second)
		if nextRun.After(now) {
			fields["last_status"] = models.FailoverTaskStatusCooldown
			fields["last_message"] = "cooldown until " + nextRun.UTC().Format(time.RFC3339)
			return false, fields, ""
		}
	}

	if strings.TrimSpace(task.WatchClientUUID) == "" {
		fields["last_status"] = models.FailoverTaskStatusUnknown
		fields["last_message"] = "task is not initialized"
		fields["trigger_failure_count"] = 0
		return false, fields, ""
	}

	if report == nil || report.CNConnectivity == nil {
		return evaluateMissingReportHealth(task, fields, "cn_connectivity report is unavailable")
	}

	reportTime := report.UpdatedAt
	if report.CNConnectivity.CheckedAt.After(reportTime) {
		reportTime = report.CNConnectivity.CheckedAt
	}
	if reportTime.IsZero() || now.Sub(reportTime) > time.Duration(task.StaleAfterSeconds)*time.Second {
		return evaluateMissingReportHealth(task, fields, "latest report is stale")
	}

	fields["trigger_failure_count"] = 0

	if report.CNConnectivity.Status == "blocked_suspected" && report.CNConnectivity.ConsecutiveFailures >= task.FailureThreshold {
		fields["last_status"] = models.FailoverTaskStatusTriggered
		fields["last_message"] = fmt.Sprintf("cn_connectivity blocked_suspected (%d failures)", report.CNConnectivity.ConsecutiveFailures)
		return true, fields, fields["last_message"].(string)
	}

	fields["last_status"] = models.FailoverTaskStatusHealthy
	fields["last_message"] = report.CNConnectivity.Status
	return false, fields, ""
}

func evaluateMissingReportHealth(task *models.FailoverTask, fields map[string]interface{}, baseMessage string) (bool, map[string]interface{}, string) {
	if fields == nil {
		fields = map[string]interface{}{}
	}
	threshold := task.FailureThreshold
	if threshold <= 0 {
		threshold = 2
	}
	failures := task.TriggerFailureCount + 1
	fields["trigger_failure_count"] = failures
	if failures >= threshold {
		fields["last_status"] = models.FailoverTaskStatusTriggered
		fields["last_message"] = fmt.Sprintf("%s (%d/%d)", strings.TrimSpace(baseMessage), failures, threshold)
		return true, fields, fields["last_message"].(string)
	}
	fields["last_status"] = models.FailoverTaskStatusUnknown
	fields["last_message"] = fmt.Sprintf("%s (%d/%d)", strings.TrimSpace(baseMessage), failures, threshold)
	return false, fields, ""
}

func queueExecution(task *models.FailoverTask, report *common.Report, reason string) (*models.FailoverExecution, error) {
	if task == nil {
		return nil, errors.New("task is required")
	}

	if !claimTaskRun(task.ID) {
		return nil, fmt.Errorf("failover task %d is already running", task.ID)
	}

	active, err := failoverdb.HasActiveExecution(task.ID)
	if err != nil {
		releaseTaskRun(task.ID)
		return nil, err
	}
	if active {
		releaseTaskRun(task.ID)
		return nil, fmt.Errorf("failover task %d already has an active execution", task.ID)
	}

	now := time.Now()
	snapshot := buildTriggerSnapshot(report)
	execution, err := failoverdb.CreateExecution(&models.FailoverExecution{
		TaskID:          task.ID,
		Status:          models.FailoverExecutionStatusQueued,
		TriggerReason:   strings.TrimSpace(reason),
		WatchClientUUID: task.WatchClientUUID,
		TriggerSnapshot: snapshot,
		DNSProvider:     task.DNSProvider,
		OldClientUUID:   task.WatchClientUUID,
		OldInstanceRef:  strings.TrimSpace(task.CurrentInstanceRef),
		OldAddresses:    marshalJSON(map[string]interface{}{"current_address": strings.TrimSpace(task.CurrentAddress)}),
		StartedAt:       models.FromTime(now),
	})
	if err != nil {
		releaseTaskRun(task.ID)
		return nil, err
	}

	if err := failoverdb.UpdateTaskFields(task.ID, map[string]interface{}{
		"last_execution_id": execution.ID,
		"last_status":       models.FailoverTaskStatusRunning,
		"last_message":      strings.TrimSpace(reason),
		"last_triggered_at": models.FromTime(now),
	}); err != nil {
		releaseTaskRun(task.ID)
		return nil, err
	}

	go func(taskCopy models.FailoverTask, execCopy models.FailoverExecution, reportCopy *common.Report) {
		defer releaseTaskRun(taskCopy.ID)
		runner := &executionRunner{
			task:      taskCopy,
			execution: &execCopy,
			startedAt: now,
		}
		runner.run(reportCopy)
	}(*task, *execution, cloneReport(report))

	return execution, nil
}

func (r *executionRunner) run(report *common.Report) {
	detectStep := r.startStep("detect", "Detect Trigger", map[string]interface{}{
		"reason": strings.TrimSpace(r.execution.TriggerReason),
	})
	if report != nil && report.CNConnectivity != nil {
		r.finishStep(detectStep, models.FailoverStepStatusSuccess, "trigger snapshot recorded", map[string]interface{}{
			"status":               report.CNConnectivity.Status,
			"consecutive_failures": report.CNConnectivity.ConsecutiveFailures,
		})
	} else {
		r.finishStep(detectStep, models.FailoverStepStatusSuccess, "manual trigger without live cn_connectivity snapshot", nil)
	}

	plans := make([]models.FailoverPlan, 0, len(r.task.Plans))
	for _, plan := range r.task.Plans {
		if plan.Enabled {
			plans = append(plans, plan)
		}
	}
	if len(plans) == 0 {
		r.failExecution("no enabled failover plans are configured")
		return
	}

	for _, plan := range plans {
		attemptStep := r.startStep(fmt.Sprintf("plan:%d", plan.ID), "Plan Attempt", map[string]interface{}{
			"plan_id":      plan.ID,
			"provider":     plan.Provider,
			"action_type":  plan.ActionType,
			"priority":     plan.Priority,
			"plan_name":    plan.Name,
			"entry_id":     plan.ProviderEntryID,
			"auto_connect": plan.AutoConnectGroup,
		})
		_ = failoverdb.UpdateExecutionFields(r.execution.ID, map[string]interface{}{
			"selected_plan_id": plan.ID,
		})

		outcome, selectedEntryID, entryAttempts, err := r.executePlan(plan)
		attempt := map[string]interface{}{
			"plan_id":            plan.ID,
			"provider":           plan.Provider,
			"action_type":        plan.ActionType,
			"preferred_entry_id": plan.ProviderEntryID,
		}
		if selectedEntryID != "" {
			attempt["provider_entry_id"] = selectedEntryID
		}
		if len(entryAttempts) > 0 {
			attempt["provider_entry_attempts"] = entryAttempts
		}
		if err != nil {
			attempt["status"] = "failed"
			attempt["error"] = err.Error()
			r.attempts = append(r.attempts, attempt)
			_ = failoverdb.UpdateExecutionFields(r.execution.ID, map[string]interface{}{
				"attempted_plans": marshalJSON(r.attempts),
			})
			r.finishStep(attemptStep, models.FailoverStepStatusFailed, err.Error(), attempt)
			continue
		}

		attempt["status"] = "success"
		r.attempts = append(r.attempts, attempt)
		_ = failoverdb.UpdateExecutionFields(r.execution.ID, map[string]interface{}{
			"attempted_plans":  marshalJSON(r.attempts),
			"new_client_uuid":  emptyToNilString(outcome.NewClientUUID),
			"new_instance_ref": marshalJSON(outcome.NewInstanceRef),
			"new_addresses":    marshalJSON(outcome.NewAddresses),
			"old_instance_ref": marshalJSON(outcome.OldInstanceRef),
		})
		r.finishStep(attemptStep, models.FailoverStepStatusSuccess, "plan completed", attempt)
		r.succeedExecution(outcome)
		return
	}

	r.failExecution("all failover plans failed")
}

func (r *executionRunner) executePlan(plan models.FailoverPlan) (*actionOutcome, string, []map[string]interface{}, error) {
	return r.executePlanActionWithProviderPool(plan)
}

func (r *executionRunner) executePlanActionWithProviderPool(plan models.FailoverPlan) (*actionOutcome, string, []map[string]interface{}, error) {
	candidates, err := listProviderPoolCandidates(r.task.UserID, plan)
	if err != nil {
		return nil, "", nil, err
	}

	entryAttempts := make([]map[string]interface{}, 0, len(candidates))
	for _, candidate := range candidates {
		candidateDetail := map[string]interface{}{
			"entry_id":   candidate.EntryID,
			"entry_name": candidate.EntryName,
		}
		if candidate.Preferred {
			candidateDetail["preferred"] = true
		}
		if candidate.Active {
			candidateDetail["active"] = true
		}

		lease, availability, reserveErr := acquireProviderEntryLease(r.task.UserID, plan, candidate)
		if len(availability) > 0 {
			candidateDetail["availability"] = availability
		}
		if reserveErr != nil && strings.TrimSpace(plan.ActionType) == models.FailoverActionProvisionInstance && strings.TrimSpace(stringMapValue(availability, "status")) == "full" {
			recycledDetail, recycleErr := r.recycleCurrentOutletForCandidate(plan, candidate)
			if recycleErr != nil {
				candidateDetail["recycle_error"] = recycleErr.Error()
			} else if len(recycledDetail) > 0 {
				candidateDetail["recycled_current_instance"] = recycledDetail
				invalidateProviderEntrySnapshot(r.task.UserID, plan.Provider, candidate.EntryID)
				lease, availability, reserveErr = acquireProviderEntryLease(r.task.UserID, plan, candidate)
				if len(availability) > 0 {
					candidateDetail["availability"] = availability
				}
			}
		}
		if reserveErr != nil {
			candidateDetail["status"] = "skipped"
			candidateDetail["error"] = reserveErr.Error()
			entryAttempts = append(entryAttempts, candidateDetail)
			continue
		}

		selectedPlan := plan
		selectedPlan.ProviderEntryID = candidate.EntryID
		maxAttempts := providerEntryMaxAttempts(selectedPlan)
		for attemptNumber := 1; attemptNumber <= maxAttempts; attemptNumber++ {
			attemptDetail := make(map[string]interface{}, len(candidateDetail)+1)
			for key, value := range candidateDetail {
				attemptDetail[key] = value
			}
			attemptDetail["attempt"] = attemptNumber

			finishOperation := func() {}
			if shouldSerializeProviderOperation(selectedPlan) {
				serialDone, serialErr := lease.BeginSerializedOperation(providerEntryOperationSpacing(selectedPlan))
				if serialErr != nil {
					lease.Release(false)
					attemptDetail["status"] = "skipped"
					attemptDetail["error"] = serialErr.Error()
					entryAttempts = append(entryAttempts, attemptDetail)
					goto nextCandidate
				}
				finishOperation = serialDone
			}

			outcome, actionErr := r.executePlanAction(selectedPlan)
			if actionErr != nil {
				lease.Release(false)
				decision := classifyProviderFailure(plan.Provider, actionErr)
				applyProviderEntryFailure(r.task.UserID, plan.Provider, candidate.EntryID, decision, actionErr)
				finishOperation()
				attemptDetail["status"] = "failed"
				attemptDetail["error"] = actionErr.Error()
				attemptDetail["failure_class"] = decision.Class
				entryAttempts = append(entryAttempts, attemptDetail)
				goto nextCandidate
			}
			if strings.TrimSpace(selectedPlan.ActionType) == models.FailoverActionProvisionInstance {
				if cleanupErr := r.attachCurrentOutletCleanup(outcome); cleanupErr != nil {
					lease.Release(false)
					finishOperation()
					attemptDetail["status"] = "failed"
					attemptDetail["error"] = cleanupErr.Error()
					attemptDetail["failure_class"] = "post_provision_error"
					entryAttempts = append(entryAttempts, attemptDetail)
					goto nextCandidate
				}
			}

			finalizeErr := r.finalizePlan(selectedPlan, outcome)
			if finalizeErr != nil {
				finishOperation()
				executionDecision := classifyPlanExecutionFailure(finalizeErr)
				attemptDetail["error"] = finalizeErr.Error()
				attemptDetail["failure_class"] = executionDecision.Class
				if executionDecision.RetrySameEntry && attemptNumber < maxAttempts {
					attemptDetail["status"] = "retry"
					entryAttempts = append(entryAttempts, attemptDetail)
					continue
				}
				if executionDecision.Cooldown > 0 {
					applyProviderEntryFailure(r.task.UserID, plan.Provider, candidate.EntryID, providerFailureDecision{
						Class:    executionDecision.Class,
						Cooldown: executionDecision.Cooldown,
					}, finalizeErr)
				}
				lease.Release(false)
				attemptDetail["status"] = "failed"
				entryAttempts = append(entryAttempts, attemptDetail)
				goto nextCandidate
			}

			lease.Release(strings.TrimSpace(plan.ActionType) == models.FailoverActionProvisionInstance)
			clearProviderEntryCooldown(r.task.UserID, plan.Provider, candidate.EntryID)
			finishOperation()
			attemptDetail["status"] = "success"
			entryAttempts = append(entryAttempts, attemptDetail)
			return outcome, candidate.EntryID, entryAttempts, nil
		}

	nextCandidate:
	}

	return nil, "", entryAttempts, buildProviderPoolUnavailableError(entryAttempts)
}

func buildProviderPoolUnavailableError(entryAttempts []map[string]interface{}) error {
	base := "no provider entry in the selected pool is currently available"
	summary := summarizeProviderEntryAttempts(entryAttempts)
	if summary == "" {
		return errors.New(base)
	}
	return fmt.Errorf("%s: %s", base, summary)
}

func summarizeProviderEntryAttempts(entryAttempts []map[string]interface{}) string {
	if len(entryAttempts) == 0 {
		return ""
	}

	type entrySummary struct {
		label  string
		reason string
	}

	order := make([]string, 0, len(entryAttempts))
	summaries := make(map[string]entrySummary, len(entryAttempts))
	for index := len(entryAttempts) - 1; index >= 0; index-- {
		attempt := entryAttempts[index]
		entryID := strings.TrimSpace(stringMapValue(attempt, "entry_id"))
		entryName := strings.TrimSpace(stringMapValue(attempt, "entry_name"))
		key := firstNonEmpty(entryID, entryName)
		if key == "" {
			key = fmt.Sprintf("entry-%d", index)
		}
		if _, exists := summaries[key]; exists {
			continue
		}

		reason := describeProviderEntryAttempt(attempt)
		if reason == "" {
			continue
		}

		summaries[key] = entrySummary{
			label:  firstNonEmpty(entryName, entryID, key),
			reason: reason,
		}
		order = append(order, key)
	}

	if len(order) == 0 {
		return ""
	}

	parts := make([]string, 0, len(order))
	for index := len(order) - 1; index >= 0; index-- {
		summary := summaries[order[index]]
		if summary.label == "" {
			parts = append(parts, summary.reason)
			continue
		}
		parts = append(parts, fmt.Sprintf("%s: %s", summary.label, summary.reason))
	}
	return strings.Join(parts, "; ")
}

func describeProviderEntryAttempt(attempt map[string]interface{}) string {
	availability := mapValue(attempt, "availability")
	availabilityStatus := strings.TrimSpace(stringMapValue(availability, "status"))
	switch availabilityStatus {
	case "cooldown":
		reason := strings.TrimSpace(stringMapValue(availability, "reason"))
		until := strings.TrimSpace(stringMapValue(availability, "cooldown_until"))
		switch {
		case reason != "" && until != "":
			return fmt.Sprintf("cooldown until %s (%s)", until, reason)
		case until != "":
			return fmt.Sprintf("cooldown until %s", until)
		case reason != "":
			return "cooldown (" + reason + ")"
		default:
			return "cooldown"
		}
	case "full":
		used := intMapValue(availability, "used")
		limit := intMapValue(availability, "limit")
		free := intMapValue(availability, "free")
		switch {
		case limit > 0:
			return fmt.Sprintf("capacity full (%d/%d used, %d free)", used, limit, free)
		case free == 0:
			return "no available capacity"
		}
	case "reserved":
		return "reserved by another running task"
	}

	failureClass := strings.TrimSpace(stringMapValue(attempt, "failure_class"))
	errorMessage := strings.TrimSpace(stringMapValue(attempt, "error"))
	switch failureClass {
	case "rate_limited":
		if errorMessage != "" {
			return "rate limited (" + errorMessage + ")"
		}
		return "rate limited"
	case "quota_exhausted":
		if errorMessage != "" {
			return "quota exhausted (" + errorMessage + ")"
		}
		return "quota exhausted"
	case "billing_locked":
		if errorMessage != "" {
			return "billing locked (" + errorMessage + ")"
		}
		return "billing locked"
	case "auth_invalid":
		if errorMessage != "" {
			return "credential invalid (" + errorMessage + ")"
		}
		return "credential invalid"
	case "outlet_blocked":
		if errorMessage != "" {
			return "new outlet blocked (" + errorMessage + ")"
		}
		return "new outlet blocked"
	case "post_provision_error":
		if errorMessage != "" {
			return errorMessage
		}
		return "post-provision step failed"
	}

	if errorMessage != "" {
		return errorMessage
	}
	status := strings.TrimSpace(stringMapValue(attempt, "status"))
	if status != "" && status != "success" {
		return status
	}
	return ""
}

func mapValue(source map[string]interface{}, key string) map[string]interface{} {
	if source == nil {
		return nil
	}
	value, ok := source[key]
	if !ok {
		return nil
	}
	object, ok := value.(map[string]interface{})
	if !ok {
		return nil
	}
	return object
}

func stringMapValue(source map[string]interface{}, key string) string {
	if source == nil {
		return ""
	}
	value, ok := source[key]
	if !ok || value == nil {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return typed
	case fmt.Stringer:
		return typed.String()
	default:
		return fmt.Sprintf("%v", typed)
	}
}

func intMapValue(source map[string]interface{}, key string) int {
	if source == nil {
		return 0
	}
	value, ok := source[key]
	if !ok || value == nil {
		return 0
	}
	switch typed := value.(type) {
	case int:
		return typed
	case int8:
		return int(typed)
	case int16:
		return int(typed)
	case int32:
		return int(typed)
	case int64:
		return int(typed)
	case uint:
		return int(typed)
	case uint8:
		return int(typed)
	case uint16:
		return int(typed)
	case uint32:
		return int(typed)
	case uint64:
		return int(typed)
	case float32:
		return int(typed)
	case float64:
		return int(typed)
	case json.Number:
		parsed, _ := typed.Int64()
		return int(parsed)
	default:
		return 0
	}
}

func (r *executionRunner) executePlanAction(plan models.FailoverPlan) (*actionOutcome, error) {
	var (
		outcome *actionOutcome
		err     error
	)

	switch plan.ActionType {
	case models.FailoverActionProvisionInstance:
		outcome, err = r.executeProvisionPlan(plan)
	case models.FailoverActionRebindPublicIP:
		outcome, err = r.executeRebindPlan(plan)
	default:
		return nil, fmt.Errorf("unsupported failover action: %s", plan.ActionType)
	}
	if err != nil {
		return nil, err
	}

	return outcome, nil
}

func providerEntryMaxAttempts(plan models.FailoverPlan) int {
	if strings.TrimSpace(plan.ActionType) == models.FailoverActionProvisionInstance {
		return providerEntryProvisionRetryLimit
	}
	return 1
}

func classifyPlanExecutionFailure(err error) planExecutionFailureDecision {
	var blockedErr *blockedOutletError
	if errors.As(err, &blockedErr) {
		return planExecutionFailureDecision{
			Class:          "outlet_blocked",
			RetrySameEntry: true,
			Cooldown:       providerEntryCooldownTransient,
		}
	}

	return planExecutionFailureDecision{
		Class:          "post_provision_error",
		RetrySameEntry: false,
	}
}

func (r *executionRunner) finalizePlan(plan models.FailoverPlan, outcome *actionOutcome) (err error) {
	defer func() {
		if err == nil || outcome == nil || outcome.Rollback == nil {
			return
		}
		if rollbackErr := r.rollbackOutcome(outcome, err); rollbackErr != nil {
			err = rollbackErr
		}
	}()

	targetClientUUID := strings.TrimSpace(outcome.TargetClientUUID)
	if targetClientUUID == "" && strings.TrimSpace(outcome.AutoConnectGroup) != "" {
		waitStep := r.startStep("wait_agent", "Wait For Agent", map[string]interface{}{
			"group": outcome.AutoConnectGroup,
		})
		_ = failoverdb.UpdateExecutionFields(r.execution.ID, map[string]interface{}{
			"status": models.FailoverExecutionStatusWaitingAgent,
		})

		clientUUID, waitErr := waitForClientByGroup(r.task.UserID, outcome.AutoConnectGroup, r.task.WatchClientUUID, r.startedAt, plan.WaitAgentTimeoutSec)
		if waitErr != nil {
			r.finishStep(waitStep, models.FailoverStepStatusFailed, waitErr.Error(), nil)
			return waitErr
		}
		targetClientUUID = clientUUID
		outcome.NewClientUUID = clientUUID
		outcome.TargetClientUUID = clientUUID
		_ = failoverdb.UpdateExecutionFields(r.execution.ID, map[string]interface{}{
			"new_client_uuid": clientUUID,
		})
		r.finishStep(waitStep, models.FailoverStepStatusSuccess, "agent connected", map[string]interface{}{
			"client_uuid": clientUUID,
		})
	}

	if validationErr := r.validateProvisionedOutlet(plan, outcome, targetClientUUID); validationErr != nil {
		return validationErr
	}

	if plan.ScriptClipboardID != nil {
		if targetClientUUID == "" {
			return errors.New("script execution requires a target client but none became available")
		}

		scriptStep := r.startStep("run_script", "Run Script", map[string]interface{}{
			"clipboard_id": *plan.ScriptClipboardID,
			"client_uuid":  targetClientUUID,
		})
		_ = failoverdb.UpdateExecutionFields(r.execution.ID, map[string]interface{}{
			"status": models.FailoverExecutionStatusRunningScript,
		})
		if err := r.runScript(plan, targetClientUUID); err != nil {
			r.finishStep(scriptStep, models.FailoverStepStatusFailed, err.Error(), nil)
			return err
		}
		r.finishStep(scriptStep, models.FailoverStepStatusSuccess, "script finished successfully", nil)
	}

	if strings.TrimSpace(r.task.DNSProvider) == "" || strings.TrimSpace(r.task.DNSEntryID) == "" {
		dnsStep := r.startStep("switch_dns", "Switch DNS", map[string]interface{}{
			"configured": false,
		})
		skippedResult := map[string]interface{}{
			"message": "dns switching is not configured for this task",
		}
		_ = failoverdb.UpdateExecutionFields(r.execution.ID, map[string]interface{}{
			"dns_status": models.FailoverDNSStatusSkipped,
			"dns_result": marshalJSON(skippedResult),
		})
		r.finishStep(dnsStep, models.FailoverStepStatusSkipped, "dns switching skipped", skippedResult)
		return nil
	}

	dnsStep := r.startStep("switch_dns", "Switch DNS", map[string]interface{}{
		"provider": r.task.DNSProvider,
	})
	_ = failoverdb.UpdateExecutionFields(r.execution.ID, map[string]interface{}{
		"status": models.FailoverExecutionStatusSwitchingDNS,
	})
	dnsResult, err := applyDNSRecord(r.task.UserID, r.task.DNSProvider, r.task.DNSEntryID, r.task.DNSPayload, outcome.IPv4, outcome.IPv6)
	if err != nil {
		_ = failoverdb.UpdateExecutionFields(r.execution.ID, map[string]interface{}{
			"dns_status": models.FailoverDNSStatusFailed,
			"dns_result": marshalJSON(map[string]interface{}{"error": err.Error()}),
		})
		r.finishStep(dnsStep, models.FailoverStepStatusFailed, err.Error(), nil)
		return err
	}
	_ = failoverdb.UpdateExecutionFields(r.execution.ID, map[string]interface{}{
		"dns_status": models.FailoverDNSStatusSuccess,
		"dns_result": marshalJSON(dnsResult),
	})
	r.finishStep(dnsStep, models.FailoverStepStatusSuccess, "dns updated", dnsResult)
	return nil
}

func (r *executionRunner) validateProvisionedOutlet(plan models.FailoverPlan, outcome *actionOutcome, targetClientUUID string) error {
	if strings.TrimSpace(plan.ActionType) != models.FailoverActionProvisionInstance {
		return nil
	}

	validateStep := r.startStep("validate_outlet", "Validate New Outlet", map[string]interface{}{
		"client_uuid": targetClientUUID,
	})

	if strings.TrimSpace(targetClientUUID) == "" {
		r.finishStep(validateStep, models.FailoverStepStatusSkipped, "connectivity validation skipped because no target client is available", nil)
		return nil
	}

	report, err := waitForHealthyClientConnectivity(r.task.UserID, targetClientUUID, r.startedAt)
	if err != nil {
		detail := map[string]interface{}{
			"client_uuid": targetClientUUID,
		}
		if report != nil && report.CNConnectivity != nil {
			detail["status"] = strings.TrimSpace(report.CNConnectivity.Status)
			detail["message"] = strings.TrimSpace(report.CNConnectivity.Message)
			detail["checked_at"] = report.CNConnectivity.CheckedAt
			detail["consecutive_failures"] = report.CNConnectivity.ConsecutiveFailures
		}
		r.finishStep(validateStep, models.FailoverStepStatusFailed, err.Error(), detail)
		return err
	}

	successDetail := map[string]interface{}{
		"client_uuid": targetClientUUID,
	}
	if report != nil && report.CNConnectivity != nil {
		successDetail["status"] = strings.TrimSpace(report.CNConnectivity.Status)
		successDetail["target"] = strings.TrimSpace(report.CNConnectivity.Target)
		successDetail["latency"] = report.CNConnectivity.Latency
		successDetail["checked_at"] = report.CNConnectivity.CheckedAt
	}
	r.finishStep(validateStep, models.FailoverStepStatusSuccess, "new outlet connectivity looks healthy", successDetail)
	return nil
}

func waitForHealthyClientConnectivity(userUUID, clientUUID string, startedAt time.Time) (*common.Report, error) {
	timeout := failoverConnectivityValidationTimeout(userUUID)
	deadline := time.Now().Add(timeout)
	clientUUID = strings.TrimSpace(clientUUID)
	var lastReport *common.Report

	for time.Now().Before(deadline) {
		report := ws.GetLatestReport()[clientUUID]
		if report != nil && report.CNConnectivity != nil {
			lastReport = cloneReport(report)
			reportTime := report.UpdatedAt
			if report.CNConnectivity.CheckedAt.After(reportTime) {
				reportTime = report.CNConnectivity.CheckedAt
			}
			if reportTime.After(startedAt) || report.CNConnectivity.CheckedAt.After(startedAt) {
				status := strings.ToLower(strings.TrimSpace(report.CNConnectivity.Status))
				switch status {
				case "ok":
					return cloneReport(report), nil
				case "blocked_suspected":
					return cloneReport(report), &blockedOutletError{
						ClientUUID: clientUUID,
						Status:     report.CNConnectivity.Status,
						Message:    report.CNConnectivity.Message,
					}
				}
			}
		}
		time.Sleep(5 * time.Second)
	}

	if lastReport != nil && lastReport.CNConnectivity != nil {
		return lastReport, fmt.Errorf("timed out waiting for a healthy cn_connectivity report from client %s (last status: %s)", clientUUID, strings.TrimSpace(lastReport.CNConnectivity.Status))
	}
	return nil, fmt.Errorf("timed out waiting for cn_connectivity report from client %s", clientUUID)
}

func persistDigitalOceanRootPassword(userUUID string, addition *digitalocean.Addition, token *digitalocean.TokenRecord, dropletID int, dropletName, passwordMode, rootPassword string) error {
	if addition == nil || token == nil || dropletID <= 0 || strings.TrimSpace(rootPassword) == "" {
		return nil
	}
	if err := token.SaveDropletPassword(dropletID, dropletName, passwordMode, rootPassword, time.Now()); err != nil {
		log.Printf("failover: failed to save DigitalOcean root password for droplet %d: %v", dropletID, err)
		return err
	}
	if err := saveDigitalOceanAddition(userUUID, addition); err != nil {
		token.RemoveSavedDropletPassword(dropletID)
		log.Printf("failover: failed to persist DigitalOcean root password for droplet %d: %v", dropletID, err)
		return err
	}
	return nil
}

func removeSavedDigitalOceanRootPassword(userUUID string, addition *digitalocean.Addition, token *digitalocean.TokenRecord, dropletID int) {
	if addition == nil || token == nil || dropletID <= 0 {
		return
	}
	if !token.RemoveSavedDropletPassword(dropletID) {
		return
	}
	if err := saveDigitalOceanAddition(userUUID, addition); err != nil {
		log.Printf("failover: failed to remove saved DigitalOcean root password for droplet %d: %v", dropletID, err)
	}
}

func persistLinodeRootPassword(userUUID string, addition *linodecloud.Addition, token *linodecloud.TokenRecord, instanceID int, instanceLabel, passwordMode, rootPassword string) error {
	if addition == nil || token == nil || instanceID <= 0 || strings.TrimSpace(rootPassword) == "" {
		return nil
	}
	if err := token.SaveInstancePassword(instanceID, instanceLabel, passwordMode, rootPassword, time.Now()); err != nil {
		log.Printf("failover: failed to save Linode root password for instance %d: %v", instanceID, err)
		return err
	}
	if err := saveLinodeAddition(userUUID, addition); err != nil {
		token.RemoveSavedInstancePassword(instanceID)
		log.Printf("failover: failed to persist Linode root password for instance %d: %v", instanceID, err)
		return err
	}
	return nil
}

func removeSavedLinodeRootPassword(userUUID string, addition *linodecloud.Addition, token *linodecloud.TokenRecord, instanceID int) {
	if addition == nil || token == nil || instanceID <= 0 {
		return
	}
	if !token.RemoveSavedInstancePassword(instanceID) {
		return
	}
	if err := saveLinodeAddition(userUUID, addition); err != nil {
		log.Printf("failover: failed to remove saved Linode root password for instance %d: %v", instanceID, err)
	}
}

func failoverConnectivityValidationTimeout(userUUID string) time.Duration {
	interval, err := config.GetAsForUser[int](userUUID, config.CNConnectivityIntervalKey, 60)
	if err != nil || interval <= 0 {
		interval = 60
	}
	timeoutSeconds := interval*2 + 20
	if timeoutSeconds < 90 {
		timeoutSeconds = 90
	}
	return time.Duration(timeoutSeconds) * time.Second
}

func (r *executionRunner) rollbackOutcome(outcome *actionOutcome, originalErr error) error {
	if outcome == nil || outcome.Rollback == nil {
		return originalErr
	}

	rollbackStep := r.startStep("rollback_new", "Cleanup Failed New Instance", map[string]interface{}{
		"label": outcome.RollbackLabel,
		"error": func() string {
			if originalErr == nil {
				return ""
			}
			return originalErr.Error()
		}(),
	})

	if err := outcome.Rollback(); err != nil {
		detail := map[string]interface{}{
			"label": outcome.RollbackLabel,
			"error": err.Error(),
		}
		r.finishStep(rollbackStep, models.FailoverStepStatusFailed, err.Error(), detail)
		return fmt.Errorf("%w; rollback failed: %v", originalErr, err)
	}

	detail := map[string]interface{}{
		"label": outcome.RollbackLabel,
	}
	r.finishStep(rollbackStep, models.FailoverStepStatusSuccess, "failed new instance deleted", detail)
	return originalErr
}

func (r *executionRunner) executeProvisionPlan(plan models.FailoverPlan) (*actionOutcome, error) {
	provisionStep := r.startStep("provision", "Provision Instance", map[string]interface{}{
		"provider": plan.Provider,
	})
	_ = failoverdb.UpdateExecutionFields(r.execution.ID, map[string]interface{}{
		"status": models.FailoverExecutionStatusProvisioning,
	})

	var (
		outcome *actionOutcome
		err     error
	)
	switch plan.Provider {
	case "aws":
		outcome, err = provisionAWSInstance(r.task.UserID, plan)
	case "digitalocean":
		outcome, err = provisionDigitalOceanDroplet(r.task.UserID, plan)
	case "linode":
		outcome, err = provisionLinodeInstance(r.task.UserID, plan)
	default:
		err = fmt.Errorf("unsupported provision provider: %s", plan.Provider)
	}
	if err != nil {
		r.finishStep(provisionStep, models.FailoverStepStatusFailed, err.Error(), nil)
		return nil, err
	}

	_ = failoverdb.UpdateExecutionFields(r.execution.ID, map[string]interface{}{
		"new_instance_ref": marshalJSON(outcome.NewInstanceRef),
		"new_addresses":    marshalJSON(outcome.NewAddresses),
	})
	r.finishStep(provisionStep, models.FailoverStepStatusSuccess, "instance provisioned", outcome.NewInstanceRef)
	return outcome, nil
}

func (r *executionRunner) executeRebindPlan(plan models.FailoverPlan) (*actionOutcome, error) {
	rebindStep := r.startStep("rebind_ip", "Rebind Public IP", map[string]interface{}{
		"provider": plan.Provider,
	})
	_ = failoverdb.UpdateExecutionFields(r.execution.ID, map[string]interface{}{
		"status": models.FailoverExecutionStatusRebindingIP,
	})

	var (
		outcome *actionOutcome
		err     error
	)
	switch plan.Provider {
	case "aws":
		outcome, err = rebindAWSIPAddress(r.task, plan)
	default:
		err = fmt.Errorf("unsupported rebind provider: %s", plan.Provider)
	}
	if err != nil {
		r.finishStep(rebindStep, models.FailoverStepStatusFailed, err.Error(), nil)
		return nil, err
	}

	_ = failoverdb.UpdateExecutionFields(r.execution.ID, map[string]interface{}{
		"new_addresses":    marshalJSON(outcome.NewAddresses),
		"old_instance_ref": marshalJSON(outcome.OldInstanceRef),
	})
	r.finishStep(rebindStep, models.FailoverStepStatusSuccess, "public ip rebound", outcome.NewAddresses)
	return outcome, nil
}

func (r *executionRunner) runScript(plan models.FailoverPlan, clientUUID string) error {
	clipboard, err := clipboarddb.GetClipboardByIDForUser(*plan.ScriptClipboardID, r.task.UserID)
	if err != nil {
		return err
	}

	result, err := dispatchScriptToClient(r.task.UserID, clientUUID, clipboard.Text, time.Duration(plan.ScriptTimeoutSec)*time.Second)
	if err != nil {
		status := models.FailoverScriptStatusFailed
		if errors.Is(err, context.DeadlineExceeded) {
			status = models.FailoverScriptStatusTimeout
		}
		_ = failoverdb.UpdateExecutionFields(r.execution.ID, map[string]interface{}{
			"script_clipboard_id":     clipboard.Id,
			"script_name_snapshot":    clipboard.Name,
			"script_task_id":          result.TaskID,
			"script_status":           status,
			"script_exit_code":        result.ExitCode,
			"script_finished_at":      result.FinishedAt,
			"script_output":           result.Output,
			"script_output_truncated": result.Truncated,
		})
		return err
	}

	_ = failoverdb.UpdateExecutionFields(r.execution.ID, map[string]interface{}{
		"script_clipboard_id":     clipboard.Id,
		"script_name_snapshot":    clipboard.Name,
		"script_task_id":          result.TaskID,
		"script_status":           models.FailoverScriptStatusSuccess,
		"script_exit_code":        result.ExitCode,
		"script_finished_at":      result.FinishedAt,
		"script_output":           result.Output,
		"script_output_truncated": result.Truncated,
	})
	return nil
}

func (r *executionRunner) succeedExecution(outcome *actionOutcome) {
	now := time.Now()
	cleanupStatus := models.FailoverCleanupStatusSkipped
	cleanupResult := map[string]interface{}{"message": "cleanup not requested"}

	if r.task.DeleteStrategy != models.FailoverDeleteStrategyKeep && outcome != nil && outcome.Cleanup != nil {
		cleanupStep := r.startStep("cleanup_old", "Cleanup Old Instance", map[string]interface{}{
			"strategy": r.task.DeleteStrategy,
			"label":    outcome.CleanupLabel,
		})
		_ = failoverdb.UpdateExecutionFields(r.execution.ID, map[string]interface{}{
			"status": models.FailoverExecutionStatusCleaningOld,
		})

		if r.task.DeleteStrategy == models.FailoverDeleteStrategyDeleteAfterSuccessDelay && r.task.DeleteDelaySeconds > 0 {
			time.Sleep(time.Duration(r.task.DeleteDelaySeconds) * time.Second)
		}

		if err := outcome.Cleanup(); err != nil {
			cleanupStatus = models.FailoverCleanupStatusFailed
			cleanupResult = map[string]interface{}{"error": err.Error()}
			r.finishStep(cleanupStep, models.FailoverStepStatusFailed, err.Error(), cleanupResult)
		} else {
			oldProvider := strings.TrimSpace(stringMapValue(outcome.OldInstanceRef, "provider"))
			oldEntryID := strings.TrimSpace(providerEntryIDFromRef(outcome.OldInstanceRef))
			if oldProvider != "" && oldEntryID != "" {
				invalidateProviderEntrySnapshot(r.task.UserID, oldProvider, oldEntryID)
			}
			cleanupStatus = models.FailoverCleanupStatusSuccess
			cleanupResult = map[string]interface{}{"message": "old instance deleted"}
			r.finishStep(cleanupStep, models.FailoverStepStatusSuccess, "old instance deleted", cleanupResult)
		}
	}

	fields := map[string]interface{}{
		"status":         models.FailoverExecutionStatusSuccess,
		"finished_at":    models.FromTime(now),
		"cleanup_status": cleanupStatus,
		"cleanup_result": marshalJSON(cleanupResult),
	}
	if outcome != nil {
		fields["new_client_uuid"] = emptyToNilString(firstNonEmpty(outcome.NewClientUUID, outcome.TargetClientUUID))
		fields["new_instance_ref"] = marshalJSON(outcome.NewInstanceRef)
		fields["new_addresses"] = marshalJSON(outcome.NewAddresses)
		fields["old_instance_ref"] = marshalJSON(outcome.OldInstanceRef)
	}
	_ = failoverdb.UpdateExecutionFields(r.execution.ID, fields)

	taskUpdates := map[string]interface{}{
		"last_status":           models.FailoverTaskStatusCooldown,
		"last_message":          "failover completed",
		"last_succeeded_at":     models.FromTime(now),
		"trigger_failure_count": 0,
	}
	if outcome != nil {
		if nextClientUUID := strings.TrimSpace(firstNonEmpty(outcome.NewClientUUID, outcome.TargetClientUUID)); nextClientUUID != "" {
			taskUpdates["watch_client_uuid"] = nextClientUUID
		}
		if nextAddress := primaryOutcomeAddress(outcome); nextAddress != "" {
			taskUpdates["current_address"] = nextAddress
		}
		if nextRef := effectiveCurrentInstanceRef(outcome); len(nextRef) > 0 {
			taskUpdates["current_instance_ref"] = marshalJSON(nextRef)
		}
	}
	_ = failoverdb.UpdateTaskFields(r.task.ID, taskUpdates)
	r.succeeded = true
}

func (r *executionRunner) failExecution(message string) {
	now := time.Now()
	_ = failoverdb.UpdateExecutionFields(r.execution.ID, map[string]interface{}{
		"status":        models.FailoverExecutionStatusFailed,
		"error_message": strings.TrimSpace(message),
		"finished_at":   models.FromTime(now),
	})
	_ = failoverdb.UpdateTaskFields(r.task.ID, map[string]interface{}{
		"last_status":    models.FailoverTaskStatusFailed,
		"last_message":   strings.TrimSpace(message),
		"last_failed_at": models.FromTime(now),
	})
}

func (r *executionRunner) startStep(key, label string, detail interface{}) *models.FailoverExecutionStep {
	r.stepSort++
	step, err := failoverdb.CreateExecutionStep(&models.FailoverExecutionStep{
		ExecutionID: r.execution.ID,
		Sort:        r.stepSort,
		StepKey:     key,
		StepLabel:   label,
		Status:      models.FailoverStepStatusRunning,
		Detail:      marshalJSON(detail),
		StartedAt:   ptrLocalTime(time.Now()),
	})
	if err != nil {
		log.Printf("failover: failed to create step for execution %d: %v", r.execution.ID, err)
		return nil
	}
	return step
}

func (r *executionRunner) finishStep(step *models.FailoverExecutionStep, status, message string, detail interface{}) {
	if step == nil {
		return
	}
	fields := map[string]interface{}{
		"status":      status,
		"message":     strings.TrimSpace(message),
		"detail":      marshalJSON(detail),
		"finished_at": models.FromTime(time.Now()),
	}
	if err := failoverdb.UpdateExecutionStepFields(step.ID, fields); err != nil {
		log.Printf("failover: failed to update step %d: %v", step.ID, err)
	}
}

func effectiveCurrentInstanceRef(outcome *actionOutcome) map[string]interface{} {
	if outcome == nil {
		return nil
	}
	if len(outcome.NewInstanceRef) > 0 {
		return outcome.NewInstanceRef
	}
	if len(outcome.OldInstanceRef) > 0 {
		return outcome.OldInstanceRef
	}
	return nil
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

func providerEntryNameFromRef(ref map[string]interface{}) string {
	return firstNonEmpty(stringMapValue(ref, "provider_entry_name"), stringMapValue(ref, "entry_name"))
}

func sameAddress(target string, values ...string) bool {
	target = strings.TrimSpace(target)
	if target == "" {
		return false
	}
	for _, value := range values {
		if strings.TrimSpace(value) == target {
			return true
		}
	}
	return false
}

func (r *executionRunner) attachCurrentOutletCleanup(outcome *actionOutcome) error {
	if outcome == nil || outcome.Cleanup != nil {
		return nil
	}
	cleanup, err := resolveCurrentInstanceCleanupFromRef(r.task.UserID, parseJSONMap(r.task.CurrentInstanceRef))
	if err != nil {
		return err
	}
	if cleanup == nil {
		return nil
	}
	outcome.OldInstanceRef = cloneJSONMap(cleanup.Ref)
	outcome.CleanupLabel = cleanup.Label
	outcome.Cleanup = cleanup.Cleanup
	return nil
}

func (r *executionRunner) recycleCurrentOutletForCandidate(plan models.FailoverPlan, candidate providerPoolCandidate) (map[string]interface{}, error) {
	cleanup, err := resolveCurrentInstanceCleanupFromRef(r.task.UserID, parseJSONMap(r.task.CurrentInstanceRef))
	if err != nil {
		return nil, err
	}
	if cleanup != nil && (strings.ToLower(strings.TrimSpace(stringMapValue(cleanup.Ref, "provider"))) != strings.ToLower(strings.TrimSpace(plan.Provider)) ||
		strings.TrimSpace(providerEntryIDFromRef(cleanup.Ref)) != strings.TrimSpace(candidate.EntryID)) {
		cleanup = nil
	}
	if cleanup == nil {
		cleanup, err = r.resolveCurrentInstanceCleanupByAddress(plan, candidate)
		if err != nil {
			return nil, err
		}
	}
	if cleanup == nil || cleanup.Cleanup == nil {
		return nil, nil
	}

	reclaimStep := r.startStep("reclaim_current", "Reclaim Current Outlet Capacity", map[string]interface{}{
		"provider": plan.Provider,
		"entry_id": candidate.EntryID,
		"ref":      cleanup.Ref,
	})
	if err := cleanup.Cleanup(); err != nil {
		r.finishStep(reclaimStep, models.FailoverStepStatusFailed, err.Error(), map[string]interface{}{
			"label": cleanup.Label,
			"ref":   cleanup.Ref,
		})
		return nil, err
	}

	detail := map[string]interface{}{
		"label": cleanup.Label,
		"ref":   cleanup.Ref,
	}
	if len(cleanup.Addresses) > 0 {
		detail["addresses"] = cleanup.Addresses
	}
	r.finishStep(reclaimStep, models.FailoverStepStatusSuccess, "current failed outlet deleted to free capacity", detail)
	return detail, nil
}

func (r *executionRunner) resolveCurrentInstanceCleanupByAddress(plan models.FailoverPlan, candidate providerPoolCandidate) (*currentInstanceCleanup, error) {
	address := strings.TrimSpace(r.task.CurrentAddress)
	if address == "" {
		return nil, nil
	}

	switch strings.ToLower(strings.TrimSpace(plan.Provider)) {
	case "digitalocean":
		addition, token, err := loadDigitalOceanToken(r.task.UserID, candidate.EntryID)
		if err != nil {
			return nil, err
		}
		client, err := digitalocean.NewClientFromToken(token.Token)
		if err != nil {
			return nil, err
		}
		droplets, err := client.ListDroplets(context.Background())
		if err != nil {
			return nil, err
		}
		for _, droplet := range droplets {
			if !sameAddress(address, digitalOceanPublicIPv4(&droplet), digitalOceanPublicIPv6(&droplet)) {
				continue
			}
			ref := map[string]interface{}{
				"provider":            "digitalocean",
				"provider_entry_id":   candidate.EntryID,
				"provider_entry_name": candidate.EntryName,
				"droplet_id":          droplet.ID,
				"name":                droplet.Name,
				"region":              strings.TrimSpace(droplet.Region.Slug),
			}
			return &currentInstanceCleanup{
				Ref: ref,
				Addresses: map[string]interface{}{
					"ipv4": droplet.Networks.V4,
					"ipv6": droplet.Networks.V6,
				},
				Label: fmt.Sprintf("delete digitalocean droplet %d", droplet.ID),
				Cleanup: func() error {
					if err := client.DeleteDroplet(context.Background(), droplet.ID); err != nil {
						return err
					}
					removeSavedDigitalOceanRootPassword(r.task.UserID, addition, token, droplet.ID)
					return nil
				},
			}, nil
		}
		return nil, nil
	case "linode":
		addition, token, err := loadLinodeToken(r.task.UserID, candidate.EntryID)
		if err != nil {
			return nil, err
		}
		client, err := linodecloud.NewClientFromToken(token.Token)
		if err != nil {
			return nil, err
		}
		instances, err := client.ListInstances(context.Background())
		if err != nil {
			return nil, err
		}
		for _, instance := range instances {
			if !sameAddress(address, append(append([]string(nil), instance.IPv4...), strings.TrimSpace(instance.IPv6))...) {
				continue
			}
			ref := map[string]interface{}{
				"provider":            "linode",
				"provider_entry_id":   candidate.EntryID,
				"provider_entry_name": candidate.EntryName,
				"instance_id":         instance.ID,
				"label":               instance.Label,
				"region":              instance.Region,
			}
			return &currentInstanceCleanup{
				Ref: ref,
				Addresses: map[string]interface{}{
					"ipv4": instance.IPv4,
					"ipv6": instance.IPv6,
				},
				Label: fmt.Sprintf("delete linode instance %d", instance.ID),
				Cleanup: func() error {
					if err := client.DeleteInstance(context.Background(), instance.ID); err != nil {
						return err
					}
					removeSavedLinodeRootPassword(r.task.UserID, addition, token, instance.ID)
					return nil
				},
			}, nil
		}
		return nil, nil
	case "aws":
		addition, credential, err := loadAWSCredential(r.task.UserID, candidate.EntryID)
		if err != nil {
			return nil, err
		}
		region := resolveAWSPlanRegion(plan, addition, credential)
		service := resolveAWSPlanService(plan)
		if service == "lightsail" {
			instances, err := awscloud.ListLightsailInstances(context.Background(), credential, region)
			if err != nil {
				return nil, err
			}
			for _, instance := range instances {
				if !sameAddress(address, append([]string{strings.TrimSpace(instance.PublicIP), strings.TrimSpace(instance.PrivateIP)}, instance.IPv6Addresses...)...) {
					continue
				}
				ref := map[string]interface{}{
					"provider":            "aws",
					"service":             "lightsail",
					"provider_entry_id":   candidate.EntryID,
					"provider_entry_name": candidate.EntryName,
					"region":              region,
					"instance_name":       instance.Name,
				}
				return &currentInstanceCleanup{
					Ref: ref,
					Addresses: map[string]interface{}{
						"public_ip":      instance.PublicIP,
						"private_ip":     instance.PrivateIP,
						"ipv6_addresses": instance.IPv6Addresses,
					},
					Label: "delete aws lightsail instance " + instance.Name,
					Cleanup: func() error {
						return awscloud.DeleteLightsailInstance(context.Background(), credential, region, instance.Name)
					},
				}, nil
			}
			return nil, nil
		}
		instances, err := awscloud.ListInstances(context.Background(), credential, region)
		if err != nil {
			return nil, err
		}
		for _, instance := range instances {
			if !sameAddress(address, instance.PublicIP, instance.PrivateIP) {
				continue
			}
			ref := map[string]interface{}{
				"provider":            "aws",
				"service":             "ec2",
				"provider_entry_id":   candidate.EntryID,
				"provider_entry_name": candidate.EntryName,
				"region":              region,
				"instance_id":         instance.InstanceID,
				"name":                instance.Name,
			}
			return &currentInstanceCleanup{
				Ref: ref,
				Addresses: map[string]interface{}{
					"public_ip":  instance.PublicIP,
					"private_ip": instance.PrivateIP,
				},
				Label: "terminate aws ec2 instance " + instance.InstanceID,
				Cleanup: func() error {
					return awscloud.TerminateInstance(context.Background(), credential, region, instance.InstanceID)
				},
			}, nil
		}
		return nil, nil
	default:
		return nil, nil
	}
}

func resolveCurrentInstanceCleanupFromRef(userUUID string, ref map[string]interface{}) (*currentInstanceCleanup, error) {
	if len(ref) == 0 {
		return nil, nil
	}

	provider := strings.ToLower(strings.TrimSpace(stringMapValue(ref, "provider")))
	entryID := strings.TrimSpace(providerEntryIDFromRef(ref))
	if provider == "" || entryID == "" {
		return nil, nil
	}

	switch provider {
	case "digitalocean":
		dropletID := intMapValue(ref, "droplet_id")
		if dropletID <= 0 {
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
		resolvedRef := cloneJSONMap(ref)
		resolvedRef["provider"] = "digitalocean"
		resolvedRef["provider_entry_id"] = entryID
		if name := providerEntryNameFromRef(ref); name != "" {
			resolvedRef["provider_entry_name"] = name
		}
		return &currentInstanceCleanup{
			Ref:   resolvedRef,
			Label: fmt.Sprintf("delete digitalocean droplet %d", dropletID),
			Cleanup: func() error {
				if err := client.DeleteDroplet(context.Background(), dropletID); err != nil {
					return err
				}
				removeSavedDigitalOceanRootPassword(userUUID, addition, token, dropletID)
				return nil
			},
		}, nil
	case "linode":
		instanceID := intMapValue(ref, "instance_id")
		if instanceID <= 0 {
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
		resolvedRef := cloneJSONMap(ref)
		resolvedRef["provider"] = "linode"
		resolvedRef["provider_entry_id"] = entryID
		if name := providerEntryNameFromRef(ref); name != "" {
			resolvedRef["provider_entry_name"] = name
		}
		return &currentInstanceCleanup{
			Ref:   resolvedRef,
			Label: fmt.Sprintf("delete linode instance %d", instanceID),
			Cleanup: func() error {
				if err := client.DeleteInstance(context.Background(), instanceID); err != nil {
					return err
				}
				removeSavedLinodeRootPassword(userUUID, addition, token, instanceID)
				return nil
			},
		}, nil
	case "aws":
		service := strings.ToLower(strings.TrimSpace(firstNonEmpty(stringMapValue(ref, "service"), "ec2")))
		region := strings.TrimSpace(stringMapValue(ref, "region"))
		if region == "" {
			return nil, nil
		}
		_, credential, err := loadAWSCredential(userUUID, entryID)
		if err != nil {
			return nil, err
		}
		resolvedRef := cloneJSONMap(ref)
		resolvedRef["provider"] = "aws"
		resolvedRef["provider_entry_id"] = entryID
		if name := providerEntryNameFromRef(ref); name != "" {
			resolvedRef["provider_entry_name"] = name
		}
		switch service {
		case "lightsail":
			instanceName := strings.TrimSpace(stringMapValue(ref, "instance_name"))
			if instanceName == "" {
				return nil, nil
			}
			return &currentInstanceCleanup{
				Ref:   resolvedRef,
				Label: "delete aws lightsail instance " + instanceName,
				Cleanup: func() error {
					return awscloud.DeleteLightsailInstance(context.Background(), credential, region, instanceName)
				},
			}, nil
		default:
			instanceID := strings.TrimSpace(stringMapValue(ref, "instance_id"))
			if instanceID == "" {
				return nil, nil
			}
			resolvedRef["service"] = "ec2"
			return &currentInstanceCleanup{
				Ref:   resolvedRef,
				Label: "terminate aws ec2 instance " + instanceID,
				Cleanup: func() error {
					return awscloud.TerminateInstance(context.Background(), credential, region, instanceID)
				},
			}, nil
		}
	default:
		return nil, nil
	}
}

func primaryOutcomeAddress(outcome *actionOutcome) string {
	if outcome == nil {
		return ""
	}
	if ipv4 := strings.TrimSpace(outcome.IPv4); ipv4 != "" {
		return ipv4
	}
	return strings.TrimSpace(outcome.IPv6)
}

type commandResult struct {
	TaskID     string
	Output     string
	ExitCode   *int
	FinishedAt *models.LocalTime
	Truncated  bool
}

func dispatchScriptToClient(userUUID, clientUUID, command string, timeout time.Duration) (*commandResult, error) {
	command = strings.TrimSpace(command)
	if command == "" {
		return nil, errors.New("script content is empty")
	}

	client := ws.GetConnectedClients()[clientUUID]
	if client == nil {
		return nil, fmt.Errorf("client is offline: %s", clientUUID)
	}

	taskID := utils.GenerateRandomString(16)
	if err := tasks.CreateTaskForUser(userUUID, taskID, []string{clientUUID}); err != nil {
		return nil, err
	}

	payload, err := json.Marshal(map[string]string{
		"message": "exec",
		"command": command,
		"task_id": taskID,
	})
	if err != nil {
		return nil, err
	}
	if err := client.WriteMessage(websocket.TextMessage, payload); err != nil {
		return &commandResult{TaskID: taskID}, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return &commandResult{TaskID: taskID}, ctx.Err()
		case <-ticker.C:
			result, err := tasks.GetSpecificTaskResultForUser(userUUID, taskID, clientUUID)
			if err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					continue
				}
				return &commandResult{TaskID: taskID}, err
			}
			if result.FinishedAt == nil {
				continue
			}
			output, truncated := truncateOutput(result.Result, 65535)
			return &commandResult{
				TaskID:     taskID,
				Output:     output,
				ExitCode:   result.ExitCode,
				FinishedAt: result.FinishedAt,
				Truncated:  truncated,
			}, nil
		}
	}
}

func truncateOutput(output string, limit int) (string, bool) {
	if limit <= 0 || len(output) <= limit {
		return output, false
	}
	return output[:limit], true
}

func waitForClientByGroup(userUUID, group, excludeUUID string, startedAt time.Time, timeoutSeconds int) (string, error) {
	if timeoutSeconds <= 0 {
		timeoutSeconds = 600
	}
	deadline := time.Now().Add(time.Duration(timeoutSeconds) * time.Second)
	group = strings.TrimSpace(group)

	for time.Now().Before(deadline) {
		clientList, err := clientdb.GetAllClientBasicInfoByUser(userUUID)
		if err != nil {
			return "", err
		}
		reports := ws.GetLatestReport()
		online := ws.GetConnectedClients()

		candidates := make([]models.Client, 0)
		for _, client := range clientList {
			if strings.TrimSpace(client.Group) != group || client.UUID == excludeUUID {
				continue
			}
			if _, ok := online[client.UUID]; !ok {
				continue
			}
			report := reports[client.UUID]
			if client.CreatedAt.ToTime().After(startedAt.Add(-2 * time.Minute)) {
				candidates = append(candidates, client)
				continue
			}
			if report != nil && report.UpdatedAt.After(startedAt) {
				candidates = append(candidates, client)
			}
		}
		if len(candidates) > 0 {
			sortClientsNewestFirst(candidates)
			return candidates[0].UUID, nil
		}
		time.Sleep(5 * time.Second)
	}
	return "", fmt.Errorf("timed out waiting for agent group %q", group)
}

func sortClientsNewestFirst(clients []models.Client) {
	for i := 0; i < len(clients); i++ {
		for j := i + 1; j < len(clients); j++ {
			if clients[j].CreatedAt.ToTime().After(clients[i].CreatedAt.ToTime()) {
				clients[i], clients[j] = clients[j], clients[i]
			}
		}
	}
}

func buildTriggerSnapshot(report *common.Report) string {
	if report == nil || report.CNConnectivity == nil {
		return "null"
	}
	return marshalJSON(map[string]interface{}{
		"status":               report.CNConnectivity.Status,
		"target":               report.CNConnectivity.Target,
		"latency":              report.CNConnectivity.Latency,
		"message":              report.CNConnectivity.Message,
		"checked_at":           report.CNConnectivity.CheckedAt,
		"consecutive_failures": report.CNConnectivity.ConsecutiveFailures,
		"report_updated_at":    report.UpdatedAt,
	})
}

func cloneReport(report *common.Report) *common.Report {
	if report == nil {
		return nil
	}
	payload, err := json.Marshal(report)
	if err != nil {
		return report
	}
	var cloned common.Report
	if err := json.Unmarshal(payload, &cloned); err != nil {
		return report
	}
	return &cloned
}

func claimTaskRun(taskID uint) bool {
	runningTasksMu.Lock()
	defer runningTasksMu.Unlock()
	if _, exists := runningTasks[taskID]; exists {
		return false
	}
	runningTasks[taskID] = struct{}{}
	return true
}

func releaseTaskRun(taskID uint) {
	runningTasksMu.Lock()
	defer runningTasksMu.Unlock()
	delete(runningTasks, taskID)
}

func ptrLocalTime(t time.Time) *models.LocalTime {
	value := models.FromTime(t)
	return &value
}

func marshalJSON(value interface{}) string {
	if value == nil {
		return ""
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	return string(payload)
}

func emptyToNilString(value string) interface{} {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return value
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			return value
		}
	}
	return ""
}

func provisionAWSInstance(userUUID string, plan models.FailoverPlan) (*actionOutcome, error) {
	var payload awsProvisionPayload
	if err := json.Unmarshal([]byte(plan.Payload), &payload); err != nil {
		return nil, fmt.Errorf("invalid aws provision payload: %w", err)
	}
	service := strings.ToLower(strings.TrimSpace(payload.Service))
	if service == "" {
		service = "ec2"
	}

	addition, credential, err := loadAWSCredential(userUUID, plan.ProviderEntryID)
	if err != nil {
		return nil, err
	}
	region := strings.TrimSpace(payload.Region)
	if region == "" {
		region = strings.TrimSpace(addition.ActiveRegion)
	}
	if region == "" {
		region = strings.TrimSpace(credential.DefaultRegion)
	}
	if region == "" {
		region = awscloud.DefaultRegion
	}

	switch service {
	case "ec2":
		name := strings.TrimSpace(payload.Name)
		if name == "" {
			name = "failover-ec2-" + strconv.FormatInt(time.Now().Unix(), 10)
		}
		userData := strings.TrimSpace(payload.UserData)
		autoConnectGroup := ""
		if plan.AutoConnectGroup != "" || plan.ScriptClipboardID != nil {
			userData, autoConnectGroup, err = buildAutoConnectUserData(autoConnectOptions{
				UserUUID:          userUUID,
				UserData:          userData,
				Provider:          "aws",
				CredentialName:    credential.Name,
				Group:             plan.AutoConnectGroup,
				WrapInShellScript: true,
			})
			if err != nil {
				return nil, err
			}
		}
		instance, err := awscloud.CreateInstance(context.Background(), credential, region, awscloud.CreateInstanceRequest{
			Name:             name,
			ImageID:          strings.TrimSpace(payload.ImageID),
			InstanceType:     strings.TrimSpace(payload.InstanceType),
			KeyName:          strings.TrimSpace(payload.KeyName),
			SubnetID:         strings.TrimSpace(payload.SubnetID),
			SecurityGroupIDs: trimStrings(payload.SecurityGroupIDs),
			UserData:         userData,
			AssignPublicIP:   payload.AssignPublicIP,
			Tags:             payload.Tags,
		})
		if err != nil {
			return nil, err
		}
		instance, detail, err := waitForAWSEC2Instance(region, credential, strings.TrimSpace(instance.InstanceID))
		if err != nil {
			return nil, err
		}
		outcome := &actionOutcome{
			IPv4:             strings.TrimSpace(instance.PublicIP),
			TargetClientUUID: "",
			AutoConnectGroup: autoConnectGroup,
			NewInstanceRef: map[string]interface{}{
				"provider":            "aws",
				"service":             "ec2",
				"provider_entry_id":   credential.ID,
				"provider_entry_name": credential.Name,
				"region":              region,
				"instance_id":         instance.InstanceID,
				"name":                instance.Name,
			},
			NewAddresses: map[string]interface{}{
				"public_ip":  instance.PublicIP,
				"private_ip": instance.PrivateIP,
				"addresses":  detail.Addresses,
			},
			RollbackLabel: "terminate failed aws ec2 instance " + strings.TrimSpace(instance.InstanceID),
			Rollback: func() error {
				return awscloud.TerminateInstance(context.Background(), credential, region, strings.TrimSpace(instance.InstanceID))
			},
		}
		if cleanupInstanceID := strings.TrimSpace(payload.CleanupInstanceID); cleanupInstanceID != "" {
			outcome.OldInstanceRef = map[string]interface{}{
				"provider":            "aws",
				"service":             "ec2",
				"provider_entry_id":   credential.ID,
				"provider_entry_name": credential.Name,
				"region":              region,
				"instance_id":         cleanupInstanceID,
			}
			outcome.CleanupLabel = "terminate aws ec2 instance " + cleanupInstanceID
			outcome.Cleanup = func() error {
				return awscloud.TerminateInstance(context.Background(), credential, region, cleanupInstanceID)
			}
		}
		return outcome, nil
	case "lightsail":
		name := strings.TrimSpace(payload.Name)
		if name == "" {
			name = "failover-ls-" + strconv.FormatInt(time.Now().Unix(), 10)
		}
		userData := strings.TrimSpace(payload.UserData)
		autoConnectGroup := ""
		if plan.AutoConnectGroup != "" || plan.ScriptClipboardID != nil {
			userData, autoConnectGroup, err = buildAutoConnectUserData(autoConnectOptions{
				UserUUID:          userUUID,
				UserData:          userData,
				Provider:          "aws",
				CredentialName:    credential.Name,
				Group:             plan.AutoConnectGroup,
				WrapInShellScript: true,
			})
			if err != nil {
				return nil, err
			}
		}
		if err := awscloud.CreateLightsailInstance(context.Background(), credential, region, awscloud.CreateLightsailInstanceRequest{
			Name:             name,
			AvailabilityZone: strings.TrimSpace(payload.AvailabilityZone),
			BlueprintID:      strings.TrimSpace(payload.BlueprintID),
			BundleID:         strings.TrimSpace(payload.BundleID),
			KeyPairName:      strings.TrimSpace(payload.KeyPairName),
			UserData:         userData,
			IPAddressType:    strings.TrimSpace(payload.IPAddressType),
			Tags:             payload.Tags,
		}); err != nil {
			return nil, err
		}
		detail, err := waitForLightsailInstance(region, credential, name)
		if err != nil {
			return nil, err
		}
		outcome := &actionOutcome{
			IPv4:             strings.TrimSpace(detail.Instance.PublicIP),
			IPv6:             firstString(detail.Instance.IPv6Addresses),
			AutoConnectGroup: autoConnectGroup,
			NewInstanceRef: map[string]interface{}{
				"provider":            "aws",
				"service":             "lightsail",
				"provider_entry_id":   credential.ID,
				"provider_entry_name": credential.Name,
				"region":              region,
				"instance_name":       name,
			},
			NewAddresses: map[string]interface{}{
				"public_ip":      detail.Instance.PublicIP,
				"private_ip":     detail.Instance.PrivateIP,
				"ipv6_addresses": detail.Instance.IPv6Addresses,
			},
			RollbackLabel: "delete failed aws lightsail instance " + name,
			Rollback: func() error {
				return awscloud.DeleteLightsailInstance(context.Background(), credential, region, name)
			},
		}
		if cleanupName := strings.TrimSpace(payload.CleanupInstanceName); cleanupName != "" {
			outcome.OldInstanceRef = map[string]interface{}{
				"provider":            "aws",
				"service":             "lightsail",
				"provider_entry_id":   credential.ID,
				"provider_entry_name": credential.Name,
				"region":              region,
				"instance_name":       cleanupName,
			}
			outcome.CleanupLabel = "delete aws lightsail instance " + cleanupName
			outcome.Cleanup = func() error {
				return awscloud.DeleteLightsailInstance(context.Background(), credential, region, cleanupName)
			}
		}
		return outcome, nil
	default:
		return nil, fmt.Errorf("unsupported aws provision service: %s", payload.Service)
	}
}

func provisionDigitalOceanDroplet(userUUID string, plan models.FailoverPlan) (*actionOutcome, error) {
	var payload digitalOceanProvisionPayload
	if err := json.Unmarshal([]byte(plan.Payload), &payload); err != nil {
		return nil, fmt.Errorf("invalid digitalocean provision payload: %w", err)
	}

	addition, token, err := loadDigitalOceanToken(userUUID, plan.ProviderEntryID)
	if err != nil {
		return nil, err
	}
	client, err := digitalocean.NewClientFromToken(token.Token)
	if err != nil {
		return nil, err
	}

	name := strings.TrimSpace(payload.Name)
	if name == "" {
		name = "failover-do-" + strconv.FormatInt(time.Now().Unix(), 10)
	}
	userData := strings.TrimSpace(payload.UserData)
	autoConnectGroup := ""
	if plan.AutoConnectGroup != "" || plan.ScriptClipboardID != nil {
		userData, autoConnectGroup, err = buildAutoConnectUserData(autoConnectOptions{
			UserUUID:          userUUID,
			UserData:          userData,
			Provider:          "digitalocean",
			CredentialName:    token.Name,
			Group:             plan.AutoConnectGroup,
			WrapInShellScript: false,
		})
		if err != nil {
			return nil, err
		}
	}

	passwordMode := strings.ToLower(strings.TrimSpace(payload.RootPasswordMode))
	rootPassword := ""
	switch passwordMode {
	case "", "random":
		passwordMode = "random"
		rootPassword, err = digitalocean.GenerateRandomPassword(20)
		if err != nil {
			return nil, err
		}
		userData, err = digitalocean.BuildRootPasswordUserData(rootPassword, userData)
		if err != nil {
			return nil, err
		}
	case "none":
	case "custom":
		if strings.TrimSpace(payload.RootPassword) == "" {
			return nil, errors.New("digitalocean root_password cannot be empty when root_password_mode=custom")
		}
		rootPassword = strings.TrimSpace(payload.RootPassword)
		userData, err = digitalocean.BuildRootPasswordUserData(rootPassword, userData)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported digitalocean root_password_mode: %s", payload.RootPasswordMode)
	}

	droplet, err := client.CreateDroplet(context.Background(), digitalocean.CreateDropletRequest{
		Name:       name,
		Region:     strings.TrimSpace(payload.Region),
		Size:       strings.TrimSpace(payload.Size),
		Image:      strings.TrimSpace(payload.Image),
		Backups:    payload.Backups,
		IPv6:       payload.IPv6,
		Monitoring: payload.Monitoring,
		Tags:       trimStrings(payload.Tags),
		UserData:   userData,
		VPCUUID:    strings.TrimSpace(payload.VPCUUID),
	})
	if err != nil {
		return nil, err
	}
	droplet, err = waitForDigitalOceanDroplet(client, droplet.ID)
	if err != nil {
		return nil, err
	}
	passwordSaveErr := persistDigitalOceanRootPassword(userUUID, addition, token, droplet.ID, droplet.Name, passwordMode, rootPassword)
	newInstanceRef := map[string]interface{}{
		"provider":            "digitalocean",
		"provider_entry_id":   token.ID,
		"provider_entry_name": token.Name,
		"region":              strings.TrimSpace(payload.Region),
		"droplet_id":          droplet.ID,
		"name":                droplet.Name,
	}
	if rootPassword != "" {
		newInstanceRef["root_password_mode"] = passwordMode
		newInstanceRef["root_password_saved"] = passwordSaveErr == nil
		if passwordSaveErr != nil {
			newInstanceRef["root_password_save_error"] = passwordSaveErr.Error()
		}
	}
	outcome := &actionOutcome{
		IPv4:             digitalOceanPublicIPv4(droplet),
		IPv6:             digitalOceanPublicIPv6(droplet),
		AutoConnectGroup: autoConnectGroup,
		NewInstanceRef:   newInstanceRef,
		NewAddresses: map[string]interface{}{
			"ipv4": droplet.Networks.V4,
			"ipv6": droplet.Networks.V6,
		},
		RollbackLabel: fmt.Sprintf("delete failed digitalocean droplet %d", droplet.ID),
		Rollback: func() error {
			if err := client.DeleteDroplet(context.Background(), droplet.ID); err != nil {
				return err
			}
			removeSavedDigitalOceanRootPassword(userUUID, addition, token, droplet.ID)
			return nil
		},
	}
	if payload.CleanupDropletID > 0 {
		outcome.OldInstanceRef = map[string]interface{}{
			"provider":            "digitalocean",
			"provider_entry_id":   token.ID,
			"provider_entry_name": token.Name,
			"droplet_id":          payload.CleanupDropletID,
		}
		outcome.CleanupLabel = fmt.Sprintf("delete digitalocean droplet %d", payload.CleanupDropletID)
		outcome.Cleanup = func() error {
			if err := client.DeleteDroplet(context.Background(), payload.CleanupDropletID); err != nil {
				return err
			}
			removeSavedDigitalOceanRootPassword(userUUID, addition, token, payload.CleanupDropletID)
			return nil
		}
	}
	return outcome, nil
}

func provisionLinodeInstance(userUUID string, plan models.FailoverPlan) (*actionOutcome, error) {
	var payload linodeProvisionPayload
	if err := json.Unmarshal([]byte(plan.Payload), &payload); err != nil {
		return nil, fmt.Errorf("invalid linode provision payload: %w", err)
	}

	addition, token, err := loadLinodeToken(userUUID, plan.ProviderEntryID)
	if err != nil {
		return nil, err
	}
	client, err := linodecloud.NewClientFromToken(token.Token)
	if err != nil {
		return nil, err
	}

	label := strings.TrimSpace(payload.Label)
	if label == "" {
		label = "failover-linode-" + strconv.FormatInt(time.Now().Unix(), 10)
	}
	userData := strings.TrimSpace(payload.UserData)
	autoConnectGroup := ""
	if plan.AutoConnectGroup != "" || plan.ScriptClipboardID != nil {
		userData, autoConnectGroup, err = buildAutoConnectUserData(autoConnectOptions{
			UserUUID:          userUUID,
			UserData:          userData,
			Provider:          "linode",
			CredentialName:    token.Name,
			Group:             plan.AutoConnectGroup,
			WrapInShellScript: true,
		})
		if err != nil {
			return nil, err
		}
	}

	rootPassword := strings.TrimSpace(payload.RootPassword)
	passwordMode := strings.ToLower(strings.TrimSpace(payload.RootPasswordMode))
	switch passwordMode {
	case "", "random":
		passwordMode = "random"
		rootPassword, err = linodecloud.GenerateRandomPassword(20)
		if err != nil {
			return nil, err
		}
	case "custom":
		if rootPassword == "" {
			return nil, errors.New("linode root_password cannot be empty when root_password_mode=custom")
		}
	default:
		return nil, fmt.Errorf("unsupported linode root_password_mode: %s", payload.RootPasswordMode)
	}

	request := linodecloud.CreateInstanceRequest{
		Label:          label,
		Region:         strings.TrimSpace(payload.Region),
		Type:           strings.TrimSpace(payload.Type),
		Image:          strings.TrimSpace(payload.Image),
		RootPass:       rootPassword,
		AuthorizedKeys: trimStrings(payload.AuthorizedKeys),
		BackupsEnabled: payload.BackupsEnabled,
		Booted:         true,
		Tags:           trimStrings(payload.Tags),
	}
	if userData != "" {
		request.Metadata = &struct {
			UserData string `json:"user_data,omitempty"`
		}{
			UserData: linodecloud.EncodeUserData(userData),
		}
	}

	instance, err := client.CreateInstance(context.Background(), request)
	if err != nil {
		return nil, err
	}
	instance, err = waitForLinodeInstance(client, instance.ID)
	if err != nil {
		return nil, err
	}
	passwordSaveErr := persistLinodeRootPassword(
		userUUID,
		addition,
		token,
		instance.ID,
		instance.Label,
		passwordMode,
		rootPassword,
	)
	newInstanceRef := map[string]interface{}{
		"provider":            "linode",
		"provider_entry_id":   token.ID,
		"provider_entry_name": token.Name,
		"region":              instance.Region,
		"instance_id":         instance.ID,
		"label":               instance.Label,
	}
	if rootPassword != "" {
		newInstanceRef["root_password_mode"] = passwordMode
		newInstanceRef["root_password_saved"] = passwordSaveErr == nil
		if passwordSaveErr != nil {
			newInstanceRef["root_password_save_error"] = passwordSaveErr.Error()
		}
	}
	outcome := &actionOutcome{
		IPv4:             firstString(instance.IPv4),
		IPv6:             strings.TrimSpace(instance.IPv6),
		AutoConnectGroup: autoConnectGroup,
		NewInstanceRef:   newInstanceRef,
		NewAddresses: map[string]interface{}{
			"ipv4": instance.IPv4,
			"ipv6": instance.IPv6,
		},
		RollbackLabel: fmt.Sprintf("delete failed linode instance %d", instance.ID),
		Rollback: func() error {
			if err := client.DeleteInstance(context.Background(), instance.ID); err != nil {
				return err
			}
			removeSavedLinodeRootPassword(userUUID, addition, token, instance.ID)
			return nil
		},
	}
	if payload.CleanupInstanceID > 0 {
		outcome.OldInstanceRef = map[string]interface{}{
			"provider":            "linode",
			"provider_entry_id":   token.ID,
			"provider_entry_name": token.Name,
			"instance_id":         payload.CleanupInstanceID,
		}
		outcome.CleanupLabel = fmt.Sprintf("delete linode instance %d", payload.CleanupInstanceID)
		outcome.Cleanup = func() error {
			if err := client.DeleteInstance(context.Background(), payload.CleanupInstanceID); err != nil {
				return err
			}
			removeSavedLinodeRootPassword(userUUID, addition, token, payload.CleanupInstanceID)
			return nil
		}
	}
	return outcome, nil
}

func rebindAWSIPAddress(task models.FailoverTask, plan models.FailoverPlan) (*actionOutcome, error) {
	var payload awsRebindPayload
	if err := json.Unmarshal([]byte(plan.Payload), &payload); err != nil {
		return nil, fmt.Errorf("invalid aws rebind payload: %w", err)
	}

	addition, credential, err := loadAWSCredential(task.UserID, plan.ProviderEntryID)
	if err != nil {
		return nil, err
	}
	region := strings.TrimSpace(payload.Region)
	if region == "" {
		region = strings.TrimSpace(addition.ActiveRegion)
	}
	if region == "" {
		region = strings.TrimSpace(credential.DefaultRegion)
	}
	if region == "" {
		region = awscloud.DefaultRegion
	}

	service := strings.ToLower(strings.TrimSpace(payload.Service))
	if service == "" {
		service = "ec2"
	}

	switch service {
	case "ec2":
		instanceID := strings.TrimSpace(payload.InstanceID)
		if instanceID == "" {
			return nil, errors.New("aws ec2 rebind requires instance_id")
		}
		detail, err := awscloud.GetInstanceDetail(context.Background(), credential, region, instanceID)
		if err != nil {
			return nil, err
		}
		address, err := awscloud.AllocateAndAssociateAddress(context.Background(), credential, region, instanceID, strings.TrimSpace(payload.PrivateIP))
		if err != nil {
			return nil, err
		}
		return &actionOutcome{
			IPv4:             strings.TrimSpace(address.PublicIP),
			TargetClientUUID: task.WatchClientUUID,
			NewClientUUID:    task.WatchClientUUID,
			OldInstanceRef: map[string]interface{}{
				"provider":            "aws",
				"service":             "ec2",
				"provider_entry_id":   credential.ID,
				"provider_entry_name": credential.Name,
				"region":              region,
				"instance_id":         instanceID,
			},
			NewAddresses: map[string]interface{}{
				"allocation_id":  address.AllocationID,
				"association_id": address.AssociationID,
				"public_ip":      address.PublicIP,
				"private_ip":     address.PrivateIP,
				"old_addresses":  detail.Addresses,
			},
		}, nil
	case "lightsail":
		instanceName := strings.TrimSpace(payload.InstanceName)
		if instanceName == "" {
			return nil, errors.New("aws lightsail rebind requires instance_name")
		}
		staticIPName := strings.TrimSpace(payload.StaticIPName)
		if staticIPName == "" {
			staticIPName = fmt.Sprintf("%s-ip-%d", instanceName, time.Now().Unix())
		}
		detail, err := awscloud.GetLightsailInstanceDetail(context.Background(), credential, region, instanceName)
		if err != nil {
			return nil, err
		}
		if err := awscloud.AllocateLightsailStaticIP(context.Background(), credential, region, staticIPName); err != nil {
			return nil, err
		}
		if err := awscloud.AttachLightsailStaticIP(context.Background(), credential, region, staticIPName, instanceName); err != nil {
			return nil, err
		}
		detail, err = waitForLightsailInstance(region, credential, instanceName)
		if err != nil {
			return nil, err
		}
		return &actionOutcome{
			IPv4:             strings.TrimSpace(detail.Instance.PublicIP),
			IPv6:             firstString(detail.Instance.IPv6Addresses),
			TargetClientUUID: task.WatchClientUUID,
			NewClientUUID:    task.WatchClientUUID,
			OldInstanceRef: map[string]interface{}{
				"provider":            "aws",
				"service":             "lightsail",
				"provider_entry_id":   credential.ID,
				"provider_entry_name": credential.Name,
				"region":              region,
				"instance_name":       instanceName,
			},
			NewAddresses: map[string]interface{}{
				"static_ip_name": staticIPName,
				"public_ip":      detail.Instance.PublicIP,
				"ipv6_addresses": detail.Instance.IPv6Addresses,
			},
		}, nil
	default:
		return nil, fmt.Errorf("unsupported aws rebind service: %s", payload.Service)
	}
}

func waitForAWSEC2Instance(region string, credential *awscloud.CredentialRecord, instanceID string) (*awscloud.Instance, *awscloud.InstanceDetail, error) {
	deadline := time.Now().Add(5 * time.Minute)
	for time.Now().Before(deadline) {
		instance, err := awscloud.GetInstance(context.Background(), credential, region, instanceID)
		if err == nil && instance != nil && strings.TrimSpace(instance.PublicIP) != "" {
			detail, detailErr := awscloud.GetInstanceDetail(context.Background(), credential, region, instanceID)
			if detailErr != nil {
				return instance, nil, nil
			}
			return instance, detail, nil
		}
		time.Sleep(5 * time.Second)
	}
	instance, err := awscloud.GetInstance(context.Background(), credential, region, instanceID)
	if err != nil {
		return nil, nil, err
	}
	detail, _ := awscloud.GetInstanceDetail(context.Background(), credential, region, instanceID)
	return instance, detail, nil
}

func waitForLightsailInstance(region string, credential *awscloud.CredentialRecord, instanceName string) (*awscloud.LightsailInstanceDetail, error) {
	deadline := time.Now().Add(5 * time.Minute)
	for time.Now().Before(deadline) {
		detail, err := awscloud.GetLightsailInstanceDetail(context.Background(), credential, region, instanceName)
		if err == nil && detail != nil && strings.TrimSpace(detail.Instance.PublicIP) != "" {
			return detail, nil
		}
		time.Sleep(5 * time.Second)
	}
	return awscloud.GetLightsailInstanceDetail(context.Background(), credential, region, instanceName)
}

func waitForDigitalOceanDroplet(client *digitalocean.Client, dropletID int) (*digitalocean.Droplet, error) {
	deadline := time.Now().Add(5 * time.Minute)
	for time.Now().Before(deadline) {
		droplets, err := client.ListDroplets(context.Background())
		if err != nil {
			return nil, err
		}
		for _, droplet := range droplets {
			if droplet.ID == dropletID && digitalOceanPublicIPv4(&droplet) != "" {
				return &droplet, nil
			}
		}
		time.Sleep(5 * time.Second)
	}
	droplets, err := client.ListDroplets(context.Background())
	if err != nil {
		return nil, err
	}
	for _, droplet := range droplets {
		if droplet.ID == dropletID {
			return &droplet, nil
		}
	}
	return nil, fmt.Errorf("digitalocean droplet not found: %d", dropletID)
}

func waitForLinodeInstance(client *linodecloud.Client, instanceID int) (*linodecloud.Instance, error) {
	deadline := time.Now().Add(5 * time.Minute)
	for time.Now().Before(deadline) {
		instance, err := client.GetInstance(context.Background(), instanceID)
		if err == nil && instance != nil && firstString(instance.IPv4) != "" {
			return instance, nil
		}
		time.Sleep(5 * time.Second)
	}
	return client.GetInstance(context.Background(), instanceID)
}

func digitalOceanPublicIPv4(droplet *digitalocean.Droplet) string {
	if droplet == nil {
		return ""
	}
	for _, network := range droplet.Networks.V4 {
		if strings.EqualFold(strings.TrimSpace(network.Type), "public") {
			return strings.TrimSpace(network.IPAddress)
		}
	}
	return ""
}

func digitalOceanPublicIPv6(droplet *digitalocean.Droplet) string {
	if droplet == nil {
		return ""
	}
	for _, network := range droplet.Networks.V6 {
		if strings.EqualFold(strings.TrimSpace(network.Type), "public") {
			return strings.TrimSpace(network.IPAddress)
		}
	}
	return ""
}

func firstString(values []string) string {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			return value
		}
	}
	return ""
}

func trimStrings(values []string) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			result = append(result, value)
		}
	}
	return result
}
