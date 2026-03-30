package failover

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

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
	runningTasksMu   sync.Mutex
	runningTasks     = map[uint]struct{}{}
	executionStopMu  sync.Mutex
	executionCancels = map[uint]context.CancelFunc{}
)

const interruptedExecutionMessage = "failover execution was interrupted before completion"

var errExecutionStopped = errors.New("failover execution stopped by user")

type executionRunner struct {
	task      models.FailoverTask
	execution *models.FailoverExecution
	ctx       context.Context
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
	Missing   bool
}

type blockedOutletError struct {
	ClientUUID string
	Status     string
	Message    string
}

func isDigitalOceanNotFoundError(err error) bool {
	var apiErr *digitalocean.APIError
	return errors.As(err, &apiErr) && apiErr != nil && apiErr.StatusCode == 404
}

func isLinodeNotFoundError(err error) bool {
	var apiErr *linodecloud.APIError
	return errors.As(err, &apiErr) && apiErr != nil && apiErr.StatusCode == 404
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

func RecoverInterruptedExecutions() error {
	recovered, err := failoverdb.RecoverInterruptedExecutions(interruptedExecutionMessage)
	if err != nil {
		return err
	}
	if recovered > 0 {
		log.Printf("failover: recovered %d interrupted execution(s)", recovered)
	}
	return nil
}

func StopExecutionForUser(userUUID string, executionID uint) (*models.FailoverExecution, error) {
	execution, err := failoverdb.StopExecutionForUser(userUUID, executionID, errExecutionStopped.Error())
	if err != nil {
		return nil, err
	}
	cancelExecution(executionID)
	return execution, nil
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
		recovered, recoverErr := failoverdb.RecoverInterruptedExecutionsForTask(task.ID, interruptedExecutionMessage)
		if recoverErr != nil {
			releaseTaskRun(task.ID)
			return nil, recoverErr
		}
		if recovered == 0 {
			releaseTaskRun(task.ID)
			return nil, fmt.Errorf("failover task %d already has an active execution", task.ID)
		}
		log.Printf("failover: recovered %d interrupted execution(s) for task %d while queueing", recovered, task.ID)
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
		ctx, cancel := context.WithCancel(context.Background())
		registerExecutionCancel(execCopy.ID, cancel)
		defer unregisterExecutionCancel(execCopy.ID)
		runner := &executionRunner{
			task:      taskCopy,
			execution: &execCopy,
			ctx:       ctx,
			startedAt: now,
		}
		runner.run(reportCopy)
	}(*task, *execution, cloneReport(report))

	return execution, nil
}

func (r *executionRunner) run(report *common.Report) {
	defer func() {
		if recovered := recover(); recovered != nil {
			message := fmt.Sprintf("failover execution panicked: %v", recovered)
			log.Printf("failover: execution %d panicked: %v\n%s", r.execution.ID, recovered, debug.Stack())
			r.failExecution(message)
		}
	}()

	if err := r.checkStopped(); err != nil {
		r.failExecution(err.Error())
		return
	}

	if err := failoverdb.UpdateExecutionFields(r.execution.ID, map[string]interface{}{
		"status": models.FailoverExecutionStatusDetecting,
	}); err != nil {
		log.Printf("failover: failed to mark execution %d detecting: %v", r.execution.ID, err)
	}

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
		if err := r.checkStopped(); err != nil {
			r.failExecution(err.Error())
			return
		}
		attemptStep := r.startStep(fmt.Sprintf("plan:%d", plan.ID), "Plan Attempt", map[string]interface{}{
			"plan_id":      plan.ID,
			"provider":     plan.Provider,
			"action_type":  plan.ActionType,
			"priority":     plan.Priority,
			"plan_name":    plan.Name,
			"entry_id":     plan.ProviderEntryID,
			"entry_group":  plan.ProviderEntryGroup,
			"auto_connect": plan.AutoConnectGroup,
		})
		_ = failoverdb.UpdateExecutionFields(r.execution.ID, map[string]interface{}{
			"selected_plan_id": plan.ID,
		})

		outcome, selectedEntryID, entryAttempts, err := r.executePlan(plan)
		attempt := map[string]interface{}{
			"plan_id":               plan.ID,
			"provider":              plan.Provider,
			"action_type":           plan.ActionType,
			"preferred_entry_id":    plan.ProviderEntryID,
			"preferred_entry_group": plan.ProviderEntryGroup,
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
			if errors.Is(err, errExecutionStopped) {
				r.failExecution(err.Error())
				return
			}
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
	if err := r.checkStopped(); err != nil {
		return nil, "", nil, err
	}
	return r.executePlanActionWithProviderPool(plan)
}

func (r *executionRunner) executePlanActionWithProviderPool(plan models.FailoverPlan) (*actionOutcome, string, []map[string]interface{}, error) {
	candidates, err := listProviderPoolCandidates(r.task.UserID, plan)
	if err != nil {
		return nil, "", nil, err
	}

	entryAttempts := make([]map[string]interface{}, 0, len(candidates))
	for _, candidate := range candidates {
		if err := r.checkStopped(); err != nil {
			return nil, "", entryAttempts, err
		}
		candidateDetail := map[string]interface{}{
			"entry_id":   candidate.EntryID,
			"entry_name": candidate.EntryName,
		}
		if candidate.EntryGroup != "" {
			candidateDetail["entry_group"] = candidate.EntryGroup
		}
		if candidate.Preferred {
			candidateDetail["preferred"] = true
		}
		if candidate.Active {
			candidateDetail["active"] = true
		}

		if strings.TrimSpace(plan.ActionType) == models.FailoverActionProvisionInstance {
			recycledDetail, recycleErr := r.recycleCurrentOutletForCandidate(plan, candidate)
			if recycleErr != nil {
				candidateDetail["status"] = "failed"
				candidateDetail["error"] = recycleErr.Error()
				candidateDetail["failure_class"] = "pre_reclaim_error"
				entryAttempts = append(entryAttempts, candidateDetail)
				continue
			}
			if len(recycledDetail) > 0 {
				candidateDetail["recycled_current_instance"] = recycledDetail
				invalidateProviderEntrySnapshot(r.task.UserID, plan.Provider, candidate.EntryID)
			}
		}

		selectedPlan := plan
		selectedPlan.ProviderEntryID = candidate.EntryID
		isProvisionPlan := strings.TrimSpace(selectedPlan.ActionType) == models.FailoverActionProvisionInstance
		maxAttempts := providerEntryMaxAttempts(selectedPlan)
		for attemptNumber := 1; attemptNumber <= maxAttempts; attemptNumber++ {
			if err := r.checkStopped(); err != nil {
				return nil, "", entryAttempts, err
			}
			attemptDetail := make(map[string]interface{}, len(candidateDetail)+1)
			for key, value := range candidateDetail {
				attemptDetail[key] = value
			}
			attemptDetail["attempt"] = attemptNumber

			lease, availability, reserveErr := acquireProviderEntryLease(r.task.UserID, selectedPlan, candidate)
			if len(availability) > 0 {
				attemptDetail["availability"] = availability
			}
			if reserveErr != nil && isProvisionPlan && strings.TrimSpace(stringMapValue(availability, "status")) == "full" {
				recycledDetail, recycleErr := r.recycleCurrentOutletForCandidate(selectedPlan, candidate)
				if recycleErr != nil {
					attemptDetail["recycle_error"] = recycleErr.Error()
				} else if len(recycledDetail) > 0 {
					attemptDetail["recycled_current_instance"] = recycledDetail
					invalidateProviderEntrySnapshot(r.task.UserID, plan.Provider, candidate.EntryID)
					lease, availability, reserveErr = acquireProviderEntryLease(r.task.UserID, selectedPlan, candidate)
					if len(availability) > 0 {
						attemptDetail["availability"] = availability
					}
				}
			}
			if reserveErr != nil {
				attemptDetail["status"] = "skipped"
				attemptDetail["error"] = reserveErr.Error()
				entryAttempts = append(entryAttempts, attemptDetail)
				goto nextCandidate
			}

			var finishOperation func()
			releaseProvisioningWindow := func(provisioned bool) {
				if lease != nil {
					lease.Release(provisioned)
					lease = nil
				}
				if finishOperation != nil {
					finishOperation()
					finishOperation = nil
				}
			}
			if shouldSerializeProviderOperation(selectedPlan) {
				serialDone, serialErr := lease.BeginSerializedOperation(providerEntryOperationSpacing(selectedPlan))
				if serialErr != nil {
					releaseProvisioningWindow(false)
					attemptDetail["status"] = "skipped"
					attemptDetail["error"] = serialErr.Error()
					entryAttempts = append(entryAttempts, attemptDetail)
					goto nextCandidate
				}
				finishOperation = serialDone
			}

			outcome, actionErr := r.executePlanAction(selectedPlan)
			if actionErr != nil {
				releaseProvisioningWindow(false)
				if errors.Is(actionErr, errExecutionStopped) {
					attemptDetail["status"] = "failed"
					attemptDetail["error"] = actionErr.Error()
					entryAttempts = append(entryAttempts, attemptDetail)
					return nil, "", entryAttempts, actionErr
				}
				decision := classifyProviderFailure(plan.Provider, actionErr)
				applyProviderEntryFailure(r.task.UserID, plan.Provider, candidate.EntryID, decision, actionErr)
				attemptDetail["status"] = "failed"
				attemptDetail["error"] = actionErr.Error()
				attemptDetail["failure_class"] = decision.Class
				entryAttempts = append(entryAttempts, attemptDetail)
				goto nextCandidate
			}
			if isProvisionPlan {
				if cleanupErr := r.attachCurrentOutletCleanup(outcome, selectedPlan, candidate); cleanupErr != nil {
					releaseProvisioningWindow(false)
					attemptDetail["status"] = "failed"
					attemptDetail["error"] = cleanupErr.Error()
					attemptDetail["failure_class"] = "post_provision_error"
					entryAttempts = append(entryAttempts, attemptDetail)
					goto nextCandidate
				}

				// Only the create-instance phase should occupy the provider queue.
				releaseProvisioningWindow(true)
			}

			finalizeErr := r.finalizePlan(selectedPlan, outcome)
			if finalizeErr != nil {
				if errors.Is(finalizeErr, errExecutionStopped) {
					releaseProvisioningWindow(false)
					attemptDetail["status"] = "failed"
					attemptDetail["error"] = finalizeErr.Error()
					entryAttempts = append(entryAttempts, attemptDetail)
					return nil, "", entryAttempts, finalizeErr
				}
				executionDecision := classifyPlanExecutionFailure(finalizeErr)
				attemptDetail["error"] = finalizeErr.Error()
				attemptDetail["failure_class"] = executionDecision.Class
				if executionDecision.RetrySameEntry && attemptNumber < maxAttempts {
					retryDetail := map[string]interface{}{
						"entry_id":       candidate.EntryID,
						"entry_name":     candidate.EntryName,
						"attempt":        attemptNumber,
						"next_attempt":   attemptNumber + 1,
						"failure_class":  executionDecision.Class,
						"error_message":  finalizeErr.Error(),
						"provider":       selectedPlan.Provider,
						"provider_entry": candidate.EntryID,
					}
					retryStep := r.startStep("retry_same_entry", "Retry Same Provider Entry", retryDetail)
					r.finishStep(
						retryStep,
						models.FailoverStepStatusSuccess,
						"retryable new-outlet failure detected; retrying the same provider entry",
						retryDetail,
					)
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
				releaseProvisioningWindow(false)
				attemptDetail["status"] = "failed"
				entryAttempts = append(entryAttempts, attemptDetail)
				goto nextCandidate
			}

			releaseProvisioningWindow(isProvisionPlan)
			clearProviderEntryCooldown(r.task.UserID, plan.Provider, candidate.EntryID)
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

		clientUUID, waitErr := waitForClientByGroup(
			r.ctx,
			r.task.UserID,
			outcome.AutoConnectGroup,
			r.task.WatchClientUUID,
			r.startedAt,
			plan.WaitAgentTimeoutSec,
			expectedClientAddresses(outcome),
		)
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

	scriptClipboardIDs := plan.EffectiveScriptClipboardIDs()
	if len(scriptClipboardIDs) > 0 {
		if targetClientUUID == "" {
			return errors.New("script execution requires a target client but none became available")
		}

		scriptStep := r.startStep("run_scripts", "Run Scripts", map[string]interface{}{
			"clipboard_ids": scriptClipboardIDs,
			"client_uuid":   targetClientUUID,
		})
		_ = failoverdb.UpdateExecutionFields(r.execution.ID, map[string]interface{}{
			"status": models.FailoverExecutionStatusRunningScript,
		})
		if err := r.runScripts(plan, targetClientUUID); err != nil {
			r.finishStep(scriptStep, models.FailoverStepStatusFailed, err.Error(), nil)
			return err
		}
		r.finishStep(scriptStep, models.FailoverStepStatusSuccess, "scripts finished successfully", map[string]interface{}{
			"count": len(scriptClipboardIDs),
		})
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

func planHasScripts(plan models.FailoverPlan) bool {
	return len(plan.EffectiveScriptClipboardIDs()) > 0
}

func joinScriptSnapshotNames(names []string) string {
	filtered := make([]string, 0, len(names))
	for _, name := range names {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		filtered = append(filtered, name)
	}
	return truncateUTF8String(strings.Join(filtered, ", "), 255)
}

func latestScriptTaskID(taskIDs []string) string {
	for index := len(taskIDs) - 1; index >= 0; index-- {
		taskID := strings.TrimSpace(taskIDs[index])
		if taskID != "" {
			return taskID
		}
	}
	return ""
}

func joinScriptOutputs(names, outputs []string) string {
	if len(outputs) == 0 {
		return ""
	}
	if len(outputs) == 1 {
		return outputs[0]
	}

	var builder strings.Builder
	for index, output := range outputs {
		if index > 0 {
			builder.WriteString("\n\n")
		}

		name := strings.TrimSpace(fmt.Sprintf("Script %d", index+1))
		if index < len(names) && strings.TrimSpace(names[index]) != "" {
			name = strings.TrimSpace(names[index])
		}
		builder.WriteString("==> ")
		builder.WriteString(name)
		builder.WriteString(" <==")
		if output != "" {
			builder.WriteByte('\n')
			builder.WriteString(output)
		}
	}
	return builder.String()
}

func truncateUTF8String(value string, limit int) string {
	if limit <= 0 || len(value) <= limit {
		return value
	}
	if limit <= 3 {
		return value[:limit]
	}

	var builder strings.Builder
	remaining := limit - 3
	used := 0
	for _, r := range value {
		runeLen := utf8.RuneLen(r)
		if runeLen < 0 || used+runeLen > remaining {
			break
		}
		builder.WriteRune(r)
		used += runeLen
	}
	builder.WriteString("...")
	return builder.String()
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

	report, err := waitForHealthyClientConnectivity(r.ctx, r.task.UserID, targetClientUUID, r.startedAt)
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

func waitForHealthyClientConnectivity(ctx context.Context, userUUID, clientUUID string, startedAt time.Time) (*common.Report, error) {
	timeout := failoverConnectivityValidationTimeout(userUUID)
	deadline := time.Now().Add(timeout)
	clientUUID = strings.TrimSpace(clientUUID)
	var lastReport *common.Report

	for time.Now().Before(deadline) {
		if err := waitContextOrDelay(ctx, 0); err != nil {
			return nil, err
		}
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
		if err := waitContextOrDelay(ctx, 5*time.Second); err != nil {
			return nil, err
		}
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
	r.invalidateProvisionedEntrySnapshot(outcome)
	r.finishStep(rollbackStep, models.FailoverStepStatusSuccess, "failed new instance deleted", detail)
	return originalErr
}

func (r *executionRunner) invalidateProvisionedEntrySnapshot(outcome *actionOutcome) {
	if outcome == nil || strings.TrimSpace(r.task.UserID) == "" {
		return
	}

	provider := strings.TrimSpace(stringMapValue(outcome.NewInstanceRef, "provider"))
	entryID := strings.TrimSpace(providerEntryIDFromRef(outcome.NewInstanceRef))
	if provider == "" || entryID == "" {
		return
	}

	invalidateProviderEntrySnapshot(r.task.UserID, provider, entryID)
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
		outcome, err = provisionAWSInstance(r.ctx, r.task.UserID, plan)
	case "digitalocean":
		outcome, err = provisionDigitalOceanDroplet(r.ctx, r.task.UserID, plan)
	case "linode":
		outcome, err = provisionLinodeInstance(r.ctx, r.task.UserID, plan)
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

func (r *executionRunner) runScripts(plan models.FailoverPlan, clientUUID string) error {
	scriptClipboardIDs := plan.EffectiveScriptClipboardIDs()
	if len(scriptClipboardIDs) == 0 {
		return nil
	}

	primaryScriptClipboardID := models.FirstFailoverScriptClipboardID(scriptClipboardIDs)
	encodedScriptClipboardIDs := models.EncodeFailoverScriptClipboardIDs(scriptClipboardIDs)
	var primaryScriptClipboardValue interface{}
	if primaryScriptClipboardID != nil {
		primaryScriptClipboardValue = *primaryScriptClipboardID
	}
	scriptNames := make([]string, 0, len(scriptClipboardIDs))
	scriptTaskIDs := make([]string, 0, len(scriptClipboardIDs))
	scriptOutputs := make([]string, 0, len(scriptClipboardIDs))
	scriptOutputTruncated := false
	var lastExitCode *int
	var lastFinishedAt *models.LocalTime

	for index, scriptClipboardID := range scriptClipboardIDs {
		clipboard, err := clipboarddb.GetClipboardByIDForUser(scriptClipboardID, r.task.UserID)
		if err != nil {
			return err
		}

		scriptNames = append(scriptNames, clipboard.Name)
		step := r.startStep(
			fmt.Sprintf("run_script:%d:%d", clipboard.Id, index+1),
			fmt.Sprintf("Run Script %d", index+1),
			map[string]interface{}{
				"clipboard_id": clipboard.Id,
				"script_name":  clipboard.Name,
				"client_uuid":  clientUUID,
				"index":        index + 1,
				"total":        len(scriptClipboardIDs),
			},
		)
		_ = failoverdb.UpdateExecutionFields(r.execution.ID, map[string]interface{}{
			"script_clipboard_id":     primaryScriptClipboardValue,
			"script_clipboard_ids":    encodedScriptClipboardIDs,
			"script_name_snapshot":    joinScriptSnapshotNames(scriptNames),
			"script_status":           models.FailoverScriptStatusRunning,
			"script_task_id":          latestScriptTaskID(scriptTaskIDs),
			"script_exit_code":        nil,
			"script_finished_at":      nil,
			"script_output":           joinScriptOutputs(scriptNames[:len(scriptOutputs)], scriptOutputs),
			"script_output_truncated": scriptOutputTruncated,
		})

		result, err := dispatchScriptToClient(r.task.UserID, clientUUID, clipboard.Text, time.Duration(plan.ScriptTimeoutSec)*time.Second)
		result = ensureCommandResult(result)
		scriptTaskIDs = append(scriptTaskIDs, result.TaskID)
		scriptOutputs = append(scriptOutputs, result.Output)
		scriptOutputTruncated = scriptOutputTruncated || result.Truncated
		lastExitCode = result.ExitCode
		lastFinishedAt = result.FinishedAt

		if err != nil {
			status := models.FailoverScriptStatusFailed
			if errors.Is(err, context.DeadlineExceeded) {
				status = models.FailoverScriptStatusTimeout
			}
			_ = failoverdb.UpdateExecutionFields(r.execution.ID, map[string]interface{}{
				"script_clipboard_id":     primaryScriptClipboardValue,
				"script_clipboard_ids":    encodedScriptClipboardIDs,
				"script_name_snapshot":    joinScriptSnapshotNames(scriptNames),
				"script_task_id":          latestScriptTaskID(scriptTaskIDs),
				"script_status":           status,
				"script_exit_code":        lastExitCode,
				"script_finished_at":      lastFinishedAt,
				"script_output":           joinScriptOutputs(scriptNames, scriptOutputs),
				"script_output_truncated": scriptOutputTruncated,
			})
			r.finishStep(step, models.FailoverStepStatusFailed, err.Error(), map[string]interface{}{
				"clipboard_id":            clipboard.Id,
				"script_name":             clipboard.Name,
				"task_id":                 result.TaskID,
				"exit_code":               result.ExitCode,
				"output_truncated":        result.Truncated,
				"script_output_available": strings.TrimSpace(result.Output) != "",
			})
			return err
		}

		if execErr := commandResultExecutionError(result); execErr != nil {
			_ = failoverdb.UpdateExecutionFields(r.execution.ID, map[string]interface{}{
				"script_clipboard_id":     primaryScriptClipboardValue,
				"script_clipboard_ids":    encodedScriptClipboardIDs,
				"script_name_snapshot":    joinScriptSnapshotNames(scriptNames),
				"script_task_id":          latestScriptTaskID(scriptTaskIDs),
				"script_status":           models.FailoverScriptStatusFailed,
				"script_exit_code":        lastExitCode,
				"script_finished_at":      lastFinishedAt,
				"script_output":           joinScriptOutputs(scriptNames, scriptOutputs),
				"script_output_truncated": scriptOutputTruncated,
			})
			r.finishStep(step, models.FailoverStepStatusFailed, execErr.Error(), map[string]interface{}{
				"clipboard_id":            clipboard.Id,
				"script_name":             clipboard.Name,
				"task_id":                 result.TaskID,
				"exit_code":               result.ExitCode,
				"output_truncated":        result.Truncated,
				"script_output_available": strings.TrimSpace(result.Output) != "",
			})
			return execErr
		}

		r.finishStep(step, models.FailoverStepStatusSuccess, "script finished successfully", map[string]interface{}{
			"clipboard_id":            clipboard.Id,
			"script_name":             clipboard.Name,
			"task_id":                 result.TaskID,
			"exit_code":               result.ExitCode,
			"output_truncated":        result.Truncated,
			"script_output_available": strings.TrimSpace(result.Output) != "",
		})
	}

	_ = failoverdb.UpdateExecutionFields(r.execution.ID, map[string]interface{}{
		"script_clipboard_id":     primaryScriptClipboardValue,
		"script_clipboard_ids":    encodedScriptClipboardIDs,
		"script_name_snapshot":    joinScriptSnapshotNames(scriptNames),
		"script_task_id":          latestScriptTaskID(scriptTaskIDs),
		"script_status":           models.FailoverScriptStatusSuccess,
		"script_exit_code":        lastExitCode,
		"script_finished_at":      lastFinishedAt,
		"script_output":           joinScriptOutputs(scriptNames, scriptOutputs),
		"script_output_truncated": scriptOutputTruncated,
	})
	return nil
}

func (r *executionRunner) succeedExecution(outcome *actionOutcome) {
	now := time.Now()
	cleanupStatus := models.FailoverCleanupStatusSkipped
	cleanupResult := map[string]interface{}{"message": "cleanup not requested"}
	deleteStrategy := effectiveTaskDeleteStrategy(r.task)

	if deleteStrategy != models.FailoverDeleteStrategyKeep && outcome != nil && outcome.Cleanup != nil {
		cleanupStep := r.startStep("cleanup_old", "Cleanup Old Instance", map[string]interface{}{
			"strategy": deleteStrategy,
			"label":    outcome.CleanupLabel,
		})
		_ = failoverdb.UpdateExecutionFields(r.execution.ID, map[string]interface{}{
			"status": models.FailoverExecutionStatusCleaningOld,
		})

		if deleteStrategy == models.FailoverDeleteStrategyDeleteAfterSuccessDelay && r.task.DeleteDelaySeconds > 0 {
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
	if err := failoverdb.FailRunningStepsForExecution(r.execution.ID, message); err != nil {
		log.Printf("failover: failed to mark running steps failed for execution %d: %v", r.execution.ID, err)
	}
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

func effectiveTaskDeleteStrategy(task models.FailoverTask) string {
	for _, plan := range task.Plans {
		if plan.Enabled && strings.TrimSpace(plan.ActionType) == models.FailoverActionProvisionInstance {
			if strings.TrimSpace(task.DeleteStrategy) == models.FailoverDeleteStrategyDeleteAfterSuccessDelay {
				return models.FailoverDeleteStrategyDeleteAfterSuccessDelay
			}
			return models.FailoverDeleteStrategyDeleteAfterSuccess
		}
	}
	return models.FailoverDeleteStrategyKeep
}

func (r *executionRunner) attachCurrentOutletCleanup(outcome *actionOutcome, plan models.FailoverPlan, candidate providerPoolCandidate) error {
	if outcome == nil || outcome.Cleanup != nil {
		return nil
	}
	cleanup, err := resolveCurrentInstanceCleanupFromRef(r.task.UserID, parseJSONMap(r.task.CurrentInstanceRef))
	if err != nil {
		return err
	}
	if cleanup == nil {
		address, addressErr := r.ensureCurrentOutletAddress()
		if addressErr != nil {
			return addressErr
		}
		if address != "" {
			cleanup, err = r.resolveCurrentInstanceCleanupByAddress(plan, candidate)
			if err != nil {
				return err
			}
		}
	}
	if cleanup == nil {
		return nil
	}
	if len(cleanup.Ref) > 0 {
		rawRef := marshalJSON(cleanup.Ref)
		r.task.CurrentInstanceRef = rawRef
		_ = failoverdb.UpdateTaskFields(r.task.ID, map[string]interface{}{
			"current_instance_ref": rawRef,
		})
	}
	outcome.OldInstanceRef = cloneJSONMap(cleanup.Ref)
	outcome.CleanupLabel = cleanup.Label
	outcome.Cleanup = cleanup.Cleanup
	return nil
}

func (r *executionRunner) ensureCurrentOutletAddress() (string, error) {
	address := strings.TrimSpace(r.task.CurrentAddress)
	if address != "" {
		return address, nil
	}

	clientUUID := strings.TrimSpace(r.task.WatchClientUUID)
	userUUID := strings.TrimSpace(r.task.UserID)
	if clientUUID == "" || userUUID == "" {
		return "", nil
	}

	client, err := clientdb.GetClientByUUIDForUser(clientUUID, userUUID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return "", nil
		}
		return "", err
	}

	address = strings.TrimSpace(firstNonEmpty(client.IPv4, client.IPv6))
	if address == "" {
		return "", nil
	}

	r.task.CurrentAddress = address
	_ = failoverdb.UpdateTaskFields(r.task.ID, map[string]interface{}{
		"current_address": address,
	})
	return address, nil
}

func (r *executionRunner) recycleCurrentOutletForCandidate(plan models.FailoverPlan, candidate providerPoolCandidate) (map[string]interface{}, error) {
	currentRef := parseJSONMap(r.task.CurrentInstanceRef)
	cleanup, err := resolveCurrentInstanceCleanupFromRef(r.task.UserID, currentRef)
	if err != nil {
		return nil, err
	}
	staleCurrentOutlet := cleanup != nil && cleanup.Missing
	if cleanup != nil && (strings.ToLower(strings.TrimSpace(stringMapValue(cleanup.Ref, "provider"))) != strings.ToLower(strings.TrimSpace(plan.Provider)) ||
		strings.TrimSpace(providerEntryIDFromRef(cleanup.Ref)) != strings.TrimSpace(candidate.EntryID)) {
		cleanup = nil
		staleCurrentOutlet = false
	}
	if staleCurrentOutlet {
		cleanup = nil
	}
	if cleanup == nil {
		cleanup, err = r.resolveCurrentInstanceCleanupByAddress(plan, candidate)
		if err != nil {
			return nil, err
		}
	}
	if cleanup == nil && staleCurrentOutlet {
		reclaimStep := r.startStep("reclaim_current", "Pre-Provision Old Instance Cleanup", map[string]interface{}{
			"provider": plan.Provider,
			"entry_id": candidate.EntryID,
			"ref":      currentRef,
		})
		detail := map[string]interface{}{
			"message": "current outlet was already missing; continuing with provisioning",
			"missing": true,
		}
		if len(currentRef) > 0 {
			detail["ref"] = currentRef
		}
		r.clearCurrentOutletTracking()
		r.finishStep(reclaimStep, models.FailoverStepStatusSkipped, "current outlet was already missing; skipping delete", detail)
		return detail, nil
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
	r.clearCurrentOutletTracking()
	r.finishStep(reclaimStep, models.FailoverStepStatusSuccess, "current failed outlet deleted to free capacity", detail)
	return detail, nil
}

func (r *executionRunner) clearCurrentOutletTracking() {
	r.task.CurrentInstanceRef = ""
	r.task.CurrentAddress = ""
	_ = failoverdb.UpdateTaskFields(r.task.ID, map[string]interface{}{
		"current_instance_ref": "",
		"current_address":      "",
	})
}

func resolveDigitalOceanCurrentInstanceCleanupForToken(userUUID, address string, tokenAddition *digitalocean.Addition, token *digitalocean.TokenRecord, entryID, entryName string) (*currentInstanceCleanup, error) {
	if tokenAddition == nil || token == nil {
		return nil, nil
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
			"provider_entry_id":   strings.TrimSpace(entryID),
			"provider_entry_name": firstNonEmpty(strings.TrimSpace(entryName), strings.TrimSpace(token.Name)),
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
					if isDigitalOceanNotFoundError(err) {
						removeSavedDigitalOceanRootPassword(userUUID, tokenAddition, token, droplet.ID)
						return nil
					}
					return err
				}
				removeSavedDigitalOceanRootPassword(userUUID, tokenAddition, token, droplet.ID)
				return nil
			},
		}, nil
	}
	return nil, nil
}

func resolveLinodeCurrentInstanceCleanupForToken(userUUID, address string, tokenAddition *linodecloud.Addition, token *linodecloud.TokenRecord, entryID, entryName string) (*currentInstanceCleanup, error) {
	if tokenAddition == nil || token == nil {
		return nil, nil
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
			"provider_entry_id":   strings.TrimSpace(entryID),
			"provider_entry_name": firstNonEmpty(strings.TrimSpace(entryName), strings.TrimSpace(token.Name)),
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
					if isLinodeNotFoundError(err) {
						removeSavedLinodeRootPassword(userUUID, tokenAddition, token, instance.ID)
						return nil
					}
					return err
				}
				removeSavedLinodeRootPassword(userUUID, tokenAddition, token, instance.ID)
				return nil
			},
		}, nil
	}
	return nil, nil
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
		cleanup, err := resolveDigitalOceanCurrentInstanceCleanupForToken(r.task.UserID, address, addition, token, candidate.EntryID, candidate.EntryName)
		if err != nil {
			return nil, err
		}
		if cleanup != nil {
			return cleanup, nil
		}
		return nil, nil
	case "linode":
		addition, token, err := loadLinodeToken(r.task.UserID, candidate.EntryID)
		if err != nil {
			return nil, err
		}
		cleanup, err := resolveLinodeCurrentInstanceCleanupForToken(r.task.UserID, address, addition, token, candidate.EntryID, candidate.EntryName)
		if err != nil {
			return nil, err
		}
		if cleanup != nil {
			return cleanup, nil
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
		droplets, err := client.ListDroplets(context.Background())
		if err != nil {
			return nil, err
		}
		exists := false
		for _, droplet := range droplets {
			if droplet.ID == dropletID {
				exists = true
				break
			}
		}
		if !exists {
			return &currentInstanceCleanup{
				Ref:     resolvedRef,
				Label:   fmt.Sprintf("delete digitalocean droplet %d", dropletID),
				Missing: true,
			}, nil
		}
		return &currentInstanceCleanup{
			Ref:   resolvedRef,
			Label: fmt.Sprintf("delete digitalocean droplet %d", dropletID),
			Cleanup: func() error {
				if err := client.DeleteDroplet(context.Background(), dropletID); err != nil {
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
		if _, err := client.GetInstance(context.Background(), instanceID); err != nil {
			if isLinodeNotFoundError(err) {
				return &currentInstanceCleanup{
					Ref:     resolvedRef,
					Label:   fmt.Sprintf("delete linode instance %d", instanceID),
					Missing: true,
				}, nil
			}
			return nil, err
		}
		return &currentInstanceCleanup{
			Ref:   resolvedRef,
			Label: fmt.Sprintf("delete linode instance %d", instanceID),
			Cleanup: func() error {
				if err := client.DeleteInstance(context.Background(), instanceID); err != nil {
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

func ensureCommandResult(result *commandResult) *commandResult {
	if result != nil {
		return result
	}
	return &commandResult{}
}

func commandResultExecutionError(result *commandResult) error {
	if result == nil || result.ExitCode == nil || *result.ExitCode == 0 {
		return nil
	}

	message := fmt.Sprintf("script exited with code %d", *result.ExitCode)
	if excerpt := firstMeaningfulOutputLine(result.Output); excerpt != "" {
		message += ": " + excerpt
	}
	return errors.New(message)
}

func firstMeaningfulOutputLine(output string) string {
	for _, line := range strings.Split(strings.ReplaceAll(output, "\r\n", "\n"), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if len(trimmed) > 200 {
			return trimmed[:200]
		}
		return trimmed
	}
	return ""
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

func waitForClientByGroup(
	ctx context.Context,
	userUUID,
	group,
	excludeUUID string,
	startedAt time.Time,
	timeoutSeconds int,
	expectedAddresses map[string]struct{},
) (string, error) {
	if timeoutSeconds <= 0 {
		timeoutSeconds = 600
	}
	deadline := time.Now().Add(time.Duration(timeoutSeconds) * time.Second)
	group = strings.TrimSpace(group)

	for time.Now().Before(deadline) {
		if err := waitContextOrDelay(ctx, 0); err != nil {
			return "", err
		}
		clientList, err := clientdb.GetAllClientBasicInfoByUser(userUUID)
		if err != nil {
			return "", err
		}
		online := ws.GetConnectedClients()

		candidates := make([]models.Client, 0)
		for _, client := range clientList {
			if strings.TrimSpace(client.Group) != group || client.UUID == excludeUUID {
				continue
			}
			if _, ok := online[client.UUID]; !ok {
				continue
			}
			candidates = append(candidates, client)
		}
		if clientUUID := pickPreferredAutoConnectClient(candidates, startedAt, expectedAddresses); clientUUID != "" {
			return clientUUID, nil
		}
		if err := waitContextOrDelay(ctx, 5*time.Second); err != nil {
			return "", err
		}
	}
	return "", fmt.Errorf("timed out waiting for agent group %q", group)
}

func expectedClientAddresses(outcome *actionOutcome) map[string]struct{} {
	addresses := make(map[string]struct{})
	if outcome == nil {
		return addresses
	}

	for _, value := range []string{
		outcome.IPv4,
		outcome.IPv6,
		primaryOutcomeAddress(outcome),
	} {
		addExpectedClientAddress(addresses, value)
	}
	collectExpectedAddresses(addresses, outcome.NewAddresses)
	return addresses
}

func addExpectedClientAddress(addresses map[string]struct{}, value string) {
	normalized := normalizeIPAddress(value)
	if normalized == "" {
		return
	}
	addresses[normalized] = struct{}{}
}

func collectExpectedAddresses(addresses map[string]struct{}, value interface{}) {
	switch raw := value.(type) {
	case map[string]interface{}:
		for _, nested := range raw {
			collectExpectedAddresses(addresses, nested)
		}
	case []interface{}:
		for _, nested := range raw {
			collectExpectedAddresses(addresses, nested)
		}
	case []string:
		for _, nested := range raw {
			addExpectedClientAddress(addresses, nested)
		}
	case string:
		addExpectedClientAddress(addresses, raw)
	}
}

func normalizeIPAddress(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	ip := net.ParseIP(value)
	if ip == nil {
		return ""
	}
	return ip.String()
}

func clientMatchesExpectedAddress(client models.Client, expectedAddresses map[string]struct{}) bool {
	if len(expectedAddresses) == 0 {
		return false
	}
	for _, value := range []string{client.IPv4, client.IPv6} {
		normalized := normalizeIPAddress(value)
		if normalized == "" {
			continue
		}
		if _, ok := expectedAddresses[normalized]; ok {
			return true
		}
	}
	return false
}

func clientCreatedForExecution(client models.Client, startedAt time.Time) bool {
	createdAt := client.CreatedAt.ToTime()
	if createdAt.IsZero() {
		return false
	}
	return !createdAt.Before(startedAt)
}

func pickPreferredAutoConnectClient(
	candidates []models.Client,
	startedAt time.Time,
	expectedAddresses map[string]struct{},
) string {
	if len(candidates) == 0 {
		return ""
	}

	ipMatches := make([]models.Client, 0, len(candidates))
	newClients := make([]models.Client, 0, len(candidates))
	for _, client := range candidates {
		if clientMatchesExpectedAddress(client, expectedAddresses) {
			ipMatches = append(ipMatches, client)
		}
		if clientCreatedForExecution(client, startedAt) {
			newClients = append(newClients, client)
		}
	}

	if len(ipMatches) > 0 {
		sortClientsNewestFirst(ipMatches)
		return ipMatches[0].UUID
	}
	if len(expectedAddresses) > 0 {
		return ""
	}
	if len(newClients) > 0 {
		sortClientsNewestFirst(newClients)
		return newClients[0].UUID
	}
	return ""
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

func registerExecutionCancel(executionID uint, cancel context.CancelFunc) {
	if executionID == 0 || cancel == nil {
		return
	}
	executionStopMu.Lock()
	defer executionStopMu.Unlock()
	executionCancels[executionID] = cancel
}

func unregisterExecutionCancel(executionID uint) {
	if executionID == 0 {
		return
	}
	executionStopMu.Lock()
	defer executionStopMu.Unlock()
	delete(executionCancels, executionID)
}

func cancelExecution(executionID uint) {
	if executionID == 0 {
		return
	}
	executionStopMu.Lock()
	cancel := executionCancels[executionID]
	executionStopMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (r *executionRunner) checkStopped() error {
	if r == nil || r.ctx == nil {
		return nil
	}
	select {
	case <-r.ctx.Done():
		return errExecutionStopped
	default:
		return nil
	}
}

func waitContextOrDelay(ctx context.Context, delay time.Duration) error {
	if ctx == nil {
		if delay > 0 {
			time.Sleep(delay)
		}
		return nil
	}
	if delay <= 0 {
		select {
		case <-ctx.Done():
			return errExecutionStopped
		default:
			return nil
		}
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return errExecutionStopped
	case <-timer.C:
		return nil
	}
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

func provisionAWSInstance(ctx context.Context, userUUID string, plan models.FailoverPlan) (*actionOutcome, error) {
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
		if plan.AutoConnectGroup != "" || planHasScripts(plan) {
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
		instance, err := awscloud.CreateInstance(ctx, credential, region, awscloud.CreateInstanceRequest{
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
		instance, detail, err := waitForAWSEC2Instance(ctx, region, credential, strings.TrimSpace(instance.InstanceID))
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
		if plan.AutoConnectGroup != "" || planHasScripts(plan) {
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
		if err := awscloud.CreateLightsailInstance(ctx, credential, region, awscloud.CreateLightsailInstanceRequest{
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
		detail, err := waitForLightsailInstance(ctx, region, credential, name)
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

func provisionDigitalOceanDroplet(ctx context.Context, userUUID string, plan models.FailoverPlan) (*actionOutcome, error) {
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
	if plan.AutoConnectGroup != "" || planHasScripts(plan) {
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

	droplet, err := client.CreateDroplet(ctx, digitalocean.CreateDropletRequest{
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
	droplet, err = waitForDigitalOceanDroplet(ctx, client, droplet.ID)
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

func provisionLinodeInstance(ctx context.Context, userUUID string, plan models.FailoverPlan) (*actionOutcome, error) {
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
	if plan.AutoConnectGroup != "" || planHasScripts(plan) {
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

	instance, err := client.CreateInstance(ctx, request)
	if err != nil {
		return nil, err
	}
	instance, err = waitForLinodeInstance(ctx, client, instance.ID)
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
		detail, err = waitForLightsailInstance(context.Background(), region, credential, instanceName)
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

func waitForAWSEC2Instance(ctx context.Context, region string, credential *awscloud.CredentialRecord, instanceID string) (*awscloud.Instance, *awscloud.InstanceDetail, error) {
	deadline := time.Now().Add(5 * time.Minute)
	for time.Now().Before(deadline) {
		if err := waitContextOrDelay(ctx, 0); err != nil {
			return nil, nil, err
		}
		instance, err := awscloud.GetInstance(ctx, credential, region, instanceID)
		if err == nil && instance != nil && strings.TrimSpace(instance.PublicIP) != "" {
			detail, detailErr := awscloud.GetInstanceDetail(ctx, credential, region, instanceID)
			if detailErr != nil {
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
		return nil, nil, err
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
		if err == nil && detail != nil && strings.TrimSpace(detail.Instance.PublicIP) != "" {
			return detail, nil
		}
		if err := waitContextOrDelay(ctx, 5*time.Second); err != nil {
			return nil, err
		}
	}
	return awscloud.GetLightsailInstanceDetail(ctx, credential, region, instanceName)
}

func waitForDigitalOceanDroplet(ctx context.Context, client *digitalocean.Client, dropletID int) (*digitalocean.Droplet, error) {
	deadline := time.Now().Add(5 * time.Minute)
	for time.Now().Before(deadline) {
		if err := waitContextOrDelay(ctx, 0); err != nil {
			return nil, err
		}
		droplets, err := client.ListDroplets(ctx)
		if err != nil {
			return nil, err
		}
		for _, droplet := range droplets {
			if droplet.ID == dropletID && digitalOceanPublicIPv4(&droplet) != "" {
				return &droplet, nil
			}
		}
		if err := waitContextOrDelay(ctx, 5*time.Second); err != nil {
			return nil, err
		}
	}
	droplets, err := client.ListDroplets(ctx)
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

func waitForLinodeInstance(ctx context.Context, client *linodecloud.Client, instanceID int) (*linodecloud.Instance, error) {
	deadline := time.Now().Add(5 * time.Minute)
	for time.Now().Before(deadline) {
		if err := waitContextOrDelay(ctx, 0); err != nil {
			return nil, err
		}
		instance, err := client.GetInstance(ctx, instanceID)
		if err == nil && instance != nil && firstString(instance.IPv4) != "" {
			return instance, nil
		}
		if err := waitContextOrDelay(ctx, 5*time.Second); err != nil {
			return nil, err
		}
	}
	return client.GetInstance(ctx, instanceID)
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
