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
}

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

func ReloadSchedule() error {
	return nil
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
		return false, fields, ""
	}

	if report == nil || report.CNConnectivity == nil {
		fields["last_status"] = models.FailoverTaskStatusUnknown
		fields["last_message"] = "cn_connectivity report is unavailable"
		return false, fields, ""
	}

	reportTime := report.UpdatedAt
	if report.CNConnectivity.CheckedAt.After(reportTime) {
		reportTime = report.CNConnectivity.CheckedAt
	}
	if reportTime.IsZero() || now.Sub(reportTime) > time.Duration(task.StaleAfterSeconds)*time.Second {
		fields["last_status"] = models.FailoverTaskStatusUnknown
		fields["last_message"] = "latest report is stale"
		return false, fields, ""
	}

	if report.CNConnectivity.Status == "blocked_suspected" && report.CNConnectivity.ConsecutiveFailures >= task.FailureThreshold {
		fields["last_status"] = models.FailoverTaskStatusTriggered
		fields["last_message"] = fmt.Sprintf("cn_connectivity blocked_suspected (%d failures)", report.CNConnectivity.ConsecutiveFailures)
		return true, fields, fields["last_message"].(string)
	}

	fields["last_status"] = models.FailoverTaskStatusHealthy
	fields["last_message"] = report.CNConnectivity.Status
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

		outcome, err := r.executePlan(plan)
		attempt := map[string]interface{}{
			"plan_id":     plan.ID,
			"provider":    plan.Provider,
			"action_type": plan.ActionType,
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

func (r *executionRunner) executePlan(plan models.FailoverPlan) (*actionOutcome, error) {
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
			return nil, waitErr
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

	if plan.ScriptClipboardID != nil {
		if targetClientUUID == "" {
			return nil, errors.New("script execution requires a target client but none became available")
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
			return nil, err
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
		return outcome, nil
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
		return nil, err
	}
	_ = failoverdb.UpdateExecutionFields(r.execution.ID, map[string]interface{}{
		"dns_status": models.FailoverDNSStatusSuccess,
		"dns_result": marshalJSON(dnsResult),
	})
	r.finishStep(dnsStep, models.FailoverStepStatusSuccess, "dns updated", dnsResult)
	return outcome, nil
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
		"last_status":       models.FailoverTaskStatusCooldown,
		"last_message":      "failover completed",
		"last_succeeded_at": models.FromTime(now),
	}
	if outcome != nil {
		if nextClientUUID := strings.TrimSpace(firstNonEmpty(outcome.NewClientUUID, outcome.TargetClientUUID)); nextClientUUID != "" {
			taskUpdates["watch_client_uuid"] = nextClientUUID
		}
		if nextAddress := primaryOutcomeAddress(outcome); nextAddress != "" {
			taskUpdates["current_address"] = nextAddress
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
				"provider":    "aws",
				"service":     "ec2",
				"region":      region,
				"instance_id": instance.InstanceID,
				"name":        instance.Name,
			},
			NewAddresses: map[string]interface{}{
				"public_ip":  instance.PublicIP,
				"private_ip": instance.PrivateIP,
				"addresses":  detail.Addresses,
			},
		}
		if cleanupInstanceID := strings.TrimSpace(payload.CleanupInstanceID); cleanupInstanceID != "" {
			outcome.OldInstanceRef = map[string]interface{}{
				"provider":    "aws",
				"service":     "ec2",
				"region":      region,
				"instance_id": cleanupInstanceID,
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
				"provider":      "aws",
				"service":       "lightsail",
				"region":        region,
				"instance_name": name,
			},
			NewAddresses: map[string]interface{}{
				"public_ip":      detail.Instance.PublicIP,
				"private_ip":     detail.Instance.PrivateIP,
				"ipv6_addresses": detail.Instance.IPv6Addresses,
			},
		}
		if cleanupName := strings.TrimSpace(payload.CleanupInstanceName); cleanupName != "" {
			outcome.OldInstanceRef = map[string]interface{}{
				"provider":      "aws",
				"service":       "lightsail",
				"region":        region,
				"instance_name": cleanupName,
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

	_, token, err := loadDigitalOceanToken(userUUID, plan.ProviderEntryID)
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
	switch passwordMode {
	case "", "none":
	case "random":
		rootPassword, err := digitalocean.GenerateRandomPassword(20)
		if err != nil {
			return nil, err
		}
		userData, err = digitalocean.BuildRootPasswordUserData(rootPassword, userData)
		if err != nil {
			return nil, err
		}
	case "custom":
		if strings.TrimSpace(payload.RootPassword) == "" {
			return nil, errors.New("digitalocean root_password cannot be empty when root_password_mode=custom")
		}
		userData, err = digitalocean.BuildRootPasswordUserData(strings.TrimSpace(payload.RootPassword), userData)
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
	outcome := &actionOutcome{
		IPv4:             digitalOceanPublicIPv4(droplet),
		IPv6:             digitalOceanPublicIPv6(droplet),
		AutoConnectGroup: autoConnectGroup,
		NewInstanceRef: map[string]interface{}{
			"provider":   "digitalocean",
			"droplet_id": droplet.ID,
			"name":       droplet.Name,
		},
		NewAddresses: map[string]interface{}{
			"ipv4": droplet.Networks.V4,
			"ipv6": droplet.Networks.V6,
		},
	}
	if payload.CleanupDropletID > 0 {
		outcome.OldInstanceRef = map[string]interface{}{
			"provider":   "digitalocean",
			"droplet_id": payload.CleanupDropletID,
		}
		outcome.CleanupLabel = fmt.Sprintf("delete digitalocean droplet %d", payload.CleanupDropletID)
		outcome.Cleanup = func() error {
			return client.DeleteDroplet(context.Background(), payload.CleanupDropletID)
		}
	}
	return outcome, nil
}

func provisionLinodeInstance(userUUID string, plan models.FailoverPlan) (*actionOutcome, error) {
	var payload linodeProvisionPayload
	if err := json.Unmarshal([]byte(plan.Payload), &payload); err != nil {
		return nil, fmt.Errorf("invalid linode provision payload: %w", err)
	}

	_, token, err := loadLinodeToken(userUUID, plan.ProviderEntryID)
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
	switch strings.ToLower(strings.TrimSpace(payload.RootPasswordMode)) {
	case "", "random":
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
	outcome := &actionOutcome{
		IPv4:             firstString(instance.IPv4),
		IPv6:             strings.TrimSpace(instance.IPv6),
		AutoConnectGroup: autoConnectGroup,
		NewInstanceRef: map[string]interface{}{
			"provider":    "linode",
			"instance_id": instance.ID,
			"label":       instance.Label,
		},
		NewAddresses: map[string]interface{}{
			"ipv4": instance.IPv4,
			"ipv6": instance.IPv6,
		},
	}
	if payload.CleanupInstanceID > 0 {
		outcome.OldInstanceRef = map[string]interface{}{
			"provider":    "linode",
			"instance_id": payload.CleanupInstanceID,
		}
		outcome.CleanupLabel = fmt.Sprintf("delete linode instance %d", payload.CleanupInstanceID)
		outcome.Cleanup = func() error {
			return client.DeleteInstance(context.Background(), payload.CleanupInstanceID)
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
				"provider":    "aws",
				"service":     "ec2",
				"region":      region,
				"instance_id": instanceID,
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
				"provider":      "aws",
				"service":       "lightsail",
				"region":        region,
				"instance_name": instanceName,
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
