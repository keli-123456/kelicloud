package awsfollowup

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"strings"
	"sync/atomic"
	"time"

	smithy "github.com/aws/smithy-go"
	"github.com/komari-monitor/komari/database"
	dbawsfollowup "github.com/komari-monitor/komari/database/awsfollowup"
	"github.com/komari-monitor/komari/database/models"
	awscloud "github.com/komari-monitor/komari/utils/cloudprovider/aws"
	"gorm.io/gorm"
)

const (
	taskLeaseDuration = 2 * time.Minute
	taskRetryDelay    = 15 * time.Second
	taskBatchSize     = 20
	taskRunTimeout    = 45 * time.Second
)

var scheduledRunInProgress atomic.Bool

func EnqueueEC2AssignIPv6(userID, credentialID, region, instanceID string, nextRunAt time.Time) error {
	return dbawsfollowup.EnqueueTask(&models.AWSFollowUpTask{
		UserID:       strings.TrimSpace(userID),
		CredentialID: strings.TrimSpace(credentialID),
		Region:       strings.TrimSpace(region),
		TaskType:     models.AWSFollowUpTaskTypeEC2AssignIPv6,
		ResourceID:   strings.TrimSpace(instanceID),
		NextRunAt:    models.FromTime(nextRunAt),
	})
}

func EnqueueEC2AllowAllTraffic(userID, credentialID, region, instanceID string, nextRunAt time.Time) error {
	return dbawsfollowup.EnqueueTask(&models.AWSFollowUpTask{
		UserID:       strings.TrimSpace(userID),
		CredentialID: strings.TrimSpace(credentialID),
		Region:       strings.TrimSpace(region),
		TaskType:     models.AWSFollowUpTaskTypeEC2AllowAllTraffic,
		ResourceID:   strings.TrimSpace(instanceID),
		NextRunAt:    models.FromTime(nextRunAt),
	})
}

func EnqueueLightsailAllowAllPorts(userID, credentialID, region, instanceName string, nextRunAt time.Time) error {
	return dbawsfollowup.EnqueueTask(&models.AWSFollowUpTask{
		UserID:       strings.TrimSpace(userID),
		CredentialID: strings.TrimSpace(credentialID),
		Region:       strings.TrimSpace(region),
		TaskType:     models.AWSFollowUpTaskTypeLightsailAllowAllPorts,
		ResourceID:   strings.TrimSpace(instanceName),
		NextRunAt:    models.FromTime(nextRunAt),
	})
}

func RunScheduledWork() {
	if !scheduledRunInProgress.CompareAndSwap(false, true) {
		return
	}
	defer scheduledRunInProgress.Store(false)

	now := time.Now()
	tasks, err := dbawsfollowup.ClaimDueTasks(taskBatchSize, now, taskLeaseDuration)
	if err != nil {
		log.Printf("aws follow-up: failed to claim due tasks: %v", err)
		return
	}

	for _, task := range tasks {
		runTask(task)
	}
}

func runTask(task models.AWSFollowUpTask) {
	now := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), taskRunTimeout)
	defer cancel()

	err := executeTask(ctx, task)
	if err == nil {
		if markErr := dbawsfollowup.MarkTaskSucceeded(task.ID, now); markErr != nil {
			log.Printf("aws follow-up: failed to mark task %d successful: %v", task.ID, markErr)
		}
		return
	}

	switch classifyTerminalTaskError(err) {
	case models.AWSFollowUpTaskStatusCancelled:
		if markErr := dbawsfollowup.MarkTaskCancelled(task.ID, now, err); markErr != nil {
			log.Printf("aws follow-up: failed to mark cancelled task %d: %v", task.ID, markErr)
		}
		return
	case models.AWSFollowUpTaskStatusSkipped:
		if markErr := dbawsfollowup.MarkTaskSkipped(task.ID, now, err); markErr != nil {
			log.Printf("aws follow-up: failed to mark skipped task %d: %v", task.ID, markErr)
		}
		return
	case models.AWSFollowUpTaskStatusFailed:
		if markErr := dbawsfollowup.MarkTaskFailed(task.ID, now, err); markErr != nil {
			log.Printf("aws follow-up: failed to mark terminal task %d: %v", task.ID, markErr)
		}
		return
	}

	if markErr := dbawsfollowup.MarkTaskAttempt(task, now, now.Add(taskRetryDelay), err); markErr != nil {
		log.Printf("aws follow-up: failed to reschedule task %d: %v", task.ID, markErr)
		return
	}
}

func executeTask(ctx context.Context, task models.AWSFollowUpTask) error {
	credential, err := loadCredential(task.UserID, task.CredentialID)
	if err != nil {
		return err
	}

	switch task.TaskType {
	case models.AWSFollowUpTaskTypeEC2AssignIPv6:
		_, err = awscloud.EnsureInstanceIPv6Address(ctx, credential, task.Region, task.ResourceID)
		return err
	case models.AWSFollowUpTaskTypeEC2AllowAllTraffic:
		_, err = awscloud.AllowAllSecurityGroupTraffic(ctx, credential, task.Region, task.ResourceID)
		return err
	case models.AWSFollowUpTaskTypeLightsailAllowAllPorts:
		return awscloud.OpenLightsailAllPublicPorts(ctx, credential, task.Region, task.ResourceID)
	default:
		return errors.New("unsupported aws follow-up task type: " + task.TaskType)
	}
}

func loadCredential(userID, credentialID string) (*awscloud.CredentialRecord, error) {
	config, err := database.GetCloudProviderConfigByUserAndName(userID, "aws")
	if err != nil {
		return nil, err
	}

	addition := &awscloud.Addition{}
	raw := strings.TrimSpace(config.Addition)
	if raw == "" {
		raw = "{}"
	}
	if err := json.Unmarshal([]byte(raw), addition); err != nil {
		return nil, err
	}
	addition.Normalize()

	credential := addition.FindCredential(credentialID)
	if credential == nil {
		return nil, errors.New("aws credential not found for follow-up task")
	}
	return credential, nil
}

func classifyTerminalTaskError(err error) string {
	if err == nil {
		return ""
	}

	if errors.Is(err, gorm.ErrRecordNotFound) {
		return models.AWSFollowUpTaskStatusCancelled
	}

	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		code := strings.TrimSpace(apiErr.ErrorCode())
		switch code {
		case "InvalidInstanceID.NotFound", "InvalidNetworkInterfaceID.NotFound", "NotFoundException":
			return models.AWSFollowUpTaskStatusSkipped
		}
	}

	message := strings.ToLower(strings.TrimSpace(err.Error()))
	switch {
	case strings.Contains(message, "aws credential not found for follow-up task"):
		return models.AWSFollowUpTaskStatusCancelled
	case strings.Contains(message, "instance not found"),
		strings.Contains(message, "network interface not found"),
		strings.Contains(message, "lightsail instance not found"),
		strings.Contains(message, "resource not found"):
		return models.AWSFollowUpTaskStatusSkipped
	default:
		return ""
	}
}
