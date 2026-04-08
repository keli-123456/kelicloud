package failoverv2

import (
	"context"
	"errors"
	"strings"
	"sync"

	failoverv2db "github.com/komari-monitor/komari/database/failoverv2"
	"github.com/komari-monitor/komari/database/models"
)

var (
	errExecutionStopped = errors.New("failover v2 execution stopped by user")

	executionStopMu  sync.Mutex
	executionCancels = map[uint]context.CancelFunc{}
)

func StopExecutionForUser(userUUID string, serviceID, executionID uint) (*models.FailoverV2Execution, error) {
	execution, err := failoverv2db.StopExecutionForUser(userUUID, serviceID, executionID, errExecutionStopped.Error())
	if err != nil {
		return nil, err
	}
	cancelExecution(executionID)
	service, serviceErr := failoverv2db.GetServiceByIDForUser(userUUID, serviceID)
	if serviceErr == nil {
		var member *models.FailoverV2Member
		if execution.MemberID > 0 {
			member, _ = findMemberOnService(service, execution.MemberID)
		}
		notifyExecutionActionCompleted(
			"stop execution request completed",
			service,
			member,
			execution,
			errExecutionStopped.Error(),
		)
	}
	return execution, nil
}

func registerExecutionCancel(executionID uint, cancel context.CancelFunc) {
	if executionID == 0 || cancel == nil {
		return
	}
	executionStopMu.Lock()
	executionCancels[executionID] = cancel
	executionStopMu.Unlock()
}

func unregisterExecutionCancel(executionID uint) {
	if executionID == 0 {
		return
	}
	executionStopMu.Lock()
	delete(executionCancels, executionID)
	executionStopMu.Unlock()
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

func (r *memberExecutionRunner) checkStopped() error {
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

func normalizeExecutionStopError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, errExecutionStopped) || errors.Is(err, context.Canceled) {
		return errExecutionStopped
	}
	return err
}

func executionFailureMessage(prefix string, err error) string {
	normalized := normalizeExecutionStopError(err)
	if normalized == nil {
		return strings.TrimSpace(prefix)
	}
	if errors.Is(normalized, errExecutionStopped) {
		return normalized.Error()
	}
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return normalized.Error()
	}
	return prefix + ": " + normalized.Error()
}
