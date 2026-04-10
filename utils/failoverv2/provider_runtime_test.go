package failoverv2

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRunAWSProvisionFollowUpPreservesParentCancellation(t *testing.T) {
	parent, cancel := context.WithTimeout(context.Background(), time.Minute)
	cancel()

	err := runAWSProvisionFollowUp(parent, func(runCtx context.Context) error {
		return runCtx.Err()
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}
