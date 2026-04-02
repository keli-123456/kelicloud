package awsfollowup

import (
	"errors"
	"testing"

	smithy "github.com/aws/smithy-go"
	"github.com/komari-monitor/komari/database/models"
	"gorm.io/gorm"
)

type testAPIError struct {
	code    string
	message string
}

func (e testAPIError) Error() string {
	if e.message != "" {
		return e.message
	}
	return e.code
}

func (e testAPIError) ErrorCode() string {
	return e.code
}

func (e testAPIError) ErrorMessage() string {
	return e.message
}

func (e testAPIError) ErrorFault() smithy.ErrorFault {
	return smithy.FaultUnknown
}

func TestClassifyTerminalTaskError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "missing credential config cancels task",
			err:  gorm.ErrRecordNotFound,
			want: models.AWSFollowUpTaskStatusCancelled,
		},
		{
			name: "missing credential cancels task",
			err:  errors.New("aws credential not found for follow-up task"),
			want: models.AWSFollowUpTaskStatusCancelled,
		},
		{
			name: "missing instance skips task",
			err:  testAPIError{code: "InvalidInstanceID.NotFound", message: "instance missing"},
			want: models.AWSFollowUpTaskStatusSkipped,
		},
		{
			name: "resource not found message skips task",
			err:  errors.New("lightsail instance not found"),
			want: models.AWSFollowUpTaskStatusSkipped,
		},
		{
			name: "generic error retries",
			err:  errors.New("temporary throttling"),
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := classifyTerminalTaskError(tt.err); got != tt.want {
				t.Fatalf("expected %q, got %q", tt.want, got)
			}
		})
	}
}
