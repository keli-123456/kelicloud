package failoverv2

import (
	"testing"

	"github.com/komari-monitor/komari/database/models"
)

func TestAugmentExpectedRecordValuesWithMemberRefsIncludesOtherMembers(t *testing.T) {
	service := &models.FailoverV2Service{
		Members: []models.FailoverV2Member{
			{ID: 1, Enabled: true},
			{ID: 2, Enabled: true},
			{ID: 3, Enabled: false},
		},
	}
	target := &models.FailoverV2Member{ID: 1}

	values := augmentExpectedRecordValuesWithMemberRefs(
		[]string{"2001:db8::1"},
		service,
		target,
		"AAAA",
		nil,
		func(member *models.FailoverV2Member, recordType string) (string, bool) {
			switch member.ID {
			case 2:
				return "2001:db8::2", true
			case 3:
				return "2001:db8::3", true
			default:
				return "", false
			}
		},
	)

	if len(values) != 2 || values[0] != "2001:db8::1" || values[1] != "2001:db8::2" {
		t.Fatalf("expected target+other enabled values, got %#v", values)
	}
}

func TestAugmentExpectedRecordValuesWithMemberRefsSkipsTargetMember(t *testing.T) {
	service := &models.FailoverV2Service{
		Members: []models.FailoverV2Member{
			{ID: 1, Enabled: true},
			{ID: 2, Enabled: true},
		},
	}
	target := &models.FailoverV2Member{ID: 1}

	values := augmentExpectedRecordValuesWithMemberRefs(
		[]string{"203.0.113.10"},
		service,
		target,
		"A",
		nil,
		func(member *models.FailoverV2Member, recordType string) (string, bool) {
			switch member.ID {
			case 1:
				return "203.0.113.99", true
			case 2:
				return "203.0.113.11", true
			default:
				return "", false
			}
		},
	)

	if len(values) != 2 || values[0] != "203.0.113.10" || values[1] != "203.0.113.11" {
		t.Fatalf("expected target value and non-target member value only, got %#v", values)
	}
}
