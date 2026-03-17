package accounts

import "testing"

func TestParseUserRole(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		ok       bool
	}{
		{name: "admin", input: "admin", expected: RoleAdmin, ok: true},
		{name: "user", input: "user", expected: RoleUser, ok: true},
		{name: "trim and lower", input: " Admin ", expected: RoleAdmin, ok: true},
		{name: "reject empty", input: "", expected: "", ok: false},
		{name: "reject unknown", input: "owner", expected: "", ok: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			role, ok := ParseUserRole(tt.input)
			if ok != tt.ok {
				t.Fatalf("expected ok=%v, got %v", tt.ok, ok)
			}
			if role != tt.expected {
				t.Fatalf("expected role %q, got %q", tt.expected, role)
			}
		})
	}
}
