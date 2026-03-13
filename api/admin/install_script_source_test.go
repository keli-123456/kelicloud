package admin

import "testing"

func TestBuildAgentInstallScriptURL(t *testing.T) {
	tests := []struct {
		name     string
		base     string
		script   string
		expected string
	}{
		{
			name:     "default official source",
			base:     "",
			script:   "install.sh",
			expected: "https://raw.githubusercontent.com/komari-monitor/komari-agent/refs/heads/main/install.sh",
		},
		{
			name:     "github repo root",
			base:     "https://github.com/example/agent-fork",
			script:   "install.sh",
			expected: "https://raw.githubusercontent.com/example/agent-fork/refs/heads/main/install.sh",
		},
		{
			name:     "github tree path",
			base:     "https://github.com/example/agent-fork/tree/master/scripts",
			script:   "install.ps1",
			expected: "https://raw.githubusercontent.com/example/agent-fork/refs/heads/master/scripts/install.ps1",
		},
		{
			name:     "raw install file",
			base:     "https://raw.githubusercontent.com/example/agent-fork/refs/heads/dev/install.sh",
			script:   "install.ps1",
			expected: "https://raw.githubusercontent.com/example/agent-fork/refs/heads/dev/install.ps1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := buildAgentInstallScriptURL(tt.base, tt.script); got != tt.expected {
				t.Fatalf("buildAgentInstallScriptURL() = %q, want %q", got, tt.expected)
			}
		})
	}
}
