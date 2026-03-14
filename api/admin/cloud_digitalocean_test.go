package admin

import "testing"

func TestNormalizeRootPasswordMode(t *testing.T) {
	cases := map[string]string{
		"":        "random",
		"random":  "random",
		"RANDOM":  "random",
		"custom":  "custom",
		"CUSTOM":  "custom",
		"ssh":     "",
		"ssh_key": "",
	}

	for input, expected := range cases {
		if actual := normalizeRootPasswordMode(input); actual != expected {
			t.Fatalf("normalizeRootPasswordMode(%q) = %q, want %q", input, actual, expected)
		}
	}
}
