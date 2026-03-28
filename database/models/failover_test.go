package models

import "testing"

func TestNormalizeFailoverScriptClipboardIDsFallsBackToLegacyField(t *testing.T) {
	legacyID := 7

	got := NormalizeFailoverScriptClipboardIDs(&legacyID, "")
	if len(got) != 1 || got[0] != legacyID {
		t.Fatalf("expected legacy clipboard id to be preserved, got %#v", got)
	}
}

func TestNormalizeFailoverScriptClipboardIDsPrefersArrayField(t *testing.T) {
	legacyID := 7

	got := NormalizeFailoverScriptClipboardIDs(&legacyID, `[9,7,9,11]`)
	expected := []int{9, 7, 11}
	if len(got) != len(expected) {
		t.Fatalf("expected %d ids, got %#v", len(expected), got)
	}
	for index := range expected {
		if got[index] != expected[index] {
			t.Fatalf("expected %#v, got %#v", expected, got)
		}
	}
}

func TestEncodeFailoverScriptClipboardIDsNormalizesDuplicates(t *testing.T) {
	got := EncodeFailoverScriptClipboardIDs([]int{4, 4, 0, 9})
	if got != `[4,9]` {
		t.Fatalf("expected normalized payload, got %q", got)
	}
}
