package admin

import (
	"testing"

	_ "github.com/komari-monitor/komari/utils/cloudprovider"
)

func TestParseCloudProviderEntriesFromLegacyObject(t *testing.T) {
	entries, err := parseCloudProviderEntries(`{"api_token":"tok_live_123"}`)
	if err != nil {
		t.Fatalf("parseCloudProviderEntries returned error: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].ID != "default" {
		t.Fatalf("expected legacy entry id to normalize to default, got %q", entries[0].ID)
	}
	if entries[0].Name != defaultCloudProviderEntryName {
		t.Fatalf("expected default entry name, got %q", entries[0].Name)
	}
	if got := entries[0].Values["api_token"]; got != "tok_live_123" {
		t.Fatalf("expected api_token to be preserved, got %#v", got)
	}
}

func TestParseCloudProviderEntriesFromDocument(t *testing.T) {
	entries, err := parseCloudProviderEntries(`{"entries":[{"id":"entry-1","name":"Primary","values":{"api_token":"tok_live_123"}}]}`)
	if err != nil {
		t.Fatalf("parseCloudProviderEntries returned error: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].ID != "entry-1" || entries[0].Name != "Primary" {
		t.Fatalf("unexpected entry: %#v", entries[0])
	}
}

func TestValidateCloudProviderEntriesRequiresName(t *testing.T) {
	_, err := validateCloudProviderEntries("cloudflare", []cloudProviderEntry{{
		Values: map[string]interface{}{"api_token": "tok_live_123"},
	}})
	if err == nil {
		t.Fatal("expected missing name error")
	}
}

func TestValidateCloudProviderEntriesCloudflare(t *testing.T) {
	entries, err := validateCloudProviderEntries("cloudflare", []cloudProviderEntry{{
		Name:   "Primary",
		Values: map[string]interface{}{"api_token": "tok_live_123"},
	}})
	if err != nil {
		t.Fatalf("validateCloudProviderEntries returned error: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].ID == "" {
		t.Fatal("expected generated entry id")
	}
}
