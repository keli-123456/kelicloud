package clientddns

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/komari-monitor/komari/database/models"
	failoversvc "github.com/komari-monitor/komari/utils/failover"
	"gorm.io/gorm"
)

func TestLoadClientForBindingFallsBackToOwnerlessClient(t *testing.T) {
	originalScoped := getClientByUUIDForUserFunc
	originalUnscoped := getClientByUUIDFunc
	t.Cleanup(func() {
		getClientByUUIDForUserFunc = originalScoped
		getClientByUUIDFunc = originalUnscoped
	})

	getClientByUUIDForUserFunc = func(uuid, userUUID string) (models.Client, error) {
		return models.Client{}, gorm.ErrRecordNotFound
	}
	getClientByUUIDFunc = func(uuid string) (models.Client, error) {
		return models.Client{UUID: uuid, UserID: "", IPv4: "1.1.1.1"}, nil
	}

	client, err := loadClientForBinding("node-a", "user-a")
	if err != nil {
		t.Fatalf("loadClientForBinding returned error: %v", err)
	}
	if client.UUID != "node-a" {
		t.Fatalf("expected ownerless fallback client, got %+v", client)
	}
}

func TestLoadClientForBindingRejectsDifferentOwner(t *testing.T) {
	originalScoped := getClientByUUIDForUserFunc
	originalUnscoped := getClientByUUIDFunc
	t.Cleanup(func() {
		getClientByUUIDForUserFunc = originalScoped
		getClientByUUIDFunc = originalUnscoped
	})

	getClientByUUIDForUserFunc = func(uuid, userUUID string) (models.Client, error) {
		return models.Client{}, gorm.ErrRecordNotFound
	}
	getClientByUUIDFunc = func(uuid string) (models.Client, error) {
		return models.Client{UUID: uuid, UserID: "user-b"}, nil
	}

	if _, err := loadClientForBinding("node-a", "user-a"); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("expected record not found error when client belongs to another user, got %v", err)
	}
}

func TestLoadClientForBindingPropagatesScopedLookupError(t *testing.T) {
	originalScoped := getClientByUUIDForUserFunc
	originalUnscoped := getClientByUUIDFunc
	t.Cleanup(func() {
		getClientByUUIDForUserFunc = originalScoped
		getClientByUUIDFunc = originalUnscoped
	})

	getClientByUUIDForUserFunc = func(uuid, userUUID string) (models.Client, error) {
		return models.Client{}, errors.New("db down")
	}
	getClientByUUIDFunc = func(uuid string) (models.Client, error) {
		t.Fatalf("unexpected fallback lookup for %s", uuid)
		return models.Client{}, nil
	}

	if _, err := loadClientForBinding("node-a", "user-a"); err == nil || err.Error() != "db down" {
		t.Fatalf("expected scoped lookup error to be returned, got %v", err)
	}
}

func TestApplyBindingDualStackUsesSingleDNSApplyPlan(t *testing.T) {
	originalApply := applyDNSRecordFunc
	t.Cleanup(func() {
		applyDNSRecordFunc = originalApply
	})

	type dnsApplyCall struct {
		userUUID string
		provider string
		entryID  string
		payload  string
		ipv4     string
		ipv6     string
	}
	calls := make([]dnsApplyCall, 0, 1)
	applyDNSRecordFunc = func(userUUID, provider, entryID, payloadJSON, ipv4, ipv6 string) (*failoversvc.DNSUpdateResult, error) {
		calls = append(calls, dnsApplyCall{
			userUUID: userUUID,
			provider: provider,
			entryID:  entryID,
			payload:  payloadJSON,
			ipv4:     ipv4,
			ipv6:     ipv6,
		})
		return &failoversvc.DNSUpdateResult{
			Provider: provider,
			Records: []failoversvc.DNSUpdateResult{
				{Provider: provider, Type: "A", Value: ipv4},
				{Provider: provider, Type: "AAAA", Value: ipv6},
			},
		}, nil
	}

	results, err := applyBinding(models.ClientDDNSBinding{
		UserID:      "user-a",
		Provider:    "cloudflare",
		EntryID:     "entry-a",
		AddressMode: models.ClientDDNSAddressModeDual,
		Payload:     `{"record_name":"node","sync_ipv6":false}`,
	}, "8.8.8.8", "2001:4860:4860::8888")
	if err != nil {
		t.Fatalf("applyBinding returned error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected one grouped DNS result, got %d", len(results))
	}
	if len(calls) != 1 {
		t.Fatalf("expected dual-stack DDNS to apply once, got %d calls", len(calls))
	}
	call := calls[0]
	if call.userUUID != "user-a" || call.provider != "cloudflare" || call.entryID != "entry-a" {
		t.Fatalf("unexpected apply call identity: %+v", call)
	}
	if call.ipv4 != "8.8.8.8" || call.ipv6 != "2001:4860:4860::8888" {
		t.Fatalf("expected both addresses in one apply call, got ipv4=%q ipv6=%q", call.ipv4, call.ipv6)
	}

	payload := map[string]interface{}{}
	if err := json.Unmarshal([]byte(call.payload), &payload); err != nil {
		t.Fatalf("payload is not valid json: %v", err)
	}
	if payload["record_type"] != "A" {
		t.Fatalf("expected dual-stack apply to anchor on A record, got %v", payload["record_type"])
	}
	if value, ok := payload["sync_ipv6"].(bool); !ok || !value {
		t.Fatalf("expected sync_ipv6=true for dual-stack apply, got %v", payload["sync_ipv6"])
	}
}

func TestApplyBindingIPv4ClearsStaleSyncIPv6Flag(t *testing.T) {
	originalApply := applyDNSRecordFunc
	t.Cleanup(func() {
		applyDNSRecordFunc = originalApply
	})

	var payload map[string]interface{}
	applyDNSRecordFunc = func(userUUID, provider, entryID, payloadJSON, ipv4, ipv6 string) (*failoversvc.DNSUpdateResult, error) {
		if ipv4 != "8.8.4.4" || ipv6 != "" {
			t.Fatalf("expected IPv4-only apply call, got ipv4=%q ipv6=%q", ipv4, ipv6)
		}
		if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
			t.Fatalf("payload is not valid json: %v", err)
		}
		return &failoversvc.DNSUpdateResult{Provider: provider, Type: "A", Value: ipv4}, nil
	}

	_, err := applyBinding(models.ClientDDNSBinding{
		UserID:      "user-a",
		Provider:    "cloudflare",
		EntryID:     "entry-a",
		AddressMode: models.ClientDDNSAddressModeIPv4,
		Payload:     `{"record_name":"node","sync_ipv6":true}`,
	}, "8.8.4.4", "")
	if err != nil {
		t.Fatalf("applyBinding returned error: %v", err)
	}
	if payload["record_type"] != "A" {
		t.Fatalf("expected A record payload, got %v", payload["record_type"])
	}
	if _, ok := payload["sync_ipv6"]; ok {
		t.Fatalf("expected stale sync_ipv6 flag to be removed from IPv4-only payload, got %+v", payload)
	}
}
