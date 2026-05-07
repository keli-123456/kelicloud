package clientddns

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	clientddnsdb "github.com/komari-monitor/komari/database/clientddns"
	clientdb "github.com/komari-monitor/komari/database/clients"
	"github.com/komari-monitor/komari/database/models"
	failoversvc "github.com/komari-monitor/komari/utils/failover"
	"gorm.io/gorm"
)

var (
	applyDNSRecordFunc         = failoversvc.ApplyDNSRecord
	getBindingByClientForUser  = clientddnsdb.GetBindingByClientForUser
	listEnabledBindings        = clientddnsdb.ListEnabledBindings
	getClientByUUIDForUserFunc = clientdb.GetClientByUUIDForUser
	getClientByUUIDFunc        = clientdb.GetClientByUUID
	updateBindingSyncStateFunc = clientddnsdb.UpdateBindingSyncStateByID
)

var (
	scheduledSyncRunning atomic.Bool
	bindingLocks         sync.Map
)

func RunScheduledSync() {
	if !scheduledSyncRunning.CompareAndSwap(false, true) {
		return
	}
	defer scheduledSyncRunning.Store(false)

	bindings, err := listEnabledBindings()
	if err != nil {
		log.Printf("client ddns: failed to list bindings: %v", err)
		return
	}

	for _, binding := range bindings {
		if _, err := SyncBindingForUser(binding.UserID, binding.ClientUUID, false); err != nil {
			log.Printf("client ddns: sync failed for client %s: %v", binding.ClientUUID, err)
		}
	}
}

func SyncBindingForUser(userUUID, clientUUID string, force bool) (models.ClientDDNSBinding, error) {
	binding, err := getBindingByClientForUser(userUUID, clientUUID)
	if err != nil {
		return models.ClientDDNSBinding{}, err
	}
	client, err := loadClientForBinding(clientUUID, userUUID)
	if err != nil {
		return models.ClientDDNSBinding{}, err
	}
	return syncBinding(binding, client, force)
}

func loadClientForBinding(clientUUID, userUUID string) (models.Client, error) {
	client, err := getClientByUUIDForUserFunc(clientUUID, userUUID)
	if err == nil {
		return client, nil
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return models.Client{}, err
	}
	unscoped, lookupErr := getClientByUUIDFunc(clientUUID)
	if lookupErr != nil {
		return models.Client{}, err
	}
	if strings.TrimSpace(unscoped.UserID) == "" || strings.TrimSpace(unscoped.UserID) == strings.TrimSpace(userUUID) {
		return unscoped, nil
	}
	return models.Client{}, err
}

func syncBinding(binding models.ClientDDNSBinding, client models.Client, force bool) (models.ClientDDNSBinding, error) {
	lock := bindingLock(binding.ClientUUID)
	lock.Lock()
	defer lock.Unlock()

	if !binding.Enabled {
		return binding, errors.New("ddns is disabled")
	}

	ipv4, ipv6, err := selectAddressesForMode(binding.AddressMode, client)
	if err != nil {
		return binding, persistBindingError(binding, strings.TrimSpace(client.IPv4), strings.TrimSpace(client.IPv6), err)
	}

	if !force &&
		strings.TrimSpace(binding.LastError) == "" &&
		binding.LastSyncedAt != nil &&
		strings.TrimSpace(binding.LastIPv4) == ipv4 &&
		strings.TrimSpace(binding.LastIPv6) == ipv6 {
		return binding, nil
	}

	results, err := applyBinding(binding, ipv4, ipv6)
	if err != nil {
		return binding, persistBindingError(binding, ipv4, ipv6, err)
	}

	resultJSON, marshalErr := json.Marshal(results)
	if marshalErr != nil {
		resultJSON = []byte("[]")
	}
	now := models.FromTime(time.Now())
	if err := updateBindingSyncStateFunc(binding.ID, ipv4, ipv6, &now, "", string(resultJSON)); err != nil {
		return binding, err
	}

	binding.LastIPv4 = ipv4
	binding.LastIPv6 = ipv6
	binding.LastSyncedAt = &now
	binding.LastError = ""
	binding.LastResult = string(resultJSON)
	return binding, nil
}

func bindingLock(clientUUID string) *sync.Mutex {
	actual, _ := bindingLocks.LoadOrStore(strings.TrimSpace(clientUUID), &sync.Mutex{})
	lock, _ := actual.(*sync.Mutex)
	return lock
}

func persistBindingError(binding models.ClientDDNSBinding, ipv4, ipv6 string, err error) error {
	message := ""
	if err != nil {
		message = err.Error()
	}
	if updateErr := updateBindingSyncStateFunc(binding.ID, ipv4, ipv6, binding.LastSyncedAt, message, binding.LastResult); updateErr != nil {
		return updateErr
	}
	return err
}

func applyBinding(binding models.ClientDDNSBinding, ipv4, ipv6 string) ([]failoversvc.DNSUpdateResult, error) {
	mode := normalizedAddressMode(binding.AddressMode)
	results := make([]failoversvc.DNSUpdateResult, 0, 2)

	switch mode {
	case models.ClientDDNSAddressModeIPv4:
		result, err := applyDNSRecordFunc(binding.UserID, binding.Provider, binding.EntryID, payloadWithRecordOptions(binding.Payload, "A", false), ipv4, "")
		if err != nil {
			return nil, err
		}
		results = append(results, *result)
	case models.ClientDDNSAddressModeIPv6:
		result, err := applyDNSRecordFunc(binding.UserID, binding.Provider, binding.EntryID, payloadWithRecordOptions(binding.Payload, "AAAA", false), "", ipv6)
		if err != nil {
			return nil, err
		}
		results = append(results, *result)
	case models.ClientDDNSAddressModeDual:
		result, err := applyDNSRecordFunc(binding.UserID, binding.Provider, binding.EntryID, payloadWithRecordOptions(binding.Payload, "A", true), ipv4, ipv6)
		if err != nil {
			return nil, err
		}
		results = append(results, *result)
	default:
		return nil, fmt.Errorf("unsupported ddns address mode: %s", binding.AddressMode)
	}

	return results, nil
}

func payloadWithRecordType(payloadJSON, recordType string) string {
	return payloadWithRecordOptions(payloadJSON, recordType, false)
}

func payloadWithRecordOptions(payloadJSON, recordType string, syncIPv6 bool) string {
	recordType = strings.ToUpper(strings.TrimSpace(recordType))
	if recordType == "" {
		recordType = "A"
	}

	raw := strings.TrimSpace(payloadJSON)
	if raw == "" {
		raw = "{}"
	}

	object := make(map[string]interface{})
	if err := json.Unmarshal([]byte(raw), &object); err != nil {
		object = map[string]interface{}{}
	}
	object["record_type"] = recordType
	if syncIPv6 {
		object["sync_ipv6"] = true
	} else {
		delete(object, "sync_ipv6")
	}

	encoded, err := json.Marshal(object)
	if err != nil {
		return fmt.Sprintf("{\"record_type\":\"%s\"}", recordType)
	}
	return string(encoded)
}

func selectAddressesForMode(addressMode string, client models.Client) (string, string, error) {
	mode := normalizedAddressMode(addressMode)
	ipv4 := normalizePublicIP(client.IPv4)
	ipv6 := normalizePublicIP(client.IPv6)

	switch mode {
	case models.ClientDDNSAddressModeIPv4:
		if ipv4 == "" {
			return "", "", errors.New("node does not have a public IPv4 address")
		}
		return ipv4, "", nil
	case models.ClientDDNSAddressModeIPv6:
		if ipv6 == "" {
			return "", "", errors.New("node does not have a public IPv6 address")
		}
		return "", ipv6, nil
	case models.ClientDDNSAddressModeDual:
		if ipv4 == "" {
			return "", "", errors.New("node does not have a public IPv4 address for dual-stack DDNS")
		}
		if ipv6 == "" {
			return "", "", errors.New("node does not have a public IPv6 address for dual-stack DDNS")
		}
		return ipv4, ipv6, nil
	default:
		return "", "", fmt.Errorf("unsupported ddns address mode: %s", addressMode)
	}
}

func normalizedAddressMode(addressMode string) string {
	switch strings.ToLower(strings.TrimSpace(addressMode)) {
	case models.ClientDDNSAddressModeIPv6:
		return models.ClientDDNSAddressModeIPv6
	case models.ClientDDNSAddressModeDual:
		return models.ClientDDNSAddressModeDual
	default:
		return models.ClientDDNSAddressModeIPv4
	}
}

func normalizePublicIP(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}

	addr, err := netip.ParseAddr(value)
	if err != nil {
		return ""
	}
	if !addr.IsValid() || addr.IsLoopback() || addr.IsMulticast() || addr.IsPrivate() || addr.IsUnspecified() {
		return ""
	}
	if addr.IsLinkLocalUnicast() || addr.IsLinkLocalMulticast() {
		return ""
	}
	return addr.String()
}
