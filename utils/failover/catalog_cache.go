package failover

import (
	"encoding/json"
	"sync"
	"time"
)

const catalogCacheTTL = 45 * time.Second

type catalogCacheEntry struct {
	expiresAt time.Time
	payload   []byte
}

var (
	catalogCacheMu sync.Mutex
	catalogCache   = map[string]catalogCacheEntry{}
)

func loadCatalogWithCache[T any](cacheKey string, loader func() (*T, error)) (*T, error) {
	if cached, ok := getCachedCatalog[T](cacheKey); ok {
		return cached, nil
	}

	value, err := loader()
	if err != nil || value == nil {
		return value, err
	}

	setCachedCatalog(cacheKey, value)
	return cloneCatalogValue(value)
}

func getCachedCatalog[T any](cacheKey string) (*T, bool) {
	now := time.Now()

	catalogCacheMu.Lock()
	defer catalogCacheMu.Unlock()

	entry, ok := catalogCache[cacheKey]
	if !ok {
		return nil, false
	}
	if now.After(entry.expiresAt) {
		delete(catalogCache, cacheKey)
		return nil, false
	}

	var value T
	if err := json.Unmarshal(entry.payload, &value); err != nil {
		delete(catalogCache, cacheKey)
		return nil, false
	}
	return &value, true
}

func setCachedCatalog[T any](cacheKey string, value *T) {
	payload, err := json.Marshal(value)
	if err != nil {
		return
	}

	catalogCacheMu.Lock()
	defer catalogCacheMu.Unlock()

	catalogCache[cacheKey] = catalogCacheEntry{
		expiresAt: time.Now().Add(catalogCacheTTL),
		payload:   payload,
	}
}

func cloneCatalogValue[T any](value *T) (*T, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return value, nil
	}

	var cloned T
	if err := json.Unmarshal(payload, &cloned); err != nil {
		return value, nil
	}
	return &cloned, nil
}
