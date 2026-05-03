package api

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/komari-monitor/komari/utils"
)

const (
	loginRateLimitMaxFailuresEnv = "KOMARI_LOGIN_MAX_FAILURES"
	loginRateLimitWindowEnv      = "KOMARI_LOGIN_WINDOW"
	loginRateLimitLockoutEnv     = "KOMARI_LOGIN_LOCKOUT"
)

type loginAttemptBucket struct {
	failures    int
	firstSeen   time.Time
	lockedUntil time.Time
	lastSeen    time.Time
}

type loginAttemptLimiter struct {
	mu      sync.Mutex
	buckets map[string]*loginAttemptBucket
}

var loginAttempts = &loginAttemptLimiter{
	buckets: make(map[string]*loginAttemptBucket),
}

func rejectLimitedLogin(c *gin.Context, username string) bool {
	limitedUntil, limited := loginAttempts.LimitedUntil(c.ClientIP(), username)
	if !limited {
		return false
	}

	retryAfter := int(time.Until(limitedUntil).Seconds())
	if retryAfter < 1 {
		retryAfter = 1
	}
	c.Header("Retry-After", strconv.Itoa(retryAfter))
	RespondError(c, http.StatusTooManyRequests, "Too many login attempts. Try again later.")
	c.Abort()
	return true
}

func recordFailedLoginAttempt(c *gin.Context, username string) {
	loginAttempts.RecordFailure(c.ClientIP(), username)
}

func recordSuccessfulLoginAttempt(c *gin.Context, username string) {
	loginAttempts.RecordSuccess(c.ClientIP(), username)
}

func (l *loginAttemptLimiter) LimitedUntil(ip, username string) (time.Time, bool) {
	settings := currentLoginRateLimitSettings()
	keys := loginRateLimitKeys(ip, username)
	now := time.Now()

	l.mu.Lock()
	defer l.mu.Unlock()
	l.pruneLocked(now, settings.window+settings.lockout)

	for _, key := range keys {
		bucket := l.buckets[key]
		if bucket == nil {
			continue
		}
		if now.Before(bucket.lockedUntil) {
			return bucket.lockedUntil, true
		}
		if now.Sub(bucket.firstSeen) > settings.window {
			delete(l.buckets, key)
		}
	}
	return time.Time{}, false
}

func (l *loginAttemptLimiter) RecordFailure(ip, username string) {
	settings := currentLoginRateLimitSettings()
	keys := loginRateLimitKeys(ip, username)
	now := time.Now()

	l.mu.Lock()
	defer l.mu.Unlock()
	l.pruneLocked(now, settings.window+settings.lockout)

	for _, key := range keys {
		bucket := l.buckets[key]
		if bucket == nil || now.Sub(bucket.firstSeen) > settings.window {
			bucket = &loginAttemptBucket{firstSeen: now}
			l.buckets[key] = bucket
		}
		bucket.failures++
		bucket.lastSeen = now
		if bucket.failures >= settings.maxFailures {
			bucket.lockedUntil = now.Add(settings.lockout)
		}
	}
}

func (l *loginAttemptLimiter) RecordSuccess(ip, username string) {
	keys := loginRateLimitKeys(ip, username)
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, key := range keys {
		delete(l.buckets, key)
	}
}

func (l *loginAttemptLimiter) pruneLocked(now time.Time, ttl time.Duration) {
	if ttl <= 0 {
		ttl = 30 * time.Minute
	}
	for key, bucket := range l.buckets {
		if now.Sub(bucket.lastSeen) > ttl && now.After(bucket.lockedUntil) {
			delete(l.buckets, key)
		}
	}
}

type loginRateLimitSettings struct {
	maxFailures int
	window      time.Duration
	lockout     time.Duration
}

func currentLoginRateLimitSettings() loginRateLimitSettings {
	window := utils.DurationFromEnv(loginRateLimitWindowEnv, 10*time.Minute)
	if window < time.Minute {
		window = time.Minute
	}
	lockout := utils.DurationFromEnv(loginRateLimitLockoutEnv, 15*time.Minute)
	if lockout < time.Minute {
		lockout = time.Minute
	}

	return loginRateLimitSettings{
		maxFailures: utils.IntFromEnv(loginRateLimitMaxFailuresEnv, 8, 1, 100),
		window:      window,
		lockout:     lockout,
	}
}

func loginRateLimitKeys(ip, username string) []string {
	ip = strings.TrimSpace(ip)
	if ip == "" {
		ip = "unknown"
	}
	username = strings.ToLower(strings.TrimSpace(username))
	if username == "" {
		username = "unknown"
	}
	return []string{
		fmt.Sprintf("ip:%s", ip),
		fmt.Sprintf("ip-user:%s:%s", ip, username),
	}
}
