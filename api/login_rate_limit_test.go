package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func resetLoginAttemptsForTest(t *testing.T) {
	t.Helper()
	loginAttempts = &loginAttemptLimiter{
		buckets: make(map[string]*loginAttemptBucket),
	}
	t.Cleanup(func() {
		loginAttempts = &loginAttemptLimiter{
			buckets: make(map[string]*loginAttemptBucket),
		}
	})
}

func TestLoginRateLimiterLocksAfterConfiguredFailures(t *testing.T) {
	resetLoginAttemptsForTest(t)
	t.Setenv(loginRateLimitMaxFailuresEnv, "2")
	t.Setenv(loginRateLimitWindowEnv, "10m")
	t.Setenv(loginRateLimitLockoutEnv, "15m")

	ip := "203.0.113.10"
	username := "admin"

	loginAttempts.RecordFailure(ip, username)
	if _, limited := loginAttempts.LimitedUntil(ip, username); limited {
		t.Fatal("expected first failed login to remain below the limit")
	}

	loginAttempts.RecordFailure(ip, username)
	if _, limited := loginAttempts.LimitedUntil(ip, username); !limited {
		t.Fatal("expected second failed login to trigger the limit")
	}

	if _, limited := loginAttempts.LimitedUntil(ip, "other-user"); !limited {
		t.Fatal("expected IP-level limit to apply across usernames")
	}
}

func TestLoginRateLimiterSuccessClearsFailures(t *testing.T) {
	resetLoginAttemptsForTest(t)
	t.Setenv(loginRateLimitMaxFailuresEnv, "2")
	t.Setenv(loginRateLimitWindowEnv, "10m")
	t.Setenv(loginRateLimitLockoutEnv, "15m")

	ip := "203.0.113.11"
	username := "admin"

	loginAttempts.RecordFailure(ip, username)
	loginAttempts.RecordFailure(ip, username)
	if _, limited := loginAttempts.LimitedUntil(ip, username); !limited {
		t.Fatal("expected login attempt to be limited before success reset")
	}

	loginAttempts.RecordSuccess(ip, username)
	if _, limited := loginAttempts.LimitedUntil(ip, username); limited {
		t.Fatal("expected successful login to clear failed attempts")
	}
}

func TestRejectLimitedLoginRespondsTooManyRequests(t *testing.T) {
	resetLoginAttemptsForTest(t)
	t.Setenv(loginRateLimitMaxFailuresEnv, "1")
	t.Setenv(loginRateLimitWindowEnv, "10m")
	t.Setenv(loginRateLimitLockoutEnv, "15m")
	gin.SetMode(gin.TestMode)

	loginAttempts.RecordFailure("192.0.2.20", "admin")

	router := gin.New()
	router.POST("/login", func(c *gin.Context) {
		if rejectLimitedLogin(c, "admin") {
			return
		}
		c.Status(http.StatusNoContent)
	})

	req := httptest.NewRequest(http.MethodPost, "/login", nil)
	req.RemoteAddr = "192.0.2.20:12345"
	recorder := httptest.NewRecorder()

	router.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusTooManyRequests {
		t.Fatalf("expected HTTP 429, got %d", recorder.Code)
	}
	if recorder.Header().Get("Retry-After") == "" {
		t.Fatal("expected Retry-After header to be set")
	}
}
