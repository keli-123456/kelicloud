package utils

import (
	"testing"
	"time"
)

func TestDurationFromEnvParsesDurationAndSeconds(t *testing.T) {
	t.Setenv("KOMARI_TEST_DURATION", "2m")
	if got := DurationFromEnv("KOMARI_TEST_DURATION", time.Second); got != 2*time.Minute {
		t.Fatalf("expected 2m, got %s", got)
	}

	t.Setenv("KOMARI_TEST_DURATION", "30")
	if got := DurationFromEnv("KOMARI_TEST_DURATION", time.Second); got != 30*time.Second {
		t.Fatalf("expected 30s, got %s", got)
	}
}

func TestDurationFromEnvFallsBackForInvalidValues(t *testing.T) {
	fallback := 5 * time.Second
	t.Setenv("KOMARI_TEST_DURATION", "not-a-duration")

	if got := DurationFromEnv("KOMARI_TEST_DURATION", fallback); got != fallback {
		t.Fatalf("expected fallback %s, got %s", fallback, got)
	}
}

func TestIntFromEnvBoundsAndFallback(t *testing.T) {
	t.Setenv("KOMARI_TEST_INT", "7")
	if got := IntFromEnv("KOMARI_TEST_INT", 3, 1, 10); got != 7 {
		t.Fatalf("expected parsed value 7, got %d", got)
	}

	t.Setenv("KOMARI_TEST_INT", "99")
	if got := IntFromEnv("KOMARI_TEST_INT", 3, 1, 10); got != 10 {
		t.Fatalf("expected max-clamped value 10, got %d", got)
	}

	t.Setenv("KOMARI_TEST_INT", "0")
	if got := IntFromEnv("KOMARI_TEST_INT", 3, 1, 10); got != 3 {
		t.Fatalf("expected fallback for below-min value 3, got %d", got)
	}

	t.Setenv("KOMARI_TEST_INT", "invalid")
	if got := IntFromEnv("KOMARI_TEST_INT", 3, 1, 10); got != 3 {
		t.Fatalf("expected fallback for invalid value 3, got %d", got)
	}
}
