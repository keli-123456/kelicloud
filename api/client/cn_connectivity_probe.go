package client

import (
	"log/slog"

	"github.com/komari-monitor/komari/config"
	"github.com/komari-monitor/komari/ws"
)

type cnConnectivityProbeConfigMessage struct {
	Message                string `json:"message"`
	CNConnectivityEnabled  bool   `json:"cn_connectivity_enabled"`
	CNConnectivityTarget   string `json:"cn_connectivity_target,omitempty"`
	CNConnectivityInterval int    `json:"cn_connectivity_interval"`
	CNConnectivityRetry    int    `json:"cn_connectivity_retry_attempts"`
	CNConnectivityRetryGap int    `json:"cn_connectivity_retry_delay_seconds"`
	CNConnectivityTimeout  int    `json:"cn_connectivity_timeout_seconds"`
}

func init() {
	config.Subscribe(func(event config.ConfigEvent) {
		if !event.IsChanged(config.CNConnectivityEnabledKey) &&
			!event.IsChanged(config.CNConnectivityTargetKey) &&
			!event.IsChanged(config.CNConnectivityIntervalKey) &&
			!event.IsChanged(config.CNConnectivityRetryAttemptsKey) &&
			!event.IsChanged(config.CNConnectivityRetryDelaySecondsKey) &&
			!event.IsChanged(config.CNConnectivityTimeoutSecondsKey) {
			return
		}

		broadcastCNConnectivityProbeConfig()
	})
}

func currentCNConnectivityProbeConfigMessage() cnConnectivityProbeConfigMessage {
	enabled, err := config.GetAs[bool](config.CNConnectivityEnabledKey, false)
	if err != nil {
		slog.Warn("load cn connectivity enabled config failed", "error", err)
	}

	target, err := config.GetAs[string](config.CNConnectivityTargetKey, "")
	if err != nil {
		slog.Warn("load cn connectivity target config failed", "error", err)
	}

	interval, err := config.GetAs[int](config.CNConnectivityIntervalKey, 60)
	if err != nil {
		slog.Warn("load cn connectivity interval config failed", "error", err)
		interval = 60
	}
	if interval <= 0 {
		interval = 60
	}
	retryAttempts, err := config.GetAs[int](config.CNConnectivityRetryAttemptsKey, 3)
	if err != nil {
		slog.Warn("load cn connectivity retry attempts config failed", "error", err)
		retryAttempts = 3
	}
	if retryAttempts <= 0 {
		retryAttempts = 3
	}
	retryDelaySeconds, err := config.GetAs[int](config.CNConnectivityRetryDelaySecondsKey, 1)
	if err != nil {
		slog.Warn("load cn connectivity retry delay seconds config failed", "error", err)
		retryDelaySeconds = 1
	}
	if retryDelaySeconds <= 0 {
		retryDelaySeconds = 1
	}
	timeoutSeconds, err := config.GetAs[int](config.CNConnectivityTimeoutSecondsKey, 5)
	if err != nil {
		slog.Warn("load cn connectivity timeout seconds config failed", "error", err)
		timeoutSeconds = 5
	}
	if timeoutSeconds <= 0 {
		timeoutSeconds = 5
	}

	return cnConnectivityProbeConfigMessage{
		Message:                "cn_connectivity_probe_config",
		CNConnectivityEnabled:  enabled,
		CNConnectivityTarget:   target,
		CNConnectivityInterval: interval,
		CNConnectivityRetry:    retryAttempts,
		CNConnectivityRetryGap: retryDelaySeconds,
		CNConnectivityTimeout:  timeoutSeconds,
	}
}

func pushCNConnectivityProbeConfig(conn *ws.SafeConn) {
	if conn == nil {
		return
	}

	if err := conn.WriteJSON(currentCNConnectivityProbeConfigMessage()); err != nil {
		slog.Warn("push cn connectivity probe config failed", "error", err)
	}
}

func broadcastCNConnectivityProbeConfig() {
	message := currentCNConnectivityProbeConfigMessage()
	for _, conn := range ws.GetConnectedClients() {
		if conn == nil {
			continue
		}
		if err := conn.WriteJSON(message); err != nil {
			slog.Warn("broadcast cn connectivity probe config failed", "error", err)
		}
	}
}
