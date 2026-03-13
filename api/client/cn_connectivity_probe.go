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
}

func init() {
	config.Subscribe(func(event config.ConfigEvent) {
		if !event.IsChanged(config.CNConnectivityEnabledKey) &&
			!event.IsChanged(config.CNConnectivityTargetKey) &&
			!event.IsChanged(config.CNConnectivityIntervalKey) {
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

	return cnConnectivityProbeConfigMessage{
		Message:                "cn_connectivity_probe_config",
		CNConnectivityEnabled:  enabled,
		CNConnectivityTarget:   target,
		CNConnectivityInterval: interval,
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
