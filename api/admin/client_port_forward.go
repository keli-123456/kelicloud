package admin

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/komari-monitor/komari/api"
	clientportforwarddb "github.com/komari-monitor/komari/database/clientportforward"
	"github.com/komari-monitor/komari/database/models"
	"github.com/komari-monitor/komari/database/tasks"
	"github.com/komari-monitor/komari/utils"
	"github.com/komari-monitor/komari/ws"
	"gorm.io/gorm"
)

const (
	clientPortForwardStatusPending  = "pending"
	clientPortForwardStatusApplied  = "applied"
	clientPortForwardStatusDisabled = "disabled"
	clientPortForwardStatusError    = "error"
)

var clientPortForwardTargetHostPattern = regexp.MustCompile(`^[A-Za-z0-9](?:[A-Za-z0-9.-]{0,253}[A-Za-z0-9])?$`)

type clientPortForwardRuleRequest struct {
	ID         uint   `json:"id"`
	Name       string `json:"name"`
	Enabled    *bool  `json:"enabled"`
	Protocol   string `json:"protocol"`
	ListenPort int    `json:"listen_port"`
	TargetHost string `json:"target_host"`
	TargetPort int    `json:"target_port"`
}

type clientPortForwardRuleView struct {
	ID            uint              `json:"id"`
	ClientUUID    string            `json:"client_uuid"`
	Name          string            `json:"name"`
	Enabled       bool              `json:"enabled"`
	Protocol      string            `json:"protocol"`
	ListenPort    int               `json:"listen_port"`
	TargetHost    string            `json:"target_host"`
	TargetPort    int               `json:"target_port"`
	Status        string            `json:"status"`
	LastTaskID    string            `json:"last_task_id"`
	LastAppliedAt *models.LocalTime `json:"last_applied_at"`
	LastError     string            `json:"last_error"`
	CreatedAt     models.LocalTime  `json:"created_at"`
	UpdatedAt     models.LocalTime  `json:"updated_at"`
}

func buildClientPortForwardRuleView(rule models.ClientPortForwardRule) clientPortForwardRuleView {
	status := clientPortForwardStatusPending
	switch {
	case !rule.Enabled:
		status = clientPortForwardStatusDisabled
	case strings.TrimSpace(rule.LastError) != "":
		status = clientPortForwardStatusError
	case strings.TrimSpace(rule.LastTaskID) != "":
		status = clientPortForwardStatusApplied
	}

	return clientPortForwardRuleView{
		ID:            rule.ID,
		ClientUUID:    rule.ClientUUID,
		Name:          rule.Name,
		Enabled:       rule.Enabled,
		Protocol:      rule.Protocol,
		ListenPort:    rule.ListenPort,
		TargetHost:    rule.TargetHost,
		TargetPort:    rule.TargetPort,
		Status:        status,
		LastTaskID:    rule.LastTaskID,
		LastAppliedAt: rule.LastAppliedAt,
		LastError:     rule.LastError,
		CreatedAt:     rule.CreatedAt,
		UpdatedAt:     rule.UpdatedAt,
	}
}

func buildClientPortForwardRuleViews(rules []models.ClientPortForwardRule) []clientPortForwardRuleView {
	views := make([]clientPortForwardRuleView, 0, len(rules))
	for _, rule := range rules {
		views = append(views, buildClientPortForwardRuleView(rule))
	}
	return views
}

func normalizeClientPortForwardProtocol(value string) (string, error) {
	protocol := strings.ToLower(strings.TrimSpace(value))
	if protocol == "" {
		protocol = models.ClientPortForwardProtocolTCP
	}
	switch protocol {
	case models.ClientPortForwardProtocolTCP, models.ClientPortForwardProtocolUDP:
		return protocol, nil
	default:
		return "", fmt.Errorf("协议只支持 tcp 或 udp")
	}
}

func validateClientPortForwardPort(name string, port int) error {
	if port < 1 || port > 65535 {
		return fmt.Errorf("%s 必须在 1 到 65535 之间", name)
	}
	return nil
}

func normalizeClientPortForwardTargetHost(value string) (string, error) {
	host := strings.TrimSpace(value)
	if host == "" {
		return "", fmt.Errorf("目标地址不能为空")
	}
	if len(host) > 253 {
		return "", fmt.Errorf("目标地址过长")
	}
	if strings.Contains(host, "..") || !clientPortForwardTargetHostPattern.MatchString(host) {
		return "", fmt.Errorf("目标地址只支持 IPv4 或普通域名")
	}
	return host, nil
}

func normalizeClientPortForwardRequest(req clientPortForwardRuleRequest) (models.ClientPortForwardRule, error) {
	protocol, err := normalizeClientPortForwardProtocol(req.Protocol)
	if err != nil {
		return models.ClientPortForwardRule{}, err
	}
	if err := validateClientPortForwardPort("监听端口", req.ListenPort); err != nil {
		return models.ClientPortForwardRule{}, err
	}
	if err := validateClientPortForwardPort("目标端口", req.TargetPort); err != nil {
		return models.ClientPortForwardRule{}, err
	}
	targetHost, err := normalizeClientPortForwardTargetHost(req.TargetHost)
	if err != nil {
		return models.ClientPortForwardRule{}, err
	}

	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}
	name := strings.TrimSpace(req.Name)
	if name == "" {
		name = fmt.Sprintf("%s %d", strings.ToUpper(protocol), req.ListenPort)
	}

	return models.ClientPortForwardRule{
		ID:         req.ID,
		Name:       name,
		Enabled:    enabled,
		Protocol:   protocol,
		ListenPort: req.ListenPort,
		TargetHost: targetHost,
		TargetPort: req.TargetPort,
	}, nil
}

func parseClientPortForwardRuleID(c *gin.Context) (uint, bool) {
	rawID := strings.TrimSpace(c.Param("id"))
	parsed, err := strconv.ParseUint(rawID, 10, 32)
	if err != nil || parsed == 0 {
		api.RespondError(c, http.StatusBadRequest, "规则 ID 无效")
		return 0, false
	}
	return uint(parsed), true
}

func GetClientPortForwardRules(c *gin.Context) {
	scope, client, ok := resolveClientOwnerScope(c)
	if !ok {
		return
	}

	rules, err := clientportforwarddb.ListRulesForUser(scope.UserUUID, client.UUID)
	if err != nil {
		api.RespondError(c, http.StatusInternalServerError, "加载端口中转规则失败: "+err.Error())
		return
	}

	api.RespondSuccess(c, buildClientPortForwardRuleViews(rules))
}

func SaveClientPortForwardRule(c *gin.Context) {
	scope, client, ok := resolveClientOwnerScope(c)
	if !ok {
		return
	}

	var req clientPortForwardRuleRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		api.RespondError(c, http.StatusBadRequest, "请求内容无效: "+err.Error())
		return
	}

	rule, err := normalizeClientPortForwardRequest(req)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, err.Error())
		return
	}

	if rule.Enabled {
		conflict, err := clientportforwarddb.FindEnabledListenPortConflictForUser(
			scope.UserUUID,
			client.UUID,
			rule.Protocol,
			rule.ListenPort,
			rule.ID,
		)
		if err == nil {
			api.RespondError(c, http.StatusBadRequest, fmt.Sprintf("端口 %d/%s 已被规则「%s」使用", conflict.ListenPort, conflict.Protocol, conflict.Name))
			return
		}
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			api.RespondError(c, http.StatusInternalServerError, "检查端口冲突失败: "+err.Error())
			return
		}
	}

	saved, err := clientportforwarddb.SaveRuleForUser(scope.UserUUID, client.UUID, &rule)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			api.RespondError(c, http.StatusNotFound, "端口中转规则不存在")
			return
		}
		api.RespondError(c, http.StatusInternalServerError, "保存端口中转规则失败: "+err.Error())
		return
	}

	api.AuditLogForCurrentUser(c, scope.UserUUID, "save client port forward:"+client.UUID, "info")
	api.RespondSuccess(c, buildClientPortForwardRuleView(saved))
}

func DeleteClientPortForwardRule(c *gin.Context) {
	scope, client, ok := resolveClientOwnerScope(c)
	if !ok {
		return
	}
	ruleID, ok := parseClientPortForwardRuleID(c)
	if !ok {
		return
	}

	if err := clientportforwarddb.DeleteRuleForUser(scope.UserUUID, client.UUID, ruleID); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			api.RespondError(c, http.StatusNotFound, "端口中转规则不存在")
			return
		}
		api.RespondError(c, http.StatusInternalServerError, "删除端口中转规则失败: "+err.Error())
		return
	}

	api.AuditLogForCurrentUser(c, scope.UserUUID, "delete client port forward:"+client.UUID, "warn")
	api.RespondSuccess(c, nil)
}

func ApplyClientPortForwardRules(c *gin.Context) {
	scope, client, ok := resolveClientOwnerScope(c)
	if !ok {
		return
	}

	rules, err := clientportforwarddb.ListRulesForUser(scope.UserUUID, client.UUID)
	if err != nil {
		api.RespondError(c, http.StatusInternalServerError, "加载端口中转规则失败: "+err.Error())
		return
	}

	enabledRules := make([]models.ClientPortForwardRule, 0, len(rules))
	for _, rule := range rules {
		if rule.Enabled {
			enabledRules = append(enabledRules, rule)
		}
	}
	sort.SliceStable(enabledRules, func(i, j int) bool {
		if enabledRules[i].ListenPort == enabledRules[j].ListenPort {
			return enabledRules[i].ID < enabledRules[j].ID
		}
		return enabledRules[i].ListenPort < enabledRules[j].ListenPort
	})

	clientConn := ws.GetConnectedClients()[client.UUID]
	if clientConn == nil {
		api.RespondError(c, http.StatusBadRequest, "节点当前未连接，无法下发 iptables 规则")
		return
	}

	taskID := utils.GenerateRandomString(16)
	if err := tasks.CreateTaskForUser(scope.UserUUID, taskID, []string{client.UUID}); err != nil {
		api.RespondError(c, http.StatusInternalServerError, "创建执行任务失败: "+err.Error())
		return
	}

	payload, _ := json.Marshal(struct {
		Message string `json:"message"`
		Command string `json:"command"`
		TaskID  string `json:"task_id"`
	}{
		Message: "exec",
		Command: buildClientPortForwardApplyScript(enabledRules),
		TaskID:  taskID,
	})
	if err := clientConn.WriteMessage(websocket.TextMessage, payload); err != nil {
		api.RespondError(c, http.StatusBadRequest, "节点连接已断开，无法下发规则: "+client.UUID)
		return
	}

	if len(enabledRules) > 0 {
		ruleIDs := make([]uint, 0, len(enabledRules))
		for _, rule := range enabledRules {
			ruleIDs = append(ruleIDs, rule.ID)
		}
		now := models.FromTime(time.Now())
		if err := clientportforwarddb.UpdateApplyStateForUser(scope.UserUUID, client.UUID, ruleIDs, taskID, &now, ""); err != nil {
			api.RespondError(c, http.StatusInternalServerError, "更新端口中转状态失败: "+err.Error())
			return
		}
	}

	updatedRules, err := clientportforwarddb.ListRulesForUser(scope.UserUUID, client.UUID)
	if err != nil {
		api.RespondError(c, http.StatusInternalServerError, "重新加载端口中转规则失败: "+err.Error())
		return
	}

	api.AuditLogForCurrentUser(c, scope.UserUUID, "apply client port forward:"+client.UUID+", task id: "+taskID, "warn")
	api.RespondSuccess(c, gin.H{
		"task_id":       taskID,
		"enabled_count": len(enabledRules),
		"rules":         buildClientPortForwardRuleViews(updatedRules),
	})
}

func buildClientPortForwardApplyScript(rules []models.ClientPortForwardRule) string {
	var builder strings.Builder
	builder.WriteString("#!/bin/sh\n")
	builder.WriteString("set -eu\n")
	builder.WriteString("if [ \"$(id -u)\" -ne 0 ]; then echo 'Komari port forwarding requires root'; exit 1; fi\n")
	builder.WriteString("if ! command -v iptables >/dev/null 2>&1; then echo 'iptables not found'; exit 1; fi\n")
	builder.WriteString("sysctl -w net.ipv4.ip_forward=1 >/dev/null 2>&1 || true\n")
	builder.WriteString("iptables -w -t nat -N KOMARI_PF 2>/dev/null || true\n")
	builder.WriteString("iptables -w -t nat -N KOMARI_PF_POST 2>/dev/null || true\n")
	builder.WriteString("iptables -w -N KOMARI_PF_FWD 2>/dev/null || true\n")
	builder.WriteString("iptables -w -t nat -C PREROUTING -j KOMARI_PF 2>/dev/null || iptables -w -t nat -A PREROUTING -j KOMARI_PF\n")
	builder.WriteString("iptables -w -t nat -C POSTROUTING -j KOMARI_PF_POST 2>/dev/null || iptables -w -t nat -A POSTROUTING -j KOMARI_PF_POST\n")
	builder.WriteString("iptables -w -C FORWARD -j KOMARI_PF_FWD 2>/dev/null || iptables -w -A FORWARD -j KOMARI_PF_FWD\n")
	builder.WriteString("iptables -w -t nat -F KOMARI_PF\n")
	builder.WriteString("iptables -w -t nat -F KOMARI_PF_POST\n")
	builder.WriteString("iptables -w -F KOMARI_PF_FWD\n")

	for _, rule := range rules {
		protocol := rule.Protocol
		if protocol != models.ClientPortForwardProtocolUDP {
			protocol = models.ClientPortForwardProtocolTCP
		}
		comment := shellSingleQuote(fmt.Sprintf("komari-pf:%d", rule.ID))
		targetHost := shellSingleQuote(rule.TargetHost)
		target := shellSingleQuote(fmt.Sprintf("%s:%d", rule.TargetHost, rule.TargetPort))
		builder.WriteString(fmt.Sprintf(
			"iptables -w -t nat -A KOMARI_PF -p %s --dport %d -m comment --comment %s -j DNAT --to-destination %s\n",
			protocol,
			rule.ListenPort,
			comment,
			target,
		))
		builder.WriteString(fmt.Sprintf(
			"iptables -w -t nat -A KOMARI_PF_POST -p %s -d %s --dport %d -m comment --comment %s -j MASQUERADE\n",
			protocol,
			targetHost,
			rule.TargetPort,
			comment,
		))
		builder.WriteString(fmt.Sprintf(
			"iptables -w -A KOMARI_PF_FWD -p %s -d %s --dport %d -m comment --comment %s -j ACCEPT\n",
			protocol,
			targetHost,
			rule.TargetPort,
			comment,
		))
	}

	builder.WriteString(fmt.Sprintf("echo 'Komari port forwarding applied: %d enabled rule(s)'\n", len(rules)))
	return builder.String()
}
