package config

import "time"

type Legacy struct {
	ID                     uint   `json:"id,omitempty"`                                        // 1
	Sitename               string `json:"sitename" default:"Komari"`                           // 站点名称，默认 "Komari"
	Description            string `json:"description" default:"A simple server monitor tool."` // 站点描述
	AllowCors              bool   `json:"allow_cors" default:"false"`                          // 是否允许跨域，默认 false
	ApiKey                 string `json:"api_key" default:""`                                  // API 密钥，默认空字符串
	AutoDiscoveryKey       string `json:"auto_discovery_key" default:""`                       // 自动发现密钥
	ScriptDomain           string `json:"script_domain" default:""`                            // 自定义脚本域名
	CNConnectivityEnabled  bool   `json:"cn_connectivity_enabled" default:"false"`             // 是否启用国内连通性探测
	CNConnectivityTarget   string `json:"cn_connectivity_target" default:""`                   // 国内连通性探测目标
	CNConnectivityInterval int    `json:"cn_connectivity_interval" default:"60"`               // 国内连通性探测间隔，单位秒
	CNConnectivityRetry    int    `json:"cn_connectivity_retry_attempts" default:"3"`          // 国内连通性探测单目标重试次数
	CNConnectivityRetryGap int    `json:"cn_connectivity_retry_delay_seconds" default:"1"`     // 国内连通性探测单目标重试间隔，单位秒
	CNConnectivityTimeout  int    `json:"cn_connectivity_timeout_seconds" default:"5"`         // 国内连通性探测单次超时时间，单位秒
	OutboundProxyEnabled   bool   `json:"outbound_proxy_enabled" default:"false"`              // 是否启用全局出站代理
	OutboundProxyProtocol  string `json:"outbound_proxy_protocol" default:"socks5"`            // 出站代理协议
	OutboundProxyHost      string `json:"outbound_proxy_host" default:""`                      // 出站代理主机
	OutboundProxyPort      int    `json:"outbound_proxy_port" default:"1080"`                  // 出站代理端口
	OutboundProxyUsername  string `json:"outbound_proxy_username" default:""`                  // 出站代理用户名
	OutboundProxyPassword  string `json:"outbound_proxy_password" default:""`                  // 出站代理密码
	SendIpAddrToGuest      bool   `json:"send_ip_addr_to_guest" default:"false"`               // 是否向访客页面发送 IP 地址，默认 false
	EulaAccepted           bool   `json:"eula_accepted" default:"false"`
	BaseScriptsURL         string `json:"base_scripts_url" default:""` // 安装脚本源地址
	// GeoIP 配置
	GeoIpEnabled  bool   `json:"geo_ip_enabled" default:"true"`
	GeoIpProvider string `json:"geo_ip_provider" default:"ipinfo"` // empty, mmdb, ip-api, geojs
	// Nezha 兼容（Agent gRPC）
	NezhaCompatEnabled bool   `json:"nezha_compat_enabled" default:"false"`
	NezhaCompatListen  string `json:"nezha_compat_listen" default:""` // 例如 0.0.0.0:5555
	// OAuth 配置
	OAuthEnabled         bool   `json:"o_auth_enabled" default:"false"`
	OAuthProvider        string `json:"o_auth_provider" default:"github"`
	DisablePasswordLogin bool   `json:"disable_password_login" default:"false"`
	// 自定义美化
	CustomHead string `json:"custom_head" default:""`
	CustomBody string `json:"custom_body" default:""`
	// 通知
	NotificationEnabled        bool    `json:"notification_enabled" default:"true"` // 通知总开关
	NotificationMethod         string  `json:"notification_method" default:"none"`
	NotificationTemplate       string  `json:"notification_template" default:"{{emoji}}{{emoji}}{{emoji}}\nEvent: {{event}}\nClients: {{client}}\nMessage: {{message}}\nTime: {{time}}"`
	ExpireNotificationEnabled  bool    `json:"expire_notification_enabled" default:"true"` // 是否启用过期通知
	ExpireNotificationLeadDays int     `json:"expire_notification_lead_days" default:"7"`  // 过期前多少天通知，默认7天
	LoginNotification          bool    `json:"login_notification" default:"true"`          // 登录通知
	TrafficLimitPercentage     float64 `json:"traffic_limit_percentage" default:"80.00"`   // 流量限制百分比，默认80.00%
	// Record
	RecordEnabled              bool   `json:"record_enabled" default:"true"`                 // 是否启用记录功能
	RecordPreserveTime         int    `json:"record_preserve_time" default:"720"`            // 记录保留时间，单位小时，默认30天
	PingRecordPreserveTime     int    `json:"ping_record_preserve_time" default:"24"`        // Ping 记录保留时间，单位小时，默认1天
	OfflineCleanupEnabled      bool   `json:"offline_cleanup_enabled" default:"false"`       // 是否启用每日自动清理离线节点
	OfflineCleanupTime         string `json:"offline_cleanup_time" default:"03:00"`          // 每日自动清理离线节点的执行时间，24小时制 HH:MM
	OfflineCleanupGraceHours   int    `json:"offline_cleanup_grace_hours" default:"24"`      // 离线超过多少小时后才允许自动清理
	OfflineCleanupLastRunAt    string `json:"offline_cleanup_last_run_at" default:""`        // 最近一次自动清理离线节点的执行时间（内部使用）
	FailoverV2SchedulerEnabled bool   `json:"failover_v2_scheduler_enabled" default:"false"` // 是否启用 V2 自动故障转移调度
	UpdatedAt                  time.Time
}

const (
	SitenameKey                            = "sitename"
	DescriptionKey                         = "description"
	AllowCorsKey                           = "allow_cors"
	ApiKeyKey                              = "api_key"
	AutoDiscoveryKeyKey                    = "auto_discovery_key"
	ScriptDomainKey                        = "script_domain"
	CNConnectivityEnabledKey               = "cn_connectivity_enabled"
	CNConnectivityTargetKey                = "cn_connectivity_target"
	CNConnectivityIntervalKey              = "cn_connectivity_interval"
	CNConnectivityRetryAttemptsKey         = "cn_connectivity_retry_attempts"
	CNConnectivityRetryDelaySecondsKey     = "cn_connectivity_retry_delay_seconds"
	CNConnectivityTimeoutSecondsKey        = "cn_connectivity_timeout_seconds"
	OutboundProxyEnabledKey                = "outbound_proxy_enabled"
	OutboundProxyProtocolKey               = "outbound_proxy_protocol"
	OutboundProxyHostKey                   = "outbound_proxy_host"
	OutboundProxyPortKey                   = "outbound_proxy_port"
	OutboundProxyUsernameKey               = "outbound_proxy_username"
	OutboundProxyPasswordKey               = "outbound_proxy_password"
	SendIpAddrToGuestKey                   = "send_ip_addr_to_guest"
	EulaAcceptedKey                        = "eula_accepted"
	BaseScriptsURLKey                      = "base_scripts_url"
	GeoIpEnabledKey                        = "geo_ip_enabled"
	GeoIpProviderKey                       = "geo_ip_provider"
	NezhaCompatEnabledKey                  = "nezha_compat_enabled"
	NezhaCompatListenKey                   = "nezha_compat_listen"
	OAuthEnabledKey                        = "o_auth_enabled"
	OAuthProviderKey                       = "o_auth_provider"
	DisablePasswordLoginKey                = "disable_password_login"
	CustomHeadKey                          = "custom_head"
	CustomBodyKey                          = "custom_body"
	NotificationEnabledKey                 = "notification_enabled"
	NotificationMethodKey                  = "notification_method"
	NotificationTemplateKey                = "notification_template"
	NotificationTelegramChatIDKey          = "notification_telegram_chat_id"
	NotificationTelegramMessageThreadIDKey = "notification_telegram_message_thread_id"
	NotificationBarkDeviceKeyKey           = "notification_bark_device_key"
	NotificationWebhookURLKey              = "notification_webhook_url"
	ExpireNotificationEnabledKey           = "expire_notification_enabled"
	ExpireNotificationLeadDaysKey          = "expire_notification_lead_days"
	LoginNotificationKey                   = "login_notification"
	TrafficLimitPercentageKey              = "traffic_limit_percentage"
	RecordEnabledKey                       = "record_enabled"
	RecordPreserveTimeKey                  = "record_preserve_time"
	PingRecordPreserveTimeKey              = "ping_record_preserve_time"
	OfflineCleanupEnabledKey               = "offline_cleanup_enabled"
	OfflineCleanupTimeKey                  = "offline_cleanup_time"
	OfflineCleanupGraceHoursKey            = "offline_cleanup_grace_hours"
	OfflineCleanupLastRunAtKey             = "offline_cleanup_last_run_at"
	FailoverV2SchedulerEnabledKey          = "failover_v2_scheduler_enabled"
	TempShareTokenKey                      = "tempory_share_token"
	TempShareTokenExpireAtKey              = "tempory_share_token_expire_at"
	UpdatedAtKey                           = "updated_at"
)

func (Legacy) TableName() string {
	return "configs"
}

// Decrepted
/*
func Update(cst map[string]interface{}) error {
	oldConfig, _ := GetManyAs[Legacy]()
	// Proceed with update
	cst["updated_at"] = time.Now().Unix()
	delete(cst, "created_at")
	delete(cst, "CreatedAt")

	// 至少有一种登录方式启用
	newDisablePasswordLogin := oldConfig.DisablePasswordLogin
	newOAuthEnabled := oldConfig.OAuthEnabled
	if val, exists := cst["disable_password_login"]; exists {
		newDisablePasswordLogin = val.(bool)
	}
	if val, exists := cst["o_auth_enabled"]; exists {
		newOAuthEnabled = val.(bool)
	}
	if newDisablePasswordLogin && !newOAuthEnabled {
		return errors.New("at least one login method must be enabled (password/oauth)")
	}
	// 没绑定账号也不能禁用
	if newDisablePasswordLogin {
		usr := &models.User{}
		if err := Db.Model(&models.User{}).First(usr).Error; err != nil {
			return errors.Join(err, errors.New("failed to retrieve user"))
		}
		if usr.SSOID == "" {
			return errors.New("cannot disable password login when no SSO-bound account exists")
		}
	}
	err := Db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&models.Config{}).Where("id = ?", oldConfig.ID).Updates(cst).Error; err != nil {
			return errors.Join(err, errors.New("failed to update configuration"))
		}
		newConfig := &models.Config{}
		if err := tx.Where("id = ?", oldConfig.ID).First(newConfig).Error; err != nil {
			return errors.Join(err, errors.New("failed to retrieve updated configuration"))
		}
		//publishEvent(oldConfig, *newConfig)
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}
*/
