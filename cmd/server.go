package cmd

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/komari-monitor/komari/api"
	"github.com/komari-monitor/komari/api/admin"
	"github.com/komari-monitor/komari/api/admin/clipboard"
	log_api "github.com/komari-monitor/komari/api/admin/log"
	"github.com/komari-monitor/komari/api/admin/notification"
	"github.com/komari-monitor/komari/api/admin/test"
	"github.com/komari-monitor/komari/api/admin/update"
	"github.com/komari-monitor/komari/api/client"
	"github.com/komari-monitor/komari/api/jsonRpc"
	public_api "github.com/komari-monitor/komari/api/public"
	"github.com/komari-monitor/komari/api/record"
	"github.com/komari-monitor/komari/api/task"
	"github.com/komari-monitor/komari/cmd/flags"

	"github.com/komari-monitor/komari/config"
	"github.com/komari-monitor/komari/database/accounts"
	"github.com/komari-monitor/komari/database/auditlog"
	"github.com/komari-monitor/komari/database/dbcore"
	"github.com/komari-monitor/komari/database/models"
	d_notification "github.com/komari-monitor/komari/database/notification"
	"github.com/komari-monitor/komari/database/records"
	"github.com/komari-monitor/komari/database/tasks"
	"github.com/komari-monitor/komari/public"
	"github.com/komari-monitor/komari/utils"
	"github.com/komari-monitor/komari/utils/awsfollowup"
	clientddns "github.com/komari-monitor/komari/utils/clientddns"
	"github.com/komari-monitor/komari/utils/cloudflared"
	_ "github.com/komari-monitor/komari/utils/cloudprovider"
	"github.com/komari-monitor/komari/utils/failover"
	"github.com/komari-monitor/komari/utils/geoip"
	logutil "github.com/komari-monitor/komari/utils/log"
	"github.com/komari-monitor/komari/utils/messageSender"
	"github.com/komari-monitor/komari/utils/notifier"
	"github.com/komari-monitor/komari/utils/oauth"
	"github.com/komari-monitor/komari/utils/offlinecleanup"
	"github.com/spf13/cobra"
)

var (
	DynamicCorsEnabled bool = false
)

var ServerCmd = &cobra.Command{
	Use:   "server",
	Short: "Start the server",
	Long:  `Start the server`,
	Run: func(cmd *cobra.Command, args []string) {
		RunServer()
	},
}

func init() {
	// 从环境变量获取监听地址
	listenAddr := GetEnv("KOMARI_LISTEN", "0.0.0.0:25774")
	ServerCmd.PersistentFlags().StringVarP(&flags.Listen, "listen", "l", listenAddr, "监听地址 [env: KOMARI_LISTEN]")
	RootCmd.AddCommand(ServerCmd)
}

func RunServer() {
	// #region 初始化
	InitDatabase()
	if utils.VersionHash != "unknown" {
		gin.SetMode(gin.ReleaseMode)
	}
	conf, err := config.GetManyAs[config.Legacy]()
	if err != nil {
		log.Fatal(err)
	}
	go geoip.InitGeoIp()
	go DoScheduledWork()
	messageSender.Initialize()
	if err := oauth.Initialize(); err != nil {
		log.Printf("Failed to initialize OIDC provider: %v", err)
		auditlog.EventLog("error", fmt.Sprintf("Failed to initialize OIDC provider: %v", err))
	}

	if conf.NezhaCompatEnabled {
		go func() {
			if err := StartNezhaCompat(conf.NezhaCompatListen); err != nil {
				log.Printf("Nezha compat server error: %v", err)
				auditlog.EventLog("error", fmt.Sprintf("Nezha compat server error: %v", err))
			}
		}()
	}

	config.Subscribe(func(event config.ConfigEvent) {
		if event.IsChanged(config.OAuthProviderKey) {
			if err := oauth.Initialize(); err != nil {
				log.Printf("Failed to reload OIDC provider: %v", err)
				auditlog.EventLog("error", fmt.Sprintf("Failed to reload OIDC provider: %v", err))
			}
		}

		if ok, t := config.IsChangedT[bool](event, config.NezhaCompatEnabledKey); ok {
			if t {
				l, _ := config.GetAs[string](config.NezhaCompatListenKey)
				if err := StartNezhaCompat(l); err != nil {
					log.Printf("start Nezha compat server error: %v", err)
					auditlog.EventLog("error", fmt.Sprintf("start Nezha compat server error: %v", err))
				}
			} else {
				if err := StopNezhaCompat(); err != nil {
					log.Printf("stop Nezha compat server error: %v", err)
					auditlog.EventLog("error", fmt.Sprintf("stop Nezha compat server error: %v", err))
				}
			}
		}

	})
	// 初始化 cloudflared
	if strings.ToLower(GetEnv("KOMARI_ENABLE_CLOUDFLARED", "false")) == "true" {
		err := cloudflared.RunCloudflared() // 阻塞，确保cloudflared跑起来
		if err != nil {
			log.Fatalf("Failed to run cloudflared: %v", err)
		}
	}

	r := gin.New()
	r.Use(logutil.GinLogger())
	r.Use(logutil.GinRecovery())

	// 动态 CORS 中间件

	DynamicCorsEnabled = conf.AllowCors
	config.Subscribe(func(event config.ConfigEvent) {
		if ok, t := config.IsChangedT[bool](event, config.AllowCorsKey); ok {
			DynamicCorsEnabled = t
		}
		if event.IsChanged(config.GeoIpProviderKey) {
			go geoip.InitGeoIp()
		}

		if event.IsChanged(config.NotificationMethodKey) {
			messageSender.Initialize()
		}

	})
	r.Use(func(c *gin.Context) {
		if DynamicCorsEnabled {
			origin := c.GetHeader("Origin")
			if origin != "" {
				c.Header("Access-Control-Allow-Origin", origin)
				c.Header("Vary", "Origin")
				c.Header("Access-Control-Allow-Credentials", "true")
			} else {
				c.Header("Access-Control-Allow-Origin", "*")
			}
			c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, HEAD, OPTIONS")
			c.Header("Access-Control-Allow-Headers", "Origin, Content-Length, Content-Type, Authorization, Accept, X-CSRF-Token, X-Requested-With, Set-Cookie")
			c.Header("Access-Control-Expose-Headers", "Content-Length, Authorization, Set-Cookie")
			c.Header("Access-Control-Max-Age", "43200") // 12 hours
			if c.Request.Method == "OPTIONS" {
				c.AbortWithStatus(204)
				return
			}
		}
		c.Next()
	})

	r.Use(api.RequireLoginForPanelDataMiddleware())

	r.Use(func(c *gin.Context) {
		if len(c.Request.URL.Path) >= 4 && c.Request.URL.Path[:4] == "/api" {
			c.Header("Cache-Control", "no-store")
		}
		c.Next()
	})

	r.Any("/ping", func(c *gin.Context) {
		c.String(200, "pong")
	})
	// #region 公开路由
	r.POST("/api/login", api.Login)
	r.GET("/api/me", api.GetMe)
	r.GET("/api/clients", api.GetClients)
	r.GET("/api/nodes", api.GetNodesInformation)
	r.GET("/api/public", api.GetPublicSettings)
	r.GET("/api/public/cloud/shares/:token", api.GetPublicCloudInstanceShare)
	r.GET("/api/oauth", api.OAuth)
	r.GET("/api/oauth_callback", api.OAuthCallback)
	r.GET("/api/logout", api.Logout)
	r.GET("/api/version", api.GetVersion)
	r.GET("/api/recent/:uuid", api.GetClientRecentRecords)

	r.GET("/api/records/load", record.GetRecordsByUUID)
	r.GET("/api/records/ping", record.GetPingRecords)
	r.GET("/api/task/ping", task.GetPublicPingTasks)
	r.GET("/api/rpc2", jsonRpc.OnRpcRequest)
	r.POST("/api/rpc2", jsonRpc.OnRpcRequest)
	r.GET("/api/mjpeg_live", public_api.MjpegLiveHandler)
	// #region Agent
	r.POST("/api/clients/register", client.RegisterClient)
	tokenAuthrized := r.Group("/api/clients", api.TokenAuthMiddleware())
	{
		tokenAuthrized.GET("/report", client.WebSocketReport) // websocket
		tokenAuthrized.POST("/uploadBasicInfo", client.UploadBasicInfo)
		tokenAuthrized.POST("/report", client.UploadReport)
		tokenAuthrized.GET("/terminal", client.EstablishConnection)
		tokenAuthrized.POST("/task/result", client.TaskResult)
		tokenAuthrized.GET("/ping/tasks", client.GetPingTasks)
		tokenAuthrized.POST("/ping/result", client.UploadPingResult)
	}
	// #region 管理员
	adminAuthrized := r.Group("/api/admin", api.AdminAuthMiddleware())
	{
		adminAuthrized.GET("/download/backup", admin.RequirePlatformAdminMiddleware(), admin.DownloadBackup)
		adminAuthrized.POST("/upload/backup", admin.RequirePlatformAdminMiddleware(), admin.UploadBackup)
		// test
		testGroup := adminAuthrized.Group("/test", admin.RequirePlatformAdminMiddleware())
		{
			testGroup.GET("/geoip", test.TestGeoIp)
			testGroup.POST("/sendMessage", test.TestSendMessage)
		}
		// update
		updateGroup := adminAuthrized.Group("/update")
		{
			updateGroup.POST("/mmdb", admin.RequirePlatformAdminMiddleware(), update.UpdateMmdbGeoIP)
			updateGroup.POST("/user", update.UpdateUser)
			updateGroup.PUT("/favicon", admin.RequirePlatformAdminMiddleware(), update.UploadFavicon)
			updateGroup.POST("/favicon", admin.RequirePlatformAdminMiddleware(), update.DeleteFavicon)
		}
		// tasks
		taskGroup := adminAuthrized.Group("/task", admin.RequireUserFeatureMiddleware(config.UserFeatureTasks))
		{
			taskGroup.GET("/all", admin.GetTasks)
			taskGroup.POST("/exec", admin.Exec)
			taskGroup.GET("/:task_id", admin.GetTaskById)
			taskGroup.GET("/:task_id/result", admin.GetTaskResultsByTaskId)
			taskGroup.GET("/:task_id/result/:uuid", admin.GetSpecificTaskResult)
			taskGroup.GET("/client/:uuid", admin.GetTasksByClientId)
		}
		// settings
		settingsGroup := adminAuthrized.Group("/settings")
		{
			settingsGroup.GET("/", admin.GetSettings)
			settingsGroup.POST("/", admin.EditSettings)
			settingsGroup.GET("/system", admin.RequirePlatformAdminMiddleware(), admin.GetSystemSettings)
			settingsGroup.POST("/system", admin.RequirePlatformAdminMiddleware(), admin.EditSystemSettings)
			settingsGroup.POST("/proxy/test", admin.RequirePlatformAdminMiddleware(), admin.TestOutboundProxy)
			settingsGroup.POST("/oidc", admin.RequirePlatformAdminMiddleware(), admin.SetOidcProvider)
			settingsGroup.GET("/oidc", admin.RequirePlatformAdminMiddleware(), admin.GetOidcProvider)
			settingsGroup.POST("/message-sender", admin.RequirePlatformAdminMiddleware(), admin.SetMessageSenderProvider)
			settingsGroup.GET("/message-sender", admin.RequirePlatformAdminMiddleware(), admin.GetMessageSenderProvider)
		}
		cloudGroup := adminAuthrized.Group(
			"/cloud",
			admin.RequireAnyUserFeatureMiddleware(
				config.UserFeatureCloudDigitalOcean,
				config.UserFeatureCloudLinode,
				config.UserFeatureCloudAzure,
				config.UserFeatureCloudAWS,
				config.UserFeatureCloudDNS,
				config.UserFeatureCloudFailover,
			),
		)
		{
			cloudGroup.GET("/providers", admin.GetCloudProviders)
			cloudGroup.GET("/providers/:provider", admin.GetCloudProvider)
			cloudGroup.POST("/providers/:provider", admin.SetCloudProvider)
			cloudGroup.GET("/shares/:provider/:resource_type/:resource_id", admin.GetCloudInstanceShare)
			cloudGroup.POST("/shares/:provider/:resource_type/:resource_id", admin.UpsertCloudInstanceShare)
			cloudGroup.DELETE("/shares/:provider/:resource_type/:resource_id", admin.DeleteCloudInstanceShare)

			digitalOceanGroup := cloudGroup.Group("/digitalocean", admin.RequireUserFeatureMiddleware(config.UserFeatureCloudDigitalOcean))
			{
				digitalOceanGroup.GET("/tokens", admin.GetDigitalOceanTokens)
				digitalOceanGroup.POST("/tokens", admin.SaveDigitalOceanTokens)
				digitalOceanGroup.POST("/tokens/active", admin.SetDigitalOceanActiveToken)
				digitalOceanGroup.POST("/tokens/check", admin.CheckDigitalOceanTokens)
				digitalOceanGroup.GET("/tokens/:id/secret", admin.GetDigitalOceanTokenSecret)
				digitalOceanGroup.GET("/tokens/:id/managed-ssh-key", admin.GetDigitalOceanManagedSSHKey)
				digitalOceanGroup.DELETE("/tokens/:id", admin.DeleteDigitalOceanToken)
				digitalOceanGroup.GET("/account", admin.GetDigitalOceanAccount)
				digitalOceanGroup.GET("/catalog", admin.GetDigitalOceanCatalog)
				digitalOceanGroup.GET("/droplets", admin.ListDigitalOceanDroplets)
				digitalOceanGroup.GET("/droplets/:id/password", admin.GetDigitalOceanDropletPassword)
				digitalOceanGroup.POST("/droplets", admin.CreateDigitalOceanDroplet)
				digitalOceanGroup.DELETE("/droplets/:id", admin.DeleteDigitalOceanDroplet)
				digitalOceanGroup.POST("/droplets/:id/actions", admin.PostDigitalOceanDropletAction)
			}
			linodeGroup := cloudGroup.Group("/linode", admin.RequireUserFeatureMiddleware(config.UserFeatureCloudLinode))
			{
				linodeGroup.GET("/tokens", admin.GetLinodeTokens)
				linodeGroup.POST("/tokens", admin.SaveLinodeTokens)
				linodeGroup.POST("/tokens/active", admin.SetLinodeActiveToken)
				linodeGroup.POST("/tokens/check", admin.CheckLinodeTokens)
				linodeGroup.GET("/tokens/:id/secret", admin.GetLinodeTokenSecret)
				linodeGroup.DELETE("/tokens/:id", admin.DeleteLinodeToken)
				linodeGroup.GET("/account", admin.GetLinodeAccount)
				linodeGroup.POST("/promo-codes", admin.RedeemLinodePromoCode)
				linodeGroup.GET("/catalog", admin.GetLinodeCatalog)
				linodeGroup.GET("/instances", admin.ListLinodeInstances)
				linodeGroup.GET("/instances/:id", admin.GetLinodeInstanceDetail)
				linodeGroup.GET("/instances/:id/password", admin.GetLinodeInstancePassword)
				linodeGroup.POST("/instances", admin.CreateLinodeInstance)
				linodeGroup.DELETE("/instances/:id", admin.DeleteLinodeInstance)
				linodeGroup.POST("/instances/:id/actions", admin.PostLinodeInstanceAction)
			}
			azureGroup := cloudGroup.Group("/azure", admin.RequireUserFeatureMiddleware(config.UserFeatureCloudAzure))
			{
				azureGroup.GET("/credentials", admin.GetAzureCredentials)
				azureGroup.POST("/credentials", admin.SaveAzureCredentials)
				azureGroup.POST("/credentials/active", admin.SetAzureActiveCredential)
				azureGroup.POST("/credentials/location", admin.SetAzureActiveLocation)
				azureGroup.POST("/credentials/check", admin.CheckAzureCredentials)
				azureGroup.GET("/credentials/:id/secret", admin.GetAzureCredentialSecret)
				azureGroup.DELETE("/credentials/:id", admin.DeleteAzureCredential)
				azureGroup.GET("/account", admin.GetAzureAccount)
				azureGroup.GET("/catalog", admin.GetAzureCatalog)
				azureGroup.GET("/instances", admin.ListAzureInstances)
				azureGroup.GET("/instances/:id", admin.GetAzureInstanceDetail)
				azureGroup.POST("/instances", admin.CreateAzureInstance)
				azureGroup.DELETE("/instances/:id", admin.DeleteAzureInstance)
				azureGroup.POST("/instances/:id/actions", admin.PostAzureInstanceAction)
			}
			awsGroup := cloudGroup.Group("/aws", admin.RequireUserFeatureMiddleware(config.UserFeatureCloudAWS))
			{
				awsGroup.GET("/credentials", admin.GetAWSCredentials)
				awsGroup.POST("/credentials", admin.SaveAWSCredentials)
				awsGroup.POST("/credentials/active", admin.SetAWSActiveCredential)
				awsGroup.POST("/credentials/region", admin.SetAWSActiveRegion)
				awsGroup.POST("/credentials/check", admin.CheckAWSCredentials)
				awsGroup.GET("/credentials/:id/secret", admin.GetAWSCredentialSecret)
				awsGroup.DELETE("/credentials/:id", admin.DeleteAWSCredential)
				awsGroup.GET("/follow-up-tasks", admin.ListAWSFollowUpTasks)
				awsGroup.POST("/follow-up-tasks/:id/retry", admin.RetryAWSFollowUpTask)
				awsGroup.DELETE("/follow-up-tasks/terminal", admin.ClearAWSFollowUpTerminalTasks)
				awsGroup.GET("/account", admin.GetAWSAccount)
				awsGroup.GET("/catalog", admin.GetAWSCatalog)
				awsGroup.GET("/instances", admin.ListAWSInstances)
				awsGroup.GET("/instances/:id/password", admin.GetAWSInstancePassword)
				awsGroup.GET("/instances/:id", admin.GetAWSInstanceDetail)
				awsGroup.POST("/instances", admin.CreateAWSInstance)
				awsGroup.DELETE("/instances/:id", admin.DeleteAWSInstance)
				awsGroup.POST("/instances/:id/actions", admin.PostAWSInstanceAction)
				lightsailGroup := awsGroup.Group("/lightsail")
				{
					lightsailGroup.GET("/catalog", admin.GetAWSLightsailCatalog)
					lightsailGroup.GET("/instances", admin.ListAWSLightsailInstances)
					lightsailGroup.GET("/instances/:name/password", admin.GetAWSLightsailInstancePassword)
					lightsailGroup.GET("/instances/:name", admin.GetAWSLightsailInstanceDetail)
					lightsailGroup.POST("/instances", admin.CreateAWSLightsailInstance)
					lightsailGroup.DELETE("/instances/:name", admin.DeleteAWSLightsailInstance)
					lightsailGroup.POST("/instances/:name/actions", admin.PostAWSLightsailInstanceAction)
				}
			}
		}
		userGroup := adminAuthrized.Group("/users", admin.RequirePlatformAdminMiddleware())
		{
			userGroup.GET("", admin.ListUsers)
			userGroup.POST("", admin.CreateUser)
			userGroup.DELETE("/:uuid", admin.DeleteUser)
		}
		// clients
		clientGroup := adminAuthrized.Group("/client", admin.RequireUserFeatureMiddleware(config.UserFeatureClients))
		{
			clientGroup.POST("/add", admin.AddClient)
			clientGroup.GET("/list", admin.ListClients)
			clientGroup.GET("/:uuid", admin.GetClient)
			clientGroup.POST("/:uuid/edit", admin.EditClient)
			clientGroup.POST("/:uuid/remove", admin.RemoveClient)
			clientGroup.GET("/:uuid/token", admin.GetClientToken)
			clientGroup.POST("/order", admin.OrderWeight)
			// client terminal
			clientGroup.GET("/:uuid/terminal", api.RequestTerminal)
			clientDDNSGroup := clientGroup.Group("/:uuid/ddns", admin.RequireUserFeatureMiddleware(config.UserFeatureCloudDNS))
			{
				clientDDNSGroup.GET("", admin.GetClientDDNSBinding)
				clientDDNSGroup.POST("", admin.SaveClientDDNSBinding)
				clientDDNSGroup.POST("/remove", admin.DeleteClientDDNSBinding)
				clientDDNSGroup.POST("/sync", admin.SyncClientDDNSBinding)
				clientDDNSGroup.GET("/catalog", admin.GetClientDDNSCatalog)
			}
		}

		// records
		recordGroup := adminAuthrized.Group("/record", admin.RequireUserFeatureMiddleware(config.UserFeatureRecords))
		{
			recordGroup.POST("/clear", admin.ClearRecord)
			recordGroup.POST("/clear/all", admin.ClearAllRecords)
		}
		// oauth2
		oauth2Group := adminAuthrized.Group("/oauth2")
		{
			oauth2Group.GET("/bind", admin.BindingExternalAccount)
			oauth2Group.POST("/unbind", admin.UnbindExternalAccount)
		}
		sessionGroup := adminAuthrized.Group("/session")
		{
			sessionGroup.GET("/get", admin.GetSessions)
			sessionGroup.POST("/remove", admin.DeleteSession)
			sessionGroup.POST("/remove/all", admin.DeleteAllSession)
		}
		two_factorGroup := adminAuthrized.Group("/2fa")
		{
			two_factorGroup.GET("/generate", admin.Generate2FA)
			two_factorGroup.POST("/enable", admin.Enable2FA)
			two_factorGroup.POST("/disable", admin.Disable2FA)
		}
		adminAuthrized.GET("/logs", admin.RequireUserFeatureMiddleware(config.UserFeatureLogs), log_api.GetLogs)

		// clipboard
		clipboardGroup := adminAuthrized.Group("/clipboard", admin.RequireUserFeatureMiddleware(config.UserFeatureClipboard))
		{
			clipboardGroup.GET("/:id", clipboard.GetClipboard)
			clipboardGroup.GET("", clipboard.ListClipboard)
			clipboardGroup.POST("", clipboard.CreateClipboard)
			clipboardGroup.POST("/:id", clipboard.UpdateClipboard)
			clipboardGroup.POST("/remove", clipboard.BatchDeleteClipboard)
			clipboardGroup.POST("/:id/remove", clipboard.DeleteClipboard)
		}

		notificationGroup := adminAuthrized.Group("/notification", admin.RequireUserFeatureMiddleware(config.UserFeatureNotifications))
		{
			// offline notifications
			notificationGroup.GET("/offline", notification.ListOfflineNotifications)
			notificationGroup.POST("/offline/edit", notification.EditOfflineNotification)
			notificationGroup.POST("/offline/enable", notification.EnableOfflineNotification)
			notificationGroup.POST("/offline/disable", notification.DisableOfflineNotification)
			loadAlertGroup := notificationGroup.Group("/load")
			{
				loadAlertGroup.GET("/", notification.GetAllLoadNotifications)
				loadAlertGroup.POST("/add", notification.AddLoadNotification)
				loadAlertGroup.POST("/delete", notification.DeleteLoadNotification)
				loadAlertGroup.POST("/edit", notification.EditLoadNotification)
			}
		}

		pingTaskGroup := adminAuthrized.Group("/ping", admin.RequireUserFeatureMiddleware(config.UserFeaturePing))
		{
			pingTaskGroup.GET("/", admin.GetAllPingTasks)
			pingTaskGroup.POST("/add", admin.AddPingTask)
			pingTaskGroup.POST("/delete", admin.DeletePingTask)
			pingTaskGroup.POST("/edit", admin.EditPingTask)

		}

		failoverGroup := adminAuthrized.Group(
			"/failover",
			admin.RequireUserFeatureMiddleware(config.UserFeatureCloudFailover),
			admin.RequireUserFeatureMiddleware(config.UserFeatureCNConnectivity),
		)
		{
			failoverGroup.GET("/tasks", admin.GetFailoverTasks)
			failoverGroup.GET("/dns/catalog", admin.GetFailoverDNSCatalog)
			failoverGroup.GET("/plans/catalog", admin.GetFailoverPlanCatalog)
			failoverGroup.POST("/tasks", admin.CreateFailoverTask)
			failoverGroup.POST("/tasks/preview", admin.PreviewFailoverTask)
			failoverGroup.GET("/tasks/:id", admin.GetFailoverTask)
			failoverGroup.POST("/tasks/:id", admin.UpdateFailoverTask)
			failoverGroup.POST("/tasks/:id/toggle", admin.ToggleFailoverTask)
			failoverGroup.POST("/tasks/:id/remove", admin.DeleteFailoverTask)
			failoverGroup.POST("/tasks/:id/run", admin.RunFailoverTask)
			failoverGroup.GET("/tasks/:id/executions", admin.GetFailoverExecutions)
			failoverGroup.GET("/executions/:id", admin.GetFailoverExecution)
			failoverGroup.POST("/executions/:id/stop", admin.StopFailoverExecution)
			failoverGroup.POST("/executions/:id/retry-dns", admin.RetryFailoverExecutionDNS)
			failoverGroup.POST("/executions/:id/retry-cleanup", admin.RetryFailoverExecutionCleanup)
		}

	}

	public.Static(r.Group("/"), func(handlers ...gin.HandlerFunc) {
		r.NoRoute(handlers...)
	})

	srv := &http.Server{
		Addr:    flags.Listen,
		Handler: r,
	}
	log.Printf("Starting server on %s ...", flags.Listen)
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			OnFatal(err)
			log.Fatalf("listen: %s\n", err)
		}
	}()
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	<-quit
	OnShutdown()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}

}

func InitDatabase() {
	// // 打印数据库类型和连接信息
	// if flags.DatabaseType == "mysql" {
	// 	log.Printf("使用 MySQL 数据库连接: %s@%s:%s/%s",
	// 		flags.DatabaseUser, flags.DatabaseHost, flags.DatabasePort, flags.DatabaseName)
	// 	log.Printf("环境变量配置: [KOMARI_DB_TYPE=%s] [KOMARI_DB_HOST=%s] [KOMARI_DB_PORT=%s] [KOMARI_DB_USER=%s] [KOMARI_DB_NAME=%s]",
	// 		os.Getenv("KOMARI_DB_TYPE"), os.Getenv("KOMARI_DB_HOST"), os.Getenv("KOMARI_DB_PORT"),
	// 		os.Getenv("KOMARI_DB_USER"), os.Getenv("KOMARI_DB_NAME"))
	// } else {
	// 	log.Printf("使用 SQLite 数据库文件: %s", flags.DatabaseFile)
	// 	log.Printf("环境变量配置: [KOMARI_DB_TYPE=%s] [KOMARI_DB_FILE=%s]",
	// 		os.Getenv("KOMARI_DB_TYPE"), os.Getenv("KOMARI_DB_FILE"))
	// }
	var count int64 = 0
	if dbcore.GetDBInstance().Model(&models.User{}).Count(&count); count == 0 {
		user, passwd, err := accounts.CreateDefaultAdminAccount()
		if err != nil {
			panic(err)
		}
		log.Println("Default admin account created. Username:", user, ", Password:", passwd)
	}
}

// #region 定时任务
func DoScheduledWork() {
	tasks.ReloadPingSchedule()
	failover.ReloadSchedule()
	if err := failover.RecoverInterruptedExecutions(); err != nil {
		log.Printf("failover: failed to recover interrupted executions: %v", err)
	}
	d_notification.ReloadLoadNotificationSchedule()
	ticker := time.NewTicker(time.Minute * 30)
	minute := time.NewTicker(60 * time.Second)
	quarterMinute := time.NewTicker(15 * time.Second)
	//records.DeleteRecordBefore(time.Now().Add(-time.Hour * 24 * 30))
	records.CompactRecord()
	go notifier.CheckExpireScheduledWork()
	for {
		cfg, _ := config.GetManyAs[config.Legacy]()
		select {
		case <-ticker.C:
			records.DeleteRecordBefore(time.Now().Add(-time.Hour * time.Duration(cfg.RecordPreserveTime)))
			records.CompactRecord()
			tasks.ClearTaskResultsByTimeBefore(time.Now().Add(-time.Hour * time.Duration(cfg.RecordPreserveTime)))
			tasks.DeletePingRecordsBefore(time.Now().Add(-time.Hour * time.Duration(cfg.PingRecordPreserveTime)))
			auditlog.RemoveOldLogs()
		case <-minute.C:
			api.SaveClientReportToDB()
			if !cfg.RecordEnabled {
				records.DeleteAll()
				tasks.DeleteAllPingRecords()
			}
			// 每分钟检查一次流量提醒
			go notifier.CheckTraffic()
			failover.RunScheduledWork()
			clientddns.RunScheduledSync()
			offlinecleanup.RunScheduledWork()
		case <-quarterMinute.C:
			awsfollowup.RunScheduledWork()
		}
	}

}

func OnShutdown() {
	auditlog.Log("", "", "server is shutting down", "info")
	cloudflared.Kill()
}

func OnFatal(err error) {
	auditlog.Log("", "", "server encountered a fatal error: "+err.Error(), "error")
	cloudflared.Kill()
}
