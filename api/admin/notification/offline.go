package notification

import (
	"github.com/gin-gonic/gin"
	"github.com/komari-monitor/komari/api"
	clientdb "github.com/komari-monitor/komari/database/clients"
	"github.com/komari-monitor/komari/database/dbcore"
	"github.com/komari-monitor/komari/database/models"
	"gorm.io/gorm/clause"
)

// POST body : []uuid
func EnableOfflineNotification(c *gin.Context) {
	var uuids []string
	if err := c.ShouldBindJSON(&uuids); err != nil {
		api.RespondError(c, 400, "Invalid request body: "+err.Error())
		return
	}
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}
	normalizedUUIDs, err := clientdb.NormalizeClientUUIDsForUser(scope.UserUUID, uuids)
	if err != nil {
		api.RespondError(c, 400, err.Error())
		return
	}
	var notifications []models.OfflineNotification
	for _, uuid := range normalizedUUIDs {
		notifications = append(notifications, models.OfflineNotification{
			Client: uuid,
			Enable: true,
		})
	}
	err = dbcore.GetDBInstance().Model(&models.OfflineNotification{}).
		Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "client"}},
			DoUpdates: clause.AssignmentColumns([]string{"enable"}),
		}).
		Select("client", "enable").
		Create(notifications).Error
	if err != nil {
		api.RespondError(c, 500, "Failed to enable offline notifications: "+err.Error())
		return
	}
	api.RespondSuccess(c, nil)
}

// POST body : []uuid
func DisableOfflineNotification(c *gin.Context) {
	var uuids []string
	if err := c.ShouldBindJSON(&uuids); err != nil {
		api.RespondError(c, 400, "Invalid request body: "+err.Error())
		return
	}
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}
	normalizedUUIDs, err := clientdb.NormalizeClientUUIDsForUser(scope.UserUUID, uuids)
	if err != nil {
		api.RespondError(c, 400, err.Error())
		return
	}
	var notifications []models.OfflineNotification
	for _, uuid := range normalizedUUIDs {
		notifications = append(notifications, models.OfflineNotification{
			Client: uuid,
			Enable: false,
		})
	}
	err = dbcore.GetDBInstance().Model(&models.OfflineNotification{}).
		Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "client"}},
			DoUpdates: clause.AssignmentColumns([]string{"enable"}),
		}).
		Select("client", "enable").
		Create(notifications).Error
	if err != nil {
		api.RespondError(c, 500, "Failed to disable offline notifications: "+err.Error())
		return
	}
	api.RespondSuccess(c, nil)
}

func EditOfflineNotification(c *gin.Context) {
	var notifications []models.OfflineNotification
	if err := c.ShouldBindJSON(&notifications); err != nil {
		api.RespondError(c, 400, "Invalid request body: "+err.Error())
		return
	}
	if len(notifications) == 0 {
		api.RespondError(c, 400, "At least one notification is required")
		return
	}
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}
	clientUUIDs := make([]string, 0, len(notifications))
	for _, noti := range notifications {
		if noti.Client == "" {
			api.RespondError(c, 400, "Client UUID cannot be empty")
			return
		}
		if noti.GracePeriod <= 0 {
			api.RespondError(c, 400, "GracePeriod must be a positive integer")
			return
		}
		clientUUIDs = append(clientUUIDs, noti.Client)
	}
	if _, err := clientdb.NormalizeClientUUIDsForUser(scope.UserUUID, clientUUIDs); err != nil {
		api.RespondError(c, 400, err.Error())
		return
	}
	err := dbcore.GetDBInstance().Model(&models.OfflineNotification{}).
		Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "client"}},
			DoUpdates: clause.AssignmentColumns([]string{"enable", "grace_period"}),
		}).
		Select("*").
		Create(notifications).Error
	if err != nil {
		api.RespondError(c, 500, "Failed to edit offline notifications: "+err.Error())
		return
	}
	api.RespondSuccess(c, nil)
}

func ListOfflineNotifications(c *gin.Context) {
	var notifications []models.OfflineNotification
	scope, ok := requireCurrentOwnerScope(c)
	if !ok {
		return
	}
	query := dbcore.GetDBInstance().
		Model(&models.OfflineNotification{}).
		Joins("JOIN clients ON clients.uuid = offline_notifications.client")
	query = query.Where("clients.user_id = ?", scope.UserUUID)
	err := query.Find(&notifications).Error
	if err != nil {
		api.RespondError(c, 500, "Failed to list offline notifications: "+err.Error())
		return
	}
	api.RespondSuccess(c, notifications)
}
