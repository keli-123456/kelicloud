package clientportforward

import (
	"errors"
	"strings"

	"github.com/komari-monitor/komari/database/dbcore"
	"github.com/komari-monitor/komari/database/models"
	"gorm.io/gorm"
)

func normalizeUserID(userUUID string) (string, error) {
	userUUID = strings.TrimSpace(userUUID)
	if userUUID == "" {
		return "", errors.New("user id is required")
	}
	return userUUID, nil
}

func normalizeClientUUID(clientUUID string) (string, error) {
	clientUUID = strings.TrimSpace(clientUUID)
	if clientUUID == "" {
		return "", errors.New("client uuid is required")
	}
	return clientUUID, nil
}

func normalizeRule(rule *models.ClientPortForwardRule, userUUID, clientUUID string) (*models.ClientPortForwardRule, error) {
	if rule == nil {
		return nil, errors.New("rule is required")
	}

	normalizedUserID, err := normalizeUserID(userUUID)
	if err != nil {
		return nil, err
	}
	normalizedClientUUID, err := normalizeClientUUID(clientUUID)
	if err != nil {
		return nil, err
	}

	next := *rule
	next.UserID = normalizedUserID
	next.ClientUUID = normalizedClientUUID
	next.Name = strings.TrimSpace(next.Name)
	next.Protocol = strings.ToLower(strings.TrimSpace(next.Protocol))
	next.TargetHost = strings.TrimSpace(next.TargetHost)
	next.LastTaskID = strings.TrimSpace(next.LastTaskID)
	next.LastError = strings.TrimSpace(next.LastError)
	return &next, nil
}

func ListRulesForUser(userUUID, clientUUID string) ([]models.ClientPortForwardRule, error) {
	normalizedUserID, err := normalizeUserID(userUUID)
	if err != nil {
		return nil, err
	}
	normalizedClientUUID, err := normalizeClientUUID(clientUUID)
	if err != nil {
		return nil, err
	}

	var rules []models.ClientPortForwardRule
	err = dbcore.GetDBInstance().
		Where("user_id = ? AND client_uuid = ?", normalizedUserID, normalizedClientUUID).
		Order("listen_port ASC, id ASC").
		Find(&rules).Error
	return rules, err
}

func GetRuleForUser(userUUID, clientUUID string, id uint) (models.ClientPortForwardRule, error) {
	normalizedUserID, err := normalizeUserID(userUUID)
	if err != nil {
		return models.ClientPortForwardRule{}, err
	}
	normalizedClientUUID, err := normalizeClientUUID(clientUUID)
	if err != nil {
		return models.ClientPortForwardRule{}, err
	}

	var rule models.ClientPortForwardRule
	err = dbcore.GetDBInstance().
		Where("user_id = ? AND client_uuid = ? AND id = ?", normalizedUserID, normalizedClientUUID, id).
		First(&rule).Error
	return rule, err
}

func SaveRuleForUser(userUUID, clientUUID string, rule *models.ClientPortForwardRule) (models.ClientPortForwardRule, error) {
	next, err := normalizeRule(rule, userUUID, clientUUID)
	if err != nil {
		return models.ClientPortForwardRule{}, err
	}

	db := dbcore.GetDBInstance()
	if next.ID == 0 {
		if err := db.Create(next).Error; err != nil {
			return models.ClientPortForwardRule{}, err
		}
		return *next, nil
	}

	var existing models.ClientPortForwardRule
	if err := db.Where("user_id = ? AND client_uuid = ? AND id = ?", next.UserID, next.ClientUUID, next.ID).First(&existing).Error; err != nil {
		return models.ClientPortForwardRule{}, err
	}

	changed := existing.Enabled != next.Enabled ||
		existing.Name != next.Name ||
		existing.Protocol != next.Protocol ||
		existing.ListenPort != next.ListenPort ||
		existing.TargetHost != next.TargetHost ||
		existing.TargetPort != next.TargetPort

	existing.Name = next.Name
	existing.Enabled = next.Enabled
	existing.Protocol = next.Protocol
	existing.ListenPort = next.ListenPort
	existing.TargetHost = next.TargetHost
	existing.TargetPort = next.TargetPort
	if changed {
		existing.LastTaskID = ""
		existing.LastAppliedAt = nil
		existing.LastError = ""
	}
	if err := db.Save(&existing).Error; err != nil {
		return models.ClientPortForwardRule{}, err
	}
	return existing, nil
}

func DeleteRuleForUser(userUUID, clientUUID string, id uint) error {
	normalizedUserID, err := normalizeUserID(userUUID)
	if err != nil {
		return err
	}
	normalizedClientUUID, err := normalizeClientUUID(clientUUID)
	if err != nil {
		return err
	}

	result := dbcore.GetDBInstance().
		Where("user_id = ? AND client_uuid = ? AND id = ?", normalizedUserID, normalizedClientUUID, id).
		Delete(&models.ClientPortForwardRule{})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}
	return nil
}

func FindEnabledListenPortConflictForUser(
	userUUID string,
	clientUUID string,
	protocol string,
	listenPort int,
	excludedID uint,
) (models.ClientPortForwardRule, error) {
	normalizedUserID, err := normalizeUserID(userUUID)
	if err != nil {
		return models.ClientPortForwardRule{}, err
	}
	normalizedClientUUID, err := normalizeClientUUID(clientUUID)
	if err != nil {
		return models.ClientPortForwardRule{}, err
	}

	query := dbcore.GetDBInstance().
		Where(
			"user_id = ? AND client_uuid = ? AND enabled = ? AND protocol = ? AND listen_port = ?",
			normalizedUserID,
			normalizedClientUUID,
			true,
			strings.ToLower(strings.TrimSpace(protocol)),
			listenPort,
		)
	if excludedID > 0 {
		query = query.Where("id <> ?", excludedID)
	}

	var rule models.ClientPortForwardRule
	err = query.First(&rule).Error
	return rule, err
}

func UpdateApplyStateForUser(
	userUUID string,
	clientUUID string,
	ruleIDs []uint,
	taskID string,
	lastAppliedAt *models.LocalTime,
	lastError string,
) error {
	normalizedUserID, err := normalizeUserID(userUUID)
	if err != nil {
		return err
	}
	normalizedClientUUID, err := normalizeClientUUID(clientUUID)
	if err != nil {
		return err
	}
	if len(ruleIDs) == 0 {
		return nil
	}

	return dbcore.GetDBInstance().
		Model(&models.ClientPortForwardRule{}).
		Where("user_id = ? AND client_uuid = ? AND id IN ?", normalizedUserID, normalizedClientUUID, ruleIDs).
		Updates(map[string]interface{}{
			"last_task_id":    strings.TrimSpace(taskID),
			"last_applied_at": lastAppliedAt,
			"last_error":      strings.TrimSpace(lastError),
		}).Error
}
