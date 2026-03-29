package accounts

import (
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/komari-monitor/komari/database/dbcore"
	"github.com/komari-monitor/komari/database/models"
	"github.com/komari-monitor/komari/utils"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

const constantSalt = "06Wm4Jv1Hkxx"

var ErrCannotDeleteLastAdmin = errors.New("cannot delete the last admin")

// CheckPassword 检查密码是否正确
//
// 如果密码正确，返回用户的 UUID 和 true；否则返回空字符串和 false
func CheckPassword(username, passwd string) (uuid string, success bool) {
	db := dbcore.GetDBInstance()
	var user models.User
	result := db.Where("username = ?", username).First(&user)
	if result.Error != nil {
		// 静默处理错误，不显示日志
		return "", false
	}
	if hashPasswd(passwd) != user.Passwd {
		return "", false
	}
	return user.UUID, true
}

// ForceResetPassword 强制重置用户密码
func ForceResetPassword(username, passwd string) (err error) {
	db := dbcore.GetDBInstance()
	result := db.Model(&models.User{}).Where("username = ?", username).Update("passwd", hashPasswd(passwd))
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return fmt.Errorf("无法找到用户名")
	}
	return nil
}

// hashPasswd 对密码进行加盐哈希
func hashPasswd(passwd string) string {
	saltedPassword := passwd + constantSalt
	hash := sha256.New()
	hash.Write([]byte(saltedPassword))
	hashedPassword := base64.StdEncoding.EncodeToString(hash.Sum(nil))
	return hashedPassword
}

func CreateAccount(username, passwd string) (user models.User, err error) {
	return CreateAccountWithRole(username, passwd, RoleUser)
}

func CreateAccountWithRole(username, passwd, role string) (user models.User, err error) {
	db := dbcore.GetDBInstance()
	hashedPassword := hashPasswd(passwd)
	normalizedRole := RoleUser
	if parsedRole, ok := ParseUserRole(role); ok {
		normalizedRole = parsedRole
	} else if strings.TrimSpace(role) != "" {
		return models.User{}, fmt.Errorf("invalid user role: %s", role)
	}
	user = models.User{
		UUID:     uuid.New().String(),
		Username: username,
		Passwd:   hashedPassword,
		Role:     normalizedRole,
	}
	err = db.Create(&user).Error
	if err != nil {
		return models.User{}, err
	}
	return user, nil
}

func DeleteAccountByUsername(username string) (err error) {
	db := dbcore.GetDBInstance()
	err = db.Where("username = ?", username).Delete(&models.User{}).Error
	if err != nil {
		return err
	}
	return nil
}

// 创建默认管理员账户，使用环境变量 ADMIN_USERNAME 作为用户名，环境变量 ADMIN_PASSWORD 作为密码
func CreateDefaultAdminAccount() (username, passwd string, err error) {
	db := dbcore.GetDBInstance()

	username = os.Getenv("ADMIN_USERNAME")
	if username == "" {
		username = "admin"
	}

	passwd = os.Getenv("ADMIN_PASSWORD")
	if passwd == "" {
		passwd = utils.GeneratePassword()
	}

	hashedPassword := hashPasswd(passwd)

	user := models.User{
		UUID:      uuid.New().String(),
		Username:  username,
		Passwd:    hashedPassword,
		Role:      RoleAdmin,
		SSOID:     "",
		CreatedAt: models.FromTime(time.Now()),
		UpdatedAt: models.FromTime(time.Now()),
	}

	err = db.Create(&user).Error
	if err != nil {
		return "", "", err
	}

	return username, passwd, nil
}

func GetUserByUUID(uuid string) (user models.User, err error) {
	db := dbcore.GetDBInstance()
	err = db.Where("uuid = ?", uuid).First(&user).Error
	if err != nil {
		return models.User{}, err
	}
	user.Role = NormalizeUserRole(user.Role)
	return user, nil
}

func GetUserByUsername(username string) (user models.User, err error) {
	db := dbcore.GetDBInstance()
	err = db.Where("username = ?", username).First(&user).Error
	if err != nil {
		return models.User{}, err
	}
	user.Role = NormalizeUserRole(user.Role)
	return user, nil
}

// 通过 SSO 信息获取用户
func GetUserBySSO(ssoID string) (user models.User, err error) {
	db := dbcore.GetDBInstance()

	// 首先尝试查找已存在的用户
	err = db.Where("sso_id = ?", ssoID).First(&user).Error
	if err == nil {
		user.Role = NormalizeUserRole(user.Role)
		return user, nil
	}

	// 如果找不到用户，返回明确的错误信息
	return models.User{}, fmt.Errorf("用户不存在：%s", ssoID)
}

func BindingExternalAccount(uuid string, sso_id string) error {
	db := dbcore.GetDBInstance()
	err := db.Model(&models.User{}).Where("uuid = ?", uuid).Update("sso_id", sso_id).Error
	if err != nil {
		return err
	}
	return nil
}

func UnbindExternalAccount(uuid string) error {
	db := dbcore.GetDBInstance()
	err := db.Model(&models.User{}).Where("uuid = ?", uuid).Update("sso_id", "").Error
	if err != nil {
		return err
	}
	return nil
}

func ListUsers() ([]models.User, error) {
	db := dbcore.GetDBInstance()
	var users []models.User
	if err := db.Order("created_at ASC").Find(&users).Error; err != nil {
		return nil, err
	}
	for i := range users {
		users[i].Role = NormalizeUserRole(users[i].Role)
		users[i].Passwd = ""
	}
	return users, nil
}

func countUsersByRoleTx(tx *gorm.DB, role string) (int64, error) {
	var total int64
	role = NormalizeUserRole(role)
	query := tx.Model(&models.User{})
	switch role {
	case RoleAdmin:
		query = query.Where("role = ? OR role = '' OR role IS NULL", RoleAdmin)
	default:
		query = query.Where("role = ?", role)
	}
	err := query.Count(&total).Error
	return total, err
}

func GetPreferredAdminUserUUID() (string, error) {
	db := dbcore.GetDBInstance()
	var user models.User
	err := db.Where("role = ? OR role = '' OR role IS NULL", RoleAdmin).
		Order("created_at ASC").
		Order("uuid ASC").
		First(&user).Error
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(user.UUID), nil
}

func UpdateUser(uuid string, name, password, ssoType, role *string) error {
	db := dbcore.GetDBInstance()
	return db.Transaction(func(tx *gorm.DB) error {
		var existingUser models.User
		result := tx.Where("uuid = ?", uuid).First(&existingUser)
		if result.Error != nil {
			return fmt.Errorf("user not found: %s", uuid)
		}

		currentRole := NormalizeUserRole(existingUser.Role)
		updates := make(map[string]interface{})
		if name != nil {
			updates["username"] = *name
		}
		if password != nil {
			updates["passwd"] = hashPasswd(*password)
		}
		if ssoType != nil {
			updates["sso_type"] = *ssoType
		}
		if role != nil {
			nextRole, ok := ParseUserRole(*role)
			if !ok {
				return fmt.Errorf("invalid user role: %s", *role)
			}
			if currentRole == RoleAdmin && nextRole != RoleAdmin {
				adminCount, err := countUsersByRoleTx(tx, RoleAdmin)
				if err != nil {
					return err
				}
				if adminCount <= 1 {
					return ErrCannotDeleteLastAdmin
				}
			}
			updates["role"] = nextRole
		}
		updates["updated_at"] = time.Now()
		if err := tx.Model(&models.User{}).Where("uuid = ?", uuid).Updates(updates).Error; err != nil {
			return err
		}
		if password != nil {
			return deleteAllSessionsByUserWithDB(tx, uuid)
		}
		return nil
	})
}

func DeleteUserByUUID(userUUID string) error {
	db := dbcore.GetDBInstance()
	return db.Transaction(func(tx *gorm.DB) error {
		var user models.User
		if err := tx.Where("uuid = ?", userUUID).First(&user).Error; err != nil {
			return err
		}
		if NormalizeUserRole(user.Role) == RoleAdmin {
			adminCount, err := countUsersByRoleTx(tx, RoleAdmin)
			if err != nil {
				return err
			}
			if adminCount <= 1 {
				return ErrCannotDeleteLastAdmin
			}
		}

		var clientUUIDs []string
		if err := tx.Model(&models.Client{}).Where("user_id = ?", userUUID).Pluck("uuid", &clientUUIDs).Error; err != nil {
			return err
		}

		if len(clientUUIDs) > 0 {
			if err := tx.Where("client IN ?", clientUUIDs).Delete(&models.OfflineNotification{}).Error; err != nil {
				return err
			}
			if err := tx.Where("client IN ?", clientUUIDs).Delete(&models.PingRecord{}).Error; err != nil {
				return err
			}
			if err := tx.Where("client IN ?", clientUUIDs).Delete(&models.TaskResult{}).Error; err != nil {
				return err
			}
			if err := tx.Where("client IN ?", clientUUIDs).Delete(&models.Record{}).Error; err != nil {
				return err
			}
			if err := tx.Where("client IN ?", clientUUIDs).Delete(&models.GPURecord{}).Error; err != nil {
				return err
			}
			if err := tx.Table("records_long_term").Where("client IN ?", clientUUIDs).Delete(&models.Record{}).Error; err != nil {
				return err
			}
			if err := tx.Table("gpu_records_long_term").Where("client IN ?", clientUUIDs).Delete(&models.GPURecord{}).Error; err != nil {
				return err
			}
			if err := tx.Where("uuid IN ?", clientUUIDs).Delete(&models.Client{}).Error; err != nil {
				return err
			}
		}

		cleanupByUserID := []interface{}{
			&models.CloudProvider{},
			&models.CloudInstanceShare{},
			&models.Clipboard{},
			&models.ClientDDNSBinding{},
			&models.Log{},
			&models.LoadNotification{},
			&models.PingTask{},
			&models.Task{},
			&models.TaskResult{},
		}
		for _, model := range cleanupByUserID {
			if err := tx.Where("user_id = ?", userUUID).Delete(model).Error; err != nil {
				return err
			}
		}

		if err := tx.Exec("DELETE FROM user_configs WHERE user_uuid = ?", userUUID).Error; err != nil {
			return err
		}
		if err := tx.Where("uuid = ?", userUUID).Delete(&models.Session{}).Error; err != nil {
			return err
		}
		return tx.Where("uuid = ?", userUUID).Delete(&models.User{}).Error
	})
}
