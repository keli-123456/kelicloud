package accounts

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/komari-monitor/komari/database/dbcore"
	"github.com/komari-monitor/komari/database/models"
	"github.com/komari-monitor/komari/utils"

	"github.com/google/uuid"
	"golang.org/x/crypto/argon2"
	"gorm.io/gorm"
)

const (
	constantSalt       = "06Wm4Jv1Hkxx"
	passwordHashPrefix = "$argon2id$"
	passwordSaltBytes  = 16
	passwordKeyBytes   = 32
	passwordTime       = uint32(3)
	passwordMemory     = uint32(64 * 1024)
	passwordThreads    = uint8(2)
)

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
	valid, needsRehash := verifyPasswd(user.Passwd, passwd)
	if !valid {
		return "", false
	}
	if needsRehash {
		if hashedPassword, err := hashPasswd(passwd); err == nil {
			_ = db.Model(&models.User{}).Where("uuid = ?", user.UUID).Update("passwd", hashedPassword).Error
		}
	}
	return user.UUID, true
}

// ForceResetPassword 强制重置用户密码
func ForceResetPassword(username, passwd string) (err error) {
	db := dbcore.GetDBInstance()
	hashedPassword, err := hashPasswd(passwd)
	if err != nil {
		return err
	}
	result := db.Model(&models.User{}).Where("username = ?", username).Update("passwd", hashedPassword)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return fmt.Errorf("无法找到用户名")
	}
	return nil
}

// hashPasswd returns an Argon2id password hash in PHC string format.
func hashPasswd(passwd string) (string, error) {
	salt := make([]byte, passwordSaltBytes)
	if _, err := rand.Read(salt); err != nil {
		return "", err
	}
	key := argon2.IDKey([]byte(passwd), salt, passwordTime, passwordMemory, passwordThreads, passwordKeyBytes)
	return fmt.Sprintf(
		"$argon2id$v=%d$m=%d,t=%d,p=%d$%s$%s",
		argon2.Version,
		passwordMemory,
		passwordTime,
		passwordThreads,
		base64.RawStdEncoding.EncodeToString(salt),
		base64.RawStdEncoding.EncodeToString(key),
	), nil
}

func verifyPasswd(storedHash, passwd string) (valid bool, needsRehash bool) {
	if strings.HasPrefix(storedHash, passwordHashPrefix) {
		return verifyArgon2idHash(storedHash, passwd), false
	}
	return legacyHashPasswd(passwd) == storedHash, true
}

func verifyArgon2idHash(storedHash, passwd string) bool {
	parts := strings.Split(storedHash, "$")
	if len(parts) != 6 || parts[1] != "argon2id" || parts[2] != fmt.Sprintf("v=%d", argon2.Version) {
		return false
	}

	var memory, timeCost uint32
	var threads uint8
	for _, item := range strings.Split(parts[3], ",") {
		key, value, ok := strings.Cut(item, "=")
		if !ok {
			return false
		}
		parsed, err := strconv.ParseUint(value, 10, 32)
		if err != nil {
			return false
		}
		switch key {
		case "m":
			memory = uint32(parsed)
		case "t":
			timeCost = uint32(parsed)
		case "p":
			if parsed == 0 || parsed > 255 {
				return false
			}
			threads = uint8(parsed)
		default:
			return false
		}
	}
	if memory == 0 || memory > 1024*1024 || timeCost == 0 || timeCost > 10 || threads == 0 || threads > 16 {
		return false
	}

	salt, err := base64.RawStdEncoding.DecodeString(parts[4])
	if err != nil || len(salt) == 0 {
		return false
	}
	expectedKey, err := base64.RawStdEncoding.DecodeString(parts[5])
	if err != nil || len(expectedKey) == 0 || len(expectedKey) > 128 {
		return false
	}

	actualKey := argon2.IDKey([]byte(passwd), salt, timeCost, memory, threads, uint32(len(expectedKey)))
	return subtle.ConstantTimeCompare(actualKey, expectedKey) == 1
}

func legacyHashPasswd(passwd string) string {
	saltedPassword := passwd + constantSalt
	hash := sha256.New()
	hash.Write([]byte(saltedPassword))
	return base64.StdEncoding.EncodeToString(hash.Sum(nil))
}

func CreateAccount(username, passwd string) (user models.User, err error) {
	return CreateAccountWithRole(username, passwd, RoleUser)
}

func CreateAccountWithRole(username, passwd, role string) (user models.User, err error) {
	db := dbcore.GetDBInstance()
	hashedPassword, err := hashPasswd(passwd)
	if err != nil {
		return models.User{}, err
	}
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

	hashedPassword, err := hashPasswd(passwd)
	if err != nil {
		return "", "", err
	}

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
			hashedPassword, err := hashPasswd(*password)
			if err != nil {
				return err
			}
			updates["passwd"] = hashedPassword
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
			&models.FailoverShare{},
			&models.FailoverV2Share{},
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
