package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/komari-monitor/komari/database/accounts"
	"github.com/komari-monitor/komari/database/auditlog"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type LoginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
	TwoFa    string `json:"2fa_code"`
}

type RegisterRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

const maxLoginRequestBodyBytes = 16 * 1024

func readAuthRequestBody(c *gin.Context, target any) bool {
	bodyBytes, err := io.ReadAll(io.LimitReader(c.Request.Body, maxLoginRequestBodyBytes+1))
	if err != nil {
		RespondError(c, http.StatusBadRequest, "Invalid request body: "+err.Error())
		return false
	}
	if len(bodyBytes) > maxLoginRequestBodyBytes {
		RespondError(c, http.StatusRequestEntityTooLarge, "Login request body is too large")
		return false
	}
	if err := json.Unmarshal(bodyBytes, target); err != nil {
		RespondError(c, http.StatusBadRequest, "Invalid request body: "+err.Error())
		return false
	}
	return true
}

func Login(c *gin.Context) {
	var data LoginRequest
	if !readAuthRequestBody(c, &data) {
		return
	}
	data.Username = strings.TrimSpace(data.Username)
	if data.Username == "" || data.Password == "" {
		RespondError(c, http.StatusBadRequest, "Invalid request body: Username and password are required")
		return
	}
	if rejectLimitedLogin(c, data.Username) {
		return
	}

	uuid, success := accounts.CheckPassword(data.Username, data.Password)
	if !success {
		recordFailedLoginAttempt(c, data.Username)
		RespondError(c, http.StatusUnauthorized, "Invalid credentials")
		return
	}
	// 2FA
	user, _ := accounts.GetUserByUUID(uuid)
	if rejectInactiveUser(c, uuid) {
		return
	}
	if user.TwoFactor != "" { // 开启了2FA
		if data.TwoFa == "" {
			recordFailedLoginAttempt(c, data.Username)
			RespondError(c, http.StatusUnauthorized, "2FA code is required")
			return
		}
		if ok, err := accounts.Verify2Fa(uuid, data.TwoFa); err != nil || !ok {
			recordFailedLoginAttempt(c, data.Username)
			RespondError(c, http.StatusUnauthorized, "Invalid 2FA code")
			return
		}
	}
	// Create session
	session, err := accounts.CreateSession(uuid, 2592000, c.Request.UserAgent(), c.ClientIP(), "password")
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "Failed to create session: "+err.Error())
		return
	}
	recordSuccessfulLoginAttempt(c, data.Username)
	SetSecureCookie(c, "session_token", session, 2592000)
	auditlog.Log(c.ClientIP(), uuid, "logged in (password)", "login")
	RespondSuccess(c, gin.H{"set-cookie": gin.H{"session_token": session}})
}

func Register(c *gin.Context) {
	var data RegisterRequest
	if !readAuthRequestBody(c, &data) {
		return
	}
	data.Username = strings.TrimSpace(data.Username)
	if data.Username == "" || data.Password == "" {
		RespondError(c, http.StatusBadRequest, "Invalid request body: Username and password are required")
		return
	}
	if len([]rune(data.Username)) < 3 {
		RespondError(c, http.StatusBadRequest, "Username must be at least 3 characters long")
		return
	}
	if len([]rune(data.Username)) > 50 {
		RespondError(c, http.StatusBadRequest, "Username must be at most 50 characters long")
		return
	}
	if strings.ContainsAny(data.Username, "\r\n\t ") {
		RespondError(c, http.StatusBadRequest, "Username cannot contain spaces")
		return
	}
	if len([]rune(data.Password)) < 6 {
		RespondError(c, http.StatusBadRequest, "Password must be at least 6 characters long")
		return
	}
	if len([]rune(data.Password)) > 128 {
		RespondError(c, http.StatusBadRequest, "Password must be at most 128 characters long")
		return
	}
	if rejectLimitedLogin(c, data.Username) {
		return
	}

	if _, err := accounts.GetUserByUsername(data.Username); err == nil {
		RespondError(c, http.StatusConflict, "Username already exists")
		return
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		RespondError(c, http.StatusInternalServerError, "Failed to check username: "+err.Error())
		return
	}

	user, err := accounts.CreateAccountWithRole(data.Username, data.Password, accounts.RoleUser)
	if err != nil {
		if isDuplicateUsernameError(err) {
			RespondError(c, http.StatusConflict, "Username already exists")
			return
		}
		RespondError(c, http.StatusBadRequest, "Failed to create user: "+err.Error())
		return
	}

	session, err := accounts.CreateSession(user.UUID, 2592000, c.Request.UserAgent(), c.ClientIP(), "password")
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "Failed to create session: "+err.Error())
		return
	}
	recordSuccessfulLoginAttempt(c, data.Username)
	SetSecureCookie(c, "session_token", session, 2592000)
	auditlog.Log(c.ClientIP(), user.UUID, "registered account", "register")
	RespondSuccess(c, gin.H{
		"uuid":       user.UUID,
		"username":   user.Username,
		"role":       user.Role,
		"set-cookie": gin.H{"session_token": session},
	})
}

func isDuplicateUsernameError(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "duplicate") ||
		strings.Contains(message, "unique") ||
		strings.Contains(message, "constraint")
}
func Logout(c *gin.Context) {
	session, _ := c.Cookie("session_token")
	accounts.DeleteSession(session)
	ClearSecureCookie(c, "session_token")
	auditlog.Log(c.ClientIP(), "", "logged out", "logout")
	c.Redirect(302, "/")
}
