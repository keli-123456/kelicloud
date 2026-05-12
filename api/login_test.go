package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/komari-monitor/komari/database/accounts"
	"github.com/stretchr/testify/assert"
)

func TestLogin(t *testing.T) {
	// 设置测试模式
	gin.SetMode(gin.TestMode)
	configureAPITestDB()
	accounts.CreateAccount("testuser", "correctpassword")
	tests := []struct {
		name           string
		requestBody    LoginRequest
		expectedStatus int
		expectedBody   map[string]interface{}
	}{
		{
			name: "成功登录",
			requestBody: LoginRequest{
				Username: "testuser",
				Password: "correctpassword",
			},
			expectedStatus: http.StatusOK,
			expectedBody: map[string]interface{}{
				"set-cookie": map[string]interface{}{
					"session_token": "",
				},
			},
		},
		{
			name: "无效的请求体",
			requestBody: LoginRequest{
				Username: "",
				Password: "",
			},
			expectedStatus: http.StatusBadRequest,
			expectedBody: map[string]interface{}{
				"status":  "error",
				"message": "Invalid request body: Username and password are required",
			},
		},
		{
			name: "错误的凭据",
			requestBody: LoginRequest{
				Username: "wronguser",
				Password: "wrongpassword",
			},
			expectedStatus: http.StatusUnauthorized,
			expectedBody: map[string]interface{}{
				"status":  "error",
				"message": "Invalid credentials",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 创建测试路由
			router := gin.New()
			router.POST("/login", Login)

			// 创建测试请求
			jsonBody, _ := json.Marshal(tt.requestBody)
			req, _ := http.NewRequest("POST", "/login", bytes.NewBuffer(jsonBody))
			req.Header.Set("Content-Type", "application/json")

			// 创建响应记录器
			w := httptest.NewRecorder()

			// 执行请求
			router.ServeHTTP(w, req)

			// 断言状态码
			assert.Equal(t, tt.expectedStatus, w.Code)

			// 解析响应体
			var response map[string]interface{}
			err := json.Unmarshal(w.Body.Bytes(), &response)
			assert.NoError(t, err)

			// 断言响应体
			if tt.expectedStatus == http.StatusOK {
				assert.Equal(t, "success", response["status"])
				data, ok := response["data"].(map[string]interface{})
				assert.True(t, ok)
				setCookie, ok := data["set-cookie"].(map[string]interface{})
				assert.True(t, ok)
				assert.NotEmpty(t, setCookie["session_token"])
			} else {
				assert.Equal(t, tt.expectedBody, response)
			}
		})
	}
	// 清除测试数据
	accounts.DeleteAccountByUsername("testuser")
	accounts.DeleteAllSessions()
}

func TestLoginRejectsOversizedBody(t *testing.T) {
	gin.SetMode(gin.TestMode)
	configureAPITestDB()

	router := gin.New()
	router.POST("/login", Login)

	req, _ := http.NewRequest(
		http.MethodPost,
		"/login",
		strings.NewReader(strings.Repeat("a", maxLoginRequestBodyBytes+1)),
	)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusRequestEntityTooLarge, w.Code)
}

func TestRegisterCreatesUserAndSession(t *testing.T) {
	gin.SetMode(gin.TestMode)
	configureAPITestDB()
	resetLoginAttemptsForTest(t)

	router := gin.New()
	router.POST("/register", Register)

	username := "register-user-session"
	t.Cleanup(func() {
		_ = accounts.DeleteAccountByUsername(username)
		accounts.DeleteAllSessions()
	})

	jsonBody, _ := json.Marshal(RegisterRequest{
		Username: username,
		Password: "correctpassword",
	})
	req, _ := http.NewRequest(http.MethodPost, "/register", bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.NotEmpty(t, w.Result().Cookies())

	var response map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &response)
	assert.NoError(t, err)
	assert.Equal(t, "success", response["status"])

	user, err := accounts.GetUserByUsername(username)
	assert.NoError(t, err)
	assert.Equal(t, accounts.RoleUser, accounts.NormalizeUserRole(user.Role))
}

func TestRegisterRejectsDuplicateUsername(t *testing.T) {
	gin.SetMode(gin.TestMode)
	configureAPITestDB()

	username := "register-duplicate-user"
	_, err := accounts.CreateAccount(username, "correctpassword")
	assert.NoError(t, err)
	t.Cleanup(func() {
		_ = accounts.DeleteAccountByUsername(username)
		accounts.DeleteAllSessions()
	})

	router := gin.New()
	router.POST("/register", Register)

	jsonBody, _ := json.Marshal(RegisterRequest{
		Username: username,
		Password: "anotherpassword",
	})
	req, _ := http.NewRequest(http.MethodPost, "/register", bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusConflict, w.Code)
	var response map[string]interface{}
	err = json.Unmarshal(w.Body.Bytes(), &response)
	assert.NoError(t, err)
	assert.Equal(t, "Username already exists", response["message"])
}
