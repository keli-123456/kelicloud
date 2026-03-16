package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/komari-monitor/komari/cmd/flags"
	"github.com/komari-monitor/komari/database/accounts"
	"github.com/stretchr/testify/assert"
)

func TestGetMe(t *testing.T) {
	// 设置测试模式
	gin.SetMode(gin.TestMode)
	flags.DatabaseType = "sqlite"
	flags.DatabaseFile = filepath.Join(t.TempDir(), "komari-test.db")

	// 创建测试用户
	user, err := accounts.CreateAccount("testuser", "password")
	if err != nil {
		t.Fatalf("Failed to create test user: %v", err)
	}
	uuid := user.UUID
	sessionToken, _ := accounts.CreateSession(uuid, 2592000, "test_user_agent", "127.0.0.1", "oauth")

	tests := []struct {
		name           string
		sessionToken   string
		expectedStatus int
		expectedName   string
		expectedLogin  bool
	}{
		{
			name:           "有效的会话",
			sessionToken:   sessionToken,
			expectedStatus: http.StatusOK,
			expectedName:   "testuser",
			expectedLogin:  true,
		},
		{
			name:           "无效的会话",
			sessionToken:   "invalid_token",
			expectedStatus: http.StatusOK,
			expectedName:   "Guest",
			expectedLogin:  false,
		},
		{
			name:           "无会话",
			sessionToken:   "",
			expectedStatus: http.StatusOK,
			expectedName:   "Guest",
			expectedLogin:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 创建测试路由
			router := gin.New()
			router.GET("/me", GetMe)

			// 创建测试请求
			req, _ := http.NewRequest("GET", "/me", nil)
			if tt.sessionToken != "" {
				req.AddCookie(&http.Cookie{
					Name:  "session_token",
					Value: tt.sessionToken,
				})
			}

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

			assert.Equal(t, tt.expectedName, response["username"])
			assert.Equal(t, tt.expectedLogin, response["logged_in"])
			_, hasTenants := response["tenants"]
			assert.True(t, hasTenants)
			if tt.expectedLogin {
				_, hasCurrentTenant := response["current_tenant"]
				assert.True(t, hasCurrentTenant)
			}
		})
	}

	// 清除测试数据
	accounts.DeleteAccountByUsername("testuser")
	accounts.DeleteAllSessions()
}
