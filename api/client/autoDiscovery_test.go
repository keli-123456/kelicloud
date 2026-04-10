package client

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/komari-monitor/komari/config"
	"github.com/komari-monitor/komari/database/models"
)

func TestRegisterClientCreatesUserOwnedClient(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := configureClientTestDB(t)
	now := models.FromTime(time.Now())

	owner := models.User{
		UUID:      "user-owner",
		Username:  "owner",
		Passwd:    "hashed",
		CreatedAt: now,
		UpdatedAt: now,
	}

	for _, item := range []interface{}{&owner} {
		if err := db.Create(item).Error; err != nil {
			t.Fatalf("failed to seed %T: %v", item, err)
		}
	}
	if err := config.SetForUser(owner.UUID, config.AutoDiscoveryKeyKey, "autodiscovery-key-123456"); err != nil {
		t.Fatalf("failed to save user autodiscovery key: %v", err)
	}

	router := gin.New()
	router.POST("/clients/register", RegisterClient)

	req := httptest.NewRequest(http.MethodPost, "/clients/register?name=edge-a", nil)
	req.Header.Set("Authorization", "Bearer autodiscovery-key-123456::group=edge")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}

	var payload struct {
		Status string `json:"status"`
		Data   struct {
			UUID  string `json:"uuid"`
			Token string `json:"token"`
		} `json:"data"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if payload.Status != "success" || payload.Data.UUID == "" || payload.Data.Token == "" {
		t.Fatalf("unexpected register response: %+v", payload)
	}

	var client models.Client
	if err := db.Where("uuid = ?", payload.Data.UUID).First(&client).Error; err != nil {
		t.Fatalf("failed to load created client: %v", err)
	}
	if client.UserID != owner.UUID {
		t.Fatalf("expected auto-discovered client owner %q, got %q", owner.UUID, client.UserID)
	}
	if client.Name != "Auto-edge-a" {
		t.Fatalf("expected auto-discovered client name %q, got %q", "Auto-edge-a", client.Name)
	}
	if client.Group != "edge" {
		t.Fatalf("expected auto-discovered client group %q, got %q", "edge", client.Group)
	}
}
