package api

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/komari-monitor/komari/common"
	"github.com/komari-monitor/komari/database/clients"
	"github.com/komari-monitor/komari/ws"
)

func GetClients(c *gin.Context) {
	userUUID, ok := RequireUserScopeFromSession(c)
	if !ok {
		return
	}
	clientList, err := clients.GetAllClientBasicInfoByUser(userUUID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"status": "error", "error": "Failed to retrieve client information"})
		return
	}

	// 升级到ws
	if !websocket.IsWebSocketUpgrade(c.Request) {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "error": "Require WebSocket upgrade"})
		return
	}
	upgrader := websocket.Upgrader{
		CheckOrigin: ws.CheckOrigin,
	}
	// Upgrade the HTTP connection to a WebSocket connection
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "error": "Failed to upgrade to WebSocket." + err.Error()})
		return
	}
	defer conn.Close()

	allowed := make(map[string]struct{}, len(clientList))
	for _, client := range clientList {
		allowed[client.UUID] = struct{}{}
	}

	// 请求
	for {
		var resp struct {
			Online []string                 `json:"online"` // 已建立连接的客户端uuid列表
			Data   map[string]common.Report `json:"data"`   // 最后上报的数据
		}

		resp.Online = []string{}
		resp.Data = map[string]common.Report{}

		_, data, err := conn.ReadMessage()
		if err != nil {
			return
		}
		message := string(data)

		uuID := ""
		if message != "get" { // 非请求全部内容
			if strings.HasPrefix(message, "get ") {
				uuID = strings.TrimSpace(strings.TrimPrefix(message, "get "))
			} else {
				conn.WriteJSON(gin.H{"status": "error", "error": "Invalid message"})
				continue
			}
		}

		if uuID != "" {
			if _, exists := allowed[uuID]; !exists {
				conn.WriteJSON(gin.H{"status": "success", "data": resp})
				continue
			}
		}

		for _, key := range ws.GetAllOnlineUUIDs() {
			if _, exists := allowed[key]; !exists {
				continue
			}
			if uuID != "" && key != uuID {
				continue
			}
			resp.Online = append(resp.Online, key)
		}

		for key, report := range ws.GetLatestReport() {
			if _, exists := allowed[key]; !exists {
				continue
			}
			if uuID != "" && key != uuID {
				continue
			}

			report.UUID = ""
			if report.CPU.Usage == 0 {
				report.CPU.Usage = 0.01
			}
			resp.Data[key] = *report
		}

		if err := conn.WriteJSON(gin.H{"status": "success", "data": resp}); err != nil {
			return
		}
	}
}
