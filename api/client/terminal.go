package client

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/komari-monitor/komari/api"
	wsconn "github.com/komari-monitor/komari/ws"
)

func EstablishConnection(c *gin.Context) {
	session_id := c.Query("id")
	api.TerminalSessionsMutex.Lock()
	session, exists := api.TerminalSessions[session_id]
	api.TerminalSessionsMutex.Unlock()
	if !exists || session == nil || session.Browser == nil {
		c.JSON(404, gin.H{"status": "error", "error": "Session not found"})
		return
	}
	// Upgrade the connection to WebSocket
	if !websocket.IsWebSocketUpgrade(c.Request) {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "error": "Require WebSocket upgrade"})
		return
	}
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true // 被控
		},
	}
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		api.TerminalSessionsMutex.Lock()
		if session.Browser != nil {
			session.Browser.Close()
		}
		delete(api.TerminalSessions, session_id)
		api.TerminalSessionsMutex.Unlock()
		return
	}
	agentConn := wsconn.NewSafeConn(conn)
	api.TerminalSessionsMutex.Lock()
	session.Agent = agentConn
	api.TerminalSessionsMutex.Unlock()
	conn.SetCloseHandler(func(code int, text string) error {
		api.TerminalSessionsMutex.Lock()
		delete(api.TerminalSessions, session_id)
		api.TerminalSessionsMutex.Unlock()
		// 通知 Browser 关闭终端连接
		if session.Browser != nil {
			session.Browser.Close()
		}
		return nil
	})
	go api.ForwardTerminal(session_id)
}
