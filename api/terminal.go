package api

import (
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/komari-monitor/komari/database/clients"
	"github.com/komari-monitor/komari/utils"
	"github.com/komari-monitor/komari/ws"
)

const (
	terminalWriteTimeout = 15 * time.Second
	terminalReadTimeout  = 2 * time.Minute
	terminalPingInterval = 30 * time.Second
)

func terminalWrite(conn *ws.SafeConn, messageType int, data []byte) error {
	if conn == nil {
		return nil
	}
	return conn.WriteMessageWithDeadline(messageType, data, time.Now().Add(terminalWriteTimeout))
}

func extendTerminalReadDeadline(conn *ws.SafeConn) error {
	if conn == nil {
		return nil
	}
	return conn.SetReadDeadline(time.Now().Add(terminalReadTimeout))
}

func configureTerminalConn(conn *ws.SafeConn) error {
	if err := extendTerminalReadDeadline(conn); err != nil {
		return err
	}
	conn.SetPongHandler(func(string) error {
		return extendTerminalReadDeadline(conn)
	})
	return nil
}

func sendTerminalErr(errChan chan<- error, err error) {
	if err == nil {
		return
	}
	select {
	case errChan <- err:
	default:
	}
}

func RequestTerminal(c *gin.Context) {
	uuid := c.Param("uuid")
	userID, ok := RequireUserScopeFromSession(c)
	if !ok {
		return
	}
	_, err := clients.GetClientByUUIDForUser(uuid, userID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{
			"status":  "error",
			"message": "Client not found",
		})
		return
	}
	// 建立ws
	if !websocket.IsWebSocketUpgrade(c.Request) {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": "Require WebSocket upgrade"})
		return
	}
	upgrader := websocket.Upgrader{
		CheckOrigin: ws.CheckOrigin,
	}
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		return
	}
	browserConn := ws.NewSafeConn(conn)
	// 新建一个终端连接
	id := utils.GenerateRandomString(32)
	session := &TerminalSession{
		UserUUID:    userID,
		UUID:        uuid,
		Browser:     browserConn,
		Agent:       nil,
		RequesterIp: c.ClientIP(),
	}

	TerminalSessionsMutex.Lock()
	TerminalSessions[id] = session
	TerminalSessionsMutex.Unlock()
	conn.SetCloseHandler(func(code int, text string) error {
		log.Println("Terminal connection closed:", code, text)
		TerminalSessionsMutex.Lock()
		delete(TerminalSessions, id)
		TerminalSessionsMutex.Unlock()
		// 通知 Agent 关闭终端连接
		if session.Agent != nil {
			session.Agent.Close()
		}
		return nil
	})

	if ws.GetConnectedClients()[uuid] == nil {
		_ = terminalWrite(browserConn, websocket.TextMessage, []byte("Client offline!\n被控端离线!"))
		browserConn.Close()
		TerminalSessionsMutex.Lock()
		delete(TerminalSessions, id)
		TerminalSessionsMutex.Unlock()
		return
	}
	err = ws.GetConnectedClients()[uuid].WriteJSONWithDeadline(gin.H{
		"message":    "terminal",
		"request_id": id,
	}, time.Now().Add(terminalWriteTimeout))
	if err != nil {
		browserConn.Close()
		TerminalSessionsMutex.Lock()
		delete(TerminalSessions, id)
		TerminalSessionsMutex.Unlock()
		return
	}
	_ = terminalWrite(browserConn, websocket.TextMessage, []byte("等待被控端连接 waiting for agent..."))
	// 如果没有连接上，则关闭连接
	time.AfterFunc(30*time.Second, func() {
		TerminalSessionsMutex.Lock()
		if session.Agent == nil {
			if session.Browser != nil {
				_ = terminalWrite(session.Browser, websocket.TextMessage, []byte("被控端连接超时 timeout"))
				session.Browser.Close()
			}
			delete(TerminalSessions, id)
		}
		TerminalSessionsMutex.Unlock()
	})
	//auditlog.Log(c.ClientIP(), user_uuid.(string), "request, terminal id:"+id+",client:"+session.UUID, "terminal")
}

func ForwardTerminal(id string) {
	TerminalSessionsMutex.Lock()
	session, exists := TerminalSessions[id]
	TerminalSessionsMutex.Unlock()

	if !exists || session == nil || session.Agent == nil || session.Browser == nil {
		return
	}

	browserConn := session.Browser
	agentConn := session.Agent
	if err := configureTerminalConn(browserConn); err != nil {
		_ = browserConn.Close()
		_ = agentConn.Close()
		return
	}
	if err := configureTerminalConn(agentConn); err != nil {
		_ = browserConn.Close()
		_ = agentConn.Close()
		return
	}

	AuditLogForUser(session.RequesterIp, session.UserUUID, "established, terminal id:"+id, "terminal")
	established_time := time.Now()
	errChan := make(chan error, 1)
	done := make(chan struct{})

	go func() {
		for {
			messageType, data, err := browserConn.ReadMessage()
			if err != nil {
				sendTerminalErr(errChan, err)
				return
			}

			if messageType == websocket.TextMessage {
				if len(data) > 0 && data[0] == '{' {
					err = terminalWrite(agentConn, websocket.TextMessage, data)
				} else {
					err = terminalWrite(agentConn, websocket.BinaryMessage, data)
				}
			} else {
				// 二进制消息，原样传递
				err = terminalWrite(agentConn, websocket.BinaryMessage, data)
			}

			if err != nil {
				sendTerminalErr(errChan, err)
				return
			}
		}
	}()

	go func() {
		for {
			_, data, err := agentConn.ReadMessage()
			if err != nil {
				sendTerminalErr(errChan, err)
				return
			}
			err = terminalWrite(browserConn, websocket.BinaryMessage, data)
			if err != nil {
				sendTerminalErr(errChan, err)
				return
			}
		}
	}()

	go func() {
		ticker := time.NewTicker(terminalPingInterval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				if err := terminalWrite(browserConn, websocket.PingMessage, nil); err != nil {
					sendTerminalErr(errChan, err)
					return
				}
				if err := terminalWrite(agentConn, websocket.PingMessage, nil); err != nil {
					sendTerminalErr(errChan, err)
					return
				}
			}
		}
	}()

	// 等待错误或主动关闭
	<-errChan
	close(done)
	// 关闭连接
	_ = agentConn.Close()
	_ = browserConn.Close()
	disconnect_time := time.Now()
	AuditLogForUser(session.RequesterIp, session.UserUUID, "disconnected, terminal id:"+id+", duration:"+disconnect_time.Sub(established_time).String(), "terminal")
	TerminalSessionsMutex.Lock()
	delete(TerminalSessions, id)
	TerminalSessionsMutex.Unlock()
}
