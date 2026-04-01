package ws

import (
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type SafeConn struct {
	conn *websocket.Conn
	mu   sync.Mutex
	ID   int64
}

func NewSafeConn(conn *websocket.Conn) *SafeConn {
	return &SafeConn{
		conn: conn,
		mu:   sync.Mutex{},
		ID:   time.Now().UnixNano(),
	}
}

func (sc *SafeConn) WriteMessage(messageType int, data []byte) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.conn.WriteMessage(messageType, data)
}

func (sc *SafeConn) WriteMessageWithDeadline(messageType int, data []byte, deadline time.Time) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if err := sc.conn.SetWriteDeadline(deadline); err != nil {
		return err
	}
	defer sc.conn.SetWriteDeadline(time.Time{})
	return sc.conn.WriteMessage(messageType, data)
}

func (sc *SafeConn) WriteJSON(v interface{}) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.conn.WriteJSON(v)
}

func (sc *SafeConn) WriteJSONWithDeadline(v interface{}, deadline time.Time) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if err := sc.conn.SetWriteDeadline(deadline); err != nil {
		return err
	}
	defer sc.conn.SetWriteDeadline(time.Time{})
	return sc.conn.WriteJSON(v)
}

func (sc *SafeConn) Close() error {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.conn.Close()
}
func (sc *SafeConn) ReadMessage() (int, []byte, error) {
	return sc.conn.ReadMessage()
}
func (sc *SafeConn) ReadJSON(v interface{}) error {
	return sc.conn.ReadJSON(v)
}
func (sc *SafeConn) SetReadDeadline(t time.Time) error {
	return sc.conn.SetReadDeadline(t)
}

func (sc *SafeConn) SetPongHandler(h func(string) error) {
	sc.conn.SetPongHandler(h)
}

func (sc *SafeConn) SetPingHandler(h func(string) error) {
	sc.conn.SetPingHandler(h)
}

func (sc *SafeConn) SetCloseHandler(h func(code int, text string) error) {
	sc.conn.SetCloseHandler(h)
}
func (sc *SafeConn) GetConn() *websocket.Conn {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.conn
}
