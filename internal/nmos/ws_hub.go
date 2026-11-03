package nmos

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	// Per AMWA IS-07 v1.0 WebSocket Transport specification:
	// Receivers send health commands every 5 seconds; if the sender misses >2 health intervals
	// plus grace (12 seconds), it cleans up subscriptions and closes the connection.
	// We allow an initial 15s grace period upon socket open before enforcing the 12.5s watchdog.
	initialHealthGrace = 15 * time.Second
	healthWatchdogTTL  = 12500 * time.Millisecond
	wsWriteTimeout     = 5 * time.Second
)

// WSClientInfo provides live telemetry on connected IS-07 WebSocket receivers (e.g., Riedel NSA-010C).
type WSClientInfo struct {
	RemoteAddr        string    `json:"remote_addr"`
	ConnectedAt       time.Time `json:"connected_at"`
	LastHealthAt      time.Time `json:"last_health_at"`
	SubscribedSources []string  `json:"subscribed_sources"`
}

type wsClient struct {
	hub        *WSHub
	conn       *websocket.Conn
	remoteAddr string
	connected  time.Time
	sendCh     chan []byte
	closeOnce  sync.Once
	done       chan struct{}

	mu          sync.RWMutex
	subs        map[string]struct{}
	lastHealthy time.Time
}

// WSHub manages all active AMWA IS-07 v1.0 WebSocket receiver connections.
type WSHub struct {
	store    *Store
	log      *slog.Logger
	upgrader websocket.Upgrader

	mu      sync.RWMutex
	clients map[*wsClient]struct{}
}

// NewWSHub creates an IS-07 WebSocket transport hub.
func NewWSHub(store *Store, logger *slog.Logger) *WSHub {
	if logger == nil {
		logger = slog.Default()
	}
	return &WSHub{
		store: store,
		log:   logger,
		upgrader: websocket.Upgrader{
			ReadBufferSize:  4096,
			WriteBufferSize: 4096,
			// Broadcast LAN devices/browsers connect across IPs without Origin restrictions.
			CheckOrigin: func(r *http.Request) bool { return true },
		},
		clients: make(map[*wsClient]struct{}),
	}
}

// ServeHTTP upgrades an HTTP request to an AMWA IS-07 WebSocket transport session.
func (h *WSHub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		h.log.Warn("IS-07 WebSocket upgrade failed", "remote", r.RemoteAddr, "err", err)
		return
	}

	now := time.Now()
	c := &wsClient{
		hub:         h,
		conn:        conn,
		remoteAddr:  r.RemoteAddr,
		connected:   now,
		sendCh:      make(chan []byte, 256),
		done:        make(chan struct{}),
		subs:        make(map[string]struct{}),
		lastHealthy: now,
	}

	conn.SetPingHandler(func(appData string) error {
		c.touchHealth()
		return conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(wsWriteTimeout))
	})

	h.mu.Lock()
	h.clients[c] = struct{}{}
	h.mu.Unlock()

	h.log.Info("IS-07 WebSocket receiver connected", "remote", c.remoteAddr)

	go c.writePump()
	go c.watchdogLoop()
	c.readPump()
}

// BroadcastState pushes an IS-07 boolean state transition to all WebSocket clients subscribed to msg.Identity.SourceID.
func (h *WSHub) BroadcastState(msg IS07StateMessage) {
	payload, err := json.Marshal(msg)
	if err != nil {
		return
	}

	h.mu.RLock()
	defer h.mu.RUnlock()

	for c := range h.clients {
		if c.isSubscribed(msg.Identity.SourceID) {
			c.enqueue(payload)
		}
	}
}

// Shutdown gracefully sends IS-07 shutdown lifecycle messages to subscribed receivers before closing sockets.
func (h *WSHub) Shutdown() {
	shutdownMsgs := h.store.GetAllShutdownMessages()

	h.mu.RLock()
	clients := make([]*wsClient, 0, len(h.clients))
	for c := range h.clients {
		clients = append(clients, c)
	}
	h.mu.RUnlock()

	for _, c := range clients {
		for _, m := range shutdownMsgs {
			if c.isSubscribed(m.Identity.SourceID) {
				if b, err := json.Marshal(m); err == nil {
					_ = c.conn.SetWriteDeadline(time.Now().Add(500 * time.Millisecond))
					_ = c.conn.WriteMessage(websocket.TextMessage, b)
				}
			}
		}
		c.close()
	}
}

// ConnectedClients returns snapshots of active IS-07 WebSocket receivers (e.g., Riedel StageLink NSA-010C).
func (h *WSHub) ConnectedClients() []WSClientInfo {
	h.mu.RLock()
	defer h.mu.RUnlock()

	out := make([]WSClientInfo, 0, len(h.clients))
	for c := range h.clients {
		c.mu.RLock()
		subs := make([]string, 0, len(c.subs))
		for s := range c.subs {
			subs = append(subs, s)
		}
		info := WSClientInfo{
			RemoteAddr:        c.remoteAddr,
			ConnectedAt:       c.connected,
			LastHealthAt:      c.lastHealthy,
			SubscribedSources: subs,
		}
		c.mu.RUnlock()
		out = append(out, info)
	}
	return out
}

func (c *wsClient) readPump() {
	defer c.close()
	c.conn.SetReadLimit(65536)

	for {
		_, data, err := c.conn.ReadMessage()
		if err != nil {
			return
		}

		var cmd IS07WSClientCommand
		if err := json.Unmarshal(data, &cmd); err != nil {
			c.hub.log.Debug("IS-07 WS ignored malformed JSON command", "remote", c.remoteAddr, "err", err)
			continue
		}

		switch cmd.Command {
		case "health":
			c.touchHealth()
			originTS := cmd.Timestamp
			if originTS == "" {
				originTS = TAITimestampNow()
			}
			resp := IS07HealthResponse{
				Timing: EventTiming{
					OriginTimestamp:   originTS,
					CreationTimestamp: TAITimestampNow(),
				},
				MessageType: "health",
			}
			if b, err := json.Marshal(resp); err == nil {
				c.enqueue(b)
			}

		case "subscription":
			c.touchHealth()
			c.handleSubscription(cmd.Sources)
		}
	}
}

func (c *wsClient) handleSubscription(requested []string) {
	var newlyAdded []string

	c.mu.Lock()
	nextSubs := make(map[string]struct{}, len(requested))
	for _, srcID := range requested {
		nextSubs[srcID] = struct{}{}
		if _, already := c.subs[srcID]; !already {
			newlyAdded = append(newlyAdded, srcID)
		}
	}
	c.subs = nextSubs
	c.mu.Unlock()

	c.hub.log.Info("IS-07 WebSocket subscription updated",
		"remote", c.remoteAddr,
		"total_sources", len(requested),
		"newly_subscribed", len(newlyAdded),
	)

	// Per AMWA IS-07 v1.0 WebSocket Transport spec:
	// Immediately emit the current state message for all newly subscribed active sources!
	for _, srcID := range newlyAdded {
		msg, masterEnabled, exists := c.hub.store.GetStateMessageBySourceID(srcID)
		if !exists || !masterEnabled {
			continue
		}
		// Refresh creation_timestamp on late-joiner sync snapshot.
		msg.Timing.CreationTimestamp = TAITimestampNow()
		if b, err := json.Marshal(msg); err == nil {
			c.enqueue(b)
		}
	}
}

func (c *wsClient) writePump() {
	for {
		select {
		case <-c.done:
			return
		case msg, ok := <-c.sendCh:
			if !ok {
				return
			}
			_ = c.conn.SetWriteDeadline(time.Now().Add(wsWriteTimeout))
			if err := c.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				c.close()
				return
			}
		}
	}
}

func (c *wsClient) watchdogLoop() {
	// Wait initial grace window first.
	timer := time.NewTimer(initialHealthGrace)
	defer timer.Stop()
	select {
	case <-c.done:
		return
	case <-timer.C:
	}

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.done:
			return
		case now := <-ticker.C:
			c.mu.RLock()
			last := c.lastHealthy
			c.mu.RUnlock()
			if now.Sub(last) > healthWatchdogTTL {
				c.hub.log.Warn("IS-07 WebSocket health timeout (>12s), closing stale receiver connection", "remote", c.remoteAddr)
				c.close()
				return
			}
		}
	}
}

func (c *wsClient) touchHealth() {
	c.mu.Lock()
	c.lastHealthy = time.Now()
	c.mu.Unlock()
}

func (c *wsClient) isSubscribed(sourceID string) bool {
	c.mu.RLock()
	_, ok := c.subs[sourceID]
	c.mu.RUnlock()
	return ok
}

func (c *wsClient) enqueue(b []byte) {
	select {
	case <-c.done:
	case c.sendCh <- b:
	default:
		c.hub.log.Warn("IS-07 WebSocket client send buffer full, dropping slow receiver", "remote", c.remoteAddr)
		c.close()
	}
}

func (c *wsClient) close() {
	c.closeOnce.Do(func() {
		close(c.done)
		c.hub.mu.Lock()
		delete(c.hub.clients, c)
		c.hub.mu.Unlock()
		_ = c.conn.Close()
		c.hub.log.Info("IS-07 WebSocket receiver disconnected", "remote", c.remoteAddr)
	})
}
