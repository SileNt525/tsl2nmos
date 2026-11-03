package controller_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"tsl2nmos/internal/config"
	"tsl2nmos/internal/controller"
	"tsl2nmos/internal/nmos"
	"tsl2nmos/internal/tally"
	"tsl2nmos/internal/tsl"
)

// mockNMOSGPIODevice simulates a standard AMWA NMOS IS-07 / IS-05 hardware GPIO box (e.g., 16-channel GPO device):
// - Exposes AMWA IS-04 v1.3 Node API (/self, /devices, /receivers with 16 GPOs)
// - Exposes AMWA IS-05 v1.1 Receiver Connection API (/single/receivers/{id}/staged & /active)
// - Connects as an AMWA IS-07 v1.0 WebSocket client when patched, sends subscription + 5s health commands,
//   and actuates 16 simulated solid-state GPO relays!
type mockNMOSGPIODevice struct {
	t      *testing.T
	server *httptest.Server

	mu         sync.RWMutex
	nodeID     string
	deviceID   string
	gpoIDs     [16]string // index 0..15 => GPO 1..16
	rcvActive  map[string]mockReceiverConnState
	relayState map[string]bool // receiverID -> physical GPO relay closed/open

	wsMu          sync.Mutex
	wsConn        *websocket.Conn
	wsURI         string
	wsCancel      context.CancelFunc
	healthReplies atomic.Int32
}

type mockReceiverConnState struct {
	SenderID        *string               `json:"sender_id"`
	MasterEnable    bool                  `json:"master_enable"`
	TransportParams []mockReceiverWSParam `json:"transport_params"`
}

type mockReceiverWSParam struct {
	ConnectionURI           string `json:"connection_uri"`
	ConnectionAuthorization bool   `json:"connection_authorization"`
	ExtIS07SourceID         string `json:"ext_is_07_source_id"`
	ExtIS07RestAPIURL       string `json:"ext_is_07_rest_api_url"`
}

func newMockNMOSGPIODevice(t *testing.T) *mockNMOSGPIODevice {
	t.Helper()
	m := &mockNMOSGPIODevice{
		t:          t,
		nodeID:     nmos.DeterministicUUIDv5("mock-nmos-gpio-device", "node"),
		deviceID:   nmos.DeterministicUUIDv5("mock-nmos-gpio-device", "device"),
		rcvActive:  make(map[string]mockReceiverConnState),
		relayState: make(map[string]bool),
	}
	for i := 0; i < 16; i++ {
		id := nmos.DeterministicUUIDv5("mock-nmos-gpio-device", fmt.Sprintf("gpo-%d", i+1))
		m.gpoIDs[i] = id
		m.rcvActive[id] = mockReceiverConnState{MasterEnable: false}
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/x-nmos/node/", m.handleNode)
	mux.HandleFunc("/x-nmos/connection/v1.1/", m.handleConnection)
	m.server = httptest.NewServer(mux)
	return m
}

func (m *mockNMOSGPIODevice) Close() {
	m.stopWS()
	m.server.Close()
}

// SimulatePowerCycle wipes ephemeral IS-05 active crosspoints and drops WebSocket,
// testing tsl2nmos's self-healing auto-recovery!
func (m *mockNMOSGPIODevice) SimulatePowerCycle() {
	m.stopWS()
	m.mu.Lock()
	for id := range m.rcvActive {
		m.rcvActive[id] = mockReceiverConnState{MasterEnable: false}
		m.relayState[id] = false
	}
	m.mu.Unlock()
}

func (m *mockNMOSGPIODevice) RelayClosed(gpoOneBased int) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.relayState[m.gpoIDs[gpoOneBased-1]]
}

func (m *mockNMOSGPIODevice) handleNode(w http.ResponseWriter, r *http.Request) {
	path := strings.Trim(strings.TrimPrefix(r.URL.Path, "/x-nmos/node"), "/")
	if path == "" {
		_ = json.NewEncoder(w).Encode([]string{"v1.2/", "v1.3/"})
		return
	}
	parts := strings.Split(path, "/")
	if len(parts) < 2 {
		_ = json.NewEncoder(w).Encode([]string{"self/", "devices/", "receivers/"})
		return
	}
	switch parts[1] {
	case "self":
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id":      m.nodeID,
			"label":   "Standard NMOS IS-07 GPIO Device",
			"version": nmos.TAITimestampNow(),
		})
	case "devices":
		// Deliberately advertise a ".local" hostname in controls.href to verify tsl2nmos's reachable-IP rewrite!
		u := strings.Replace(m.server.URL, "127.0.0.1", "nmos-gpio-mock.local", 1)
		_ = json.NewEncoder(w).Encode([]map[string]any{
			{
				"id":    m.deviceID,
				"label": "NMOS IS-07 GPIO Device",
				"controls": []map[string]any{
					{
						"type": nmos.ControlSRv11,
						"href": u + "/x-nmos/connection/v1.1/",
					},
				},
			},
		})
	case "receivers":
		m.mu.RLock()
		// Emit in reverse order (GPO 16 .. GPO 1) to verify InspectDevice natural sort (GPO 1, GPO 2, ..., GPO 16)!
		rcvs := make([]map[string]any, 0, 16)
		for i := 15; i >= 0; i-- {
			id := m.gpoIDs[i]
			act := m.rcvActive[id]
			rcvs = append(rcvs, map[string]any{
				"id":          id,
				"label":       fmt.Sprintf("GPO %d", i+1),
				"description": fmt.Sprintf("Solid-State Relay Output %d", i+1),
				"device_id":   m.deviceID,
				"format":      nmos.FormatData,
				"transport":   nmos.TransportWebsocket,
				"subscription": map[string]any{
					"sender_id": act.SenderID,
					"active":    act.MasterEnable,
				},
			})
		}
		m.mu.RUnlock()
		_ = json.NewEncoder(w).Encode(rcvs)
	}
}

func (m *mockNMOSGPIODevice) handleConnection(w http.ResponseWriter, r *http.Request) {
	path := strings.Trim(strings.TrimPrefix(r.URL.Path, "/x-nmos/connection/v1.1/single/receivers/"), "/")
	parts := strings.Split(path, "/")
	if len(parts) != 2 {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	rcvID, leaf := parts[0], parts[1]

	switch leaf {
	case "active":
		m.mu.RLock()
		act, ok := m.rcvActive[rcvID]
		m.mu.RUnlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(act)

	case "staged":
		if r.Method != http.MethodPatch {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		body, _ := io.ReadAll(r.Body)
		var patch struct {
			SenderID        *string               `json:"sender_id"`
			MasterEnable    bool                  `json:"master_enable"`
			TransportParams []mockReceiverWSParam `json:"transport_params"`
		}
		if err := json.Unmarshal(body, &patch); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		m.mu.Lock()
		m.rcvActive[rcvID] = mockReceiverConnState{
			SenderID:        patch.SenderID,
			MasterEnable:    patch.MasterEnable,
			TransportParams: patch.TransportParams,
		}
		m.mu.Unlock()

		m.refreshWebSocketSubscriptions()
		_ = json.NewEncoder(w).Encode(patch)
	}
}

func (m *mockNMOSGPIODevice) refreshWebSocketSubscriptions() {
	m.mu.RLock()
	var wsURI string
	sourceToGPOs := make(map[string][]string)
	var sources []string
	seen := make(map[string]bool)

	for rcvID, st := range m.rcvActive {
		if !st.MasterEnable || len(st.TransportParams) == 0 {
			continue
		}
		tp := st.TransportParams[0]
		if wsURI == "" {
			wsURI = tp.ConnectionURI
		}
		srcID := tp.ExtIS07SourceID
		if srcID != "" {
			sourceToGPOs[srcID] = append(sourceToGPOs[srcID], rcvID)
			if !seen[srcID] {
				seen[srcID] = true
				sources = append(sources, srcID)
			}
		}
	}
	m.mu.RUnlock()

	if wsURI == "" || len(sources) == 0 {
		m.stopWS()
		return
	}

	m.wsMu.Lock()
	defer m.wsMu.Unlock()

	if m.wsConn == nil || m.wsURI != wsURI {
		if m.wsCancel != nil {
			m.wsCancel()
		}
		if m.wsConn != nil {
			_ = m.wsConn.Close()
		}
		conn, _, err := websocket.DefaultDialer.Dial(wsURI, nil)
		if err != nil {
			return
		}
		ctx, cancel := context.WithCancel(context.Background())
		m.wsConn = conn
		m.wsURI = wsURI
		m.wsCancel = cancel

		go m.wsReadLoop(ctx, conn)
		go m.wsHealthLoop(ctx, conn)
	}

	// Send IS-07 subscription command!
	subCmd := map[string]any{
		"command": "subscription",
		"sources": sources,
	}
	_ = m.wsConn.WriteJSON(subCmd)
}

func (m *mockNMOSGPIODevice) wsHealthLoop(ctx context.Context, conn *websocket.Conn) {
	// Send an immediate health command, then every 2s in test.
	sendHealth := func() {
		m.wsMu.Lock()
		defer m.wsMu.Unlock()
		if m.wsConn == conn {
			_ = conn.WriteJSON(map[string]any{
				"command":   "health",
				"timestamp": nmos.TAITimestampNow(),
			})
		}
	}
	sendHealth()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sendHealth()
		}
	}
}

func (m *mockNMOSGPIODevice) wsReadLoop(ctx context.Context, conn *websocket.Conn) {
	for {
		_, data, err := conn.ReadMessage()
		if err != nil || ctx.Err() != nil {
			return
		}
		var base struct {
			MessageType string `json:"message_type"`
			Identity    struct {
				SourceID string `json:"source_id"`
			} `json:"identity"`
			Payload struct {
				Value bool `json:"value"`
			} `json:"payload"`
		}
		if err := json.Unmarshal(data, &base); err != nil {
			continue
		}
		switch base.MessageType {
		case "health":
			m.healthReplies.Add(1)
		case "state":
			m.mu.Lock()
			for rcvID, st := range m.rcvActive {
				if st.MasterEnable && len(st.TransportParams) > 0 && st.TransportParams[0].ExtIS07SourceID == base.Identity.SourceID {
					m.relayState[rcvID] = base.Payload.Value
				}
			}
			m.mu.Unlock()
		}
	}
}

func (m *mockNMOSGPIODevice) stopWS() {
	m.wsMu.Lock()
	defer m.wsMu.Unlock()
	if m.wsCancel != nil {
		m.wsCancel()
		m.wsCancel = nil
	}
	if m.wsConn != nil {
		_ = m.wsConn.Close()
		m.wsConn = nil
	}
	m.wsURI = ""
}

func TestEndToEnd_TSL5ToNMOSGPIO_WithRegistryAndPowerCycleSelfHeal(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// 1. Spin up Mock IS-04 Registration Server.
	var regMu sync.Mutex
	regCounts := make(map[string]int)
	var regHeartbeats atomic.Int32
	mockRegistry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/x-nmos/registration/v1.3/":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`["resource/","health/"]`))
		case r.Method == http.MethodPost && r.URL.Path == "/x-nmos/registration/v1.3/resource":
			var body struct {
				Type string `json:"type"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			regMu.Lock()
			regCounts[body.Type]++
			regMu.Unlock()
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{}`))
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/x-nmos/registration/v1.3/health/nodes/"):
			regHeartbeats.Add(1)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"health":"1730550000"}`))
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer mockRegistry.Close()

	// 2. Spin up Mock NMOS IS-07 GPIO Device (16 GPOs).
	gpioDevice := newMockNMOSGPIODevice(t)
	defer gpioDevice.Close()

	// 3. Allocate free ephemeral TCP port for tsl2nmos HTTP/WS server (never collides with running processes).
	tmpLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen ephemeral tcp: %v", err)
	}
	gwHTTPPort := tmpLn.Addr().(*net.TCPAddr).Port
	_ = tmpLn.Close()

	// 4. Create config with 8 cameras + DirectTarget pointing to Mock NMOS GPIO device.
	cfgPath := filepath.Join(t.TempDir(), "config.yaml")
	cfgMgr, err := config.LoadOrCreate(cfgPath)
	if err != nil {
		t.Fatalf("LoadOrCreate: %v", err)
	}
	cfg := cfgMgr.Get()
	cfg.Node.HostIP = "127.0.0.1"
	cfg.Node.BindAddr = "127.0.0.1"
	cfg.Node.HTTPPort = gwHTTPPort
	cfg.TSL.ListenAddr = "127.0.0.1:0"
	cfg.NMOS.MDNSEnabled = false
	cfg.NMOS.RegistryMode = "static"
	cfg.NMOS.StaticRegistryURL = mockRegistry.URL + "/x-nmos/registration/v1.3"
	cfg.NMOS.HeartbeatIntervalSec = 1

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := nmos.NewStore(cfg)
	engine := tally.NewEngine(cfg, func(channelID int, color tally.Color) (string, string) {
		return nmos.ChannelSourceID(cfg.Node.IDSeed, channelID, color),
			nmos.ChannelSenderID(cfg.Node.IDSeed, channelID, color)
	})
	wsHub := nmos.NewWSHub(store, logger)
	engine.OnChange(func(ev tally.StateChangeEvent) {
		msg, enabled, ok := store.UpdateTallyValue(ev.ChannelID, ev.Color, ev.Value, ev.Timestamp)
		if ok && enabled {
			wsHub.BroadcastState(msg)
		}
	})

	mux := http.NewServeMux()
	nmos.RegisterRoutes(mux, store, wsHub)
	httpSrv := &http.Server{Addr: fmt.Sprintf("127.0.0.1:%d", gwHTTPPort), Handler: mux}
	go func() { _ = httpSrv.ListenAndServe() }()
	defer httpSrv.Close()

	tslListener := tsl.NewListener(cfg.TSL.ListenAddr, engine.HandlePacket, logger)
	if err := tslListener.Start(ctx); err != nil {
		t.Fatalf("TSL listener start: %v", err)
	}

	regCli := nmos.NewRegistryClient(store, nil, cfg.NMOS, logger)
	regCli.Start(ctx)

	patcher := controller.NewDirectPatcher(cfgMgr, store, logger)

	// 5. Inspect Mock GPIO Device via DirectPatcher and verify natural sorting of GPO 1..16!
	info, err := patcher.InspectDevice(ctx, gpioDevice.server.URL)
	if err != nil {
		t.Fatalf("InspectDevice NMOS GPIO failed: %v", err)
	}
	if len(info.Receivers) != 16 {
		t.Fatalf("expected 16 GPO receivers on GPIO device, got %d", len(info.Receivers))
	}
	if info.Receivers[0].Label != "GPO 1" || info.Receivers[9].Label != "GPO 10" || info.Receivers[15].Label != "GPO 16" {
		t.Fatalf("natural sort failed: [0]=%s [9]=%s [15]=%s",
			info.Receivers[0].Label, info.Receivers[9].Label, info.Receivers[15].Label)
	}

	// Bind GPO 1..4 -> CAM 1 Red/Green, CAM 2 Red/Green!
	bindings := []config.GPOBindingConfig{
		{ReceiverID: info.Receivers[0].ID, ReceiverLabel: "GPO 1", ChannelID: 1, Color: "red", Enabled: true},
		{ReceiverID: info.Receivers[1].ID, ReceiverLabel: "GPO 2", ChannelID: 1, Color: "green", Enabled: true},
		{ReceiverID: info.Receivers[2].ID, ReceiverLabel: "GPO 3", ChannelID: 2, Color: "red", Enabled: true},
		{ReceiverID: info.Receivers[3].ID, ReceiverLabel: "GPO 4", ChannelID: 2, Color: "green", Enabled: true},
	}
	cfg.DirectTargets = []config.DirectTargetConfig{
		{
			ID:       "gpio-studio-1",
			Label:    info.NodeLabel,
			BaseURL:  gpioDevice.server.URL,
			Enabled:  true,
			Bindings: bindings,
		},
	}
	if err := cfgMgr.Save(cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}

	patcher.Start(ctx)

	// Wait for IS-05 patching + IS-07 WebSocket subscription + health reply.
	waitUntil(t, 3*time.Second, func() bool {
		return len(wsHub.ConnectedClients()) == 1 && gpioDevice.healthReplies.Load() >= 1
	}, "NMOS GPIO Device WebSocket connection & IS-07 health handshake")

	// Verify IS-04 Registry received Node, Device, 16 Sources, 16 Flows, 16 Senders + Heartbeat!
	waitUntil(t, 3*time.Second, func() bool {
		regMu.Lock()
		defer regMu.Unlock()
		return regCounts["node"] >= 1 &&
			regCounts["device"] >= 1 &&
			regCounts["source"] >= 16 &&
			regCounts["flow"] >= 16 &&
			regCounts["sender"] >= 16 &&
			regHeartbeats.Load() >= 1
	}, "IS-04 Registry registration & heartbeat")

	// 6. Send live TSL 5.0 UDP datagram: CAM 1 = RED (PGM), CAM 2 = GREEN (PVW)!
	udpConn, err := net.Dial("udp", tslListener.BoundAddr())
	if err != nil {
		t.Fatalf("dial TSL UDP: %v", err)
	}
	defer udpConn.Close()

	pkt1 := tsl.MarshalPacket(0, false, []tsl.DMSG{
		{Index: 1, RHTally: tsl.LampRed, LHTally: tsl.LampOff, Text: "CAM 1"},
		{Index: 2, RHTally: tsl.LampOff, LHTally: tsl.LampGreen, Text: "CAM 2"},
	}, false)
	if _, err := udpConn.Write(pkt1); err != nil {
		t.Fatalf("send TSL UDP: %v", err)
	}

	// Assert physical GPO relays on Mock NMOS GPIO device actuate within milliseconds!
	waitUntil(t, 2*time.Second, func() bool {
		return gpioDevice.RelayClosed(1) && !gpioDevice.RelayClosed(2) && !gpioDevice.RelayClosed(3) && gpioDevice.RelayClosed(4)
	}, "GPO 1 (Cam1 Red) & GPO 4 (Cam2 Green) relays closed")

	// 7. Verify IS-07 REST /state endpoint also reflects live state for late joiners!
	cam1RedSrc := nmos.ChannelSourceID(cfg.Node.IDSeed, 1, tally.ColorRed)
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/x-nmos/events/v1.0/sources/%s/state/", gwHTTPPort, cam1RedSrc))
	if err != nil {
		t.Fatalf("GET IS-07 /state: %v", err)
	}
	var stateResp nmos.IS07StateMessage
	_ = json.NewDecoder(resp.Body).Decode(&stateResp)
	_ = resp.Body.Close()
	if !stateResp.Payload.Value || stateResp.EventType != "boolean" || stateResp.MessageType != "state" {
		t.Fatalf("unexpected IS-07 REST /state response: %+v", stateResp)
	}

	// 8. Simulate NMOS GPIO device power-cycle reboot (wipes relays & IS-05 active crosspoints),
	// trigger patcher sync, and verify automatic re-patching + late-joiner state restoration!
	gpioDevice.SimulatePowerCycle()
	if gpioDevice.RelayClosed(1) || gpioDevice.RelayClosed(4) {
		t.Fatalf("expected relays cleared immediately after simulated power cycle")
	}

	patcher.TriggerSync()
	waitUntil(t, 3*time.Second, func() bool {
		return gpioDevice.RelayClosed(1) && !gpioDevice.RelayClosed(2) && !gpioDevice.RelayClosed(3) && gpioDevice.RelayClosed(4)
	}, "Self-healing IS-05 re-patch & IS-07 late-joiner relay restoration after GPIO device reboot")
}

func waitUntil(t *testing.T, timeout time.Duration, cond func() bool, desc string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", desc)
}
