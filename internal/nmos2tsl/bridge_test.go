package nmos2tsl_test

import (
	"context"
	"encoding/json"
	"fmt"
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
	"tsl2nmos/internal/nmos"
	"tsl2nmos/internal/nmos2tsl"
	"tsl2nmos/internal/tsl"
)

type mockWSClient struct {
	conn    *websocket.Conn
	writeMu sync.Mutex
}

func (c *mockWSClient) writeJSON(v any) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	_ = c.conn.WriteJSON(v)
}

type mockNSA010CGPI struct {
	t        *testing.T
	nodeSrv  *httptest.Server
	wsSrv    *httptest.Server
	upgrader websocket.Upgrader

	mu         sync.RWMutex
	nodeID     string
	deviceID   string
	srcIDs     [16]string
	flwIDs     [16]string
	sndIDs     [16]string
	gpiStates  map[string]bool // sourceID -> boolean
	wsClients  map[*mockWSClient]map[string]bool
	wsConnHits atomic.Int32
}

func newMockNSA010CGPI(t *testing.T) *mockNSA010CGPI {
	t.Helper()
	m := &mockNSA010CGPI{
		t:         t,
		nodeID:    nmos.DeterministicUUIDv5("mock-nsa-gpi", "node"),
		deviceID:  nmos.DeterministicUUIDv5("mock-nsa-gpi", "device"),
		gpiStates: make(map[string]bool),
		wsClients: make(map[*mockWSClient]map[string]bool),
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool { return true },
		},
	}
	for i := 0; i < 16; i++ {
		m.srcIDs[i] = nmos.DeterministicUUIDv5("mock-nsa-gpi", fmt.Sprintf("src-%d", i+1))
		m.flwIDs[i] = nmos.DeterministicUUIDv5("mock-nsa-gpi", fmt.Sprintf("flw-%d", i+1))
		m.sndIDs[i] = nmos.DeterministicUUIDv5("mock-nsa-gpi", fmt.Sprintf("snd-%d", i+1))
		m.gpiStates[m.srcIDs[i]] = false
	}

	m.startNewWSServer()

	mux := http.NewServeMux()
	mux.HandleFunc("/x-nmos/node/", m.handleNode)
	mux.HandleFunc("/x-nmos/connection/v1.1/", m.handleConnection)
	mux.HandleFunc("/x-nmos/events/v1.0/", m.handleEventsREST)
	m.nodeSrv = httptest.NewServer(mux)
	return m
}

func (m *mockNSA010CGPI) startNewWSServer() {
	wsMux := http.NewServeMux()
	wsMux.HandleFunc("/x-nmos/events/v1.0/device_id/", m.handleWS)
	srv := httptest.NewServer(wsMux)

	m.mu.Lock()
	oldSrv := m.wsSrv
	for c := range m.wsClients {
		_ = c.conn.Close()
	}
	m.wsClients = make(map[*mockWSClient]map[string]bool)
	m.wsSrv = srv
	m.mu.Unlock()

	if oldSrv != nil {
		oldSrv.Close()
	}
}

func (m *mockNSA010CGPI) Close() {
	m.mu.Lock()
	ws := m.wsSrv
	for c := range m.wsClients {
		_ = c.conn.Close()
	}
	m.mu.Unlock()
	if ws != nil {
		ws.Close()
	}
	m.nodeSrv.Close()
}

func (m *mockNSA010CGPI) SetGPI(oneBased int, closed bool) {
	srcID := m.srcIDs[oneBased-1]
	flwID := m.flwIDs[oneBased-1]

	msg := nmos.IS07StateMessage{
		Identity:    nmos.EventIdentity{SourceID: srcID, FlowID: flwID},
		EventType:   "boolean",
		Timing:      nmos.EventTiming{CreationTimestamp: nmos.TAITimestampNow()},
		Payload:     nmos.BooleanPayload{Value: closed},
		MessageType: "state",
	}

	m.mu.Lock()
	m.gpiStates[srcID] = closed
	var targets []*mockWSClient
	for c, subs := range m.wsClients {
		if subs[srcID] {
			targets = append(targets, c)
		}
	}
	m.mu.Unlock()

	for _, c := range targets {
		c.writeJSON(msg)
	}
}

func (m *mockNSA010CGPI) handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := m.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	m.wsConnHits.Add(1)
	client := &mockWSClient{conn: conn}

	m.mu.Lock()
	m.wsClients[client] = make(map[string]bool)
	m.mu.Unlock()

	defer func() {
		m.mu.Lock()
		delete(m.wsClients, client)
		m.mu.Unlock()
		_ = conn.Close()
	}()

	for {
		var cmd nmos.IS07WSClientCommand
		if err := conn.ReadJSON(&cmd); err != nil {
			return
		}
		switch cmd.Command {
		case "health":
			client.writeJSON(nmos.IS07HealthResponse{
				Timing: nmos.EventTiming{
					OriginTimestamp:   cmd.Timestamp,
					CreationTimestamp: nmos.TAITimestampNow(),
				},
				MessageType: "health",
			})
		case "subscription":
			m.mu.Lock()
			subMap := make(map[string]bool, len(cmd.Sources))
			var initial []nmos.IS07StateMessage
			for _, sid := range cmd.Sources {
				subMap[sid] = true
				val, ok := m.gpiStates[sid]
				if ok {
					initial = append(initial, nmos.IS07StateMessage{
						Identity:    nmos.EventIdentity{SourceID: sid},
						EventType:   "boolean",
						Timing:      nmos.EventTiming{CreationTimestamp: nmos.TAITimestampNow()},
						Payload:     nmos.BooleanPayload{Value: val},
						MessageType: "state",
					})
				}
			}
			m.wsClients[client] = subMap
			m.mu.Unlock()

			for _, st := range initial {
				client.writeJSON(st)
			}
		}
	}
}

func (m *mockNSA010CGPI) handleNode(w http.ResponseWriter, r *http.Request) {
	path := strings.Trim(strings.TrimPrefix(r.URL.Path, "/x-nmos/node"), "/")
	if path == "" {
		_ = json.NewEncoder(w).Encode([]string{"v1.3/"})
		return
	}
	parts := strings.Split(path, "/")
	if len(parts) < 2 {
		_ = json.NewEncoder(w).Encode([]string{"self/", "devices/", "sources/", "flows/", "senders/"})
		return
	}
	analogSrc := nmos.DeterministicUUIDv5("mock-nsa-gpi", "analog-src-1")
	analogFlw := nmos.DeterministicUUIDv5("mock-nsa-gpi", "analog-flw-1")
	analogSnd := nmos.DeterministicUUIDv5("mock-nsa-gpi", "analog-snd-1")

	switch parts[1] {
	case "self":
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id":      m.nodeID,
			"label":   "Riedel-NSA-010C",
			"version": nmos.TAITimestampNow(),
		})
	case "devices":
		u := strings.Replace(m.nodeSrv.URL, "127.0.0.1", "nsa-010c.local", 1)
		_ = json.NewEncoder(w).Encode([]map[string]any{
			{
				"id":    m.deviceID,
				"label": "Riedel-NSA-010C I/O Device",
				"controls": []map[string]any{
					{"type": nmos.ControlSRv11, "href": u + "/x-nmos/connection/v1.1"},
				},
			},
		})
	case "sources":
		var out []map[string]any
		for i := 0; i < 16; i++ {
			out = append(out, map[string]any{
				"id":         m.srcIDs[i],
				"label":      fmt.Sprintf("GPInput %d", i+1),
				"format":     nmos.FormatData,
				"event_type": "boolean",
			})
		}
		out = append(out, map[string]any{
			"id":         analogSrc,
			"label":      "AnalogGPInput 1",
			"format":     nmos.FormatData,
			"event_type": "number",
		})
		_ = json.NewEncoder(w).Encode(out)
	case "flows":
		var out []map[string]any
		for i := 0; i < 16; i++ {
			out = append(out, map[string]any{
				"id":         m.flwIDs[i],
				"source_id":  m.srcIDs[i],
				"event_type": "boolean",
			})
		}
		out = append(out, map[string]any{
			"id":         analogFlw,
			"source_id":  analogSrc,
			"event_type": "number",
		})
		_ = json.NewEncoder(w).Encode(out)
	case "senders":
		var out []map[string]any
		// Emit in reverse order to verify natural sort (GPInput 1 .. GPInput 16)
		for i := 15; i >= 0; i-- {
			out = append(out, map[string]any{
				"id":        m.sndIDs[i],
				"label":     fmt.Sprintf("GPInput %d (ws)", i+1),
				"device_id": m.deviceID,
				"flow_id":   m.flwIDs[i],
				"transport": nmos.TransportWebsocket,
				"subscription": map[string]any{
					"active": true,
				},
			})
		}
		out = append(out, map[string]any{
			"id":        analogSnd,
			"label":     "AnalogGPInput 1 (ws)",
			"device_id": m.deviceID,
			"flow_id":   analogFlw,
			"transport": nmos.TransportWebsocket,
		})
		_ = json.NewEncoder(w).Encode(out)
	}
}

func (m *mockNSA010CGPI) handleConnection(w http.ResponseWriter, r *http.Request) {
	path := strings.Trim(strings.TrimPrefix(r.URL.Path, "/x-nmos/connection/v1.1/single/senders/"), "/")
	parts := strings.Split(path, "/")
	if len(parts) != 2 || parts[1] != "active" {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	sndID := parts[0]
	idx := -1
	for i := 0; i < 16; i++ {
		if m.sndIDs[i] == sndID {
			idx = i
			break
		}
	}
	if idx < 0 {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	m.mu.RLock()
	wsURL := strings.Replace(m.wsSrv.URL, "http://127.0.0.1", "ws://nsa-010c.local", 1) + "/x-nmos/events/v1.0/device_id/" + m.deviceID
	m.mu.RUnlock()

	_ = json.NewEncoder(w).Encode(map[string]any{
		"master_enable": true,
		"receiver_id":   nil,
		"transport_params": []map[string]any{
			{
				"connection_authorization": false,
				"connection_uri":           wsURL,
				"ext_is_07_source_id":      m.srcIDs[idx],
				"ext_is_07_rest_api_url":   m.nodeSrv.URL + "/x-nmos/events/v1.0/sources/" + m.srcIDs[idx] + "/",
			},
		},
	})
}

func (m *mockNSA010CGPI) handleEventsREST(w http.ResponseWriter, r *http.Request) {
	path := strings.Trim(strings.TrimPrefix(r.URL.Path, "/x-nmos/events/v1.0/sources/"), "/")
	parts := strings.Split(path, "/")
	if len(parts) != 2 || parts[1] != "state" {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	m.mu.RLock()
	val := m.gpiStates[parts[0]]
	m.mu.RUnlock()
	_ = json.NewEncoder(w).Encode(map[string]any{
		"message_type": "state",
		"event_type":   "boolean",
		"identity":     map[string]any{"source_id": parts[0]},
		"payload":      map[string]any{"value": val},
	})
}

func TestEndToEnd_NMOS2TSL_NSA010CGPIToSimplyLiveUDPAndRebootSelfHeal(t *testing.T) {
	nsa := newMockNSA010CGPI(t)
	defer nsa.Close()

	// Mock SimplyLive ViBox TSL_Input UDP receiver.
	udpAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	slUDP, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer slUDP.Close()

	var rxMu sync.RWMutex
	rxDisplays := make(map[uint16]tsl.DMSG)
	go func() {
		buf := make([]byte, 2048)
		for {
			n, _, rerr := slUDP.ReadFromUDP(buf)
			if rerr != nil {
				return
			}
			pkts, perr := tsl.ParsePackets(buf[:n])
			if perr != nil {
				continue
			}
			rxMu.Lock()
			for _, p := range pkts {
				for _, m := range p.Messages {
					rxDisplays[m.Index] = m
				}
			}
			rxMu.Unlock()
		}
	}()

	cfgPath := filepath.Join(t.TempDir(), "config.yaml")
	cfgMgr, err := config.LoadOrCreate(cfgPath)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := cfgMgr.Get()
	store := nmos.NewStore(cfg)
	tx := tsl.NewTransmitter(cfg.NMOSToTSL.Output, nil)
	bridge := nmos2tsl.NewBridge(cfgMgr, store, tx, nil)

	// 1. Inspect mock NSA-010C GPI Senders: must return 16 boolean GPIs in natural order (AnalogGPInput excluded).
	info, err := bridge.InspectSourceDevice(ctx, nsa.nodeSrv.URL)
	if err != nil {
		t.Fatalf("InspectSourceDevice failed: %v", err)
	}
	if len(info.Senders) != 16 {
		t.Fatalf("expected 16 boolean GPI senders, got %d", len(info.Senders))
	}
	if info.Senders[0].Label != "GPInput 1 (ws)" || info.Senders[15].Label != "GPInput 16 (ws)" {
		t.Fatalf("natural sort failed: first=%s last=%s", info.Senders[0].Label, info.Senders[15].Label)
	}

	// 2. Configure NMOS -> TSL:
	//    - GPInput 1 -> Index 0 Red (PGM)
	//    - GPInput 2 -> Index 0 Green (PVW)
	//    - GPInput 3 (Inverted NC) -> Index 1 Red (PGM)
	cfg.NMOSToTSL.Output = config.TSLOutputConfig{
		Enabled:          true,
		Transport:        "udp",
		TargetAddr:       slUDP.LocalAddr().String(),
		Protocol:         "v5.0",
		LampMapping:      "lh_red_rh_green", // SimplyLive Left=Red, Right=Green
		TextFollowsTally: false,
		Brightness:       3,
		RepeatIntervalMs: 0,
	}
	cfg.NMOSToTSL.ExposeIS04Receivers = true
	cfg.NMOSToTSL.DirectSources = []config.DirectSourceConfig{
		{
			ID:      "src-nsa",
			Label:   info.NodeLabel,
			BaseURL: info.BaseURL,
			Enabled: true,
			Bindings: []config.GPIBindingConfig{
				{
					ID:          "b-gpi1",
					SenderID:    info.Senders[0].ID,
					SenderLabel: info.Senders[0].Label,
					SourceID:    info.Senders[0].SourceID,
					Screen:      0,
					Index:       0,
					Role:        "red",
					UMDText:     "CAM 1",
					Enabled:     true,
				},
				{
					ID:          "b-gpi2",
					SenderID:    info.Senders[1].ID,
					SenderLabel: info.Senders[1].Label,
					SourceID:    info.Senders[1].SourceID,
					Screen:      0,
					Index:       0,
					Role:        "green",
					UMDText:     "CAM 1",
					Enabled:     true,
				},
				{
					ID:          "b-gpi3",
					SenderID:    info.Senders[2].ID,
					SenderLabel: info.Senders[2].Label,
					SourceID:    info.Senders[2].SourceID,
					Screen:      0,
					Index:       1,
					Role:        "red",
					UMDText:     "CAM 2",
					Invert:      true,
					Enabled:     true,
				},
			},
		},
	}
	if err := cfgMgr.Save(cfg); err != nil {
		t.Fatal(err)
	}
	saved := cfgMgr.Get()
	store.RebuildFromConfig(saved, nil)
	if len(store.SnapshotReceivers()) != 3 {
		t.Fatalf("expected 3 exposed IS-04 receivers, got %d", len(store.SnapshotReceivers()))
	}

	if err := bridge.Start(ctx); err != nil {
		t.Fatal(err)
	}
	bridge.Reload(saved)

	waitFor := func(desc string, cond func() bool) {
		t.Helper()
		deadline := time.Now().Add(4 * time.Second)
		for time.Now().Before(deadline) {
			if cond() {
				return
			}
			time.Sleep(15 * time.Millisecond)
		}
		t.Fatalf("timed out waiting for: %s", desc)
	}

	// Verify multiplexed single WebSocket connection and active-low GPI 3 initial state -> Index 1 LH=RED!
	waitFor("multiplexed WS connected & inverted GPI 3 initial state", func() bool {
		rxMu.RLock()
		d1, ok := rxDisplays[1]
		rxMu.RUnlock()
		return ok && d1.LHTally == tsl.LampRed && d1.RHTally == tsl.LampOff
	})
	if hits := nsa.wsConnHits.Load(); hits != 1 {
		t.Fatalf("expected all GPIs to multiplex onto 1 WebSocket connection, got %d", hits)
	}

	// 3. Trigger GPI 1 (Red) & GPI 2 (Green) on Index 0!
	nsa.SetGPI(1, true)
	waitFor("Index 0 PGM Red (LH=RED, RH=OFF)", func() bool {
		rxMu.RLock()
		d0 := rxDisplays[0]
		rxMu.RUnlock()
		return d0.LHTally == tsl.LampRed && d0.RHTally == tsl.LampOff && d0.Text == "CAM 1"
	})

	nsa.SetGPI(2, true)
	waitFor("Index 0 PGM+PVW (LH=RED, RH=GREEN)", func() bool {
		rxMu.RLock()
		d0 := rxDisplays[0]
		rxMu.RUnlock()
		return d0.LHTally == tsl.LampRed && d0.RHTally == tsl.LampGreen
	})

	nsa.SetGPI(1, false)
	waitFor("Index 0 PVW only (LH=OFF, RH=GREEN)", func() bool {
		rxMu.RLock()
		d0 := rxDisplays[0]
		rxMu.RUnlock()
		return d0.LHTally == tsl.LampOff && d0.RHTally == tsl.LampGreen
	})

	// 4. Test Web UI manual override without hardware GPI signal!
	bridge.SetOverride("b-gpi1", nmos2tsl.OverrideForceOn)
	waitFor("Manual Override ForceOn on GPI 1", func() bool {
		rxMu.RLock()
		d0 := rxDisplays[0]
		rxMu.RUnlock()
		return d0.LHTally == tsl.LampRed && d0.RHTally == tsl.LampGreen
	})
	bridge.SetOverride("b-gpi1", nmos2tsl.OverrideAuto)

	// 5. Simulate NSA-010C reboot onto a brand-new dynamic WebSocket port!
	nsa.startNewWSServer()
	waitFor("Auto-reconnect to new dynamic WS port after NSA-010C reboot", func() bool {
		st := bridge.Status()
		return len(st.Sessions) == 1 && st.Sessions[0].Connected && nsa.wsConnHits.Load() >= 2
	})

	nsa.SetGPI(1, true)
	waitFor("Post-reboot GPI 1 update delivered to SimplyLive UDP", func() bool {
		rxMu.RLock()
		d0 := rxDisplays[0]
		rxMu.RUnlock()
		return d0.LHTally == tsl.LampRed && d0.RHTally == tsl.LampGreen
	})
}
