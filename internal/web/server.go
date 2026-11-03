package web

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"time"

	"tsl2nmos/internal/config"
	"tsl2nmos/internal/controller"
	"tsl2nmos/internal/nmos"
	"tsl2nmos/internal/tally"
	"tsl2nmos/internal/tsl"
)

//go:embed static/*
var staticFS embed.FS

// ReloadCallback is invoked when configuration is updated via the Web UI so all runtime components hot-reload.
type ReloadCallback func(newCfg config.Config)

// Server provides the embedded Web UI and JSON/SSE management APIs.
type Server struct {
	cfgMgr   *config.Manager
	tslList  *tsl.Listener
	engine   *tally.Engine
	store    *nmos.Store
	wsHub    *nmos.WSHub
	regCli   *nmos.RegistryClient
	mdnsSvc  *nmos.MDNSService
	patcher  *controller.DirectPatcher
	onReload ReloadCallback
	log      *slog.Logger
	started  time.Time
}

// NewServer constructs the Web Console & Management API handler.
func NewServer(
	cfgMgr *config.Manager,
	tslList *tsl.Listener,
	engine *tally.Engine,
	store *nmos.Store,
	wsHub *nmos.WSHub,
	regCli *nmos.RegistryClient,
	mdnsSvc *nmos.MDNSService,
	patcher *controller.DirectPatcher,
	onReload ReloadCallback,
	logger *slog.Logger,
) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{
		cfgMgr:   cfgMgr,
		tslList:  tslList,
		engine:   engine,
		store:    store,
		wsHub:    wsHub,
		regCli:   regCli,
		mdnsSvc:  mdnsSvc,
		patcher:  patcher,
		onReload: onReload,
		log:      logger,
		started:  time.Now(),
	}
}

// RegisterRoutes mounts the Web UI ("/") and management APIs ("/api/v1/...") onto mux.
func (s *Server) RegisterRoutes(mux *http.ServeMux) {
	sub, _ := fs.Sub(staticFS, "static")
	fileServer := http.FileServer(http.FS(sub))

	mux.Handle("/", fileServer)
	mux.HandleFunc("/api/v1/status", s.handleStatus)
	mux.HandleFunc("/api/v1/stream", s.handleSSE)
	mux.HandleFunc("/api/v1/sniffer", s.handleSniffer)
	mux.HandleFunc("/api/v1/channels/override", s.handleOverride)
	mux.HandleFunc("/api/v1/tsl/simulate", s.handleSimulateTSL)
	mux.HandleFunc("/api/v1/config", s.handleConfig)
	mux.HandleFunc("/api/v1/targets/inspect", s.handleInspectTarget)
	mux.HandleFunc("/api/v1/targets/resync", s.handleResyncTargets)
	mux.HandleFunc("/api/v1/targets/disconnect", s.handleDisconnectReceiver)
	mux.HandleFunc("/api/v1/mdns/nodes", s.handleMDNSNodes)
}

func (s *Server) buildStatusPayload() map[string]any {
	node, _, _, _, _ := s.store.SnapshotResources()
	hostIP, port := s.store.HostEndpoint()

	return map[string]any{
		"uptime_sec":       int64(time.Since(s.started).Seconds()),
		"node_id":          node.ID,
		"node_label":       node.Label,
		"advertised_http":  fmt.Sprintf("http://%s:%d", hostIP, port),
		"advertised_ws":    fmt.Sprintf("ws://%s:%d/x-nmos/events/v1.0/ws", hostIP, port),
		"config_path":      s.cfgMgr.Path(),
		"tsl_stats":        s.tslList.GetStats(),
		"registry_status":  s.regCli.Status(),
		"ws_clients":       s.wsHub.ConnectedClients(),
		"binding_statuses": s.patcher.Statuses(),
		"channels":         s.engine.Snapshots(),
	}
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	writeJSON(w, http.StatusOK, s.buildStatusPayload())
}

func (s *Server) handleSniffer(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	writeJSON(w, http.StatusOK, s.tslList.RecentSnifferEntries())
}

func (s *Server) handleSSE(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeErr(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	chSnap, unsubUI := s.engine.SubscribeUI()
	defer unsubUI()

	chSniff, unsubSniff := s.tslList.SubscribeSniffer()
	defer unsubSniff()

	sendEvent := func(evType string, data any) error {
		b, err := json.Marshal(data)
		if err != nil {
			return err
		}
		if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", evType, b); err != nil {
			return err
		}
		flusher.Flush()
		return nil
	}

	_ = sendEvent("status", s.buildStatusPayload())

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case snap, ok := <-chSnap:
			if !ok {
				return
			}
			if sendEvent("channels", snap) != nil {
				return
			}
		case pkt, ok := <-chSniff:
			if !ok {
				return
			}
			if sendEvent("sniffer", pkt) != nil {
				return
			}
		case <-ticker.C:
			if sendEvent("status", s.buildStatusPayload()) != nil {
				return
			}
		}
	}
}

func (s *Server) handleOverride(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req struct {
		ChannelID int                `json:"channel_id"` // 0 = apply to all channels
		Mode      tally.OverrideMode `json:"mode"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}

	if req.ChannelID == 0 {
		for _, ch := range s.engine.Snapshots() {
			s.engine.SetOverride(ch.ID, req.Mode)
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
		return
	}

	if !s.engine.SetOverride(req.ChannelID, req.Mode) {
		writeErr(w, http.StatusBadRequest, "invalid channel_id or override mode")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleSimulateTSL(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req struct {
		Screen    uint16        `json:"screen"`
		Index     uint16        `json:"index"`
		RHTally   tsl.LampColor `json:"rh_tally"`
		TextTally tsl.LampColor `json:"text_tally"`
		LHTally   tsl.LampColor `json:"lh_tally"`
		Text      string        `json:"text"`
		UTF16     bool          `json:"utf16"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}

	raw := tsl.MarshalPacket(req.Screen, req.UTF16, []tsl.DMSG{
		{
			Index:      req.Index,
			RHTally:    req.RHTally,
			TextTally:  req.TextTally,
			LHTally:    req.LHTally,
			Brightness: 3,
			Text:       req.Text,
		},
	}, false)

	if err := s.tslList.InjectRawDatagram(raw, "sim://webui"); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "bytes_sent": len(raw)})
}

func (s *Server) handleConfig(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, s.cfgMgr.Get())

	case http.MethodPut:
		var nextCfg config.Config
		if err := json.NewDecoder(io.LimitReader(r.Body, 2<<20)).Decode(&nextCfg); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid config JSON: "+err.Error())
			return
		}
		if err := s.cfgMgr.Save(nextCfg); err != nil {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
		saved := s.cfgMgr.Get()
		if s.onReload != nil {
			s.onReload(saved)
		}
		writeJSON(w, http.StatusOK, saved)

	default:
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *Server) handleInspectTarget(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req struct {
		BaseURL string `json:"base_url"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	info, err := s.patcher.InspectDevice(ctx, req.BaseURL)
	if err != nil {
		writeErr(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, info)
}

func (s *Server) handleResyncTargets(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	s.patcher.TriggerSync()
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleDisconnectReceiver(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req struct {
		BaseURL    string `json:"base_url"`
		ReceiverID string `json:"receiver_id"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := s.patcher.DisconnectReceiver(ctx, req.BaseURL, req.ReceiverID); err != nil {
		writeErr(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleMDNSNodes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if s.mdnsSvc == nil {
		writeJSON(w, http.StatusOK, []any{})
		return
	}
	if r.URL.Query().Get("scan") == "1" {
		s.mdnsSvc.TriggerScan()
		time.Sleep(450 * time.Millisecond)
	}
	writeJSON(w, http.StatusOK, s.mdnsSvc.DiscoveredNodes())
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]any{"error": msg})
}
