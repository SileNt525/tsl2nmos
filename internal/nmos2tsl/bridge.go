package nmos2tsl

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/gorilla/websocket"

	"tsl2nmos/internal/config"
	"tsl2nmos/internal/nmos"
	"tsl2nmos/internal/tsl"
)

// OverrideMode allows manual testing of NMOS -> TSL channels from the Web UI without shorting physical GPI pins.
type OverrideMode string

const (
	OverrideAuto     OverrideMode = "auto"
	OverrideForceOn  OverrideMode = "force_on"
	OverrideForceOff OverrideMode = "force_off"
)

// RemoteSender describes an IS-07 Boolean WebSocket Sender discovered on a remote NMOS device
// (such as Riedel StageLink NSA-010C GPInput 1..16).
type RemoteSender struct {
	ID            string `json:"id"`
	Label         string `json:"label"`
	Description   string `json:"description"`
	DeviceID      string `json:"device_id"`
	FlowID        string `json:"flow_id"`
	SourceID      string `json:"source_id"`
	Transport     string `json:"transport"`
	MasterEnable  bool   `json:"master_enable"`
	ConnectionURI string `json:"connection_uri"`
	RestAPIURL    string `json:"rest_api_url"`
}

// SourceDeviceInspection holds the result of probing a remote NMOS IS-07-GPIO device for GPI Senders.
type SourceDeviceInspection struct {
	BaseURL     string         `json:"base_url"`
	NodeID      string         `json:"node_id"`
	NodeLabel   string         `json:"node_label"`
	NodeVersion string         `json:"node_version"`
	APIVersion  string         `json:"api_version"`
	Senders     []RemoteSender `json:"senders"`
	InspectedAt time.Time      `json:"inspected_at"`
}

// GPIBindingSnapshot represents the real-time state of one GPI -> TSL mapping rule for Web UI display.
type GPIBindingSnapshot struct {
	BindingID      string       `json:"binding_id"`
	SourceDevID    string       `json:"source_dev_id"`
	SourceDevLabel string       `json:"source_dev_label"`
	SenderID       string       `json:"sender_id"`
	SenderLabel    string       `json:"sender_label"`
	SourceID       string       `json:"source_id"`
	ReceiverID     string       `json:"receiver_id"`
	Screen         uint16       `json:"screen"`
	Index          uint16       `json:"index"`
	Role           string       `json:"role"`
	UMDText        string       `json:"umd_text"`
	Invert         bool         `json:"invert"`
	Enabled        bool         `json:"enabled"`
	Subscribed     bool         `json:"subscribed"`
	ConnectionURI  string       `json:"connection_uri,omitempty"`
	HasSignal      bool         `json:"has_signal"`
	RawValue       bool         `json:"raw_value"`
	Override       OverrideMode `json:"override"`
	EffectiveValue bool         `json:"effective_value"`
	LastUpdatedAt  *time.Time   `json:"last_updated_at,omitempty"`
	LastError      string       `json:"last_error,omitempty"`
}

// OutboundDisplaySnapshot represents an aggregated outbound TSL (Screen, Index) display generated from GPIs.
type OutboundDisplaySnapshot struct {
	Screen     uint16 `json:"screen"`
	Index      uint16 `json:"index"`
	RedActive  bool   `json:"red_active"`
	GreenActive bool  `json:"green_active"`
	LHTally    string `json:"lh_tally"`
	TextTally  string `json:"text_tally"`
	RHTally    string `json:"rh_tally"`
	UMDText    string `json:"umd_text"`
}

// WSSubSessionInfo reports telemetry for an active outbound IS-07 WebSocket subscription session.
type WSSubSessionInfo struct {
	ConnectionURI     string    `json:"connection_uri"`
	Connected         bool      `json:"connected"`
	ConnectedAt       *time.Time `json:"connected_at,omitempty"`
	LastHealthAt      *time.Time `json:"last_health_at,omitempty"`
	SubscribedSources int       `json:"subscribed_sources"`
	LastError         string    `json:"last_error,omitempty"`
}

// StatusSummary aggregates all NMOS -> TSL telemetry for /api/v1/status and SSE updates.
type StatusSummary struct {
	TxStats  tsl.TxStats               `json:"tx_stats"`
	Sessions []WSSubSessionInfo        `json:"sessions"`
	Bindings []GPIBindingSnapshot      `json:"bindings"`
	Displays []OutboundDisplaySnapshot `json:"displays"`
}

type sourceRuntimeVal struct {
	hasSignal bool
	rawVal    bool
	updatedAt time.Time
}

type resolvedSubTarget struct {
	sourceID string
	wsURI    string
	restURL  string
	lastErr  string
}

type dispKey struct {
	screen uint16
	index  uint16
}

// Bridge manages the complete NMOS IS-07 Boolean GPI -> TSL UMD v5.0 / v3.1 pipeline.
type Bridge struct {
	cfgMgr *config.Manager
	store  *nmos.Store
	tx     *tsl.Transmitter
	client *http.Client
	log    *slog.Logger

	mu          sync.RWMutex
	parentCtx   context.Context
	sourceVals  map[string]sourceRuntimeVal  // key: IS-07 source_id
	overrides   map[string]OverrideMode      // key: binding_id
	resolved    map[string]resolvedSubTarget // key: binding_id
	lastSentDMSG map[dispKey]tsl.DMSG
	workers     map[string]*wsSubWorker      // key: wsURI
	uiSubs      map[chan StatusSummary]struct{}
	syncNow     chan struct{}
}

// NewBridge constructs the NMOS -> TSL Bridge.
func NewBridge(cfgMgr *config.Manager, store *nmos.Store, tx *tsl.Transmitter, logger *slog.Logger) *Bridge {
	if logger == nil {
		logger = slog.Default()
	}
	return &Bridge{
		cfgMgr: cfgMgr,
		store:  store,
		tx:     tx,
		client: &http.Client{
			Timeout: 4 * time.Second,
		},
		log:          logger,
		sourceVals:   make(map[string]sourceRuntimeVal),
		overrides:    make(map[string]OverrideMode),
		resolved:     make(map[string]resolvedSubTarget),
		lastSentDMSG: make(map[dispKey]tsl.DMSG),
		workers:      make(map[string]*wsSubWorker),
		uiSubs:       make(map[chan StatusSummary]struct{}),
		syncNow:      make(chan struct{}, 1),
	}
}

// Start launches the background IS-07 GPI discovery, WebSocket subscription, and TSL transmitter loops.
func (b *Bridge) Start(ctx context.Context) error {
	b.mu.Lock()
	b.parentCtx = ctx
	b.mu.Unlock()

	if err := b.tx.Start(ctx); err != nil {
		b.log.Warn("Initial TSL output start warning", "err", err)
	}

	b.recomputeAndPushTSL(true)

	go func() {
		b.reconcileAll(ctx)

		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				b.stopAllWorkers()
				return
			case <-b.syncNow:
				b.reconcileAll(ctx)
			case <-ticker.C:
				b.reconcileAll(ctx)
			}
		}
	}()
	return nil
}

// Reload hot-applies updated configuration to the TSL Transmitter and triggers immediate IS-07 re-reconciliation.
func (b *Bridge) Reload(newCfg config.Config) {
	if err := b.tx.ApplyConfig(newCfg.NMOSToTSL.Output); err != nil {
		b.log.Warn("Hot-reload TSL output transport warning", "err", err)
	}
	b.recomputeAndPushTSL(true)
	b.TriggerSync()
}

// TriggerSync schedules an immediate IS-07 sender discovery & WebSocket subscription pass.
func (b *Bridge) TriggerSync() {
	select {
	case b.syncNow <- struct{}{}:
	default:
	}
}

// SetOverride sets a manual test override ("auto", "force_on", "force_off") on a single binding (or all if bindingID == "*").
func (b *Bridge) SetOverride(bindingID string, mode OverrideMode) bool {
	switch mode {
	case OverrideAuto, OverrideForceOn, OverrideForceOff:
	default:
		return false
	}

	cfg := b.cfgMgr.Get()
	b.mu.Lock()
	if bindingID == "" || bindingID == "*" {
		for _, ds := range cfg.NMOSToTSL.DirectSources {
			for _, bind := range ds.Bindings {
				if mode == OverrideAuto {
					delete(b.overrides, bind.ID)
				} else {
					b.overrides[bind.ID] = mode
				}
			}
		}
	} else {
		found := false
		for _, ds := range cfg.NMOSToTSL.DirectSources {
			for _, bind := range ds.Bindings {
				if bind.ID == bindingID {
					found = true
					break
				}
			}
		}
		if !found {
			b.mu.Unlock()
			return false
		}
		if mode == OverrideAuto {
			delete(b.overrides, bindingID)
		} else {
			b.overrides[bindingID] = mode
		}
	}
	b.mu.Unlock()

	b.recomputeAndPushTSL(false)
	b.notifyUISubs()
	return true
}

// SubscribeUI returns a channel receiving real-time StatusSummary snapshots whenever GPIs or sessions change.
func (b *Bridge) SubscribeUI() (chan StatusSummary, func()) {
	ch := make(chan StatusSummary, 16)
	b.mu.Lock()
	b.uiSubs[ch] = struct{}{}
	b.mu.Unlock()

	unsub := func() {
		b.mu.Lock()
		if _, ok := b.uiSubs[ch]; ok {
			delete(b.uiSubs, ch)
			close(ch)
		}
		b.mu.Unlock()
	}
	return ch, unsub
}

func (b *Bridge) notifyUISubs() {
	snap := b.Status()
	b.mu.RLock()
	defer b.mu.RUnlock()
	for ch := range b.uiSubs {
		select {
		case ch <- snap:
		default:
		}
	}
}

// ComputeOutboundLamps maps logical Red (PGM) and Green (PVW) booleans to TSL 5.0 LH, Text, and RH lamps.
func ComputeOutboundLamps(redOn, greenOn bool, lampMapping string, textFollows bool) (lh, txt, rh tsl.LampColor) {
	switch lampMapping {
	case "rh_red_lh_green":
		if redOn {
			rh = tsl.LampRed
		}
		if greenOn {
			lh = tsl.LampGreen
		}
	case "both_color":
		switch {
		case redOn && greenOn:
			lh = tsl.LampRed
			rh = tsl.LampGreen
		case redOn:
			lh = tsl.LampRed
			rh = tsl.LampRed
		case greenOn:
			lh = tsl.LampGreen
			rh = tsl.LampGreen
		}
	default: // "lh_red_rh_green" (SimplyLive ViBox default: Left=Red, Right=Green)
		if redOn {
			lh = tsl.LampRed
		}
		if greenOn {
			rh = tsl.LampGreen
		}
	}

	if textFollows {
		switch {
		case redOn && greenOn:
			txt = tsl.LampAmber
		case redOn:
			txt = tsl.LampRed
		case greenOn:
			txt = tsl.LampGreen
		}
	}
	return lh, txt, rh
}

func (b *Bridge) recomputeAndPushTSL(fullReplace bool) {
	cfg := b.cfgMgr.Get()
	outCfg := cfg.NMOSToTSL.Output

	type aggDisp struct {
		redOn   bool
		greenOn bool
		umdText string
	}

	b.mu.Lock()
	aggMap := make(map[dispKey]*aggDisp)
	var keys []dispKey

	for _, ds := range cfg.NMOSToTSL.DirectSources {
		if !ds.Enabled {
			continue
		}
		for _, bind := range ds.Bindings {
			if !bind.Enabled {
				continue
			}
			k := dispKey{screen: bind.Screen, index: bind.Index}
			ad, exists := aggMap[k]
			if !exists {
				ad = &aggDisp{}
				aggMap[k] = ad
				keys = append(keys, k)
			}
			if ad.umdText == "" && bind.UMDText != "" {
				ad.umdText = bind.UMDText
			}

			eff := b.effectiveBindingValueLocked(bind)
			if eff {
				switch bind.Role {
				case "green":
					ad.greenOn = true
				case "amber":
					ad.redOn = true
					ad.greenOn = true
				default: // "red"
					ad.redOn = true
				}
			}
		}
	}

	sort.Slice(keys, func(i, j int) bool {
		if keys[i].screen != keys[j].screen {
			return keys[i].screen < keys[j].screen
		}
		return keys[i].index < keys[j].index
	})

	allByScreen := make(map[uint16][]tsl.DMSG)
	type deltaItem struct {
		screen uint16
		msg    tsl.DMSG
	}
	var deltas []deltaItem

	nextSent := make(map[dispKey]tsl.DMSG, len(keys))
	for _, k := range keys {
		ad := aggMap[k]
		lh, txt, rh := ComputeOutboundLamps(ad.redOn, ad.greenOn, outCfg.LampMapping, outCfg.TextFollowsTally)
		dmsg := tsl.DMSG{
			Index:      k.index,
			RHTally:    rh,
			TextTally:  txt,
			LHTally:    lh,
			Brightness: outCfg.Brightness,
			Text:       ad.umdText,
			HasText:    len(ad.umdText) > 0,
		}
		nextSent[k] = dmsg
		allByScreen[k.screen] = append(allByScreen[k.screen], dmsg)

		if prev, ok := b.lastSentDMSG[k]; !ok || prev != dmsg {
			deltas = append(deltas, deltaItem{screen: k.screen, msg: dmsg})
		}
	}
	b.lastSentDMSG = nextSent
	b.mu.Unlock()

	if fullReplace {
		b.tx.ReplaceDisplays(allByScreen)
		return
	}
	for _, d := range deltas {
		b.tx.SendDisplay(d.screen, d.msg)
	}
}

func (b *Bridge) effectiveBindingValueLocked(bind config.GPIBindingConfig) bool {
	ov := b.overrides[bind.ID]
	switch ov {
	case OverrideForceOn:
		return true
	case OverrideForceOff:
		return false
	}

	srcID := bind.SourceID
	if res, ok := b.resolved[bind.ID]; ok && res.sourceID != "" {
		srcID = res.sourceID
	}
	if srcID == "" {
		return false
	}
	rv, ok := b.sourceVals[srcID]
	if !ok || !rv.hasSignal {
		return false
	}
	if bind.Invert {
		return !rv.rawVal
	}
	return rv.rawVal
}

func (b *Bridge) handleSourceStateUpdate(sourceID string, val bool, rxTime time.Time) {
	b.mu.Lock()
	prev, had := b.sourceVals[sourceID]
	b.sourceVals[sourceID] = sourceRuntimeVal{
		hasSignal: true,
		rawVal:    val,
		updatedAt: rxTime,
	}
	changed := !had || !prev.hasSignal || prev.rawVal != val
	b.mu.Unlock()

	if changed {
		b.recomputeAndPushTSL(false)
		b.notifyUISubs()
	}
}

// Status builds a complete live snapshot of NMOS -> TSL state for the Web UI.
func (b *Bridge) Status() StatusSummary {
	cfg := b.cfgMgr.Get()
	outCfg := cfg.NMOSToTSL.Output
	nodeSeed := b.store.NodeSeed()

	b.mu.RLock()
	sessions := make([]WSSubSessionInfo, 0, len(b.workers))
	connectedByURI := make(map[string]bool, len(b.workers))
	for uri, w := range b.workers {
		info := w.snapshot()
		sessions = append(sessions, info)
		connectedByURI[uri] = info.Connected
	}
	sort.Slice(sessions, func(i, j int) bool { return sessions[i].ConnectionURI < sessions[j].ConnectionURI })

	bindings := make([]GPIBindingSnapshot, 0)
	type dispAgg struct {
		redOn   bool
		greenOn bool
		umdText string
	}
	dispMap := make(map[dispKey]*dispAgg)
	var dispKeys []dispKey

	for _, ds := range cfg.NMOSToTSL.DirectSources {
		for _, bind := range ds.Bindings {
			res := b.resolved[bind.ID]
			srcID := bind.SourceID
			if res.sourceID != "" {
				srcID = res.sourceID
			}
			rv := b.sourceVals[srcID]
			ov := b.overrides[bind.ID]
			if ov == "" {
				ov = OverrideAuto
			}
			eff := b.effectiveBindingValueLocked(bind)
			sub := ds.Enabled && bind.Enabled && res.wsURI != "" && connectedByURI[res.wsURI]

			snap := GPIBindingSnapshot{
				BindingID:      bind.ID,
				SourceDevID:    ds.ID,
				SourceDevLabel: ds.Label,
				SenderID:       bind.SenderID,
				SenderLabel:    bind.SenderLabel,
				SourceID:       srcID,
				ReceiverID:     nmos.GPIReceiverID(nodeSeed, bind.ID),
				Screen:         bind.Screen,
				Index:          bind.Index,
				Role:           bind.Role,
				UMDText:        bind.UMDText,
				Invert:         bind.Invert,
				Enabled:        ds.Enabled && bind.Enabled,
				Subscribed:     sub,
				ConnectionURI:  res.wsURI,
				HasSignal:      rv.hasSignal,
				RawValue:       rv.rawVal,
				Override:       ov,
				EffectiveValue: eff,
				LastError:      res.lastErr,
			}
			if rv.hasSignal && !rv.updatedAt.IsZero() {
				t := rv.updatedAt
				snap.LastUpdatedAt = &t
			}
			bindings = append(bindings, snap)

			if ds.Enabled && bind.Enabled {
				k := dispKey{screen: bind.Screen, index: bind.Index}
				da, ok := dispMap[k]
				if !ok {
					da = &dispAgg{}
					dispMap[k] = da
					dispKeys = append(dispKeys, k)
				}
				if da.umdText == "" && bind.UMDText != "" {
					da.umdText = bind.UMDText
				}
				if eff {
					switch bind.Role {
					case "green":
						da.greenOn = true
					case "amber":
						da.redOn = true
						da.greenOn = true
					default:
						da.redOn = true
					}
				}
			}
		}
	}
	b.mu.RUnlock()

	sort.Slice(dispKeys, func(i, j int) bool {
		if dispKeys[i].screen != dispKeys[j].screen {
			return dispKeys[i].screen < dispKeys[j].screen
		}
		return dispKeys[i].index < dispKeys[j].index
	})

	displays := make([]OutboundDisplaySnapshot, 0, len(dispKeys))
	for _, k := range dispKeys {
		da := dispMap[k]
		lh, txt, rh := ComputeOutboundLamps(da.redOn, da.greenOn, outCfg.LampMapping, outCfg.TextFollowsTally)
		displays = append(displays, OutboundDisplaySnapshot{
			Screen:      k.screen,
			Index:       k.index,
			RedActive:   da.redOn,
			GreenActive: da.greenOn,
			LHTally:     lh.String(),
			TextTally:   txt.String(),
			RHTally:     rh.String(),
			UMDText:     da.umdText,
		})
	}

	return StatusSummary{
		TxStats:  b.tx.GetStats(),
		Sessions: sessions,
		Bindings: bindings,
		Displays: displays,
	}
}

// InspectSourceDevice probes a remote NMOS Node (such as Riedel StageLink NSA-010C) and returns
// its sorted IS-07 Boolean WebSocket GPI Senders along with their active WebSocket URIs.
func (b *Bridge) InspectSourceDevice(ctx context.Context, rawBaseURL string) (*SourceDeviceInspection, error) {
	candidates, targetHost, err := candidateBaseURLs(rawBaseURL)
	if err != nil {
		return nil, err
	}

	var baseURL, apiVer string
	var lastErr error
	for _, cand := range candidates {
		ver, pErr := b.detectNodeAPIVersion(ctx, cand)
		if pErr == nil {
			baseURL = cand
			apiVer = ver
			break
		}
		lastErr = pErr
	}
	if baseURL == "" {
		return nil, fmt.Errorf("probe IS-04 Node API at %v: %w", candidates, lastErr)
	}

	nodePrefix := fmt.Sprintf("%s/x-nmos/node/%s", baseURL, apiVer)

	var self struct {
		ID      string `json:"id"`
		Label   string `json:"label"`
		Version string `json:"version"`
	}
	if err := b.getJSON(ctx, nodePrefix+"/self", &self); err != nil {
		return nil, fmt.Errorf("read /self: %w", err)
	}

	var devices []struct {
		ID       string `json:"id"`
		Controls []struct {
			Type string `json:"type"`
			Href string `json:"href"`
		} `json:"controls"`
	}
	if err := b.getJSON(ctx, nodePrefix+"/devices", &devices); err != nil {
		return nil, fmt.Errorf("read /devices: %w", err)
	}

	srCtrlByDev := make(map[string]string, len(devices))
	for _, d := range devices {
		best := ""
		for _, c := range d.Controls {
			if strings.HasPrefix(c.Type, "urn:x-nmos:control:sr-ctrl/") {
				rw := rewriteReachableHref(c.Href, targetHost)
				if c.Type == nmos.ControlSRv11 || best == "" {
					best = rw
				}
			}
		}
		if best == "" {
			best = baseURL + "/x-nmos/connection/v1.1/"
		}
		if !strings.HasSuffix(best, "/") {
			best += "/"
		}
		srCtrlByDev[d.ID] = best
	}

	var rawSources []struct {
		ID        string `json:"id"`
		Format    string `json:"format"`
		EventType string `json:"event_type"`
	}
	_ = b.getJSON(ctx, nodePrefix+"/sources", &rawSources)
	boolSources := make(map[string]bool, len(rawSources))
	for _, s := range rawSources {
		if s.EventType == "" || isBooleanEventType(s.EventType) {
			boolSources[s.ID] = true
		}
	}

	var rawFlows []struct {
		ID        string `json:"id"`
		SourceID  string `json:"source_id"`
		EventType string `json:"event_type"`
	}
	_ = b.getJSON(ctx, nodePrefix+"/flows", &rawFlows)
	flowToSource := make(map[string]string, len(rawFlows))
	flowBool := make(map[string]bool, len(rawFlows))
	for _, f := range rawFlows {
		flowToSource[f.ID] = f.SourceID
		if f.EventType != "" {
			flowBool[f.ID] = isBooleanEventType(f.EventType)
		} else if len(boolSources) > 0 {
			flowBool[f.ID] = boolSources[f.SourceID]
		} else {
			flowBool[f.ID] = true
		}
	}

	var rawSenders []struct {
		ID          string `json:"id"`
		Label       string `json:"label"`
		Description string `json:"description"`
		DeviceID    string `json:"device_id"`
		FlowID      string `json:"flow_id"`
		Transport   string `json:"transport"`
		Subscription struct {
			Active bool `json:"active"`
		} `json:"subscription"`
	}
	if err := b.getJSON(ctx, nodePrefix+"/senders", &rawSenders); err != nil {
		return nil, fmt.Errorf("read /senders: %w", err)
	}

	candidatesSnd := make([]RemoteSender, 0, len(rawSenders))
	for _, s := range rawSenders {
		if !strings.Contains(strings.ToLower(s.Transport), "websocket") {
			continue
		}
		if len(flowBool) > 0 {
			if ok, exists := flowBool[s.FlowID]; exists && !ok {
				// Skip non-boolean senders such as AnalogGPInput 1/2 (event_type: "number").
				continue
			}
		}
		candidatesSnd = append(candidatesSnd, RemoteSender{
			ID:           s.ID,
			Label:        s.Label,
			Description:  s.Description,
			DeviceID:     s.DeviceID,
			FlowID:       s.FlowID,
			SourceID:     flowToSource[s.FlowID],
			Transport:    s.Transport,
			MasterEnable: s.Subscription.Active,
		})
	}

	sort.Slice(candidatesSnd, func(i, j int) bool {
		return naturalLess(candidatesSnd[i].Label, candidatesSnd[j].Label)
	})

	// Query IS-05 /single/senders/{id}/active concurrently (bounded) to retrieve live connection_uri.
	var wg sync.WaitGroup
	sem := make(chan struct{}, 6)
	for i := range candidatesSnd {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			snd := &candidatesSnd[idx]
			ctrl := srCtrlByDev[snd.DeviceID]
			if ctrl == "" {
				ctrl = baseURL + "/x-nmos/connection/v1.1/"
			}
			activeURL := fmt.Sprintf("%ssingle/senders/%s/active", ctrl, snd.ID)
			var act struct {
				MasterEnable    bool `json:"master_enable"`
				TransportParams []struct {
					ConnectionURI     string `json:"connection_uri"`
					ExtIS07SourceID   string `json:"ext_is_07_source_id"`
					ExtIS07RestAPIURL string `json:"ext_is_07_rest_api_url"`
				} `json:"transport_params"`
			}
			if err := b.getJSON(ctx, activeURL, &act); err == nil {
				snd.MasterEnable = act.MasterEnable
				if len(act.TransportParams) > 0 {
					tp := act.TransportParams[0]
					if tp.ConnectionURI != "" && tp.ConnectionURI != "auto" {
						snd.ConnectionURI = rewriteReachableHref(tp.ConnectionURI, targetHost)
					}
					if tp.ExtIS07SourceID != "" {
						snd.SourceID = tp.ExtIS07SourceID
					}
					if tp.ExtIS07RestAPIURL != "" {
						snd.RestAPIURL = rewriteReachableHref(tp.ExtIS07RestAPIURL, targetHost)
					}
				}
			}
		}(i)
	}
	wg.Wait()

	return &SourceDeviceInspection{
		BaseURL:     baseURL,
		NodeID:      self.ID,
		NodeLabel:   self.Label,
		NodeVersion: self.Version,
		APIVersion:  apiVer,
		Senders:     candidatesSnd,
		InspectedAt: time.Now(),
	}, nil
}

func (b *Bridge) reconcileAll(ctx context.Context) {
	cfg := b.cfgMgr.Get()
	extRcvConns := b.store.SnapshotActiveReceiverConnections()

	nextResolved := make(map[string]resolvedSubTarget)
	uriSources := make(map[string]map[string]string) // wsURI -> (sourceID -> restURL)

	for _, ds := range cfg.NMOSToTSL.DirectSources {
		if !ds.Enabled {
			continue
		}
		hasEnabledBinding := false
		for _, bind := range ds.Bindings {
			if bind.Enabled {
				hasEnabledBinding = true
				break
			}
		}
		if !hasEnabledBinding {
			continue
		}

		info, err := b.InspectSourceDevice(ctx, ds.BaseURL)
		var sndMap map[string]RemoteSender
		if err == nil && info != nil {
			sndMap = make(map[string]RemoteSender, len(info.Senders))
			for _, s := range info.Senders {
				sndMap[s.ID] = s
			}
		}

		for _, bind := range ds.Bindings {
			if !bind.Enabled {
				continue
			}

			var res resolvedSubTarget
			// Check if an external NMOS IS-05 Broadcast Controller explicitly patched this slot's Receiver!
			if ext, ok := extRcvConns[bind.ID]; ok && ext.MasterEnable && len(ext.TransportParams) > 0 {
				tp := ext.TransportParams[0]
				if tp.ConnectionURI != nil && *tp.ConnectionURI != "" && tp.ExtIS07SourceID != nil && *tp.ExtIS07SourceID != "" {
					res.wsURI = *tp.ConnectionURI
					res.sourceID = *tp.ExtIS07SourceID
					if tp.ExtIS07RestAPIURL != nil {
						res.restURL = *tp.ExtIS07RestAPIURL
					}
				}
			}

			// Otherwise resolve via built-in Direct GPI Sender inspection on ds.BaseURL.
			if res.wsURI == "" {
				if err != nil {
					res.lastErr = err.Error()
					res.sourceID = bind.SourceID
				} else if snd, exists := sndMap[bind.SenderID]; !exists {
					res.lastErr = fmt.Sprintf("sender %s not found on device", bind.SenderID)
					res.sourceID = bind.SourceID
				} else {
					res.sourceID = snd.SourceID
					if res.sourceID == "" {
						res.sourceID = bind.SourceID
					}
					res.wsURI = snd.ConnectionURI
					res.restURL = snd.RestAPIURL
					if res.wsURI == "" {
						res.lastErr = "remote sender has no active WebSocket connection_uri"
					}
				}
			}

			nextResolved[bind.ID] = res

			if res.wsURI != "" && res.sourceID != "" {
				if uriSources[res.wsURI] == nil {
					uriSources[res.wsURI] = make(map[string]string)
				}
				uriSources[res.wsURI][res.sourceID] = res.restURL
				b.store.SyncLocalReceiverFromDirect(bind.ID, bind.SenderID, res.sourceID, res.wsURI, res.restURL, true)
			}
		}
	}

	b.mu.Lock()
	b.resolved = nextResolved
	parentCtx := b.parentCtx
	if parentCtx == nil {
		parentCtx = ctx
	}

	// Stop workers for WebSocket URIs no longer needed.
	for uri, w := range b.workers {
		if _, keep := uriSources[uri]; !keep {
			w.stop()
			delete(b.workers, uri)
		}
	}

	// Update existing workers or launch new multiplexed WebSocket subscriber workers.
	for uri, srcMap := range uriSources {
		if existing, ok := b.workers[uri]; ok {
			existing.updateSources(srcMap)
		} else {
			w := newWSSubWorker(parentCtx, uri, srcMap, b)
			b.workers[uri] = w
			go w.run()
		}
	}
	b.mu.Unlock()

	b.recomputeAndPushTSL(false)
	b.notifyUISubs()
}

func (b *Bridge) stopAllWorkers() {
	b.mu.Lock()
	workers := make([]*wsSubWorker, 0, len(b.workers))
	for _, w := range b.workers {
		workers = append(workers, w)
	}
	b.workers = make(map[string]*wsSubWorker)
	b.mu.Unlock()

	for _, w := range workers {
		w.stop()
	}
}

// --- Multiplexed AMWA IS-07 v1.0 WebSocket Subscriber Worker ---

type wsSubWorker struct {
	ctx    context.Context
	cancel context.CancelFunc
	wsURI  string
	bridge *Bridge

	mu          sync.RWMutex
	sources     map[string]string // sourceID -> restURL
	subUpdateCh chan struct{}
	connected   bool
	connectedAt time.Time
	lastHealth  time.Time
	lastErr     string
}

func newWSSubWorker(parent context.Context, wsURI string, srcMap map[string]string, bridge *Bridge) *wsSubWorker {
	ctx, cancel := context.WithCancel(parent)
	cp := make(map[string]string, len(srcMap))
	for k, v := range srcMap {
		cp[k] = v
	}
	return &wsSubWorker{
		ctx:         ctx,
		cancel:      cancel,
		wsURI:       wsURI,
		bridge:      bridge,
		sources:     cp,
		subUpdateCh: make(chan struct{}, 1),
	}
}

func (w *wsSubWorker) stop() {
	w.cancel()
}

func (w *wsSubWorker) updateSources(next map[string]string) {
	w.mu.Lock()
	changed := len(next) != len(w.sources)
	if !changed {
		for k := range next {
			if _, ok := w.sources[k]; !ok {
				changed = true
				break
			}
		}
	}
	cp := make(map[string]string, len(next))
	for k, v := range next {
		cp[k] = v
	}
	w.sources = cp
	w.mu.Unlock()

	if changed {
		select {
		case w.subUpdateCh <- struct{}{}:
		default:
		}
	}
}

func (w *wsSubWorker) sourceList() ([]string, map[string]string) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	ids := make([]string, 0, len(w.sources))
	rest := make(map[string]string, len(w.sources))
	for id, u := range w.sources {
		ids = append(ids, id)
		rest[id] = u
	}
	sort.Strings(ids)
	return ids, rest
}

func (w *wsSubWorker) snapshot() WSSubSessionInfo {
	w.mu.RLock()
	defer w.mu.RUnlock()
	info := WSSubSessionInfo{
		ConnectionURI:     w.wsURI,
		Connected:         w.connected,
		SubscribedSources: len(w.sources),
		LastError:         w.lastErr,
	}
	if !w.connectedAt.IsZero() && w.connected {
		t := w.connectedAt
		info.ConnectedAt = &t
	}
	if !w.lastHealth.IsZero() && w.connected {
		t := w.lastHealth
		info.LastHealthAt = &t
	}
	return info
}

func (w *wsSubWorker) run() {
	dialer := websocket.Dialer{
		HandshakeTimeout: 4 * time.Second,
	}

	for {
		if w.ctx.Err() != nil {
			return
		}

		shouldResyncPort := w.runSingleSession(&dialer)
		if w.ctx.Err() != nil {
			return
		}

		if shouldResyncPort {
			// Remote NMOS device may have rebooted and allocated a new dynamic WebSocket port
			// (e.g., Riedel StageLink NSA-010C). Trigger Bridge IS-05 /active re-inspection!
			w.bridge.TriggerSync()
		}

		select {
		case <-w.ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
}

func (w *wsSubWorker) runSingleSession(dialer *websocket.Dialer) (resyncPort bool) {
	conn, _, err := dialer.DialContext(w.ctx, w.wsURI, nil)
	if err != nil {
		w.mu.Lock()
		w.connected = false
		w.lastErr = err.Error()
		w.mu.Unlock()
		w.bridge.notifyUISubs()
		return true
	}
	defer conn.Close()

	now := time.Now()
	w.mu.Lock()
	w.connected = true
	w.connectedAt = now
	w.lastHealth = now
	w.lastErr = ""
	w.mu.Unlock()

	srcIDs, restMap := w.sourceList()
	w.bridge.log.Info("IS-07 WebSocket GPI subscriber connected", "uri", w.wsURI, "sources", len(srcIDs))
	w.bridge.notifyUISubs()

	var writeMu sync.Mutex
	sendJSON := func(v any) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		_ = conn.SetWriteDeadline(time.Now().Add(4 * time.Second))
		return conn.WriteJSON(v)
	}

	conn.SetPongHandler(func(string) error {
		w.mu.Lock()
		w.lastHealth = time.Now()
		w.mu.Unlock()
		return nil
	})

	if err := sendJSON(nmos.IS07WSClientCommand{
		Command: "subscription",
		Sources: srcIDs,
	}); err != nil {
		return false
	}
	_ = sendJSON(nmos.IS07WSClientCommand{
		Command:   "health",
		Timestamp: nmos.TAITimestampNow(),
	})

	// Optional initial REST /state fallback after 350ms for any source that hasn't emitted late-joiner state.
	go w.fetchInitialRESTStates(restMap)

	sessDone := make(chan struct{})
	defer close(sessDone)

	// Writer & 5s IS-07 health heartbeat + 13s watchdog goroutine.
	go func() {
		hbTicker := time.NewTicker(5 * time.Second)
		defer hbTicker.Stop()
		for {
			select {
			case <-w.ctx.Done():
				_ = conn.Close()
				return
			case <-sessDone:
				return
			case <-w.subUpdateCh:
				ids, rm := w.sourceList()
				_ = sendJSON(nmos.IS07WSClientCommand{
					Command: "subscription",
					Sources: ids,
				})
				go w.fetchInitialRESTStates(rm)
			case <-hbTicker.C:
				w.mu.RLock()
				lastH := w.lastHealth
				w.mu.RUnlock()
				if time.Since(lastH) > 13*time.Second {
					w.bridge.log.Warn("IS-07 GPI sender health timeout, reconnecting", "uri", w.wsURI)
					_ = conn.Close()
					return
				}
				if err := sendJSON(nmos.IS07WSClientCommand{
					Command:   "health",
					Timestamp: nmos.TAITimestampNow(),
				}); err != nil {
					_ = conn.Close()
					return
				}
			}
		}
	}()

	conn.SetReadLimit(65536)
	for {
		_, data, rerr := conn.ReadMessage()
		if rerr != nil {
			w.mu.Lock()
			w.connected = false
			if w.ctx.Err() == nil {
				w.lastErr = rerr.Error()
			}
			w.mu.Unlock()
			w.bridge.notifyUISubs()
			return true
		}

		var env struct {
			MessageType string `json:"message_type"`
			EventType   string `json:"event_type"`
			Identity    struct {
				SourceID string `json:"source_id"`
			} `json:"identity"`
			Payload struct {
				Value any `json:"value"`
			} `json:"payload"`
		}
		if json.Unmarshal(data, &env) != nil {
			continue
		}

		rxNow := time.Now()
		w.mu.Lock()
		w.lastHealth = rxNow
		w.mu.Unlock()

		switch env.MessageType {
		case "health":
			// Health acknowledged; lastHealth already updated above.
		case "state":
			if env.Identity.SourceID != "" {
				if bv, ok := parseFlexibleBool(env.Payload.Value); ok {
					w.bridge.handleSourceStateUpdate(env.Identity.SourceID, bv, rxNow)
				}
			}
		case "reboot", "shutdown":
			w.bridge.log.Info("Remote IS-07 GPI sender signalled lifecycle event, reconnecting",
				"uri", w.wsURI, "event", env.MessageType)
			return true
		}
	}
}

func (w *wsSubWorker) fetchInitialRESTStates(restMap map[string]string) {
	select {
	case <-w.ctx.Done():
		return
	case <-time.After(350 * time.Millisecond):
	}

	for srcID, restURL := range restMap {
		if w.ctx.Err() != nil {
			return
		}
		if restURL == "" {
			continue
		}
		w.bridge.mu.RLock()
		already := w.bridge.sourceVals[srcID].hasSignal
		w.bridge.mu.RUnlock()
		if already {
			continue
		}

		stateURL := strings.TrimRight(restURL, "/") + "/state"
		ctx, cancel := context.WithTimeout(w.ctx, 2*time.Second)
		var st struct {
			MessageType string `json:"message_type"`
			Payload     struct {
				Value any `json:"value"`
			} `json:"payload"`
		}
		err := w.bridge.getJSON(ctx, stateURL, &st)
		cancel()
		if err == nil && st.MessageType == "state" {
			if bv, ok := parseFlexibleBool(st.Payload.Value); ok {
				w.bridge.handleSourceStateUpdate(srcID, bv, time.Now())
			}
		}
	}
}

func parseFlexibleBool(v any) (bool, bool) {
	switch val := v.(type) {
	case bool:
		return val, true
	case float64:
		return val != 0, true
	case string:
		if strings.EqualFold(val, "true") || val == "1" {
			return true, true
		}
		if strings.EqualFold(val, "false") || val == "0" {
			return false, true
		}
	}
	return false, false
}

func isBooleanEventType(ev string) bool {
	tl := strings.ToLower(strings.TrimSpace(ev))
	return tl == "boolean" || strings.HasPrefix(tl, "boolean/")
}

func (b *Bridge) detectNodeAPIVersion(ctx context.Context, baseURL string) (string, error) {
	var rawVers []string
	if err := b.getJSON(ctx, baseURL+"/x-nmos/node/", &rawVers); err != nil {
		return "", err
	}
	best := ""
	for _, v := range rawVers {
		clean := strings.Trim(v, "/")
		if clean > best {
			best = clean
		}
	}
	if best == "" {
		return "v1.3", nil
	}
	return best, nil
}

func (b *Bridge) getJSON(ctx context.Context, u string, dst any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	resp, err := b.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return json.NewDecoder(resp.Body).Decode(dst)
}

func candidateBaseURLs(raw string) ([]string, string, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return nil, "", fmt.Errorf("empty target URL")
	}
	hasScheme := strings.Contains(s, "://")
	if !hasScheme {
		s = "http://" + s
	}
	u, err := url.Parse(s)
	if err != nil || u.Host == "" {
		return nil, "", fmt.Errorf("invalid target URL %q", raw)
	}
	u.Path = ""
	u.RawQuery = ""
	u.Fragment = ""

	if _, port, splitErr := net.SplitHostPort(u.Host); splitErr == nil && port != "" {
		return []string{strings.TrimRight(u.String(), "/")}, u.Hostname(), nil
	}

	host := u.Hostname()
	scheme := u.Scheme
	if scheme == "" {
		scheme = "http"
	}
	return []string{
		fmt.Sprintf("%s://%s:4450", scheme, host),
		fmt.Sprintf("%s://%s", scheme, host),
		fmt.Sprintf("%s://%s:8080", scheme, host),
	}, host, nil
}

func rewriteReachableHref(rawHref, targetHost string) string {
	if rawHref == "" || targetHost == "" {
		return rawHref
	}
	u, err := url.Parse(rawHref)
	if err != nil || u.Host == "" {
		return rawHref
	}
	h := u.Hostname()
	if h == "127.0.0.1" || strings.EqualFold(h, "localhost") || net.ParseIP(h) == nil {
		if port := u.Port(); port != "" {
			u.Host = net.JoinHostPort(targetHost, port)
		} else {
			u.Host = targetHost
		}
		return u.String()
	}
	return rawHref
}

func naturalLess(a, b string) bool {
	ra, rb := []rune(a), []rune(b)
	ia, ib := 0, 0
	for ia < len(ra) && ib < len(rb) {
		if unicode.IsDigit(ra[ia]) && unicode.IsDigit(rb[ib]) {
			ja := ia
			for ja < len(ra) && unicode.IsDigit(ra[ja]) {
				ja++
			}
			jb := ib
			for jb < len(rb) && unicode.IsDigit(rb[jb]) {
				jb++
			}
			na, _ := strconv.Atoi(string(ra[ia:ja]))
			nb, _ := strconv.Atoi(string(rb[ib:jb]))
			if na != nb {
				return na < nb
			}
			ia, ib = ja, jb
			continue
		}
		ca, cb := unicode.ToLower(ra[ia]), unicode.ToLower(rb[ib])
		if ca != cb {
			return ca < cb
		}
		ia++
		ib++
	}
	return len(ra) < len(rb)
}
