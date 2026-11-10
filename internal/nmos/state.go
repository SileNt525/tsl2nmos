package nmos

import (
	"fmt"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"tsl2nmos/internal/config"
	"tsl2nmos/internal/tally"
)

// TallyFlowEntry links a logical camera channel + color to its NMOS UUIDs and IS-05/IS-07 state.
type TallyFlowEntry struct {
	ChannelID   int
	ChannelName string
	Color       tally.Color
	Source      SourceResource
	Flow        FlowResource
	Sender      SenderResource

	Staged SenderConnectionState
	Active SenderConnectionState

	CurrentValue bool
	LastTiming   string
}

// ReceiverEntry links an NMOS -> TSL GPI binding slot to its IS-04 Receiver and IS-05 connection state.
type ReceiverEntry struct {
	BindingID          string
	Receiver           ReceiverResource
	Staged             ReceiverConnectionState
	Active             ReceiverConnectionState
	ExternalControlled bool
}

// Store holds the authoritative NMOS IS-04, IS-05, and IS-07 resource state.
type Store struct {
	mu sync.RWMutex

	nodeSeed string
	hostIP   string
	httpPort int
	ifName   string
	ifMAC    string

	node   NodeResource
	device DeviceResource

	// Keyed by SourceID and SenderID for O(1) lookup in REST & WebSocket handlers.
	bySourceID       map[string]*TallyFlowEntry
	bySenderID       map[string]*TallyFlowEntry
	byFlowID         map[string]*TallyFlowEntry
	ordered          []*TallyFlowEntry
	byReceiverID     map[string]*ReceiverEntry
	orderedReceivers []*ReceiverEntry

	// Monotonic 0..255 version counters for IS-04 mDNS TXT records (ver_slf, ver_src, etc.).
	verSlf uint8
	verDvc uint8
	verSrc uint8
	verFlw uint8
	verSnd uint8
	verRcv uint8

	onResourceChange func()
	onReceiverChange func()
}

// GPIReceiverID deterministically computes the IS-04 Receiver UUID for an NMOS -> TSL binding slot.
func GPIReceiverID(nodeSeed, bindingID string) string {
	return DeterministicUUIDv5(nodeSeed, "receiver:nmos2tsl:"+bindingID)
}

// ChannelSourceID deterministically computes the Source UUID for (nodeSeed, channelID, color).
func ChannelSourceID(nodeSeed string, channelID int, color tally.Color) string {
	return DeterministicUUIDv5(nodeSeed, fmt.Sprintf("source:ch%d:%s", channelID, color))
}

// ChannelFlowID deterministically computes the Flow UUID for (nodeSeed, channelID, color).
func ChannelFlowID(nodeSeed string, channelID int, color tally.Color) string {
	return DeterministicUUIDv5(nodeSeed, fmt.Sprintf("flow:ch%d:%s", channelID, color))
}

// ChannelSenderID deterministically computes the Sender UUID for (nodeSeed, channelID, color).
func ChannelSenderID(nodeSeed string, channelID int, color tally.Color) string {
	return DeterministicUUIDv5(nodeSeed, fmt.Sprintf("sender:ch%d:%s", channelID, color))
}

// NewStore builds the initial NMOS Store from Config.
func NewStore(cfg config.Config) *Store {
	s := &Store{
		bySourceID:   make(map[string]*TallyFlowEntry),
		bySenderID:   make(map[string]*TallyFlowEntry),
		byFlowID:     make(map[string]*TallyFlowEntry),
		byReceiverID: make(map[string]*ReceiverEntry),
	}
	s.RebuildFromConfig(cfg, nil)
	return s
}

// SetOnResourceChange registers a hook triggered when IS-04 resources or Sender subscriptions change
// (used to trigger immediate Registry sync and mDNS TXT ver_* increment).
func (s *Store) SetOnResourceChange(fn func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onResourceChange = fn
}

// SetOnReceiverChange registers a hook triggered when an external IS-05 Controller patches a local Receiver.
func (s *Store) SetOnReceiverChange(fn func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onReceiverChange = fn
}

// RebuildFromConfig updates Node, Device, Sources, Flows, and Senders while preserving active
// IS-05 receiver bindings and current boolean values for unchanged channels.
func (s *Store) RebuildFromConfig(cfg config.Config, initialValueFn func(chID int, c tally.Color) bool) {
	nowVer := TAITimestampNow()
	hostIP := config.ResolveHostIP(cfg.Node.HostIP, cfg.Node.InterfaceName, "")
	ifName, ifMAC := detectInterfaceInfo(cfg.Node.InterfaceName, hostIP)
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "TSL2NMOS"
	}

	s.mu.Lock()
	prevSenders := s.bySenderID
	prevReceivers := s.byReceiverID

	s.nodeSeed = cfg.Node.IDSeed
	s.hostIP = hostIP
	s.httpPort = cfg.Node.HTTPPort
	s.ifName = ifName
	s.ifMAC = ifMAC

	nodeID := DeterministicUUIDv5(s.nodeSeed, "node")
	deviceID := DeterministicUUIDv5(s.nodeSeed, "device")
	baseHTTP := fmt.Sprintf("http://%s:%d", hostIP, cfg.Node.HTTPPort)
	wsURI := fmt.Sprintf("ws://%s:%d/x-nmos/events/v1.0/ws", hostIP, cfg.Node.HTTPPort)

	chassis := ifMAC
	s.node = NodeResource{
		ID:          nodeID,
		Version:     nowVer,
		Label:       cfg.Node.Label,
		Description: cfg.Node.Description,
		Tags:        map[string][]string{},
		Href:        baseHTTP + "/",
		Hostname:    hostname,
		API: NodeAPIInfo{
			Versions: []string{"v1.2", "v1.3"},
			Endpoints: []APIEndpoint{
				{
					Host:          hostIP,
					Port:          cfg.Node.HTTPPort,
					Protocol:      "http",
					Authorization: false,
				},
			},
		},
		Caps:     map[string]any{},
		Services: []any{},
		Clocks: []NodeClock{
			{Name: "clk0", RefType: "internal"},
		},
		Interfaces: []NodeInterface{
			{
				Name:      ifName,
				ChassisID: &chassis,
				PortID:    ifMAC,
			},
		},
	}

	newBySource := make(map[string]*TallyFlowEntry, len(cfg.Channels)*2)
	newBySender := make(map[string]*TallyFlowEntry, len(cfg.Channels)*2)
	newByFlow := make(map[string]*TallyFlowEntry, len(cfg.Channels)*2)
	ordered := make([]*TallyFlowEntry, 0, len(cfg.Channels)*2)
	senderIDs := make([]string, 0, len(cfg.Channels)*2)

	sortedCh := append([]config.ChannelConfig(nil), cfg.Channels...)
	sort.Slice(sortedCh, func(i, j int) bool { return sortedCh[i].ID < sortedCh[j].ID })

	clkName := "clk0"
	colors := []struct {
		color tally.Color
		title string
	}{
		{tally.ColorRed, "Red Tally (PGM)"},
		{tally.ColorGreen, "Green Tally (PVW)"},
	}

	for _, ch := range sortedCh {
		for _, cd := range colors {
			srcID := ChannelSourceID(s.nodeSeed, ch.ID, cd.color)
			flwID := ChannelFlowID(s.nodeSeed, ch.ID, cd.color)
			sndID := ChannelSenderID(s.nodeSeed, ch.ID, cd.color)
			restURL := fmt.Sprintf("%s/x-nmos/events/v1.0/sources/%s/", baseHTTP, srcID)

			label := fmt.Sprintf("%s - %s", ch.Name, cd.title)
			desc := fmt.Sprintf("TSL 5.0 Screen %d Index %d %s", ch.Screen, ch.Index, cd.title)
			tags := map[string][]string{
				"channel_id":  {fmt.Sprintf("%d", ch.ID)},
				"tsl_index":   {fmt.Sprintf("%d", ch.Index)},
				"tally_color": {string(cd.color)},
			}

			src := SourceResource{
				ID:          srcID,
				Version:     nowVer,
				Label:       label,
				Description: desc,
				Tags:        tags,
				Format:      FormatData,
				Caps:        map[string]any{},
				DeviceID:    deviceID,
				Parents:     []string{},
				ClockName:   &clkName,
				EventType:   EventTypeBoolean,
			}
			flw := FlowResource{
				ID:          flwID,
				Version:     nowVer,
				Label:       label,
				Description: desc,
				Tags:        tags,
				Format:      FormatData,
				MediaType:   MediaTypeJSON,
				EventType:   EventTypeBoolean,
				SourceID:    srcID,
				DeviceID:    deviceID,
				Parents:     []string{},
				Caps:        map[string]any{},
			}

			defaultTP := []WebSocketTransportParams{
				{
					ConnectionURI:           wsURI,
					ConnectionAuthorization: false,
					ExtIS07SourceID:         srcID,
					ExtIS07RestAPIURL:       restURL,
				},
			}
			defaultConn := SenderConnectionState{
				ReceiverID:      nil,
				MasterEnable:    true,
				Activation:      ActivationParams{Mode: nil, RequestedTime: nil},
				TransportParams: defaultTP,
			}

			curVal := false
			lastTiming := nowVer
			staged := cloneConnState(defaultConn)
			active := cloneConnState(defaultConn)

			if prev, ok := prevSenders[sndID]; ok {
				curVal = prev.CurrentValue
				lastTiming = prev.LastTiming
				staged = prev.Staged
				active = prev.Active
				// Keep advertised URLs in sync if hostIP/port changed.
				if len(staged.TransportParams) > 0 {
					staged.TransportParams[0].ConnectionURI = wsURI
					staged.TransportParams[0].ExtIS07SourceID = srcID
					staged.TransportParams[0].ExtIS07RestAPIURL = restURL
				}
				if len(active.TransportParams) > 0 {
					active.TransportParams[0].ConnectionURI = wsURI
					active.TransportParams[0].ExtIS07SourceID = srcID
					active.TransportParams[0].ExtIS07RestAPIURL = restURL
				}
			} else if initialValueFn != nil {
				curVal = initialValueFn(ch.ID, cd.color)
			}

			snd := SenderResource{
				ID:                sndID,
				Version:           nowVer,
				Label:             label,
				Description:       desc,
				Tags:              tags,
				FlowID:            flwID,
				Transport:         TransportWebsocket,
				DeviceID:          deviceID,
				ManifestHref:      nil,
				InterfaceBindings: []string{ifName},
				Subscription: SenderSubscription{
					ReceiverID: active.ReceiverID,
					Active:     active.MasterEnable,
				},
			}

			entry := &TallyFlowEntry{
				ChannelID:    ch.ID,
				ChannelName:  ch.Name,
				Color:        cd.color,
				Source:       src,
				Flow:         flw,
				Sender:       snd,
				Staged:       staged,
				Active:       active,
				CurrentValue: curVal,
				LastTiming:   lastTiming,
			}

			newBySource[srcID] = entry
			newBySender[sndID] = entry
			newByFlow[flwID] = entry
			ordered = append(ordered, entry)
			senderIDs = append(senderIDs, sndID)
		}
	}

	newByReceiver := make(map[string]*ReceiverEntry)
	orderedReceivers := make([]*ReceiverEntry, 0)
	receiverIDs := make([]string, 0)

	if cfg.NMOSToTSL.ExposeIS04Receivers {
		for _, srcDev := range cfg.NMOSToTSL.DirectSources {
			if !srcDev.Enabled {
				continue
			}
			for _, b := range srcDev.Bindings {
				if !b.Enabled {
					continue
				}
				rcvID := GPIReceiverID(s.nodeSeed, b.ID)
				if _, dup := newByReceiver[rcvID]; dup {
					continue
				}
				roleLabel := strings.ToUpper(b.Role)
				lbl := fmt.Sprintf("NMOS2TSL Scr:%d Idx:%d %s (%s)", b.Screen, b.Index, roleLabel, b.SenderLabel)
				desc := fmt.Sprintf("NMOS IS-07 Boolean Receiver -> TSL Screen %d Index %d %s", b.Screen, b.Index, roleLabel)
				defaultRcvConn := ReceiverConnectionState{
					SenderID:      nil,
					MasterEnable:  false,
					Activation:    ActivationParams{Mode: nil, RequestedTime: nil},
					TransportFile: TransportFileState{Data: nil, Type: nil},
					TransportParams: []ReceiverWebSocketParams{
						{
							ConnectionAuthorization: false,
							ConnectionURI:           nil,
							ExtIS07RestAPIURL:       nil,
							ExtIS07SourceID:         nil,
						},
					},
				}
				stagedRcv := cloneReceiverConnState(defaultRcvConn)
				activeRcv := cloneReceiverConnState(defaultRcvConn)
				extControlled := false
				if prev, ok := prevReceivers[rcvID]; ok {
					stagedRcv = cloneReceiverConnState(prev.Staged)
					activeRcv = cloneReceiverConnState(prev.Active)
					extControlled = prev.ExternalControlled
				}
				rcvRes := ReceiverResource{
					ID:                rcvID,
					Version:           nowVer,
					Label:             lbl,
					Description:       desc,
					Tags:              map[string][]string{"tsl_index": {fmt.Sprintf("%d", b.Index)}, "role": {b.Role}},
					DeviceID:          deviceID,
					Format:            FormatData,
					Transport:         TransportWebsocket,
					InterfaceBindings: []string{ifName},
					Caps: ReceiverCaps{
						MediaTypes: []string{MediaTypeJSON},
						EventTypes: []string{EventTypeBoolean},
					},
					Subscription: ReceiverSubscription{
						SenderID: activeRcv.SenderID,
						Active:   activeRcv.MasterEnable,
					},
				}
				re := &ReceiverEntry{
					BindingID:          b.ID,
					Receiver:           rcvRes,
					Staged:             stagedRcv,
					Active:             activeRcv,
					ExternalControlled: extControlled,
				}
				newByReceiver[rcvID] = re
				orderedReceivers = append(orderedReceivers, re)
				receiverIDs = append(receiverIDs, rcvID)
			}
		}
	}

	s.device = DeviceResource{
		ID:          deviceID,
		Version:     nowVer,
		Label:       cfg.Node.Label + " Device",
		Description: cfg.Node.Description,
		Tags:        map[string][]string{},
		Type:        DeviceTypeGeneric,
		NodeID:      nodeID,
		Senders:     senderIDs,
		Receivers:   receiverIDs,
		Controls: []DeviceControl{
			{
				Type:          ControlSRv11,
				Href:          baseHTTP + "/x-nmos/connection/v1.1/",
				Authorization: false,
			},
			{
				Type:          ControlSRv10,
				Href:          baseHTTP + "/x-nmos/connection/v1.0/",
				Authorization: false,
			},
			{
				Type:          ControlEvents10,
				Href:          baseHTTP + "/x-nmos/events/v1.0/",
				Authorization: false,
			},
		},
	}

	s.bySourceID = newBySource
	s.bySenderID = newBySender
	s.byFlowID = newByFlow
	s.ordered = ordered
	s.byReceiverID = newByReceiver
	s.orderedReceivers = orderedReceivers

	s.verSlf++
	s.verDvc++
	s.verSrc++
	s.verFlw++
	s.verSnd++
	s.verRcv++

	cb := s.onResourceChange
	s.mu.Unlock()

	if cb != nil {
		cb()
	}
}

// UpdateTallyValue records a boolean transition for (channelID, color) and returns the IS-07
// state message plus whether the corresponding IS-05 Sender is currently master_enable=true.
func (s *Store) UpdateTallyValue(channelID int, color tally.Color, val bool, ts time.Time) (IS07StateMessage, bool, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	srcID := ChannelSourceID(s.nodeSeed, channelID, color)
	entry, ok := s.bySourceID[srcID]
	if !ok {
		return IS07StateMessage{}, false, false
	}

	tai := FormatTAITimestamp(ts)
	entry.CurrentValue = val
	entry.LastTiming = tai

	msg := IS07StateMessage{
		Identity: EventIdentity{
			SourceID: entry.Source.ID,
			FlowID:   entry.Flow.ID,
		},
		EventType: EventTypeBoolean,
		Timing: EventTiming{
			CreationTimestamp: tai,
		},
		Payload:     BooleanPayload{Value: val},
		MessageType: "state",
	}
	return msg, entry.Active.MasterEnable, true
}

// GetStateMessageBySourceID builds the current IS-07 state snapshot for REST /state or WebSocket sync.
func (s *Store) GetStateMessageBySourceID(sourceID string) (IS07StateMessage, bool, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entry, ok := s.bySourceID[sourceID]
	if !ok {
		return IS07StateMessage{}, false, false
	}
	msg := IS07StateMessage{
		Identity: EventIdentity{
			SourceID: entry.Source.ID,
			FlowID:   entry.Flow.ID,
		},
		EventType: EventTypeBoolean,
		Timing: EventTiming{
			CreationTimestamp: entry.LastTiming,
		},
		Payload:     BooleanPayload{Value: entry.CurrentValue},
		MessageType: "state",
	}
	return msg, entry.Active.MasterEnable, true
}

// GetAllShutdownMessages generates IS-07 shutdown messages for all sources on graceful exit.
func (s *Store) GetAllShutdownMessages() []IS07LifecycleMessage {
	s.mu.RLock()
	defer s.mu.RUnlock()

	nowTAI := TAITimestampNow()
	out := make([]IS07LifecycleMessage, 0, len(s.ordered))
	for _, e := range s.ordered {
		out = append(out, IS07LifecycleMessage{
			Identity: EventIdentity{
				SourceID: e.Source.ID,
				FlowID:   e.Flow.ID,
			},
			Timing:      EventTiming{CreationTimestamp: nowTAI},
			MessageType: "shutdown",
		})
	}
	return out
}

// SnapshotResources returns copies of Node, Device, Sources, Flows, and Senders for IS-04 APIs & Registry sync.
func (s *Store) SnapshotResources() (NodeResource, DeviceResource, []SourceResource, []FlowResource, []SenderResource) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	sources := make([]SourceResource, 0, len(s.ordered))
	flows := make([]FlowResource, 0, len(s.ordered))
	senders := make([]SenderResource, 0, len(s.ordered))
	for _, e := range s.ordered {
		sources = append(sources, e.Source)
		flows = append(flows, e.Flow)
		senders = append(senders, e.Sender)
	}
	return s.node, s.device, sources, flows, senders
}

// MDNSVersions returns the current 0..255 TXT version counters for Peer-to-Peer IS-04 discovery.
func (s *Store) MDNSVersions() (slf, dvc, src, flw, snd, rcv uint8) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.verSlf, s.verDvc, s.verSrc, s.verFlw, s.verSnd, s.verRcv
}

// HostEndpoint returns the advertised host IP and HTTP port.
func (s *Store) HostEndpoint() (string, int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.hostIP, s.httpPort
}

// ResolvePatchParamsForTarget computes the exact IS-05 Receiver transport_params needed to patch
// an external IS-07 WebSocket Receiver (such as Riedel StageLink NSA-010C GPO 1..16) to (channelID, color).
// It dynamically selects the best local source IP toward targetBaseURL if host_ip is "auto".
func (s *Store) ResolvePatchParamsForTarget(configuredHostIP, ifName, targetHost string, channelID int, color tally.Color) (senderID, sourceID, wsURI, restURL string, ok bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	srcID := ChannelSourceID(s.nodeSeed, channelID, color)
	entry, exists := s.bySourceID[srcID]
	if !exists {
		return "", "", "", "", false
	}

	localIP := config.ResolveHostIP(configuredHostIP, ifName, targetHost)
	if localIP == "" {
		localIP = s.hostIP
	}
	wsURI = fmt.Sprintf("ws://%s:%d/x-nmos/events/v1.0/ws", localIP, s.httpPort)
	restURL = fmt.Sprintf("http://%s:%d/x-nmos/events/v1.0/sources/%s/", localIP, s.httpPort, srcID)
	return entry.Sender.ID, entry.Source.ID, wsURI, restURL, true
}

// ActivateLocalSenderForReceiver ensures the local IS-05 Sender is marked active and tracks receiverID.
func (s *Store) ActivateLocalSenderForReceiver(senderID string, receiverID string) {
	s.mu.Lock()
	entry, ok := s.bySenderID[senderID]
	if !ok {
		s.mu.Unlock()
		return
	}
	changed := false
	if !entry.Active.MasterEnable {
		entry.Active.MasterEnable = true
		entry.Staged.MasterEnable = true
		changed = true
	}
	if receiverID != "" && (entry.Active.ReceiverID == nil || *entry.Active.ReceiverID != receiverID) {
		rid := receiverID
		entry.Active.ReceiverID = &rid
		entry.Staged.ReceiverID = &rid
		entry.Sender.Subscription.ReceiverID = &rid
		changed = true
	}
	entry.Sender.Subscription.Active = entry.Active.MasterEnable
	if changed {
		entry.Sender.Version = TAITimestampNow()
		s.verSnd++
	}
	cb := s.onResourceChange
	s.mu.Unlock()

	if changed && cb != nil {
		cb()
	}
}

// SnapshotReceivers returns copies of all IS-04 Receiver resources exposed for NMOS -> TSL.
func (s *Store) SnapshotReceivers() []ReceiverResource {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]ReceiverResource, 0, len(s.orderedReceivers))
	for _, r := range s.orderedReceivers {
		out = append(out, r.Receiver)
	}
	return out
}

// NodeSeed returns the active deterministic UUIDv5 seed string.
func (s *Store) NodeSeed() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.nodeSeed
}

// SnapshotActiveReceiverConnections returns externally patched IS-05 Receiver states keyed by BindingID.
func (s *Store) SnapshotActiveReceiverConnections() map[string]ReceiverConnectionState {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make(map[string]ReceiverConnectionState, len(s.orderedReceivers))
	for _, r := range s.orderedReceivers {
		if r.ExternalControlled {
			out[r.BindingID] = cloneReceiverConnState(r.Active)
		}
	}
	return out
}

// SyncLocalReceiverFromDirect updates the local IS-04/IS-05 Receiver state to reflect an active
// built-in Direct GPI subscription so NMOS Registries & Explorers see accurate crosspoints.
func (s *Store) SyncLocalReceiverFromDirect(bindingID, senderID, sourceID, wsURI, restURL string, active bool) {
	s.mu.Lock()
	rcvID := GPIReceiverID(s.nodeSeed, bindingID)
	re, ok := s.byReceiverID[rcvID]
	if !ok {
		s.mu.Unlock()
		return
	}
	re.ExternalControlled = false
	changed := false
	if re.Active.MasterEnable != active {
		re.Active.MasterEnable = active
		re.Staged.MasterEnable = active
		changed = true
	}
	curSnd := ""
	if re.Active.SenderID != nil {
		curSnd = *re.Active.SenderID
	}
	if curSnd != senderID {
		if senderID == "" {
			re.Active.SenderID = nil
			re.Staged.SenderID = nil
		} else {
			cp := senderID
			re.Active.SenderID = &cp
			re.Staged.SenderID = &cp
		}
		changed = true
	}
	if len(re.Active.TransportParams) == 0 {
		re.Active.TransportParams = []ReceiverWebSocketParams{{ConnectionAuthorization: false}}
	}
	tp := &re.Active.TransportParams[0]
	if wsURI != "" && (tp.ConnectionURI == nil || *tp.ConnectionURI != wsURI) {
		cp := wsURI
		tp.ConnectionURI = &cp
		changed = true
	}
	if sourceID != "" && (tp.ExtIS07SourceID == nil || *tp.ExtIS07SourceID != sourceID) {
		cp := sourceID
		tp.ExtIS07SourceID = &cp
		changed = true
	}
	if restURL != "" && (tp.ExtIS07RestAPIURL == nil || *tp.ExtIS07RestAPIURL != restURL) {
		cp := restURL
		tp.ExtIS07RestAPIURL = &cp
		changed = true
	}
	re.Staged.TransportParams = cloneReceiverConnState(re.Active).TransportParams
	re.Receiver.Subscription.SenderID = re.Active.SenderID
	re.Receiver.Subscription.Active = re.Active.MasterEnable
	if changed {
		nowTAI := TAITimestampNow()
		modeCp := "activate_immediate"
		re.Active.Activation.Mode = &modeCp
		re.Active.Activation.ActivationTime = &nowTAI
		re.Receiver.Version = nowTAI
		s.verRcv++
	}
	cb := s.onResourceChange
	s.mu.Unlock()

	if changed && cb != nil {
		cb()
	}
}

func cloneReceiverConnState(in ReceiverConnectionState) ReceiverConnectionState {
	out := in
	if in.SenderID != nil {
		cp := *in.SenderID
		out.SenderID = &cp
	}
	if in.Activation.Mode != nil {
		cp := *in.Activation.Mode
		out.Activation.Mode = &cp
	}
	if in.Activation.RequestedTime != nil {
		cp := *in.Activation.RequestedTime
		out.Activation.RequestedTime = &cp
	}
	if in.Activation.ActivationTime != nil {
		cp := *in.Activation.ActivationTime
		out.Activation.ActivationTime = &cp
	}
	if in.TransportFile.Data != nil {
		cp := *in.TransportFile.Data
		out.TransportFile.Data = &cp
	}
	if in.TransportFile.Type != nil {
		cp := *in.TransportFile.Type
		out.TransportFile.Type = &cp
	}
	out.TransportParams = make([]ReceiverWebSocketParams, len(in.TransportParams))
	for i, p := range in.TransportParams {
		cp := p
		if p.ConnectionURI != nil {
			u := *p.ConnectionURI
			cp.ConnectionURI = &u
		}
		if p.ExtIS07RestAPIURL != nil {
			r := *p.ExtIS07RestAPIURL
			cp.ExtIS07RestAPIURL = &r
		}
		if p.ExtIS07SourceID != nil {
			sid := *p.ExtIS07SourceID
			cp.ExtIS07SourceID = &sid
		}
		out.TransportParams[i] = cp
	}
	return out
}

func cloneConnState(in SenderConnectionState) SenderConnectionState {
	out := in
	if in.ReceiverID != nil {
		cp := *in.ReceiverID
		out.ReceiverID = &cp
	}
	if in.Activation.Mode != nil {
		cp := *in.Activation.Mode
		out.Activation.Mode = &cp
	}
	if in.Activation.RequestedTime != nil {
		cp := *in.Activation.RequestedTime
		out.Activation.RequestedTime = &cp
	}
	if in.Activation.ActivationTime != nil {
		cp := *in.Activation.ActivationTime
		out.Activation.ActivationTime = &cp
	}
	out.TransportParams = append([]WebSocketTransportParams(nil), in.TransportParams...)
	return out
}

func detectInterfaceInfo(preferredName, hostIP string) (name string, macHyphen string) {
	ifaces, err := net.Interfaces()
	if err == nil {
		if preferredName != "" {
			for _, iface := range ifaces {
				if iface.Name == preferredName && len(iface.HardwareAddr) >= 6 {
					return iface.Name, formatMACHyphen(iface.HardwareAddr)
				}
			}
		}
		for _, iface := range ifaces {
			addrs, _ := iface.Addrs()
			for _, a := range addrs {
				if ipnet, ok := a.(*net.IPNet); ok && ipnet.IP.String() == hostIP && len(iface.HardwareAddr) >= 6 {
					return iface.Name, formatMACHyphen(iface.HardwareAddr)
				}
			}
		}
		for _, iface := range ifaces {
			if iface.Flags&net.FlagUp != 0 && iface.Flags&net.FlagLoopback == 0 && len(iface.HardwareAddr) >= 6 {
				return iface.Name, formatMACHyphen(iface.HardwareAddr)
			}
		}
	}
	return "eth0", "02-00-00-00-00-01"
}

func formatMACHyphen(hw net.HardwareAddr) string {
	parts := make([]string, 6)
	for i := 0; i < 6; i++ {
		parts[i] = fmt.Sprintf("%02x", hw[i])
	}
	return strings.Join(parts, "-")
}
