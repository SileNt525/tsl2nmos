package tally

import (
	"context"
	"sort"
	"sync"
	"time"

	"tsl2nmos/internal/config"
	"tsl2nmos/internal/tsl"
)

// OverrideMode allows manual GPO relay & camera lamp testing from the Web UI without switcher input.
type OverrideMode string

const (
	OverrideAuto       OverrideMode = "auto"
	OverrideForceRed   OverrideMode = "force_red"
	OverrideForceGreen OverrideMode = "force_green"
	OverrideForceBoth  OverrideMode = "force_both"
	OverrideForceOff   OverrideMode = "force_off"
)

// Color identifies one of the two boolean event flows per camera channel.
type Color string

const (
	ColorRed   Color = "red"
	ColorGreen Color = "green"
)

// StateChangeEvent is emitted whenever an effective Red or Green tally state transitions.
type StateChangeEvent struct {
	ChannelID int       `json:"channel_id"`
	Color     Color     `json:"color"`
	Value     bool      `json:"value"`
	Timestamp time.Time `json:"timestamp"`
}

// ChannelSnapshot represents the complete runtime status of a single camera channel.
type ChannelSnapshot struct {
	ID            int                   `json:"id"`
	Name          string                `json:"name"`
	Screen        uint16                `json:"screen"`
	MatchAllScr   bool                  `json:"match_all_screens"`
	Index         uint16                `json:"index"`
	TSLLabel      string                `json:"tsl_label"`
	RawRH         string                `json:"raw_rh"`
	RawText       string                `json:"raw_text"`
	RawLH         string                `json:"raw_lh"`
	RawRed        bool                  `json:"raw_red"`
	RawGreen      bool                  `json:"raw_green"`
	EffectiveRed  bool                  `json:"effective_red"`
	EffectiveGrn  bool                  `json:"effective_green"`
	Override      OverrideMode          `json:"override"`
	EffectiveRule config.TallyRuleConfig `json:"effective_rule"`
	RedSourceID   string                `json:"red_source_id"`
	GreenSourceID string                `json:"green_source_id"`
	RedSenderID   string                `json:"red_sender_id"`
	GreenSenderID string                `json:"green_sender_id"`
	UpdatedAt     *time.Time            `json:"updated_at,omitempty"`
}

type channelState struct {
	cfg       config.ChannelConfig
	tslLabel  string
	rawRH     tsl.LampColor
	rawText   tsl.LampColor
	rawLH     tsl.LampColor
	hasRaw    bool
	rawRed    bool
	rawGreen  bool
	effRed    bool
	effGreen  bool
	override  OverrideMode
	updatedAt time.Time

	redSourceID   string
	greenSourceID string
	redSenderID   string
	greenSenderID string
}

// ChangeCallback receives real-time boolean transitions.
type ChangeCallback func(ev StateChangeEvent)

// UUIDResolver resolves NMOS Source and Sender UUIDs for display/reference.
type UUIDResolver func(channelID int, color Color) (sourceID, senderID string)

// Engine manages per-channel Tally evaluation, overrides, signal-loss failsafe, and deduplication.
type Engine struct {
	mu            sync.RWMutex
	defaultRule   config.TallyRuleConfig
	timeoutMs     int
	timeoutAction string
	lastRxTime    time.Time
	channels      map[int]*channelState
	listeners     []ChangeCallback
	uuidResolver  UUIDResolver
	uiSubs        map[chan []ChannelSnapshot]struct{}
}

// NewEngine creates a Tally Engine initialized from cfg.
func NewEngine(cfg config.Config, resolver UUIDResolver) *Engine {
	e := &Engine{
		channels:     make(map[int]*channelState),
		uuidResolver: resolver,
		uiSubs:       make(map[chan []ChannelSnapshot]struct{}),
	}
	e.ApplyConfig(cfg)
	return e
}

// OnChange registers a callback invoked whenever an effective boolean state changes.
func (e *Engine) OnChange(cb ChangeCallback) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.listeners = append(e.listeners, cb)
}

// ApplyConfig hot-reloads channel definitions and rules while preserving current lamp states
// for existing channels. If rules or polarity changed, any resulting effective state transitions
// are immediately emitted!
func (e *Engine) ApplyConfig(cfg config.Config) {
	now := time.Now()
	var pending []StateChangeEvent

	e.mu.Lock()
	e.defaultRule = cfg.TSL.DefaultRule
	e.timeoutMs = cfg.TSL.TimeoutMs
	e.timeoutAction = cfg.TSL.TimeoutAction

	nextMap := make(map[int]*channelState, len(cfg.Channels))
	for _, chCfg := range cfg.Channels {
		st, exists := e.channels[chCfg.ID]
		if !exists {
			st = &channelState{
				cfg:      chCfg,
				override: OverrideAuto,
			}
		} else {
			st.cfg = chCfg
		}

		if e.uuidResolver != nil {
			st.redSourceID, st.redSenderID = e.uuidResolver(chCfg.ID, ColorRed)
			st.greenSourceID, st.greenSenderID = e.uuidResolver(chCfg.ID, ColorGreen)
		}

		prevRed, prevGreen := st.effRed, st.effGreen
		rule := e.ruleForLocked(st)
		if st.hasRaw {
			st.rawRed, st.rawGreen = EvaluateRule(st.rawRH, st.rawText, st.rawLH, rule)
		} else {
			// Even before any packet arrives, InvertRed/InvertGreen determines initial idle state.
			st.rawRed, st.rawGreen = EvaluateRule(tsl.LampOff, tsl.LampOff, tsl.LampOff, rule)
		}
		st.effRed, st.effGreen = computeEffective(st.rawRed, st.rawGreen, st.override)

		if exists {
			if st.effRed != prevRed {
				pending = append(pending, StateChangeEvent{ChannelID: chCfg.ID, Color: ColorRed, Value: st.effRed, Timestamp: now})
			}
			if st.effGreen != prevGreen {
				pending = append(pending, StateChangeEvent{ChannelID: chCfg.ID, Color: ColorGreen, Value: st.effGreen, Timestamp: now})
			}
		}
		nextMap[chCfg.ID] = st
	}
	e.channels = nextMap
	listeners := append([]ChangeCallback(nil), e.listeners...)
	snap := e.snapshotsLocked()
	e.broadcastUILocked(snap)
	e.mu.Unlock()

	for _, ev := range pending {
		for _, cb := range listeners {
			cb(ev)
		}
	}
}

// HandlePacket processes a decoded TSL 5.0 packet and emits any resulting boolean state transitions.
func (e *Engine) HandlePacket(pkt tsl.Packet, _ string, rxTime time.Time) {
	if pkt.IsSControl || len(pkt.Messages) == 0 {
		return
	}

	var pending []StateChangeEvent

	e.mu.Lock()
	e.lastRxTime = rxTime

	for _, msg := range pkt.Messages {
		for _, st := range e.channels {
			if !matchesScreen(st.cfg, pkt.Screen) || !matchesIndex(st.cfg.Index, msg.Index) {
				continue
			}

			st.hasRaw = true
			st.rawRH = msg.RHTally
			st.rawText = msg.TextTally
			st.rawLH = msg.LHTally
			if msg.HasText && !msg.IsControlData {
				st.tslLabel = msg.Text
			}
			st.updatedAt = rxTime

			rule := e.ruleForLocked(st)
			st.rawRed, st.rawGreen = EvaluateRule(msg.RHTally, msg.TextTally, msg.LHTally, rule)

			prevRed, prevGreen := st.effRed, st.effGreen
			st.effRed, st.effGreen = computeEffective(st.rawRed, st.rawGreen, st.override)

			if st.effRed != prevRed {
				pending = append(pending, StateChangeEvent{
					ChannelID: st.cfg.ID,
					Color:     ColorRed,
					Value:     st.effRed,
					Timestamp: rxTime,
				})
			}
			if st.effGreen != prevGreen {
				pending = append(pending, StateChangeEvent{
					ChannelID: st.cfg.ID,
					Color:     ColorGreen,
					Value:     st.effGreen,
					Timestamp: rxTime,
				})
			}
		}
	}

	listeners := append([]ChangeCallback(nil), e.listeners...)
	snap := e.snapshotsLocked()
	e.broadcastUILocked(snap)
	e.mu.Unlock()

	for _, ev := range pending {
		for _, cb := range listeners {
			cb(ev)
		}
	}
}

// SetOverride sets manual test override for a channel ("auto", "force_red", "force_green", "force_both", "force_off").
func (e *Engine) SetOverride(channelID int, mode OverrideMode) bool {
	switch mode {
	case OverrideAuto, OverrideForceRed, OverrideForceGreen, OverrideForceBoth, OverrideForceOff:
	default:
		return false
	}

	now := time.Now()
	var pending []StateChangeEvent

	e.mu.Lock()
	st, ok := e.channels[channelID]
	if !ok {
		e.mu.Unlock()
		return false
	}
	st.override = mode
	prevRed, prevGreen := st.effRed, st.effGreen
	st.effRed, st.effGreen = computeEffective(st.rawRed, st.rawGreen, st.override)
	st.updatedAt = now

	if st.effRed != prevRed {
		pending = append(pending, StateChangeEvent{ChannelID: channelID, Color: ColorRed, Value: st.effRed, Timestamp: now})
	}
	if st.effGreen != prevGreen {
		pending = append(pending, StateChangeEvent{ChannelID: channelID, Color: ColorGreen, Value: st.effGreen, Timestamp: now})
	}

	listeners := append([]ChangeCallback(nil), e.listeners...)
	snap := e.snapshotsLocked()
	e.broadcastUILocked(snap)
	e.mu.Unlock()

	for _, ev := range pending {
		for _, cb := range listeners {
			cb(ev)
		}
	}
	return true
}

// GetChannelValue returns the current effective boolean value for (channelID, color).
func (e *Engine) GetChannelValue(channelID int, color Color) (bool, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	st, ok := e.channels[channelID]
	if !ok {
		return false, false
	}
	if color == ColorRed {
		return st.effRed, true
	}
	return st.effGreen, true
}

// Snapshots returns an ordered slice of all channel states for REST/UI inspection.
func (e *Engine) Snapshots() []ChannelSnapshot {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.snapshotsLocked()
}

// SubscribeUI subscribes to live channel snapshot updates for the Web Console.
func (e *Engine) SubscribeUI() (chan []ChannelSnapshot, func()) {
	ch := make(chan []ChannelSnapshot, 16)
	e.mu.Lock()
	e.uiSubs[ch] = struct{}{}
	// Push initial snapshot immediately.
	ch <- e.snapshotsLocked()
	e.mu.Unlock()

	unsub := func() {
		e.mu.Lock()
		if _, ok := e.uiSubs[ch]; ok {
			delete(e.uiSubs, ch)
			close(ch)
		}
		e.mu.Unlock()
	}
	return ch, unsub
}

// StartWatchdog runs the optional TSL signal-loss failsafe timer until ctx is cancelled.
func (e *Engine) StartWatchdog(ctx context.Context) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			e.checkTimeout(now)
		}
	}
}

func (e *Engine) checkTimeout(now time.Time) {
	var pending []StateChangeEvent

	e.mu.Lock()
	if e.timeoutMs <= 0 || e.timeoutAction != "clear" || e.lastRxTime.IsZero() {
		e.mu.Unlock()
		return
	}
	if now.Sub(e.lastRxTime) < time.Duration(e.timeoutMs)*time.Millisecond {
		e.mu.Unlock()
		return
	}

	changed := false
	for _, st := range e.channels {
		if !st.hasRaw {
			continue
		}
		st.hasRaw = false
		st.rawRH = tsl.LampOff
		st.rawText = tsl.LampOff
		st.rawLH = tsl.LampOff
		rule := e.ruleForLocked(st)
		st.rawRed, st.rawGreen = EvaluateRule(tsl.LampOff, tsl.LampOff, tsl.LampOff, rule)
		prevRed, prevGreen := st.effRed, st.effGreen
		st.effRed, st.effGreen = computeEffective(st.rawRed, st.rawGreen, st.override)
		if st.effRed != prevRed {
			changed = true
			pending = append(pending, StateChangeEvent{ChannelID: st.cfg.ID, Color: ColorRed, Value: st.effRed, Timestamp: now})
		}
		if st.effGreen != prevGreen {
			changed = true
			pending = append(pending, StateChangeEvent{ChannelID: st.cfg.ID, Color: ColorGreen, Value: st.effGreen, Timestamp: now})
		}
	}

	var listeners []ChangeCallback
	if changed {
		listeners = append([]ChangeCallback(nil), e.listeners...)
		e.broadcastUILocked(e.snapshotsLocked())
	}
	e.mu.Unlock()

	for _, ev := range pending {
		for _, cb := range listeners {
			cb(ev)
		}
	}
}

func (e *Engine) ruleForLocked(st *channelState) config.TallyRuleConfig {
	if st.cfg.Rule != nil {
		return *st.cfg.Rule
	}
	return e.defaultRule
}

func (e *Engine) snapshotsLocked() []ChannelSnapshot {
	ids := make([]int, 0, len(e.channels))
	for id := range e.channels {
		ids = append(ids, id)
	}
	sort.Ints(ids)

	out := make([]ChannelSnapshot, 0, len(ids))
	for _, id := range ids {
		st := e.channels[id]
		snap := ChannelSnapshot{
			ID:            st.cfg.ID,
			Name:          st.cfg.Name,
			Screen:        st.cfg.Screen,
			MatchAllScr:   st.cfg.MatchAllScr,
			Index:         st.cfg.Index,
			TSLLabel:      st.tslLabel,
			RawRH:         st.rawRH.String(),
			RawText:       st.rawText.String(),
			RawLH:         st.rawLH.String(),
			RawRed:        st.rawRed,
			RawGreen:      st.rawGreen,
			EffectiveRed:  st.effRed,
			EffectiveGrn:  st.effGreen,
			Override:      st.override,
			EffectiveRule: e.ruleForLocked(st),
			RedSourceID:   st.redSourceID,
			GreenSourceID: st.greenSourceID,
			RedSenderID:   st.redSenderID,
			GreenSenderID: st.greenSenderID,
		}
		if !st.updatedAt.IsZero() {
			t := st.updatedAt
			snap.UpdatedAt = &t
		}
		out = append(out, snap)
	}
	return out
}

func (e *Engine) broadcastUILocked(snap []ChannelSnapshot) {
	for ch := range e.uiSubs {
		select {
		case ch <- snap:
		default:
		}
	}
}

// EvaluateRule maps raw 2-bit RH/Text/LH TSL 5.0 lamp states into boolean Red (PGM) & Green (PVW).
func EvaluateRule(rh, text, lh tsl.LampColor, rule config.TallyRuleConfig) (red bool, green bool) {
	isRedLamp := func(c tsl.LampColor) bool { return c == tsl.LampRed || c == tsl.LampAmber }
	isGreenLamp := func(c tsl.LampColor) bool { return c == tsl.LampGreen || c == tsl.LampAmber }

	switch rule.Mode {
	case config.RuleRHPgmLHPvw:
		red = rh != tsl.LampOff
		green = lh != tsl.LampOff
	case config.RuleLHPgmRHPvw:
		red = lh != tsl.LampOff
		green = rh != tsl.LampOff
	case config.RuleRHOnly:
		red = isRedLamp(rh)
		green = isGreenLamp(rh)
	case config.RuleLHOnly:
		red = isRedLamp(lh)
		green = isGreenLamp(lh)
	case config.RuleTextOnly:
		red = isRedLamp(text)
		green = isGreenLamp(text)
	default: // config.RuleAnyColor: evaluate LH & RH tally lamps only (ignore TextTally UMD character backlight)
		red = isRedLamp(rh) || isRedLamp(lh)
		green = isGreenLamp(rh) || isGreenLamp(lh)
	}

	if rule.PGMOverridesPVW && red {
		green = false
	}
	if rule.InvertRed {
		red = !red
	}
	if rule.InvertGreen {
		green = !green
	}
	return red, green
}

func computeEffective(rawRed, rawGreen bool, ov OverrideMode) (bool, bool) {
	switch ov {
	case OverrideForceRed:
		return true, false
	case OverrideForceGreen:
		return false, true
	case OverrideForceBoth:
		return true, true
	case OverrideForceOff:
		return false, false
	default:
		return rawRed, rawGreen
	}
}

func matchesScreen(ch config.ChannelConfig, pktScreen uint16) bool {
	if ch.MatchAllScr || pktScreen == tsl.BroadcastIndex || ch.Screen == tsl.BroadcastIndex {
		return true
	}
	return ch.Screen == pktScreen
}

func matchesIndex(chIndex, msgIndex uint16) bool {
	if msgIndex == tsl.BroadcastIndex {
		return true
	}
	return chIndex == msgIndex
}
