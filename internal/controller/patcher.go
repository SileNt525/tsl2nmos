package controller

import (
	"bytes"
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

	"tsl2nmos/internal/config"
	"tsl2nmos/internal/nmos"
	"tsl2nmos/internal/tally"
)

// RemoteReceiver describes an IS-07 Data/GPO Receiver discovered on a target device (e.g. Riedel NSA-010C GPO 1..16).
type RemoteReceiver struct {
	ID             string  `json:"id"`
	Label          string  `json:"label"`
	Description    string  `json:"description"`
	DeviceID       string  `json:"device_id"`
	Format         string  `json:"format"`
	Transport      string  `json:"transport"`
	ControlHref    string  `json:"control_href"`
	ActiveSenderID *string `json:"active_sender_id,omitempty"`
	MasterEnable   bool    `json:"master_enable"`
}

// TargetDeviceInspection holds the full result of probing an NMOS IS-07-GPIO device.
type TargetDeviceInspection struct {
	BaseURL     string           `json:"base_url"`
	NodeID      string           `json:"node_id"`
	NodeLabel   string           `json:"node_label"`
	NodeVersion string           `json:"node_version"`
	APIVersion  string           `json:"api_version"`
	Receivers   []RemoteReceiver `json:"receivers"`
	InspectedAt time.Time        `json:"inspected_at"`
}

// BindingRuntimeStatus tracks the live health of each configured Direct GPO Binding.
type BindingRuntimeStatus struct {
	TargetID      string     `json:"target_id"`
	TargetLabel   string     `json:"target_label"`
	ReceiverID    string     `json:"receiver_id"`
	ReceiverLabel string     `json:"receiver_label"`
	ChannelID     int        `json:"channel_id"`
	Color         string     `json:"color"`
	Enabled       bool       `json:"enabled"`
	Patched       bool       `json:"patched"`
	LastCheckedAt *time.Time `json:"last_checked_at,omitempty"`
	LastPatchedAt *time.Time `json:"last_patched_at,omitempty"`
	LastError     string     `json:"last_error,omitempty"`
}

// DirectPatcher discovers NMOS IS-07 GPIO receivers (such as Riedel StageLink NSA-010C)
// and manages IS-05 connection patching + self-healing when no external Broadcast Controller is used.
type DirectPatcher struct {
	cfgMgr *config.Manager
	store  *nmos.Store
	client *http.Client
	log    *slog.Logger

	mu       sync.RWMutex
	statuses map[string]BindingRuntimeStatus // key: targetID + "|" + receiverID
	syncNow  chan struct{}
}

// NewDirectPatcher creates a DirectPatcher instance.
func NewDirectPatcher(cfgMgr *config.Manager, store *nmos.Store, logger *slog.Logger) *DirectPatcher {
	if logger == nil {
		logger = slog.Default()
	}
	return &DirectPatcher{
		cfgMgr: cfgMgr,
		store:  store,
		client: &http.Client{
			Timeout: 4 * time.Second,
		},
		log:      logger,
		statuses: make(map[string]BindingRuntimeStatus),
		syncNow:  make(chan struct{}, 1),
	}
}

// Start runs the background reconciliation & self-healing loop until ctx is cancelled.
func (p *DirectPatcher) Start(ctx context.Context) {
	go func() {
		// Immediate initial sync on startup.
		p.reconcileAll(ctx)

		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-p.syncNow:
				p.reconcileAll(ctx)
			case <-ticker.C:
				p.reconcileAll(ctx)
			}
		}
	}()
}

// TriggerSync schedules an immediate reconciliation pass (called after saving config/bindings).
func (p *DirectPatcher) TriggerSync() {
	select {
	case p.syncNow <- struct{}{}:
	default:
	}
}

// Statuses returns the current runtime health of all configured GPO bindings.
func (p *DirectPatcher) Statuses() []BindingRuntimeStatus {
	p.mu.RLock()
	defer p.mu.RUnlock()

	out := make([]BindingRuntimeStatus, 0, len(p.statuses))
	for _, s := range p.statuses {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].TargetID != out[j].TargetID {
			return out[i].TargetID < out[j].TargetID
		}
		return naturalLess(out[i].ReceiverLabel, out[j].ReceiverLabel)
	})
	return out
}

// InspectDevice queries a remote NMOS Node (e.g., Riedel StageLink NSA-010C) and returns its
// Node metadata and sorted IS-07 Boolean Data/WebSocket Receivers (e.g. GPOutput 1..N).
// If rawBaseURL omits an explicit port (e.g. "192.168.42.59"), it automatically probes
// Riedel StageLink's native NMOS port (4450) as well as standard ports (80, 8080).
func (p *DirectPatcher) InspectDevice(ctx context.Context, rawBaseURL string) (*TargetDeviceInspection, error) {
	candidates, targetHost, err := candidateBaseURLs(rawBaseURL)
	if err != nil {
		return nil, err
	}

	var baseURL, apiVer string
	var lastProbeErr error
	for _, cand := range candidates {
		ver, probeErr := p.detectNodeAPIVersion(ctx, cand)
		if probeErr == nil {
			baseURL = cand
			apiVer = ver
			break
		}
		lastProbeErr = probeErr
	}
	if baseURL == "" {
		return nil, fmt.Errorf("probe IS-04 Node API at %v: %w", candidates, lastProbeErr)
	}

	nodePrefix := fmt.Sprintf("%s/x-nmos/node/%s", baseURL, apiVer)

	var self struct {
		ID      string `json:"id"`
		Label   string `json:"label"`
		Version string `json:"version"`
	}
	if err := p.getJSON(ctx, nodePrefix+"/self", &self); err != nil {
		return nil, fmt.Errorf("read /self: %w", err)
	}

	var devices []struct {
		ID       string `json:"id"`
		Controls []struct {
			Type string `json:"type"`
			Href string `json:"href"`
		} `json:"controls"`
	}
	if err := p.getJSON(ctx, nodePrefix+"/devices", &devices); err != nil {
		return nil, fmt.Errorf("read /devices: %w", err)
	}

	ctrlByDevice := make(map[string]string, len(devices))
	for _, d := range devices {
		bestHref := ""
		for _, c := range d.Controls {
			if strings.HasPrefix(c.Type, "urn:x-nmos:control:sr-ctrl/") {
				rewritten := rewriteReachableControlHref(c.Href, targetHost)
				if c.Type == nmos.ControlSRv11 || bestHref == "" {
					bestHref = rewritten
				}
			}
		}
		if bestHref == "" {
			bestHref = baseURL + "/x-nmos/connection/v1.1/"
		}
		if !strings.HasSuffix(bestHref, "/") {
			bestHref += "/"
		}
		ctrlByDevice[d.ID] = bestHref
	}

	var rawReceivers []struct {
		ID        string `json:"id"`
		Label     string `json:"label"`
		Description string `json:"description"`
		DeviceID  string `json:"device_id"`
		Format    string `json:"format"`
		Transport string `json:"transport"`
		Caps      struct {
			EventTypes []string `json:"event_types"`
		} `json:"caps"`
		Subscription struct {
			SenderID *string `json:"sender_id"`
			Active   bool    `json:"active"`
		} `json:"subscription"`
	}
	if err := p.getJSON(ctx, nodePrefix+"/receivers", &rawReceivers); err != nil {
		return nil, fmt.Errorf("read /receivers: %w", err)
	}

	receivers := make([]RemoteReceiver, 0, len(rawReceivers))
	for _, r := range rawReceivers {
		// Keep IS-07 data receivers (WebSocket transport). Ignore ST 2110 audio/video RTP receivers if hybrid device.
		if r.Format != "" && r.Format != nmos.FormatData {
			continue
		}
		if r.Transport != "" && !strings.Contains(r.Transport, "websocket") {
			continue
		}
		// Exclude non-boolean receivers (e.g., Riedel NSA-010C AnalogGPOutput 1/2 which declare event_types: ["number"]).
		if len(r.Caps.EventTypes) > 0 && !acceptsBooleanEvent(r.Caps.EventTypes) {
			continue
		}
		ctrlHref := ctrlByDevice[r.DeviceID]
		if ctrlHref == "" {
			ctrlHref = baseURL + "/x-nmos/connection/v1.1/"
		}
		receivers = append(receivers, RemoteReceiver{
			ID:             r.ID,
			Label:          r.Label,
			Description:    r.Description,
			DeviceID:       r.DeviceID,
			Format:         r.Format,
			Transport:      r.Transport,
			ControlHref:    ctrlHref,
			ActiveSenderID: r.Subscription.SenderID,
			MasterEnable:   r.Subscription.Active,
		})
	}

	// Natural sort so "GPO 1", "GPO 2", ..., "GPO 10", ..., "GPO 16" order cleanly!
	sort.Slice(receivers, func(i, j int) bool {
		return naturalLess(receivers[i].Label, receivers[j].Label)
	})

	return &TargetDeviceInspection{
		BaseURL:     baseURL,
		NodeID:      self.ID,
		NodeLabel:   self.Label,
		NodeVersion: self.Version,
		APIVersion:  apiVer,
		Receivers:   receivers,
		InspectedAt: time.Now(),
	}, nil
}

// DisconnectReceiver explicitly unpatches an IS-05 Receiver (sets master_enable=false, sender_id=null).
func (p *DirectPatcher) DisconnectReceiver(ctx context.Context, rawBaseURL, receiverID string) error {
	info, err := p.InspectDevice(ctx, rawBaseURL)
	if err != nil {
		return err
	}
	ctrlHref := info.BaseURL + "/x-nmos/connection/v1.1/"
	for _, r := range info.Receivers {
		if r.ID == receiverID && r.ControlHref != "" {
			ctrlHref = r.ControlHref
			break
		}
	}

	stagedURL := fmt.Sprintf("%ssingle/receivers/%s/staged", ctrlHref, receiverID)
	body := map[string]any{
		"sender_id":     nil,
		"master_enable": false,
		"activation": map[string]any{
			"mode": "activate_immediate",
		},
	}
	return p.patchJSON(ctx, stagedURL, body)
}

func (p *DirectPatcher) reconcileAll(ctx context.Context) {
	cfg := p.cfgMgr.Get()
	nextStatuses := make(map[string]BindingRuntimeStatus)

	// Carry over previous LastPatchedAt timestamps.
	p.mu.RLock()
	prevStatuses := make(map[string]BindingRuntimeStatus, len(p.statuses))
	for k, v := range p.statuses {
		prevStatuses[k] = v
	}
	p.mu.RUnlock()

	for _, target := range cfg.DirectTargets {
		if !target.Enabled || len(target.Bindings) == 0 {
			for _, b := range target.Bindings {
				key := target.ID + "|" + b.ReceiverID
				nextStatuses[key] = BindingRuntimeStatus{
					TargetID:      target.ID,
					TargetLabel:   target.Label,
					ReceiverID:    b.ReceiverID,
					ReceiverLabel: b.ReceiverLabel,
					ChannelID:     b.ChannelID,
					Color:         b.Color,
					Enabled:       target.Enabled && b.Enabled,
				}
			}
			continue
		}

		info, err := p.InspectDevice(ctx, target.BaseURL)
		now := time.Now()
		if err != nil {
			for _, b := range target.Bindings {
				key := target.ID + "|" + b.ReceiverID
				prev := prevStatuses[key]
				nextStatuses[key] = BindingRuntimeStatus{
					TargetID:      target.ID,
					TargetLabel:   target.Label,
					ReceiverID:    b.ReceiverID,
					ReceiverLabel: b.ReceiverLabel,
					ChannelID:     b.ChannelID,
					Color:         b.Color,
					Enabled:       b.Enabled,
					Patched:       false,
					LastCheckedAt: &now,
					LastPatchedAt: prev.LastPatchedAt,
					LastError:     err.Error(),
				}
			}
			continue
		}

		rcvMap := make(map[string]RemoteReceiver, len(info.Receivers))
		for _, r := range info.Receivers {
			rcvMap[r.ID] = r
		}

		_, targetHost, _ := normalizeBaseURL(target.BaseURL)

		for _, b := range target.Bindings {
			key := target.ID + "|" + b.ReceiverID
			prev := prevStatuses[key]
			st := BindingRuntimeStatus{
				TargetID:      target.ID,
				TargetLabel:   target.Label,
				ReceiverID:    b.ReceiverID,
				ReceiverLabel: b.ReceiverLabel,
				ChannelID:     b.ChannelID,
				Color:         b.Color,
				Enabled:       b.Enabled,
				LastCheckedAt: &now,
				LastPatchedAt: prev.LastPatchedAt,
			}

			if !b.Enabled {
				nextStatuses[key] = st
				continue
			}

			rcv, exists := rcvMap[b.ReceiverID]
			if !exists {
				st.LastError = fmt.Sprintf("receiver %s not found on device", b.ReceiverID)
				nextStatuses[key] = st
				continue
			}
			if st.ReceiverLabel == "" {
				st.ReceiverLabel = rcv.Label
			}

			col := tally.ColorRed
			if strings.EqualFold(b.Color, "green") {
				col = tally.ColorGreen
			}
			senderID, sourceID, wsURI, restURL, ok := p.store.ResolvePatchParamsForTarget(
				cfg.Node.HostIP, cfg.Node.InterfaceName, targetHost, b.ChannelID, col,
			)
			if !ok {
				st.LastError = fmt.Sprintf("channel %d not found in local configuration", b.ChannelID)
				nextStatuses[key] = st
				continue
			}

			// Ensure local Sender is master_enable=true!
			p.store.ActivateLocalSenderForReceiver(senderID, b.ReceiverID)

			// Check if remote receiver /active already matches exact desired parameters (idempotent self-healing!).
			alreadyActive, _ := p.isReceiverAlreadyPatched(ctx, rcv.ControlHref, b.ReceiverID, senderID, sourceID, wsURI)
			if alreadyActive {
				st.Patched = true
				nextStatuses[key] = st
				continue
			}

			// Patch remote IS-05 Receiver /staged with activate_immediate.
			stagedURL := fmt.Sprintf("%ssingle/receivers/%s/staged", rcv.ControlHref, b.ReceiverID)
			patchPayload := map[string]any{
				"sender_id":     senderID,
				"master_enable": true,
				"activation": map[string]any{
					"mode": "activate_immediate",
				},
				"transport_params": []map[string]any{
					{
						"connection_uri":           wsURI,
						"connection_authorization": false,
						"ext_is_07_source_id":      sourceID,
						"ext_is_07_rest_api_url":   restURL,
					},
				},
			}

			if err := p.patchJSON(ctx, stagedURL, patchPayload); err != nil {
				st.LastError = err.Error()
				p.log.Warn("IS-05 direct GPO patch failed",
					"target", target.Label,
					"gpo", st.ReceiverLabel,
					"channel", b.ChannelID,
					"color", b.Color,
					"err", err,
				)
			} else {
				patchedTime := time.Now()
				st.Patched = true
				st.LastPatchedAt = &patchedTime
				p.log.Info("IS-05 direct GPO route activated",
					"target", target.Label,
					"gpo", st.ReceiverLabel,
					"channel", b.ChannelID,
					"color", b.Color,
					"ws_uri", wsURI,
				)
			}

			nextStatuses[key] = st
		}
	}

	p.mu.Lock()
	p.statuses = nextStatuses
	p.mu.Unlock()
}

func (p *DirectPatcher) isReceiverAlreadyPatched(ctx context.Context, ctrlHref, receiverID, wantSenderID, wantSourceID, wantWSURI string) (bool, error) {
	activeURL := fmt.Sprintf("%ssingle/receivers/%s/active", ctrlHref, receiverID)
	var active struct {
		SenderID        *string `json:"sender_id"`
		MasterEnable    bool    `json:"master_enable"`
		TransportParams []struct {
			ConnectionURI   string `json:"connection_uri"`
			ExtIS07SourceID string `json:"ext_is_07_source_id"`
		} `json:"transport_params"`
	}
	if err := p.getJSON(ctx, activeURL, &active); err != nil {
		return false, err
	}
	if !active.MasterEnable || len(active.TransportParams) == 0 {
		return false, nil
	}
	if active.SenderID != nil && *active.SenderID != "" && *active.SenderID != wantSenderID {
		return false, nil
	}
	tp := active.TransportParams[0]
	return tp.ConnectionURI == wantWSURI && tp.ExtIS07SourceID == wantSourceID, nil
}

func (p *DirectPatcher) detectNodeAPIVersion(ctx context.Context, baseURL string) (string, error) {
	var rawVers []string
	if err := p.getJSON(ctx, baseURL+"/x-nmos/node/", &rawVers); err != nil {
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

func (p *DirectPatcher) getJSON(ctx context.Context, u string, dst any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	return json.NewDecoder(resp.Body).Decode(dst)
}

func (p *DirectPatcher) patchJSON(ctx context.Context, u string, payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPatch, u, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("PATCH HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

func acceptsBooleanEvent(types []string) bool {
	for _, t := range types {
		tl := strings.ToLower(strings.TrimSpace(t))
		if tl == "boolean" || strings.HasPrefix(tl, "boolean/") {
			return true
		}
	}
	return false
}

// candidateBaseURLs returns ordered HTTP base URLs to probe. If the user supplied an explicit port,
// only that exact URL is returned; if no port was supplied, Riedel StageLink's NMOS port (4450)
// is probed first, followed by 80 and 8080.
func candidateBaseURLs(raw string) ([]string, string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, "", fmt.Errorf("empty device URL")
	}
	if !strings.Contains(raw, "://") {
		raw = "http://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return nil, "", fmt.Errorf("invalid device URL %q", raw)
	}
	hostOnly := u.Hostname()
	if u.Port() != "" {
		return []string{fmt.Sprintf("%s://%s", u.Scheme, u.Host)}, hostOnly, nil
	}
	// Bare IP or hostname without port: probe Riedel StageLink port 4450 first, then 80, 8080!
	return []string{
		fmt.Sprintf("%s://%s:4450", u.Scheme, hostOnly),
		fmt.Sprintf("%s://%s:80", u.Scheme, hostOnly),
		fmt.Sprintf("%s://%s:8080", u.Scheme, hostOnly),
	}, hostOnly, nil
}

func normalizeBaseURL(raw string) (baseURL string, hostOnly string, err error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", "", fmt.Errorf("empty device URL")
	}
	if !strings.Contains(raw, "://") {
		raw = "http://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return "", "", fmt.Errorf("invalid device URL %q", raw)
	}
	h := u.Hostname()
	return fmt.Sprintf("%s://%s", u.Scheme, u.Host), h, nil
}

// rewriteReachableControlHref preserves the device's advertised IS-05 port & path, while substituting
// targetHost if the device advertised 127.0.0.1 or an unresolvable mDNS ".local" hostname.
func rewriteReachableControlHref(href, targetHost string) string {
	u, err := url.Parse(href)
	if err != nil || u.Host == "" || targetHost == "" {
		return href
	}
	advHost := u.Hostname()
	needReplace := advHost == "127.0.0.1" || advHost == "localhost" || strings.HasSuffix(strings.ToLower(advHost), ".local")
	if !needReplace && net.ParseIP(advHost) == nil && net.ParseIP(targetHost) != nil {
		// If device advertised bare hostname without DNS in studio LAN, prefer reachable IP!
		if _, lookupErr := net.LookupHost(advHost); lookupErr != nil {
			needReplace = true
		}
	}
	if needReplace {
		if port := u.Port(); port != "" {
			u.Host = net.JoinHostPort(targetHost, port)
		} else {
			u.Host = targetHost
		}
	}
	return u.String()
}

// naturalLess compares strings with embedded integers naturally (e.g. "GPO 2" < "GPO 10").
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
		ca := unicode.ToLower(ra[ia])
		cb := unicode.ToLower(rb[ib])
		if ca != cb {
			return ca < cb
		}
		ia++
		ib++
	}
	return len(ra) < len(rb)
}
