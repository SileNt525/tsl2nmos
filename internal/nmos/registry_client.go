package nmos

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"tsl2nmos/internal/config"
)

// RegistryStatus reports current IS-04 Registration API connection state for UI diagnostics.
type RegistryStatus struct {
	Mode              string     `json:"mode"`
	ActiveRegistryURL string     `json:"active_registry_url,omitempty"`
	Registered        bool       `json:"registered"`
	LastHeartbeatAt   *time.Time `json:"last_heartbeat_at,omitempty"`
	LastError         string     `json:"last_error,omitempty"`
}

// RegistryClient manages AMWA IS-04 Registration API registration, resource updates, and 5s heartbeats.
type RegistryClient struct {
	store  *Store
	mdns   *MDNSService
	client *http.Client
	log    *slog.Logger

	mu                sync.RWMutex
	mode              string
	staticURL         string
	heartbeatSec      int
	activeURL         string
	registered        bool
	lastHeartbeat     time.Time
	lastErr           string
	registeredSenders   map[string]struct{}
	registeredReceivers map[string]struct{}
	registeredFlows     map[string]struct{}
	registeredSources   map[string]struct{}
	resyncCh            chan struct{}
}

// NewRegistryClient creates an IS-04 Registration API client.
func NewRegistryClient(store *Store, mdns *MDNSService, cfg config.NMOSConfig, logger *slog.Logger) *RegistryClient {
	if logger == nil {
		logger = slog.Default()
	}
	rc := &RegistryClient{
		store: store,
		mdns:  mdns,
		client: &http.Client{
			Timeout: 4 * time.Second,
		},
		log:                 logger,
		mode:                cfg.RegistryMode,
		staticURL:           strings.TrimRight(cfg.StaticRegistryURL, "/"),
		heartbeatSec:        cfg.HeartbeatIntervalSec,
		registeredSenders:   make(map[string]struct{}),
		registeredReceivers: make(map[string]struct{}),
		registeredFlows:     make(map[string]struct{}),
		registeredSources:   make(map[string]struct{}),
		resyncCh:            make(chan struct{}, 1),
	}
	return rc
}

// UpdateConfig hot-reloads registry mode / URL settings and triggers a sync.
func (r *RegistryClient) UpdateConfig(cfg config.NMOSConfig) {
	r.mu.Lock()
	r.mode = cfg.RegistryMode
	r.staticURL = strings.TrimRight(cfg.StaticRegistryURL, "/")
	if cfg.HeartbeatIntervalSec > 0 {
		r.heartbeatSec = cfg.HeartbeatIntervalSec
	} else {
		r.heartbeatSec = 5
	}
	r.registered = false
	r.activeURL = ""
	r.mu.Unlock()
	r.TriggerSync()
}

// TriggerSync schedules an immediate resource registration update.
func (r *RegistryClient) TriggerSync() {
	select {
	case r.resyncCh <- struct{}{}:
	default:
	}
}

// Status returns the current IS-04 Registration API state.
func (r *RegistryClient) Status() RegistryStatus {
	r.mu.RLock()
	defer r.mu.RUnlock()
	st := RegistryStatus{
		Mode:              r.mode,
		ActiveRegistryURL: r.activeURL,
		Registered:        r.registered,
		LastError:         r.lastErr,
	}
	if !r.lastHeartbeat.IsZero() {
		t := r.lastHeartbeat
		st.LastHeartbeatAt = &t
	}
	return st
}

// Start runs the background registration and heartbeat loop until ctx is cancelled.
func (r *RegistryClient) Start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(time.Duration(r.getHeartbeatSec()) * time.Second)
		defer ticker.Stop()

		r.step(ctx, true)

		for {
			select {
			case <-ctx.Done():
				r.unregisterNode()
				return
			case <-r.resyncCh:
				r.step(ctx, true)
			case <-ticker.C:
				r.step(ctx, false)
			}
		}
	}()
}

func (r *RegistryClient) getHeartbeatSec() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.heartbeatSec <= 0 {
		return 5
	}
	return r.heartbeatSec
}

func (r *RegistryClient) step(ctx context.Context, forceResourceSync bool) {
	r.mu.RLock()
	mode := r.mode
	staticURL := r.staticURL
	wasReg := r.registered
	activeURL := r.activeURL
	r.mu.RUnlock()

	if mode == "disabled" {
		r.mu.Lock()
		r.registered = false
		r.activeURL = ""
		r.lastErr = ""
		r.mu.Unlock()
		return
	}

	targetBase := activeURL
	if targetBase == "" {
		targetBase = r.resolveRegistryURL(ctx, mode, staticURL)
		if targetBase == "" {
			r.mu.Lock()
			r.registered = false
			if mode == "static" && staticURL == "" {
				r.lastErr = "static_registry_url is empty"
			} else {
				r.lastErr = "" // In auto mode without registry on LAN, Peer-to-Peer is normal.
			}
			r.mu.Unlock()
			return
		}
	}

	if !wasReg || forceResourceSync {
		if err := r.registerAll(ctx, targetBase); err != nil {
			r.mu.Lock()
			r.registered = false
			r.activeURL = ""
			r.lastErr = err.Error()
			r.mu.Unlock()
			r.log.Warn("IS-04 Registry sync failed", "registry", targetBase, "err", err)
			return
		}
	}

	node, _, _, _, _ := r.store.SnapshotResources()
	hbURL := fmt.Sprintf("%s/health/nodes/%s", targetBase, node.ID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, hbURL, nil)
	if err != nil {
		return
	}
	resp, err := r.client.Do(req)
	if err != nil {
		r.mu.Lock()
		r.registered = false
		r.activeURL = ""
		r.lastErr = err.Error()
		r.mu.Unlock()
		return
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		// Registry restarted or expired our lease -> immediate full re-registration per IS-04 spec!
		if err := r.registerAll(ctx, targetBase); err != nil {
			r.mu.Lock()
			r.registered = false
			r.activeURL = ""
			r.lastErr = err.Error()
			r.mu.Unlock()
			return
		}
	} else if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		r.mu.Lock()
		r.registered = false
		r.activeURL = ""
		r.lastErr = fmt.Sprintf("heartbeat HTTP %d", resp.StatusCode)
		r.mu.Unlock()
		return
	}

	now := time.Now()
	r.mu.Lock()
	r.activeURL = targetBase
	r.registered = true
	r.lastHeartbeat = now
	r.lastErr = ""
	r.mu.Unlock()
}

func (r *RegistryClient) resolveRegistryURL(ctx context.Context, mode, staticURL string) string {
	if mode == "static" {
		if staticURL == "" {
			return ""
		}
		if strings.Contains(staticURL, "/x-nmos/registration/") {
			return strings.TrimRight(staticURL, "/")
		}
		return r.probeRegistrationVersion(ctx, staticURL)
	}

	if r.mdns == nil {
		return ""
	}
	for _, svc := range r.mdns.DiscoveredRegistries() {
		if u := r.probeRegistrationVersion(ctx, svc.BaseURL); u != "" {
			return u
		}
	}
	return ""
}

func (r *RegistryClient) probeRegistrationVersion(ctx context.Context, base string) string {
	base = strings.TrimRight(base, "/")
	for _, ver := range []string{"v1.3", "v1.2"} {
		candidate := fmt.Sprintf("%s/x-nmos/registration/%s", base, ver)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, candidate+"/", nil)
		if err != nil {
			continue
		}
		resp, err := r.client.Do(req)
		if err != nil {
			continue
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			return candidate
		}
	}
	return ""
}

func (r *RegistryClient) registerAll(ctx context.Context, regBase string) error {
	node, device, sources, flows, senders := r.store.SnapshotResources()
	receivers := r.store.SnapshotReceivers()

	if err := r.postResource(ctx, regBase, "node", node); err != nil {
		return fmt.Errorf("register node: %w", err)
	}
	if err := r.postResource(ctx, regBase, "device", device); err != nil {
		return fmt.Errorf("register device: %w", err)
	}

	curSrc := make(map[string]struct{}, len(sources))
	for _, s := range sources {
		curSrc[s.ID] = struct{}{}
		if err := r.postResource(ctx, regBase, "source", s); err != nil {
			return fmt.Errorf("register source %s: %w", s.ID, err)
		}
	}
	curFlw := make(map[string]struct{}, len(flows))
	for _, f := range flows {
		curFlw[f.ID] = struct{}{}
		if err := r.postResource(ctx, regBase, "flow", f); err != nil {
			return fmt.Errorf("register flow %s: %w", f.ID, err)
		}
	}
	curSnd := make(map[string]struct{}, len(senders))
	for _, s := range senders {
		curSnd[s.ID] = struct{}{}
		if err := r.postResource(ctx, regBase, "sender", s); err != nil {
			return fmt.Errorf("register sender %s: %w", s.ID, err)
		}
	}
	curRcv := make(map[string]struct{}, len(receivers))
	for _, rcv := range receivers {
		curRcv[rcv.ID] = struct{}{}
		if err := r.postResource(ctx, regBase, "receiver", rcv); err != nil {
			return fmt.Errorf("register receiver %s: %w", rcv.ID, err)
		}
	}

	// Clean up stale resources if channel/receiver count was reduced.
	r.mu.Lock()
	for oldRcv := range r.registeredReceivers {
		if _, keep := curRcv[oldRcv]; !keep {
			_ = r.deleteResource(ctx, regBase, "receivers", oldRcv)
		}
	}
	for oldSnd := range r.registeredSenders {
		if _, keep := curSnd[oldSnd]; !keep {
			_ = r.deleteResource(ctx, regBase, "senders", oldSnd)
		}
	}
	for oldFlw := range r.registeredFlows {
		if _, keep := curFlw[oldFlw]; !keep {
			_ = r.deleteResource(ctx, regBase, "flows", oldFlw)
		}
	}
	for oldSrc := range r.registeredSources {
		if _, keep := curSrc[oldSrc]; !keep {
			_ = r.deleteResource(ctx, regBase, "sources", oldSrc)
		}
	}
	r.registeredSources = curSrc
	r.registeredFlows = curFlw
	r.registeredSenders = curSnd
	r.registeredReceivers = curRcv
	r.mu.Unlock()

	r.log.Info("Registered NMOS IS-04 resources",
		"registry", regBase,
		"sources", len(sources),
		"senders", len(senders),
		"receivers", len(receivers),
	)
	return nil
}

func (r *RegistryClient) postResource(ctx context.Context, regBase, resType string, data any) error {
	payload, err := json.Marshal(map[string]any{
		"type": resType,
		"data": data,
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, regBase+"/resource", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

func (r *RegistryClient) deleteResource(ctx context.Context, regBase, pluralType, id string) error {
	u := fmt.Sprintf("%s/resource/%s/%s", regBase, pluralType, id)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, u, nil)
	if err != nil {
		return err
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return err
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	return nil
}

func (r *RegistryClient) unregisterNode() {
	r.mu.RLock()
	regBase := r.activeURL
	wasReg := r.registered
	r.mu.RUnlock()
	if !wasReg || regBase == "" {
		return
	}
	node, _, _, _, _ := r.store.SnapshotResources()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = r.deleteResource(ctx, regBase, "nodes", node.ID)
}
