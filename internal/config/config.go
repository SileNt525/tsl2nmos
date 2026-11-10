package config

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"gopkg.in/yaml.v3"
)

// TallyRuleMode defines how TSL 5.0 2-bit lamp states map to Red (PGM) and Green (PVW) booleans.
type TallyRuleMode string

const (
	// RuleAnyColor triggers Red if any of RH/Text/LH is Red(1) or Amber(3),
	// and Green if any of RH/Text/LH is Green(2) or Amber(3).
	RuleAnyColor TallyRuleMode = "any_color"

	// RuleRHPgmLHPvw maps RH Tally (non-off) -> Red (PGM), LH Tally (non-off) -> Green (PVW).
	// Ideal for switchers/gateways using TSL 3.1-style Tally1=PGM, Tally2=PVW semantics over V5.
	RuleRHPgmLHPvw TallyRuleMode = "rh_pgm_lh_pvw"

	// RuleLHPgmRHPvw maps LH Tally (non-off) -> Red (PGM), RH Tally (non-off) -> Green (PVW).
	RuleLHPgmRHPvw TallyRuleMode = "lh_pgm_rh_pvw"

	// RuleRHOnly evaluates only RH Tally (1=Red, 2=Green, 3=Red+Green).
	RuleRHOnly TallyRuleMode = "rh_only"

	// RuleLHOnly evaluates only LH Tally (1=Red, 2=Green, 3=Red+Green).
	RuleLHOnly TallyRuleMode = "lh_only"

	// RuleTextOnly evaluates only Text Tally (1=Red, 2=Green, 3=Red+Green).
	RuleTextOnly TallyRuleMode = "text_only"
)

// TallyRuleConfig controls boolean extraction and polarity per channel or globally.
type TallyRuleConfig struct {
	Mode            TallyRuleMode `yaml:"mode" json:"mode"`
	PGMOverridesPVW bool          `yaml:"pgm_overrides_pvw" json:"pgm_overrides_pvw"`
	InvertRed       bool          `yaml:"invert_red" json:"invert_red"`
	InvertGreen     bool          `yaml:"invert_green" json:"invert_green"`
}

// ChannelConfig maps a TSL 5.0 (Screen, Index) address to a pair of NMOS IS-07 boolean sources (Red & Green).
type ChannelConfig struct {
	ID          int              `yaml:"id" json:"id"`
	Name        string           `yaml:"name" json:"name"`
	Screen      uint16           `yaml:"screen" json:"screen"`
	MatchAllScr bool             `yaml:"match_all_screens" json:"match_all_screens"`
	Index       uint16           `yaml:"index" json:"index"`
	Rule        *TallyRuleConfig `yaml:"rule,omitempty" json:"rule,omitempty"`
}

// GPOBindingConfig binds a local Tally channel+color to a specific IS-07 Receiver (e.g., Riedel NSA-010C GPO 1..16).
type GPOBindingConfig struct {
	ReceiverID    string `yaml:"receiver_id" json:"receiver_id"`
	ReceiverLabel string `yaml:"receiver_label" json:"receiver_label"`
	ChannelID     int    `yaml:"channel_id" json:"channel_id"`
	Color         string `yaml:"color" json:"color"` // "red" or "green"
	Enabled       bool   `yaml:"enabled" json:"enabled"`
}

// DirectTargetConfig represents an NMOS IS-07-GPIO device (such as Riedel StageLink NSA-010C)
// managed directly via IS-05 when operating standalone or in hybrid mode.
type DirectTargetConfig struct {
	ID                  string             `yaml:"id" json:"id"`
	Label               string             `yaml:"label" json:"label"`
	BaseURL             string             `yaml:"base_url" json:"base_url"` // e.g., "http://192.168.1.50:80"
	Enabled             bool               `yaml:"enabled" json:"enabled"`
	AutoHealIntervalSec int                `yaml:"auto_heal_interval_sec" json:"auto_heal_interval_sec"`
	Bindings            []GPOBindingConfig `yaml:"bindings" json:"bindings"`
}

// NodeConfig configures local network identity and HTTP/WebSocket endpoint binding.
type NodeConfig struct {
	IDSeed        string `yaml:"id_seed" json:"id_seed"`
	Label         string `yaml:"label" json:"label"`
	Description   string `yaml:"description" json:"description"`
	HostIP        string `yaml:"host_ip" json:"host_ip"`             // "auto" or explicit IP advertised in NMOS URLs
	BindAddr      string `yaml:"bind_addr" json:"bind_addr"`         // e.g., "0.0.0.0"
	HTTPPort      int    `yaml:"http_port" json:"http_port"`         // Default 8080
	InterfaceName string `yaml:"interface_name" json:"interface_name"` // Optional NIC name for IS-04 MAC discovery
}

// TSLInputConfig configures the TSL UMD v5.0 transport receiver/connector.
type TSLInputConfig struct {
	TransportMode string          `yaml:"transport_mode" json:"transport_mode"` // "listen", "tcp_client", "udp_client"
	ListenAddr    string          `yaml:"listen_addr" json:"listen_addr"`       // Default "0.0.0.0:8901"
	RemoteAddr    string          `yaml:"remote_addr" json:"remote_addr"`       // Switcher IP:Port when switcher is Listener
	DefaultRule   TallyRuleConfig `yaml:"default_rule" json:"default_rule"`
	TimeoutMs     int             `yaml:"timeout_ms" json:"timeout_ms"`         // 0 = disabled
	TimeoutAction string          `yaml:"timeout_action" json:"timeout_action"` // "hold" or "clear"
}

// NMOSConfig configures IS-04 discovery/registration behavior.
type NMOSConfig struct {
	RegistryMode         string `yaml:"registry_mode" json:"registry_mode"` // "auto", "static", "disabled"
	StaticRegistryURL    string `yaml:"static_registry_url" json:"static_registry_url"`
	MDNSEnabled          bool   `yaml:"mdns_enabled" json:"mdns_enabled"`
	HeartbeatIntervalSec int    `yaml:"heartbeat_interval_sec" json:"heartbeat_interval_sec"`
}

// TSLOutputConfig configures outbound TSL UMD v5.0 / v3.1 transmission for NMOS -> TSL mode
// (e.g., feeding SimplyLive ViBox TSL_Input on UDP port 9001).
type TSLOutputConfig struct {
	Enabled          bool   `yaml:"enabled" json:"enabled"`
	Transport        string `yaml:"transport" json:"transport"`                   // "udp", "tcp_client", "tcp_server"
	TargetAddr       string `yaml:"target_addr" json:"target_addr"`               // e.g. "192.168.42.15:9001" or "0.0.0.0:8902"
	Protocol         string `yaml:"protocol" json:"protocol"`                     // "v5.0" or "v3.1"
	UseDLEFraming    bool   `yaml:"use_dle_framing" json:"use_dle_framing"`       // Wrap TSL 5.0 packets with DLE/STX
	UTF16            bool   `yaml:"utf16" json:"utf16"`                           // Encode TSL 5.0 UMD text as UTF-16LE
	LampMapping      string `yaml:"lamp_mapping" json:"lamp_mapping"`             // "lh_red_rh_green", "rh_red_lh_green", "both_color"
	TextFollowsTally bool   `yaml:"text_follows_tally" json:"text_follows_tally"` // Illuminate TextTally along with lamp tally
	Brightness       uint8  `yaml:"brightness" json:"brightness"`                 // 0..3 (default 3)
	RepeatIntervalMs int    `yaml:"repeat_interval_ms" json:"repeat_interval_ms"` // Periodic state refresh in ms (0 = on-change only)
}

// GPIBindingConfig maps a remote NMOS IS-07 Boolean Sender (e.g. Riedel NSA-010C GPInput 1..16)
// onto an outbound TSL (Screen, Index) display and tally role.
type GPIBindingConfig struct {
	ID          string `yaml:"id" json:"id"`
	SenderID    string `yaml:"sender_id" json:"sender_id"`
	SenderLabel string `yaml:"sender_label" json:"sender_label"`
	SourceID    string `yaml:"source_id,omitempty" json:"source_id,omitempty"`
	Screen      uint16 `yaml:"screen" json:"screen"`
	Index       uint16 `yaml:"index" json:"index"`
	Role        string `yaml:"role" json:"role"` // "red", "green", "amber"
	UMDText     string `yaml:"umd_text,omitempty" json:"umd_text,omitempty"`
	Invert      bool   `yaml:"invert" json:"invert"`
	Enabled     bool   `yaml:"enabled" json:"enabled"`
}

// DirectSourceConfig represents an NMOS IS-07-GPIO input device (such as Riedel StageLink NSA-010C's 16 GPIs)
// subscribed via IS-07 WebSocket to generate outbound TSL tally messages.
type DirectSourceConfig struct {
	ID       string             `yaml:"id" json:"id"`
	Label    string             `yaml:"label" json:"label"`
	BaseURL  string             `yaml:"base_url" json:"base_url"`
	Enabled  bool               `yaml:"enabled" json:"enabled"`
	Bindings []GPIBindingConfig `yaml:"bindings" json:"bindings"`
}

// NMOSToTSLConfig configures the reverse NMOS IS-07 -> TSL gateway pipeline.
type NMOSToTSLConfig struct {
	Output              TSLOutputConfig      `yaml:"output" json:"output"`
	ExposeIS04Receivers bool                 `yaml:"expose_is04_receivers" json:"expose_is04_receivers"`
	DirectSources       []DirectSourceConfig `yaml:"direct_sources" json:"direct_sources"`
}

// Config is the root configuration document for TSL2NMOS.
type Config struct {
	Node          NodeConfig           `yaml:"node" json:"node"`
	TSL           TSLInputConfig       `yaml:"tsl" json:"tsl"`
	NMOS          NMOSConfig           `yaml:"nmos" json:"nmos"`
	Channels      []ChannelConfig      `yaml:"channels" json:"channels"`
	DirectTargets []DirectTargetConfig `yaml:"direct_targets" json:"direct_targets"`
	NMOSToTSL     NMOSToTSLConfig      `yaml:"nmos_to_tsl" json:"nmos_to_tsl"`
}

// DefaultConfig generates a sensible studio default configuration (8 cameras = 16 Red/Green tally flows,
// matching 1x Riedel StageLink NSA-010C's 16 GPOs out of the box, expandable up to 32+ cameras).
func DefaultConfig() Config {
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "TSL2NMOS-gateway"
	}

	channels := make([]ChannelConfig, 0, 8)
	for i := 1; i <= 8; i++ {
		channels = append(channels, ChannelConfig{
			ID:          i,
			Name:        fmt.Sprintf("CAM %d", i),
			Screen:      0,
			MatchAllScr: true,
			Index:       uint16(i),
		})
	}

	return Config{
		Node: NodeConfig{
			IDSeed:      fmt.Sprintf("TSL2NMOS-%s", hostname),
			Label:       "TSL2NMOS - TSL 5.0 to NMOS IS-07 Tally Gateway",
			Description: "Lightweight TSL UMD v5.0 to AMWA NMOS IS-07 Event & Tally Gateway",
			HostIP:      "auto",
			BindAddr:    "0.0.0.0",
			HTTPPort:    8080,
		},
		TSL: TSLInputConfig{
			TransportMode: "listen",
			ListenAddr:    "0.0.0.0:8901",
			RemoteAddr:    "",
			DefaultRule: TallyRuleConfig{
				Mode:            RuleAnyColor,
				PGMOverridesPVW: false,
				InvertRed:       false,
				InvertGreen:     false,
			},
			TimeoutMs:     0,
			TimeoutAction: "hold",
		},
		NMOS: NMOSConfig{
			RegistryMode:         "auto",
			StaticRegistryURL:    "",
			MDNSEnabled:          true,
			HeartbeatIntervalSec: 5,
		},
		Channels:      channels,
		DirectTargets: []DirectTargetConfig{},
		NMOSToTSL: NMOSToTSLConfig{
			Output: TSLOutputConfig{
				Enabled:          false,
				Transport:        "udp",
				TargetAddr:       "",
				Protocol:         "v5.0",
				UseDLEFraming:    false,
				UTF16:            false,
				LampMapping:      "lh_red_rh_green",
				TextFollowsTally: false,
				Brightness:       3,
				RepeatIntervalMs: 1000,
			},
			ExposeIS04Receivers: true,
			DirectSources:       []DirectSourceConfig{},
		},
	}
}

// Manager provides thread-safe reading, validation, and atomic disk persistence of Config.
type Manager struct {
	mu   sync.RWMutex
	path string
	cfg  Config
}

// LoadOrCreate loads configuration from path, or creates a default configuration file if missing.
func LoadOrCreate(path string) (*Manager, error) {
	m := &Manager{path: path}
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			cfg := DefaultConfig()
			// Add a random suffix to id_seed when first generating a new config file so multiple
			// gateways on the same LAN with identical container hostnames never collide.
			var rnd [4]byte
			_, _ = rand.Read(rnd[:])
			cfg.Node.IDSeed = fmt.Sprintf("%s-%s", cfg.Node.IDSeed, hex.EncodeToString(rnd[:]))
			if err := ValidateAndNormalize(&cfg); err != nil {
				return nil, err
			}
			m.cfg = cfg
			if saveErr := m.Save(cfg); saveErr != nil {
				return nil, fmt.Errorf("create default config %s: %w", path, saveErr)
			}
			return m, nil
		}
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}

	cfg := DefaultConfig()
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse yaml %s: %w", path, err)
	}
	if err := ValidateAndNormalize(&cfg); err != nil {
		return nil, err
	}
	m.cfg = cfg
	return m, nil
}

// Get returns a deep copy of the active Config.
func (m *Manager) Get() Config {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return cloneConfig(m.cfg)
}

// Path returns the backing YAML file path.
func (m *Manager) Path() string {
	return m.path
}

// Save validates, updates in-memory state, and atomically writes cfg to disk.
func (m *Manager) Save(cfg Config) error {
	if err := ValidateAndNormalize(&cfg); err != nil {
		return err
	}

	out, err := yaml.Marshal(&cfg)
	if err != nil {
		return fmt.Errorf("marshal yaml: %w", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.path != "" {
		dir := filepath.Dir(m.path)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("mkdir %s: %w", dir, err)
		}
		tmp := m.path + ".tmp"
		if err := os.WriteFile(tmp, out, 0o644); err != nil {
			return fmt.Errorf("write temp config: %w", err)
		}
		if err := os.Rename(tmp, m.path); err != nil {
			_ = os.Remove(tmp)
			return fmt.Errorf("rename temp config: %w", err)
		}
	}

	m.cfg = cloneConfig(cfg)
	return nil
}

// ValidateAndNormalize checks invariants and populates defaults.
func ValidateAndNormalize(cfg *Config) error {
	if strings.TrimSpace(cfg.Node.IDSeed) == "" {
		cfg.Node.IDSeed = "TSL2NMOS-default-seed"
	}
	if strings.TrimSpace(cfg.Node.Label) == "" {
		cfg.Node.Label = "TSL2NMOS - TSL 5.0 to NMOS IS-07 Gateway"
	}
	if strings.TrimSpace(cfg.Node.BindAddr) == "" {
		cfg.Node.BindAddr = "0.0.0.0"
	}
	if cfg.Node.HTTPPort <= 0 || cfg.Node.HTTPPort > 65535 {
		cfg.Node.HTTPPort = 8080
	}
	if strings.TrimSpace(cfg.Node.HostIP) == "" {
		cfg.Node.HostIP = "auto"
	}
	switch cfg.TSL.TransportMode {
	case "listen", "tcp_client", "udp_client":
	default:
		cfg.TSL.TransportMode = "listen"
	}
	if strings.TrimSpace(cfg.TSL.ListenAddr) == "" {
		cfg.TSL.ListenAddr = "0.0.0.0:8901"
	}
	cfg.TSL.RemoteAddr = strings.TrimSpace(cfg.TSL.RemoteAddr)
	if (cfg.TSL.TransportMode == "tcp_client" || cfg.TSL.TransportMode == "udp_client") && cfg.TSL.RemoteAddr != "" {
		if !strings.Contains(cfg.TSL.RemoteAddr, ":") {
			// Default to standard TSL 5.0 port 8901 if user entered bare switcher IP!
			cfg.TSL.RemoteAddr = net.JoinHostPort(cfg.TSL.RemoteAddr, "8901")
		}
	}
	if cfg.TSL.DefaultRule.Mode == "" {
		cfg.TSL.DefaultRule.Mode = RuleAnyColor
	}
	if cfg.TSL.TimeoutAction != "clear" {
		cfg.TSL.TimeoutAction = "hold"
	}
	switch cfg.NMOS.RegistryMode {
	case "auto", "static", "disabled":
	default:
		cfg.NMOS.RegistryMode = "auto"
	}
	if cfg.NMOS.HeartbeatIntervalSec <= 0 {
		cfg.NMOS.HeartbeatIntervalSec = 5
	}

	seenID := make(map[int]bool, len(cfg.Channels))
	for i := range cfg.Channels {
		ch := &cfg.Channels[i]
		if ch.ID <= 0 {
			return fmt.Errorf("channel[%d]: id must be >= 1", i)
		}
		if seenID[ch.ID] {
			return fmt.Errorf("duplicate channel id %d", ch.ID)
		}
		seenID[ch.ID] = true
		if strings.TrimSpace(ch.Name) == "" {
			ch.Name = fmt.Sprintf("CAM %d", ch.ID)
		}
	}

	for i := range cfg.DirectTargets {
		t := &cfg.DirectTargets[i]
		t.BaseURL = strings.TrimRight(strings.TrimSpace(t.BaseURL), "/")
		if t.ID == "" {
			t.ID = fmt.Sprintf("target-%d", i+1)
		}
		if t.Label == "" {
			t.Label = t.BaseURL
		}
		if t.AutoHealIntervalSec <= 0 {
			t.AutoHealIntervalSec = 10
		}
		for j := range t.Bindings {
			b := &t.Bindings[j]
			b.Color = strings.ToLower(strings.TrimSpace(b.Color))
			if b.Color != "red" && b.Color != "green" {
				return fmt.Errorf("target %s binding[%d]: color must be 'red' or 'green'", t.ID, j)
			}
		}
	}

	outCfg := &cfg.NMOSToTSL.Output
	switch outCfg.Transport {
	case "udp", "tcp_client", "tcp_server":
	default:
		outCfg.Transport = "udp"
	}
	outCfg.TargetAddr = strings.TrimSpace(outCfg.TargetAddr)
	if outCfg.TargetAddr != "" && !strings.Contains(outCfg.TargetAddr, ":") {
		// Default bare IP to port 9001 (SimplyLive ViBox TSL_Input default; user can specify any :port).
		outCfg.TargetAddr = net.JoinHostPort(outCfg.TargetAddr, "9001")
	}
	switch outCfg.Protocol {
	case "v5.0", "v3.1":
	default:
		outCfg.Protocol = "v5.0"
	}
	switch outCfg.LampMapping {
	case "lh_red_rh_green", "rh_red_lh_green", "both_color":
	default:
		outCfg.LampMapping = "lh_red_rh_green"
	}
	if outCfg.Brightness > 3 {
		outCfg.Brightness = 3
	}
	if outCfg.RepeatIntervalMs < 0 {
		outCfg.RepeatIntervalMs = 0
	}

	for i := range cfg.NMOSToTSL.DirectSources {
		src := &cfg.NMOSToTSL.DirectSources[i]
		src.BaseURL = strings.TrimRight(strings.TrimSpace(src.BaseURL), "/")
		if src.ID == "" {
			src.ID = fmt.Sprintf("source-%d", i+1)
		}
		if src.Label == "" {
			src.Label = src.BaseURL
		}
		for j := range src.Bindings {
			b := &src.Bindings[j]
			if b.ID == "" {
				if b.SenderID != "" {
					b.ID = fmt.Sprintf("%s:%s", src.ID, b.SenderID)
				} else {
					b.ID = fmt.Sprintf("%s:gpi-%d", src.ID, j+1)
				}
			}
			b.Role = strings.ToLower(strings.TrimSpace(b.Role))
			switch b.Role {
			case "red", "green", "amber":
			default:
				b.Role = "red"
			}
		}
	}
	return nil
}

// ResolveHostIP determines the best IPv4 address to advertise in NMOS URLs.
// If targetIP is non-empty, it uses UDP routing table lookup to pick the exact local NIC IP
// reachable from that target (e.g., Riedel NSA-010C on a specific media VLAN!).
func ResolveHostIP(configuredIP, interfaceName, targetHost string) string {
	if configuredIP != "" && configuredIP != "auto" {
		return configuredIP
	}

	if interfaceName != "" {
		if iface, err := net.InterfaceByName(interfaceName); err == nil {
			if ip := firstIPv4FromInterface(iface); ip != "" {
				return ip
			}
		}
	}

	if targetHost != "" {
		host := targetHost
		if h, _, err := net.SplitHostPort(targetHost); err == nil {
			host = h
		}
		if parsed := net.ParseIP(host); parsed != nil && !parsed.IsLoopback() {
			if conn, err := net.Dial("udp4", net.JoinHostPort(parsed.String(), "80")); err == nil {
				localAddr := conn.LocalAddr().(*net.UDPAddr)
				_ = conn.Close()
				if localAddr.IP != nil && !localAddr.IP.IsLoopback() {
					return localAddr.IP.String()
				}
			}
		}
	}

	ifaces, err := net.Interfaces()
	if err == nil {
		for _, iface := range ifaces {
			if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
				continue
			}
			if ip := firstIPv4FromInterface(&iface); ip != "" {
				return ip
			}
		}
	}

	return "127.0.0.1"
}

func firstIPv4FromInterface(iface *net.Interface) string {
	addrs, err := iface.Addrs()
	if err != nil {
		return ""
	}
	for _, a := range addrs {
		var ip net.IP
		switch v := a.(type) {
		case *net.IPNet:
			ip = v.IP
		case *net.IPAddr:
			ip = v.IP
		}
		if ip == nil || ip.IsLoopback() {
			continue
		}
		if v4 := ip.To4(); v4 != nil {
			return v4.String()
		}
	}
	return ""
}

func cloneConfig(in Config) Config {
	out := in
	out.Channels = make([]ChannelConfig, len(in.Channels))
	for i, ch := range in.Channels {
		out.Channels[i] = ch
		if ch.Rule != nil {
			r := *ch.Rule
			out.Channels[i].Rule = &r
		}
	}
	out.DirectTargets = make([]DirectTargetConfig, len(in.DirectTargets))
	for i, dt := range in.DirectTargets {
		out.DirectTargets[i] = dt
		out.DirectTargets[i].Bindings = make([]GPOBindingConfig, len(dt.Bindings))
		copy(out.DirectTargets[i].Bindings, dt.Bindings)
	}
	out.NMOSToTSL.DirectSources = make([]DirectSourceConfig, len(in.NMOSToTSL.DirectSources))
	for i, ds := range in.NMOSToTSL.DirectSources {
		out.NMOSToTSL.DirectSources[i] = ds
		out.NMOSToTSL.DirectSources[i].Bindings = make([]GPIBindingConfig, len(ds.Bindings))
		copy(out.NMOSToTSL.DirectSources[i].Bindings, ds.Bindings)
	}
	return out
}
