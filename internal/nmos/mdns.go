package nmos

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/net/dns/dnsmessage"
	"golang.org/x/net/ipv4"
)

const (
	mdnsMulticastIPv4 = "224.0.0.251:5353"
	serviceNMOSNode   = "_nmos-node._tcp.local."
	serviceNMOSReg    = "_nmos-register._tcp.local."
)

// DiscoveredService represents an NMOS Node (e.g., Riedel StageLink NSA-010C) or Registry found via mDNS.
type DiscoveredService struct {
	Instance string            `json:"instance"`
	Service  string            `json:"service"`
	Host     string            `json:"host"`
	IP       string            `json:"ip"`
	Port     int               `json:"port"`
	Priority int               `json:"priority"`
	TXT      map[string]string `json:"txt"`
	BaseURL  string            `json:"base_url"`
	SeenAt   time.Time         `json:"seen_at"`
}

// MDNSService implements lightweight RFC 6762 / RFC 6763 multicast DNS advertising for _nmos-node._tcp
// and active discovery of _nmos-register._tcp (IS-04 Registries) and _nmos-node._tcp (IS-07 GPIO Peers).
type MDNSService struct {
	store *Store
	log   *slog.Logger

	mu         sync.RWMutex
	conn       *net.UDPConn
	discovered map[string]DiscoveredService // key: service + "|" + instance
}

// NewMDNSService creates an mDNS responder & browser bound to Store.
func NewMDNSService(store *Store, logger *slog.Logger) *MDNSService {
	if logger == nil {
		logger = slog.Default()
	}
	return &MDNSService{
		store:      store,
		log:        logger,
		discovered: make(map[string]DiscoveredService),
	}
}

// Start opens UDP multicast 224.0.0.251:5353 with SO_REUSEADDR so it coexists cleanly with
// system mDNS daemons (Avahi / systemd-resolved), answers _nmos-node._tcp queries, and scans peers.
func (m *MDNSService) Start(ctx context.Context) error {
	lc := net.ListenConfig{
		Control: func(_, _ string, c syscall.RawConn) error {
			return setSocketReuseAddr(c)
		},
	}

	pc, err := lc.ListenPacket(ctx, "udp4", "0.0.0.0:5353")
	if err != nil {
		m.log.Warn("mDNS UDP :5353 bind unavailable, continuing without multicast DNS", "err", err)
		return nil
	}

	udpConn := pc.(*net.UDPConn)
	p4 := ipv4.NewPacketConn(udpConn)
	mcastIP := net.IPv4(224, 0, 0, 251)

	ifaces, _ := net.Interfaces()
	joined := 0
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagMulticast == 0 {
			continue
		}
		if err := p4.JoinGroup(&iface, &net.UDPAddr{IP: mcastIP}); err == nil {
			joined++
		}
	}

	m.mu.Lock()
	m.conn = udpConn
	m.mu.Unlock()

	m.log.Info("NMOS mDNS responder & scanner active", "multicast_interfaces", joined)

	go func() {
		<-ctx.Done()
		_ = udpConn.Close()
	}()

	go m.readLoop(ctx, udpConn)
	go m.periodicAnnounceAndBrowse(ctx)

	return nil
}

// AnnounceNow broadcasts an unsolicited mDNS announcement reflecting updated ver_* TXT records.
func (m *MDNSService) AnnounceNow() {
	m.mu.RLock()
	conn := m.conn
	m.mu.RUnlock()
	if conn == nil {
		return
	}
	pkt, err := m.buildNodeAnnouncement()
	if err != nil {
		return
	}
	dst, _ := net.ResolveUDPAddr("udp4", mdnsMulticastIPv4)
	_, _ = conn.WriteToUDP(pkt, dst)
}

// TriggerScan sends mDNS PTR queries for both _nmos-register._tcp.local. and _nmos-node._tcp.local.
func (m *MDNSService) TriggerScan() {
	m.mu.RLock()
	conn := m.conn
	m.mu.RUnlock()
	if conn == nil {
		return
	}

	b := dnsmessage.NewBuilder(nil, dnsmessage.Header{})
	_ = b.StartQuestions()
	_ = b.Question(dnsmessage.Question{
		Name:  mustDNSName(serviceNMOSReg),
		Type:  dnsmessage.TypePTR,
		Class: dnsmessage.ClassINET,
	})
	_ = b.Question(dnsmessage.Question{
		Name:  mustDNSName(serviceNMOSNode),
		Type:  dnsmessage.TypePTR,
		Class: dnsmessage.ClassINET,
	})
	pkt, err := b.Finish()
	if err != nil {
		return
	}
	dst, _ := net.ResolveUDPAddr("udp4", mdnsMulticastIPv4)
	_, _ = conn.WriteToUDP(pkt, dst)
}

// DiscoveredRegistries returns discovered IS-04 Registration APIs sorted by priority (lowest pri first).
func (m *MDNSService) DiscoveredRegistries() []DiscoveredService {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var list []DiscoveredService
	cutoff := time.Now().Add(-2 * time.Minute)
	for _, svc := range m.discovered {
		if svc.Service == serviceNMOSReg && svc.SeenAt.After(cutoff) && svc.IP != "" && svc.Port > 0 {
			list = append(list, svc)
		}
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Priority < list[j].Priority })
	return list
}

// DiscoveredNodes returns other NMOS Nodes discovered on the LAN (excluding this gateway itself).
func (m *MDNSService) DiscoveredNodes() []DiscoveredService {
	node, _, _, _, _ := m.store.SnapshotResources()
	selfHostIP, selfPort := m.store.HostEndpoint()

	m.mu.RLock()
	defer m.mu.RUnlock()

	var list []DiscoveredService
	cutoff := time.Now().Add(-5 * time.Minute)
	for _, svc := range m.discovered {
		if svc.Service != serviceNMOSNode || svc.SeenAt.Before(cutoff) {
			continue
		}
		if strings.Contains(svc.Instance, node.ID) || (svc.IP == selfHostIP && svc.Port == selfPort) {
			continue
		}
		if svc.IP != "" && svc.Port > 0 {
			list = append(list, svc)
		}
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Instance < list[j].Instance })
	return list
}

func (m *MDNSService) periodicAnnounceAndBrowse(ctx context.Context) {
	m.AnnounceNow()
	m.TriggerScan()

	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.AnnounceNow()
			m.TriggerScan()
		}
	}
}

func (m *MDNSService) readLoop(ctx context.Context, conn *net.UDPConn) {
	buf := make([]byte, 8192)
	dstMcast, _ := net.ResolveUDPAddr("udp4", mdnsMulticastIPv4)

	for {
		n, src, err := conn.ReadFromUDP(buf)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			continue
		}

		var msg dnsmessage.Message
		if err := msg.Unpack(buf[:n]); err != nil {
			continue
		}

		if !msg.Header.Response {
			// Answer incoming PTR/ANY queries for _nmos-node._tcp.local.
			for _, q := range msg.Questions {
				qName := q.Name.String()
				if strings.EqualFold(qName, serviceNMOSNode) {
					if resp, err := m.buildNodeAnnouncement(); err == nil {
						_, _ = conn.WriteToUDP(resp, dstMcast)
					}
					break
				}
			}
			continue
		}

		m.processResponse(&msg, src)
	}
}

func (m *MDNSService) processResponse(msg *dnsmessage.Message, src *net.UDPAddr) {
	allRRs := append([]dnsmessage.Resource{}, msg.Answers...)
	allRRs = append(allRRs, msg.Additionals...)

	hostIPs := make(map[string]string)
	for _, rr := range allRRs {
		if a, ok := rr.Body.(*dnsmessage.AResource); ok {
			ip := net.IP(a.A[:]).String()
			hostIPs[strings.ToLower(rr.Header.Name.String())] = ip
		}
	}

	type partial struct {
		service  string
		instance string
		target   string
		port     int
		txt      map[string]string
	}
	partials := make(map[string]*partial)
	ensure := func(inst string) *partial {
		key := strings.ToLower(inst)
		p, ok := partials[key]
		if !ok {
			p = &partial{instance: inst, txt: make(map[string]string)}
			partials[key] = p
		}
		return p
	}

	for _, rr := range allRRs {
		switch body := rr.Body.(type) {
		case *dnsmessage.PTRResource:
			svcName := rr.Header.Name.String()
			if strings.EqualFold(svcName, serviceNMOSNode) || strings.EqualFold(svcName, serviceNMOSReg) {
				p := ensure(body.PTR.String())
				if strings.EqualFold(svcName, serviceNMOSReg) {
					p.service = serviceNMOSReg
				} else {
					p.service = serviceNMOSNode
				}
			}
		case *dnsmessage.SRVResource:
			rName := rr.Header.Name.String()
			if strings.HasSuffix(strings.ToLower(rName), "."+serviceNMOSNode) {
				p := ensure(rName)
				p.service = serviceNMOSNode
				p.port = int(body.Port)
				p.target = body.Target.String()
			} else if strings.HasSuffix(strings.ToLower(rName), "."+serviceNMOSReg) {
				p := ensure(rName)
				p.service = serviceNMOSReg
				p.port = int(body.Port)
				p.target = body.Target.String()
			}
		case *dnsmessage.TXTResource:
			rName := rr.Header.Name.String()
			if strings.HasSuffix(strings.ToLower(rName), "."+serviceNMOSNode) ||
				strings.HasSuffix(strings.ToLower(rName), "."+serviceNMOSReg) {
				p := ensure(rName)
				for _, kv := range body.TXT {
					if idx := strings.IndexByte(kv, '='); idx > 0 {
						p.txt[kv[:idx]] = kv[idx+1:]
					}
				}
			}
		}
	}

	now := time.Now()
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, p := range partials {
		if p.service == "" {
			continue
		}
		key := p.service + "|" + strings.ToLower(p.instance)
		existing := m.discovered[key]
		existing.Service = p.service
		existing.Instance = strings.TrimSuffix(p.instance, "."+p.service)
		existing.SeenAt = now

		if p.target != "" {
			existing.Host = strings.TrimSuffix(p.target, ".")
			if ip, ok := hostIPs[strings.ToLower(p.target)]; ok {
				existing.IP = ip
			}
		}
		if existing.IP == "" && src != nil && src.IP != nil {
			existing.IP = src.IP.String()
		}
		if p.port > 0 {
			existing.Port = p.port
		}
		if existing.TXT == nil {
			existing.TXT = make(map[string]string)
		}
		for k, v := range p.txt {
			existing.TXT[k] = v
		}
		if priStr, ok := existing.TXT["pri"]; ok {
			if pri, err := strconv.Atoi(priStr); err == nil {
				existing.Priority = pri
			}
		} else {
			existing.Priority = 100
		}

		proto := existing.TXT["api_proto"]
		if proto == "" {
			proto = "http"
		}
		if existing.IP != "" && existing.Port > 0 {
			existing.BaseURL = fmt.Sprintf("%s://%s:%d", proto, existing.IP, existing.Port)
		}
		m.discovered[key] = existing
	}
}

func (m *MDNSService) buildNodeAnnouncement() ([]byte, error) {
	node, _, _, _, _ := m.store.SnapshotResources()
	hostIP, port := m.store.HostEndpoint()
	slf, dvc, src, flw, snd, rcv := m.store.MDNSVersions()

	ip4 := net.ParseIP(hostIP).To4()
	if ip4 == nil {
		return nil, fmt.Errorf("non-ipv4 host %s", hostIP)
	}

	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "TSL2NMOS"
	}
	cleanHost := sanitizeDNSLabel(hostname)
	fqdnHost := cleanHost + ".local."
	instanceFQDN := fmt.Sprintf("TSL2NMOS-%s.%s", node.ID[:8], serviceNMOSNode)

	txtStrings := []string{
		"api_proto=http",
		"api_ver=v1.2,v1.3",
		"api_auth=false",
		fmt.Sprintf("ver_slf=%d", slf),
		fmt.Sprintf("ver_dvc=%d", dvc),
		fmt.Sprintf("ver_src=%d", src),
		fmt.Sprintf("ver_flw=%d", flw),
		fmt.Sprintf("ver_snd=%d", snd),
		fmt.Sprintf("ver_rcv=%d", rcv),
	}

	b := dnsmessage.NewBuilder(nil, dnsmessage.Header{Response: true, Authoritative: true})
	_ = b.StartAnswers()

	_ = b.PTRResource(dnsmessage.ResourceHeader{
		Name:  mustDNSName(serviceNMOSNode),
		Type:  dnsmessage.TypePTR,
		Class: dnsmessage.ClassINET,
		TTL:   120,
	}, dnsmessage.PTRResource{PTR: mustDNSName(instanceFQDN)})

	_ = b.SRVResource(dnsmessage.ResourceHeader{
		Name:  mustDNSName(instanceFQDN),
		Type:  dnsmessage.TypeSRV,
		Class: dnsmessage.ClassINET,
		TTL:   120,
	}, dnsmessage.SRVResource{
		Priority: 0,
		Weight:   0,
		Port:     uint16(port),
		Target:   mustDNSName(fqdnHost),
	})

	_ = b.TXTResource(dnsmessage.ResourceHeader{
		Name:  mustDNSName(instanceFQDN),
		Type:  dnsmessage.TypeTXT,
		Class: dnsmessage.ClassINET,
		TTL:   120,
	}, dnsmessage.TXTResource{TXT: txtStrings})

	var aBytes [4]byte
	copy(aBytes[:], ip4)
	_ = b.AResource(dnsmessage.ResourceHeader{
		Name:  mustDNSName(fqdnHost),
		Type:  dnsmessage.TypeA,
		Class: dnsmessage.ClassINET,
		TTL:   120,
	}, dnsmessage.AResource{A: aBytes})

	return b.Finish()
}

func sanitizeDNSLabel(s string) string {
	var out strings.Builder
	for _, r := range strings.ToLower(s) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
			out.WriteRune(r)
		}
	}
	res := strings.Trim(out.String(), "-")
	if res == "" {
		return "tsl2nmos"
	}
	if len(res) > 60 {
		res = res[:60]
	}
	return res
}

func mustDNSName(s string) dnsmessage.Name {
	if !strings.HasSuffix(s, ".") {
		s += "."
	}
	n, err := dnsmessage.NewName(s)
	if err != nil {
		n, _ = dnsmessage.NewName("local.")
	}
	return n
}
