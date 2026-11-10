package tsl

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"tsl2nmos/internal/config"
)

// TxStats exposes runtime telemetry for the outbound TSL UMD v5.0 / v3.1 transmitter (NMOS -> TSL).
type TxStats struct {
	Enabled           bool       `json:"enabled"`
	Transport         string     `json:"transport"`
	TargetAddr        string     `json:"target_addr"`
	Protocol          string     `json:"protocol"`
	Connected         bool       `json:"connected"`
	ConnectionState   string     `json:"connection_state"`
	ConnectionStateEn string     `json:"connection_state_en"`
	TotalPackets      uint64     `json:"total_packets"`
	TotalMessages     uint64     `json:"total_messages"`
	SendErrors        uint64     `json:"send_errors"`
	LastSentTime      *time.Time `json:"last_sent_time,omitempty"`
}

type screenIndexKey struct {
	screen uint16
	index  uint16
}

// Transmitter encodes and sends TSL UMD v5.0 / v3.1 packets over UDP, TCP Client, or TCP Server.
type Transmitter struct {
	log *slog.Logger

	totalPackets  atomic.Uint64
	totalMessages atomic.Uint64
	sendErrors    atomic.Uint64

	mu            sync.RWMutex
	parentCtx     context.Context
	loopCancel    context.CancelFunc
	activeClosers []io.Closer
	cfg           config.TSLOutputConfig

	udpConn    *net.UDPConn
	udpDst     *net.UDPAddr
	tcpPeers   map[net.Conn]struct{}
	connected  bool
	connState  string
	connStateE string
	lastSent   time.Time

	// Authoritative display state table for periodic refresh & new TCP client sync.
	displays map[screenIndexKey]DMSG
}

// NewTransmitter constructs a TSL Transmitter for NMOS -> TSL output.
func NewTransmitter(cfg config.TSLOutputConfig, logger *slog.Logger) *Transmitter {
	if logger == nil {
		logger = slog.Default()
	}
	return &Transmitter{
		log:        logger,
		cfg:        cfg,
		tcpPeers:   make(map[net.Conn]struct{}),
		displays:   make(map[screenIndexKey]DMSG),
		connState:  "未启用",
		connStateE: "Disabled",
	}
}

// Start launches the configured outbound transport and periodic refresh workers until ctx is cancelled.
func (t *Transmitter) Start(ctx context.Context) error {
	t.mu.Lock()
	t.parentCtx = ctx
	cfg := t.cfg
	t.mu.Unlock()
	return t.ApplyConfig(cfg)
}

// ApplyConfig hot-reloads the outbound TSL transport/protocol settings without restarting the gateway.
func (t *Transmitter) ApplyConfig(next config.TSLOutputConfig) error {
	t.mu.Lock()
	parentCtx := t.parentCtx
	if parentCtx == nil {
		parentCtx = context.Background()
	}
	oldCancel := t.loopCancel
	oldClosers := t.activeClosers
	oldPeers := make([]net.Conn, 0, len(t.tcpPeers))
	for c := range t.tcpPeers {
		oldPeers = append(oldPeers, c)
	}
	t.activeClosers = nil
	t.tcpPeers = make(map[net.Conn]struct{})
	t.udpConn = nil
	t.udpDst = nil
	t.cfg = next
	t.mu.Unlock()

	if oldCancel != nil {
		oldCancel()
	}
	for _, c := range oldClosers {
		_ = c.Close()
	}
	for _, p := range oldPeers {
		_ = p.Close()
	}

	if !next.Enabled {
		t.mu.Lock()
		t.loopCancel = nil
		t.connected = false
		t.connState = "未启用 (点击开启 TSL 输出)"
		t.connStateE = "Disabled (Enable TSL Output to transmit)"
		t.mu.Unlock()
		return nil
	}

	if next.TargetAddr == "" {
		t.mu.Lock()
		t.loopCancel = nil
		t.connected = false
		t.connState = "未填写目标接收端 IP:端口"
		t.connStateE = "Target IP:Port not configured"
		t.mu.Unlock()
		return nil
	}

	loopCtx, loopCancel := context.WithCancel(parentCtx)

	switch next.Transport {
	case "tcp_client":
		t.mu.Lock()
		t.loopCancel = loopCancel
		t.connected = false
		t.connState = "正在连接 TSL TCP 接收端: " + next.TargetAddr
		t.connStateE = "Connecting to TSL TCP receiver: " + next.TargetAddr
		t.mu.Unlock()

		go t.runTCPClientLoop(loopCtx, next.TargetAddr)
		if next.RepeatIntervalMs > 0 {
			go t.runRepeatLoop(loopCtx, time.Duration(next.RepeatIntervalMs)*time.Millisecond)
		}
		return nil

	case "tcp_server":
		ln, err := net.Listen("tcp", next.TargetAddr)
		if err != nil {
			loopCancel()
			t.mu.Lock()
			t.connected = false
			t.connState = "TCP 监听失败: " + err.Error()
			t.connStateE = "TCP listen failed: " + err.Error()
			t.mu.Unlock()
			return err
		}
		bound := ln.Addr().String()
		t.mu.Lock()
		t.loopCancel = loopCancel
		t.activeClosers = []io.Closer{ln}
		t.connected = true
		t.connState = fmt.Sprintf("TCP 服务端就绪 (%s, 已连 0 台)", bound)
		t.connStateE = fmt.Sprintf("TCP Server ready (%s, 0 clients)", bound)
		t.mu.Unlock()

		go func() {
			<-loopCtx.Done()
			_ = ln.Close()
		}()
		go t.acceptTCPServerLoop(loopCtx, ln, bound)
		if next.RepeatIntervalMs > 0 {
			go t.runRepeatLoop(loopCtx, time.Duration(next.RepeatIntervalMs)*time.Millisecond)
		}
		return nil

	default: // "udp"
		dstAddr, err := net.ResolveUDPAddr("udp", next.TargetAddr)
		if err != nil {
			loopCancel()
			t.mu.Lock()
			t.connected = false
			t.connState = "UDP 目标地址解析失败: " + err.Error()
			t.connStateE = "UDP target resolve failed: " + err.Error()
			t.mu.Unlock()
			return err
		}
		laddr, _ := net.ResolveUDPAddr("udp", "0.0.0.0:0")
		uconn, err := net.ListenUDP("udp", laddr)
		if err != nil {
			loopCancel()
			t.mu.Lock()
			t.connected = false
			t.connState = "UDP 套接字创建失败: " + err.Error()
			t.connStateE = "UDP socket open failed: " + err.Error()
			t.mu.Unlock()
			return err
		}

		t.mu.Lock()
		t.loopCancel = loopCancel
		t.activeClosers = []io.Closer{uconn}
		t.udpConn = uconn
		t.udpDst = dstAddr
		t.connected = true
		t.connState = fmt.Sprintf("UDP 发送就绪 -> %s (%s)", dstAddr.String(), next.Protocol)
		t.connStateE = fmt.Sprintf("UDP TX ready -> %s (%s)", dstAddr.String(), next.Protocol)
		t.mu.Unlock()

		go func() {
			<-loopCtx.Done()
			_ = uconn.Close()
		}()

		// Flush initial state snapshot immediately on enable/reload.
		t.FlushSnapshot()

		if next.RepeatIntervalMs > 0 {
			go t.runRepeatLoop(loopCtx, time.Duration(next.RepeatIntervalMs)*time.Millisecond)
		}
		return nil
	}
}

// ReplaceDisplays atomically replaces the authoritative display table (called on config/mapping rebuild).
func (t *Transmitter) ReplaceDisplays(all map[uint16][]DMSG) {
	t.mu.Lock()
	nextMap := make(map[screenIndexKey]DMSG)
	for scr, list := range all {
		for _, m := range list {
			nextMap[screenIndexKey{screen: scr, index: m.Index}] = m
		}
	}
	t.displays = nextMap
	enabled := t.cfg.Enabled
	t.mu.Unlock()

	if enabled {
		t.FlushSnapshot()
	}
}

// SendDisplay updates a single (screen, index) entry and immediately transmits it on the wire.
func (t *Transmitter) SendDisplay(screen uint16, msg DMSG) {
	t.mu.Lock()
	t.displays[screenIndexKey{screen: screen, index: msg.Index}] = msg
	enabled := t.cfg.Enabled
	cfg := t.cfg
	t.mu.Unlock()

	if !enabled {
		return
	}
	t.transmitScreenMessages(cfg, screen, []DMSG{msg})
}

// FlushSnapshot transmits all currently configured displays grouped by Screen.
func (t *Transmitter) FlushSnapshot() {
	t.mu.RLock()
	if !t.cfg.Enabled || len(t.displays) == 0 {
		t.mu.RUnlock()
		return
	}
	cfg := t.cfg
	byScreen := make(map[uint16][]DMSG)
	for k, m := range t.displays {
		byScreen[k.screen] = append(byScreen[k.screen], m)
	}
	t.mu.RUnlock()

	screens := make([]int, 0, len(byScreen))
	for scr := range byScreen {
		screens = append(screens, int(scr))
	}
	sort.Ints(screens)

	for _, s := range screens {
		scr := uint16(s)
		msgs := byScreen[scr]
		sort.Slice(msgs, func(i, j int) bool { return msgs[i].Index < msgs[j].Index })
		// Batch up to 16 DMSGs per UDP datagram so MTU never fragments.
		const maxPerPkt = 16
		for start := 0; start < len(msgs); start += maxPerPkt {
			end := start + maxPerPkt
			if end > len(msgs) {
				end = len(msgs)
			}
			t.transmitScreenMessages(cfg, scr, msgs[start:end])
		}
	}
}

func (t *Transmitter) transmitScreenMessages(cfg config.TSLOutputConfig, screen uint16, msgs []DMSG) {
	if len(msgs) == 0 {
		return
	}

	isTCP := cfg.Transport == "tcp_client" || cfg.Transport == "tcp_server"

	if cfg.Protocol == "v3.1" {
		for _, m := range msgs {
			pkt := MarshalV31Packet(m)
			t.writeWireBytes(pkt[:], 1, isTCP)
		}
		return
	}

	// Standard TSL UMD v5.0: TCP streams require DLE/STX framing per TSL v5.0 specification;
	// UDP uses raw PBC datagrams by default unless UseDLEFraming is explicitly enabled.
	withDLE := cfg.UseDLEFraming || isTCP
	raw := MarshalPacket(screen, cfg.UTF16, msgs, withDLE)
	t.writeWireBytes(raw, uint64(len(msgs)), isTCP)
}

func (t *Transmitter) writeWireBytes(raw []byte, msgCount uint64, isTCP bool) {
	now := time.Now()
	if !isTCP {
		t.mu.RLock()
		uconn := t.udpConn
		udst := t.udpDst
		t.mu.RUnlock()
		if uconn == nil || udst == nil {
			return
		}
		if _, err := uconn.WriteToUDP(raw, udst); err != nil {
			t.sendErrors.Add(1)
			return
		}
		t.totalPackets.Add(1)
		t.totalMessages.Add(msgCount)
		t.mu.Lock()
		t.lastSent = now
		t.mu.Unlock()
		return
	}

	t.mu.RLock()
	peers := make([]net.Conn, 0, len(t.tcpPeers))
	for c := range t.tcpPeers {
		peers = append(peers, c)
	}
	t.mu.RUnlock()

	if len(peers) == 0 {
		return
	}

	sentAny := false
	for _, p := range peers {
		_ = p.SetWriteDeadline(time.Now().Add(2 * time.Second))
		if _, err := p.Write(raw); err != nil {
			t.sendErrors.Add(1)
			_ = p.Close()
			t.removeTCPPeer(p)
		} else {
			sentAny = true
		}
	}
	if sentAny {
		t.totalPackets.Add(1)
		t.totalMessages.Add(msgCount)
		t.mu.Lock()
		t.lastSent = now
		t.mu.Unlock()
	}
}

func (t *Transmitter) runRepeatLoop(ctx context.Context, interval time.Duration) {
	if interval < 100*time.Millisecond {
		interval = 100 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			t.FlushSnapshot()
		}
	}
}

func (t *Transmitter) runTCPClientLoop(ctx context.Context, targetAddr string) {
	dialer := net.Dialer{
		Timeout:   3 * time.Second,
		KeepAlive: 10 * time.Second,
	}

	for {
		if ctx.Err() != nil {
			return
		}
		conn, err := dialer.DialContext(ctx, "tcp", targetAddr)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			t.mu.Lock()
			t.connected = false
			t.connState = fmt.Sprintf("等待 TSL TCP 接收端就绪 (%s 重连中...)", targetAddr)
			t.connStateE = fmt.Sprintf("Waiting for TSL TCP receiver (%s reconnecting...)", targetAddr)
			t.mu.Unlock()

			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
				continue
			}
		}

		t.mu.Lock()
		t.tcpPeers[conn] = struct{}{}
		t.connected = true
		t.connState = "已连接 TSL TCP 接收端: " + targetAddr
		t.connStateE = "Connected to TSL TCP receiver: " + targetAddr
		t.mu.Unlock()

		// Immediately push current state snapshot upon connecting!
		t.FlushSnapshot()

		// Monitor remote EOF/disconnect.
		done := make(chan struct{})
		go func() {
			select {
			case <-ctx.Done():
				_ = conn.Close()
			case <-done:
			}
		}()

		var discard [256]byte
		for {
			if _, rerr := conn.Read(discard[:]); rerr != nil {
				break
			}
		}
		close(done)
		t.removeTCPPeer(conn)
		_ = conn.Close()

		if ctx.Err() != nil {
			return
		}
		t.mu.Lock()
		t.connected = false
		t.connState = fmt.Sprintf("TSL TCP 接收端断开，2s 后重连 (%s)", targetAddr)
		t.connStateE = fmt.Sprintf("TSL TCP receiver disconnected, retrying in 2s (%s)", targetAddr)
		t.mu.Unlock()

		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
}

func (t *Transmitter) acceptTCPServerLoop(ctx context.Context, ln net.Listener, bound string) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			continue
		}
		t.mu.Lock()
		t.tcpPeers[conn] = struct{}{}
		peerCount := len(t.tcpPeers)
		t.connState = fmt.Sprintf("TCP 服务端就绪 (%s, 已连 %d 台)", bound, peerCount)
		t.connStateE = fmt.Sprintf("TCP Server ready (%s, %d client(s))", bound, peerCount)
		t.mu.Unlock()

		t.FlushSnapshot()

		go func(c net.Conn) {
			done := make(chan struct{})
			go func() {
				select {
				case <-ctx.Done():
					_ = c.Close()
				case <-done:
				}
			}()
			var buf [256]byte
			for {
				if _, rerr := c.Read(buf[:]); rerr != nil {
					break
				}
			}
			close(done)
			_ = c.Close()
			t.removeTCPPeer(c)
			t.mu.Lock()
			if t.cfg.Transport == "tcp_server" && t.cfg.Enabled {
				cnt := len(t.tcpPeers)
				t.connState = fmt.Sprintf("TCP 服务端就绪 (%s, 已连 %d 台)", bound, cnt)
				t.connStateE = fmt.Sprintf("TCP Server ready (%s, %d client(s))", bound, cnt)
			}
			t.mu.Unlock()
		}(conn)
	}
}

func (t *Transmitter) removeTCPPeer(c net.Conn) {
	t.mu.Lock()
	delete(t.tcpPeers, c)
	t.mu.Unlock()
}

// GetStats returns a thread-safe snapshot of outbound TSL transmitter statistics.
func (t *Transmitter) GetStats() TxStats {
	t.mu.RLock()
	defer t.mu.RUnlock()

	st := TxStats{
		Enabled:           t.cfg.Enabled,
		Transport:         t.cfg.Transport,
		TargetAddr:        t.cfg.TargetAddr,
		Protocol:          t.cfg.Protocol,
		Connected:         t.connected,
		ConnectionState:   t.connState,
		ConnectionStateEn: t.connStateE,
		TotalPackets:      t.totalPackets.Load(),
		TotalMessages:     t.totalMessages.Load(),
		SendErrors:        t.sendErrors.Load(),
	}
	if !t.lastSent.IsZero() {
		ts := t.lastSent
		st.LastSentTime = &ts
	}
	return st
}
