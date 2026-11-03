package tsl

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// SnifferEntry records a decoded TSL 5.0 message for real-time diagnostics in the Web UI.
type SnifferEntry struct {
	Timestamp  time.Time `json:"timestamp"`
	SourceAddr string    `json:"source_addr"`
	Screen     uint16    `json:"screen"`
	Index      uint16    `json:"index"`
	RHTally    string    `json:"rh_tally"`
	TextTally  string    `json:"text_tally"`
	LHTally    string    `json:"lh_tally"`
	Brightness uint8     `json:"brightness"`
	Text       string    `json:"text"`
	WasFramed  bool      `json:"was_framed"`
}

// Stats exposes runtime telemetry for the TSL v5.0 transport layer.
type Stats struct {
	TransportMode     string     `json:"transport_mode"`
	ListenAddr        string     `json:"listen_addr"`
	RemoteAddr        string     `json:"remote_addr,omitempty"`
	Connected         bool       `json:"connected"`
	ConnectionState   string     `json:"connection_state"`
	ConnectionStateEn string     `json:"connection_state_en"`
	TotalPackets      uint64     `json:"total_packets"`
	TotalMessages     uint64     `json:"total_messages"`
	ParseErrors       uint64     `json:"parse_errors"`
	LastPacketTime    *time.Time `json:"last_packet_time,omitempty"`
	LastSourceAddr    string     `json:"last_source_addr,omitempty"`
}

// PacketHandler is invoked synchronously for each valid decoded TSL 5.0 Packet.
type PacketHandler func(pkt Packet, srcAddr string, rxTime time.Time)

// Listener manages TSL UMD v5.0 transport ingestion across all 3 studio topologies:
//   - "listen"     : Passive server listening simultaneously on UDP and TCP at ListenAddr (default :8901)
//   - "tcp_client" : Active TCP client connecting to switcher Listener at RemoteAddr with 2s auto-reconnect
//   - "udp_client" : Active UDP subscriber sending periodic keepalives to switcher at RemoteAddr
type Listener struct {
	handler PacketHandler
	log     *slog.Logger

	totalPackets  atomic.Uint64
	totalMessages atomic.Uint64
	parseErrors   atomic.Uint64

	mu               sync.RWMutex
	parentCtx        context.Context
	loopCancel       context.CancelFunc
	activeClosers    []io.Closer
	mode             string
	listenAddr       string
	remoteAddr       string
	actualAddr       string
	connected        bool
	connState        string
	connStateEn      string
	activeTCPClients int
	lastPacketTime   time.Time
	lastSourceAddr   string
	lastWarnTime     time.Time

	ring         []SnifferEntry
	ringCap      int
	ringWriteIdx int
	ringFull     bool
	snifferSubs  map[chan SnifferEntry]struct{}
}

// NewListener creates a TSL 5.0 multi-transport listener/connector (defaults to "listen" mode on listenAddr).
func NewListener(listenAddr string, handler PacketHandler, logger *slog.Logger) *Listener {
	if logger == nil {
		logger = slog.Default()
	}
	const defaultRingCap = 120
	return &Listener{
		mode:        "listen",
		listenAddr:  listenAddr,
		handler:     handler,
		log:         logger,
		ring:        make([]SnifferEntry, defaultRingCap),
		ringCap:     defaultRingCap,
		snifferSubs: make(map[chan SnifferEntry]struct{}),
	}
}

// SetTransportMode configures mode ("listen", "tcp_client", "udp_client") and remoteAddr before Start.
func (l *Listener) SetTransportMode(mode, listenAddr, remoteAddr string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if mode != "" {
		l.mode = mode
	}
	if listenAddr != "" {
		l.listenAddr = listenAddr
	}
	l.remoteAddr = remoteAddr
}

// Start launches the configured TSL transport workers until ctx is cancelled.
func (l *Listener) Start(ctx context.Context) error {
	l.mu.Lock()
	l.parentCtx = ctx
	mode, listenAddr, remoteAddr := l.mode, l.listenAddr, l.remoteAddr
	l.mu.Unlock()
	return l.ApplyTransport(mode, listenAddr, remoteAddr)
}

// Rebind is a convenience wrapper for ApplyTransport in passive "listen" mode.
func (l *Listener) Rebind(newListenAddr string) error {
	l.mu.RLock()
	mode, remote := l.mode, l.remoteAddr
	l.mu.RUnlock()
	return l.ApplyTransport(mode, newListenAddr, remote)
}

// ApplyTransport hot-switches between passive UDP+TCP listening and active TCP/UDP client pulling
// without restarting the process.
func (l *Listener) ApplyTransport(mode, listenAddr, remoteAddr string) error {
	if mode == "" {
		mode = "listen"
	}
	if listenAddr == "" {
		listenAddr = "0.0.0.0:8901"
	}

	l.mu.Lock()
	if l.loopCancel != nil && l.mode == mode && l.listenAddr == listenAddr && l.remoteAddr == remoteAddr {
		l.mu.Unlock()
		return nil
	}
	parentCtx := l.parentCtx
	if parentCtx == nil {
		parentCtx = context.Background()
	}
	oldCancel := l.loopCancel
	oldClosers := l.activeClosers
	l.activeClosers = nil
	l.mu.Unlock()

	if oldCancel != nil {
		oldCancel()
	}
	for _, c := range oldClosers {
		_ = c.Close()
	}

	loopCtx, loopCancel := context.WithCancel(parentCtx)

	switch mode {
	case "tcp_client":
		l.mu.Lock()
		l.mode = mode
		l.listenAddr = listenAddr
		l.remoteAddr = remoteAddr
		l.actualAddr = ""
		l.connected = false
		if remoteAddr == "" {
			l.connState = "未填写导播服务器地址"
			l.connStateEn = "Switcher address not configured"
		} else {
			l.connState = "正在连接导播台 TCP Listener: " + remoteAddr
			l.connStateEn = "Connecting to switcher TCP Listener: " + remoteAddr
		}
		l.loopCancel = loopCancel
		l.mu.Unlock()

		if remoteAddr != "" {
			go l.runTCPClientLoop(loopCtx, remoteAddr)
		}
		return nil

	case "udp_client":
		// Bind unconnected UDP socket on listenAddr (e.g. 0.0.0.0:8901) so we BOTH:
		//   1) Receive standard UDP Sender packets (unicast/broadcast from ANY source port) on :8901
		//   2) Periodically send SCONTROL keepalive frames to remoteAddr for UDP Listener servers
		udpAddr, err := net.ResolveUDPAddr("udp", listenAddr)
		if err != nil {
			loopCancel()
			return err
		}
		udpConn, err := net.ListenUDP("udp", udpAddr)
		if err != nil {
			// Fallback to ephemeral local port only if listenAddr is already bound externally
			fallbackAddr, _ := net.ResolveUDPAddr("udp", "0.0.0.0:0")
			udpConn, err = net.ListenUDP("udp", fallbackAddr)
			if err != nil {
				loopCancel()
				return err
			}
		}
		_ = udpConn.SetReadBuffer(1 << 20)
		boundStr := udpConn.LocalAddr().String()

		closers := []io.Closer{udpConn}
		tcpLn, tcpErr := net.Listen("tcp", boundStr)
		if tcpErr == nil && tcpLn != nil {
			closers = append(closers, tcpLn)
		}

		l.mu.Lock()
		l.mode = "udp_client"
		l.listenAddr = listenAddr
		l.remoteAddr = remoteAddr
		l.actualAddr = boundStr
		l.connected = remoteAddr == ""
		if remoteAddr == "" {
			l.connState = "未填写远端地址 (已在 " + boundStr + " 被动监听)"
			l.connStateEn = "No remote address (passive listening on " + boundStr + ")"
		} else {
			l.connState = fmt.Sprintf("UDP 监听 %s + 保活 %s", boundStr, remoteAddr)
			l.connStateEn = fmt.Sprintf("UDP listening %s + keepalive %s", boundStr, remoteAddr)
		}
		l.loopCancel = loopCancel
		l.activeClosers = closers
		l.mu.Unlock()

		go func() {
			<-loopCtx.Done()
			for _, c := range closers {
				_ = c.Close()
			}
		}()

		go l.readUDPLoop(loopCtx, udpConn)
		if tcpLn != nil {
			go l.acceptTCPLoop(loopCtx, tcpLn)
		}
		if remoteAddr != "" {
			go l.sendUDPKeepaliveLoop(loopCtx, udpConn, remoteAddr)
		}
		return nil

	default: // "listen" -> bind UDP (primary) + TCP (secondary) on listenAddr!
		udpAddr, err := net.ResolveUDPAddr("udp", listenAddr)
		if err != nil {
			loopCancel()
			return err
		}
		udpConn, err := net.ListenUDP("udp", udpAddr)
		if err != nil {
			loopCancel()
			return err
		}
		_ = udpConn.SetReadBuffer(1 << 20)
		boundStr := udpConn.LocalAddr().String()

		closers := []io.Closer{udpConn}
		// Also bind TCP on the exact same host:port so switchers configured as TCP Senders work automatically!
		tcpLn, tcpErr := net.Listen("tcp", boundStr)
		if tcpErr != nil {
			l.log.Debug("TSL secondary TCP listener skipped on same port", "addr", boundStr, "err", tcpErr)
		} else if tcpLn != nil {
			closers = append(closers, tcpLn)
		}

		l.mu.Lock()
		l.mode = "listen"
		l.listenAddr = listenAddr
		l.remoteAddr = remoteAddr
		l.actualAddr = boundStr
		l.connected = true
		if tcpLn != nil {
			l.connState = "双栈监听就绪 (UDP + TCP " + boundStr + ")"
			l.connStateEn = "Dual-stack ready (UDP + TCP " + boundStr + ")"
		} else {
			l.connState = "UDP 监听就绪 (" + boundStr + ")"
			l.connStateEn = "UDP listener ready (" + boundStr + ")"
		}
		l.loopCancel = loopCancel
		l.activeClosers = closers
		l.mu.Unlock()

		l.log.Info("TSL 5.0 passive listener ready", "udp_addr", boundStr, "tcp_enabled", tcpLn != nil)

		go func() {
			<-loopCtx.Done()
			for _, c := range closers {
				_ = c.Close()
			}
		}()

		go l.readUDPLoop(loopCtx, udpConn)
		if tcpLn != nil {
			go l.acceptTCPLoop(loopCtx, tcpLn)
		}
		return nil
	}
}

// InjectRawDatagram feeds a simulated raw TSL 5.0 frame directly into the parser & sniffer pipeline,
// working identically regardless of whether the gateway is in "listen", "tcp_client", or "udp_client" mode.
func (l *Listener) InjectRawDatagram(raw []byte, srcLabel string) error {
	pkts, err := ParsePackets(raw)
	if err != nil {
		l.parseErrors.Add(1)
		return err
	}
	l.dispatchPackets(pkts, srcLabel, time.Now())
	return nil
}

// BoundAddr returns the actual bound host:port in "listen" mode.
func (l *Listener) BoundAddr() string {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.actualAddr != "" {
		return l.actualAddr
	}
	return l.listenAddr
}

func (l *Listener) readUDPLoop(ctx context.Context, conn *net.UDPConn) {
	buf := make([]byte, 8192)
	for {
		n, remoteAddr, err := conn.ReadFromUDP(buf)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			continue
		}
		if n == 0 {
			continue
		}
		srcStr := ""
		if remoteAddr != nil {
			srcStr = "udp://" + remoteAddr.String()
		}
		pkts, err := ParsePackets(buf[:n])
		if err != nil {
			l.parseErrors.Add(1)
			l.logParseError(srcStr, buf[:n], err)
			continue
		}
		l.dispatchPackets(pkts, srcStr, time.Now())
	}
}

func (l *Listener) logParseError(src string, raw []byte, err error) {
	now := time.Now()
	l.mu.Lock()
	if now.Sub(l.lastWarnTime) < 2*time.Second {
		l.mu.Unlock()
		return
	}
	l.lastWarnTime = now
	l.mu.Unlock()

	previewLen := len(raw)
	if previewLen > 32 {
		previewLen = 32
	}
	hint := ""
	if len(raw) == 18 && (raw[0]&0x80) != 0 {
		hint = " (疑似 TSL UMD v3.1 18字节报文，请确认发送端协议为 TSL v5.0)"
	}
	l.log.Warn("Dropped malformed TSL UDP datagram"+hint,
		"src", src,
		"bytes", len(raw),
		"err", err,
		"hex_prefix", hex.EncodeToString(raw[:previewLen]),
	)
}

func (l *Listener) acceptTCPLoop(ctx context.Context, ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			continue
		}
		go l.handleTCPStream(ctx, conn, "tcp://"+conn.RemoteAddr().String(), false)
	}
}

func (l *Listener) runTCPClientLoop(ctx context.Context, remoteAddr string) {
	dialer := net.Dialer{
		Timeout:   3 * time.Second,
		KeepAlive: 10 * time.Second,
	}

	l.log.Info("TSL 5.0 TCP client mode started (auto-reconnecting to switcher Listener)", "remote_addr", remoteAddr)

	for {
		if ctx.Err() != nil {
			return
		}

		conn, err := dialer.DialContext(ctx, "tcp", remoteAddr)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			l.mu.Lock()
			l.connected = false
			l.connState = fmt.Sprintf("等待导播台开机/监听 (%s 重连中...)", remoteAddr)
			l.connStateEn = fmt.Sprintf("Waiting for switcher (%s reconnecting...)", remoteAddr)
			l.mu.Unlock()

			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
				continue
			}
		}

		if tc, ok := conn.(*net.TCPConn); ok {
			_ = tc.SetKeepAlive(true)
			_ = tc.SetKeepAlivePeriod(10 * time.Second)
		}

		l.mu.Lock()
		l.connected = true
		l.connState = "已连接导播台 TCP Listener: " + remoteAddr
		l.connStateEn = "Connected to switcher TCP Listener: " + remoteAddr
		l.mu.Unlock()
		l.log.Info("Connected to switcher TSL 5.0 TCP Listener", "remote_addr", remoteAddr)

		l.handleTCPStream(ctx, conn, "tcp://"+remoteAddr, true)

		if ctx.Err() != nil {
			return
		}
		l.mu.Lock()
		l.connected = false
		l.connState = fmt.Sprintf("导播台连接断开，2s 后重连 (%s)", remoteAddr)
		l.connStateEn = fmt.Sprintf("Switcher disconnected, retrying in 2s (%s)", remoteAddr)
		l.mu.Unlock()
		l.log.Warn("Switcher TSL 5.0 TCP connection closed, retrying in 2s", "remote_addr", remoteAddr)

		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
}

func (l *Listener) handleTCPStream(ctx context.Context, conn net.Conn, srcLabel string, isOutbound bool) {
	done := make(chan struct{})
	defer func() {
		close(done)
		_ = conn.Close()
	}()

	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-done:
		}
	}()

	if !isOutbound {
		l.mu.Lock()
		l.activeTCPClients++
		l.mu.Unlock()
		defer func() {
			l.mu.Lock()
			l.activeTCPClients--
			l.mu.Unlock()
		}()
	}

	var dec StreamDecoder
	buf := make([]byte, 4096)
	for {
		n, err := conn.Read(buf)
		if n > 0 {
			pkts, errs := dec.Push(buf[:n])
			if errs > 0 {
				l.parseErrors.Add(uint64(errs))
			}
			if len(pkts) > 0 {
				l.dispatchPackets(pkts, srcLabel, time.Now())
			}
		}
		if err != nil {
			if !errors.Is(err, io.EOF) && ctx.Err() == nil {
				l.log.Debug("TSL TCP stream read ended", "src", srcLabel, "err", err)
			}
			return
		}
	}
}

func (l *Listener) sendUDPKeepaliveLoop(ctx context.Context, conn *net.UDPConn, remoteAddr string) {
	raddr, err := net.ResolveUDPAddr("udp", remoteAddr)
	if err != nil {
		l.mu.Lock()
		l.connected = false
		l.connState = "远端地址解析失败: " + err.Error()
		l.connStateEn = "Remote address resolve failed: " + err.Error()
		l.mu.Unlock()
		return
	}

	// Periodically send minimal TSL 5.0 SCONTROL keepalive so UDP Listener servers learn our IP:Port,
	// while conn remains an unconnected UDP listener on listenAddr (:8901) accepting packets from ANY port!
	keepalivePkt := MarshalSControlKeepalive()
	_, _ = conn.WriteToUDP(keepalivePkt, raddr)

	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, _ = conn.WriteToUDP(keepalivePkt, raddr)
		}
	}
}

func (l *Listener) dispatchPackets(pkts []Packet, srcStr string, rxTime time.Time) {
	l.totalPackets.Add(uint64(len(pkts)))

	var msgCount uint64
	for _, pkt := range pkts {
		msgCount += uint64(len(pkt.Messages))
		if l.handler != nil {
			l.handler(pkt, srcStr, rxTime)
		}
		for _, m := range pkt.Messages {
			l.recordSniffer(SnifferEntry{
				Timestamp:  rxTime,
				SourceAddr: srcStr,
				Screen:     pkt.Screen,
				Index:      m.Index,
				RHTally:    m.RHTally.String(),
				TextTally:  m.TextTally.String(),
				LHTally:    m.LHTally.String(),
				Brightness: m.Brightness,
				Text:       m.Text,
				WasFramed:  pkt.WasFramed,
			})
		}
	}
	l.totalMessages.Add(msgCount)

	l.mu.Lock()
	l.lastPacketTime = rxTime
	l.lastSourceAddr = srcStr
	if l.mode == "udp_client" {
		l.connected = true
		l.connState = fmt.Sprintf("正在接收 UDP (%s <- %s)", l.actualAddr, srcStr)
		l.connStateEn = fmt.Sprintf("Receiving UDP (%s <- %s)", l.actualAddr, srcStr)
	}
	l.mu.Unlock()
}

func (l *Listener) recordSniffer(entry SnifferEntry) {
	l.mu.Lock()
	l.ring[l.ringWriteIdx] = entry
	l.ringWriteIdx = (l.ringWriteIdx + 1) % l.ringCap
	if l.ringWriteIdx == 0 {
		l.ringFull = true
	}
	for ch := range l.snifferSubs {
		select {
		case ch <- entry:
		default:
		}
	}
	l.mu.Unlock()
}

// GetStats returns a snapshot of listener/connector statistics.
func (l *Listener) GetStats() Stats {
	l.mu.RLock()
	defer l.mu.RUnlock()

	st := Stats{
		TransportMode:     l.mode,
		ListenAddr:        l.actualAddr,
		RemoteAddr:        l.remoteAddr,
		Connected:         l.connected,
		ConnectionState:   l.connState,
		ConnectionStateEn: l.connStateEn,
		TotalPackets:      l.totalPackets.Load(),
		TotalMessages:     l.totalMessages.Load(),
		ParseErrors:       l.parseErrors.Load(),
		LastSourceAddr:    l.lastSourceAddr,
	}
	if st.ListenAddr == "" {
		st.ListenAddr = l.listenAddr
	}
	if !l.lastPacketTime.IsZero() {
		t := l.lastPacketTime
		st.LastPacketTime = &t
	}
	return st
}

// RecentSnifferEntries returns recent decoded TSL messages in chronological order (newest last).
func (l *Listener) RecentSnifferEntries() []SnifferEntry {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if !l.ringFull {
		out := make([]SnifferEntry, l.ringWriteIdx)
		copy(out, l.ring[:l.ringWriteIdx])
		return out
	}
	out := make([]SnifferEntry, l.ringCap)
	n := copy(out, l.ring[l.ringWriteIdx:])
	copy(out[n:], l.ring[:l.ringWriteIdx])
	return out
}

// SubscribeSniffer registers a live channel for incoming TSL sniffer events.
func (l *Listener) SubscribeSniffer() (chan SnifferEntry, func()) {
	ch := make(chan SnifferEntry, 64)
	l.mu.Lock()
	l.snifferSubs[ch] = struct{}{}
	l.mu.Unlock()

	unsub := func() {
		l.mu.Lock()
		if _, ok := l.snifferSubs[ch]; ok {
			delete(l.snifferSubs, ch)
			close(ch)
		}
		l.mu.Unlock()
	}
	return ch, unsub
}
