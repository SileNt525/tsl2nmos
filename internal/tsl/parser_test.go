package tsl

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"
)

func TestParseRawMultiDMSGAndUTF16(t *testing.T) {
	msgs := []DMSG{
		{
			Index:      1,
			RHTally:    LampRed,
			TextTally:  LampOff,
			LHTally:    LampGreen,
			Brightness: 3,
			Text:       "演播室主机位 CAM1",
		},
		{
			Index:      2,
			RHTally:    LampOff,
			TextTally:  LampAmber,
			LHTally:    LampOff,
			Brightness: 2,
			Text:       "侧机位 CAM2",
		},
	}

	raw := MarshalPacket(0, true, msgs, false)
	pkts, err := ParsePackets(raw)
	if err != nil {
		t.Fatalf("ParsePackets unexpected error: %v", err)
	}
	if len(pkts) != 1 {
		t.Fatalf("expected 1 packet, got %d", len(pkts))
	}

	p := pkts[0]
	if !p.IsUTF16 || p.Screen != 0 || p.WasFramed {
		t.Fatalf("unexpected header flags: %+v", p)
	}
	if len(p.Messages) != 2 {
		t.Fatalf("expected 2 DMSG blocks, got %d", len(p.Messages))
	}

	if p.Messages[0].Index != 1 || p.Messages[0].RHTally != LampRed || p.Messages[0].LHTally != LampGreen {
		t.Fatalf("unexpected msg[0] tally: %+v", p.Messages[0])
	}
	if p.Messages[0].Text != "演播室主机位 CAM1" {
		t.Fatalf("UTF-16LE text mismatch: got %q", p.Messages[0].Text)
	}
	if p.Messages[1].Index != 2 || p.Messages[1].TextTally != LampAmber || p.Messages[1].Text != "侧机位 CAM2" {
		t.Fatalf("unexpected msg[1]: %+v", p.Messages[1])
	}
}

func TestParseDLEStuffedFrameWith0xFEIndex(t *testing.T) {
	// Index 254 (0x00FE) forces literal 0xFE inside payload, exercising DLE byte-unstuffing!
	msgs := []DMSG{
		{
			Index:      0x00FE,
			RHTally:    LampGreen,
			LHTally:    LampRed,
			Brightness: 3,
			Text:       "CRANE 254",
		},
	}

	framed := MarshalPacket(2, false, msgs, true)
	pkts, err := ParsePackets(framed)
	if err != nil {
		t.Fatalf("ParsePackets DLE framed failed: %v", err)
	}
	if len(pkts) != 1 || !pkts[0].WasFramed {
		t.Fatalf("expected 1 WasFramed packet, got %+v", pkts)
	}
	got := pkts[0].Messages[0]
	if got.Index != 0x00FE || got.RHTally != LampGreen || got.LHTally != LampRed || got.Text != "CRANE 254" {
		t.Fatalf("unstuffed message mismatch: %+v", got)
	}
}

func TestParseMalformedPacketsNeverPanic(t *testing.T) {
	cases := [][]byte{
		nil,
		{},
		{0x01, 0x00},
		{0xFF, 0x0F, 0x00, 0x00, 0x00, 0x00}, // PBC larger than buffer
		{0xFE, 0x02, 0x01},                   // Incomplete DLE/STX frame
	}
	for i, c := range cases {
		if _, err := ParsePackets(c); err == nil {
			t.Errorf("case %d expected error for malformed input %x", i, c)
		}
	}
}

func TestStreamDecoderHalfAndStickyPacketsZeroLag(t *testing.T) {
	pkt1 := MarshalPacket(0, false, []DMSG{
		{Index: 0x00FE, RHTally: LampRed, LHTally: LampOff, Brightness: 3, Text: "CAM 254"},
	}, true) // DLE/STX framed with 0xFE 0xFE byte stuffing
	pkt2 := MarshalPacket(0, true, []DMSG{
		{Index: 2, RHTally: LampOff, LHTally: LampGreen, Brightness: 2, Text: "主机位2"},
	}, true) // DLE/STX framed UTF-16LE
	pkt3 := MarshalPacket(0, false, []DMSG{
		{Index: 0x02FE, RHTally: LampRed, LHTally: LampGreen, Brightness: 3, Text: "RAW TCP"},
	}, false) // Raw PBC-prefixed over TCP containing 0xFE 0x02 in payload

	// 1) Feed 1 byte at a time and assert each packet emits immediately on its final byte (zero lag).
	var dec StreamDecoder
	for i, b := range pkt1 {
		out, errs := dec.Push([]byte{b})
		if errs != 0 {
			t.Fatalf("pkt1 byte %d unexpected parse error", i)
		}
		if i < len(pkt1)-1 && len(out) != 0 {
			t.Fatalf("pkt1 emitted prematurely at byte %d", i)
		}
		if i == len(pkt1)-1 {
			if len(out) != 1 || out[0].Messages[0].Index != 0x00FE || out[0].Messages[0].RHTally != LampRed {
				t.Fatalf("pkt1 failed to emit on final byte with zero lag: %+v", out)
			}
		}
	}

	for i, b := range pkt2 {
		out, _ := dec.Push([]byte{b})
		if i == len(pkt2)-1 {
			if len(out) != 1 || out[0].Messages[0].Text != "主机位2" {
				t.Fatalf("pkt2 failed to emit on final byte: %+v", out)
			}
		}
	}

	for i, b := range pkt3 {
		out, _ := dec.Push([]byte{b})
		if i == len(pkt3)-1 {
			if len(out) != 1 || out[0].Messages[0].Index != 0x02FE {
				t.Fatalf("pkt3 raw TCP failed to emit on final byte: %+v", out)
			}
		}
	}

	// 2) Sticky stream: garbage prefix + all 3 packets concatenated in a single TCP read.
	var sticky bytes.Buffer
	sticky.Write([]byte{0xAA, 0xBB}) // garbage prefix before DLE/STX
	sticky.Write(pkt1)
	sticky.Write(pkt2)
	sticky.Write(pkt3)

	var stickyDec StreamDecoder
	allOut, _ := stickyDec.Push(sticky.Bytes())
	if len(allOut) != 3 {
		t.Fatalf("expected 3 packets from sticky stream, got %d", len(allOut))
	}
}

func TestListenerTCPClientAutoReconnect(t *testing.T) {
	// Allocate an ephemeral port, then close it so the switcher Listener is initially "offline".
	probeLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("probe listen: %v", err)
	}
	switcherAddr := probeLn.Addr().String()
	_ = probeLn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()

	gotCh := make(chan DMSG, 4)
	silentLog := slog.New(slog.NewTextHandler(io.Discard, nil))
	listener := NewListener("127.0.0.1:0", func(pkt Packet, _ string, _ time.Time) {
		for _, m := range pkt.Messages {
			gotCh <- m
		}
	}, silentLog)
	listener.SetTransportMode("tcp_client", "127.0.0.1:0", switcherAddr)
	if err := listener.Start(ctx); err != nil {
		t.Fatalf("Start tcp_client: %v", err)
	}

	// Let initial dial fail while switcher is offline, then bring switcher TCP Listener online.
	time.Sleep(150 * time.Millisecond)
	switcherLn, err := net.Listen("tcp", switcherAddr)
	if err != nil {
		t.Fatalf("re-open switcher TCP listener: %v", err)
	}
	defer switcherLn.Close()

	go func() {
		conn, err := switcherLn.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		frame := MarshalPacket(0, false, []DMSG{
			{Index: 3, RHTally: LampRed, LHTally: LampGreen, Brightness: 3, Text: "TCP PULL CAM3"},
		}, true)
		// Write in two split TCP chunks to test half-packet reassembly over real socket.
		_, _ = conn.Write(frame[:5])
		time.Sleep(20 * time.Millisecond)
		_, _ = conn.Write(frame[5:])
		<-ctx.Done()
	}()

	select {
	case msg := <-gotCh:
		if msg.Index != 3 || msg.RHTally != LampRed || msg.LHTally != LampGreen || msg.Text != "TCP PULL CAM3" {
			t.Fatalf("unexpected TCP pulled DMSG: %+v", msg)
		}
	case <-ctx.Done():
		t.Fatalf("timed out waiting for tcp_client auto-reconnect and TSL frame reception")
	}

	st := listener.GetStats()
	if st.TransportMode != "tcp_client" || !st.Connected {
		t.Fatalf("expected tcp_client Connected=true, got %+v", st)
	}
}

func TestListenerUDPClientAndDualStackListen(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 1) Mock a UDP Listener server that waits for client keepalive punch before pushing TSL 5.0 back.
	srvAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("resolve udp: %v", err)
	}
	srvConn, err := net.ListenUDP("udp", srvAddr)
	if err != nil {
		t.Fatalf("listen udp: %v", err)
	}
	defer srvConn.Close()

	go func() {
		buf := make([]byte, 256)
		n, clientAddr, err := srvConn.ReadFromUDP(buf)
		if err != nil || n == 0 || clientAddr == nil {
			return
		}
		reply := MarshalPacket(0, false, []DMSG{
			{Index: 5, RHTally: LampGreen, Text: "UDP PUNCH CAM5"},
		}, false)
		_, _ = srvConn.WriteToUDP(reply, clientAddr)
	}()

	gotCh := make(chan DMSG, 4)
	silentLog := slog.New(slog.NewTextHandler(io.Discard, nil))
	listener := NewListener("127.0.0.1:0", func(pkt Packet, _ string, _ time.Time) {
		for _, m := range pkt.Messages {
			gotCh <- m
		}
	}, silentLog)
	listener.SetTransportMode("udp_client", "127.0.0.1:0", srvConn.LocalAddr().String())
	if err := listener.Start(ctx); err != nil {
		t.Fatalf("Start udp_client: %v", err)
	}

	select {
	case msg := <-gotCh:
		if msg.Index != 5 || msg.RHTally != LampGreen {
			t.Fatalf("unexpected udp_client message: %+v", msg)
		}
	case <-ctx.Done():
		t.Fatalf("timed out waiting for udp_client keepalive punch response")
	}

	// 1b) Verify that EVEN in "udp_client" mode, a standard UDP Sender pushing from a random
	// ephemeral source port to listener.BoundAddr() (e.g. :8901) is still received cleanly!
	ephemeralSender, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("ephemeral sender: %v", err)
	}
	defer ephemeralSender.Close()
	boundUDPAddr, err := net.ResolveUDPAddr("udp", listener.BoundAddr())
	if err != nil {
		t.Fatalf("resolve bound udp addr: %v", err)
	}
	_, _ = ephemeralSender.WriteTo(MarshalPacket(0, false, []DMSG{
		{Index: 7, RHTally: LampRed, Text: "EPHEMERAL SENDER CAM7"},
	}, false), boundUDPAddr)

	select {
	case msg := <-gotCh:
		if msg.Index != 7 || msg.RHTally != LampRed || msg.Text != "EPHEMERAL SENDER CAM7" {
			t.Fatalf("unexpected message on bound port in udp_client mode: %+v", msg)
		}
	case <-ctx.Done():
		t.Fatalf("timed out waiting for standard UDP Sender packet in udp_client mode")
	}

	// 2) Hot-switch to passive dual-stack "listen" mode and push via TCP to BoundAddr().
	if err := listener.ApplyTransport("listen", "127.0.0.1:0", ""); err != nil {
		t.Fatalf("ApplyTransport listen: %v", err)
	}
	bound := listener.BoundAddr()
	tcpConn, err := net.DialTimeout("tcp", bound, 2*time.Second)
	if err != nil {
		t.Fatalf("dial dual-stack passive TCP listener %s: %v", bound, err)
	}
	defer tcpConn.Close()

	_, _ = tcpConn.Write(MarshalPacket(0, false, []DMSG{
		{Index: 6, LHTally: LampRed, Text: "PASSIVE TCP CAM6"},
	}, true))

	select {
	case msg := <-gotCh:
		if msg.Index != 6 || msg.LHTally != LampRed {
			t.Fatalf("unexpected passive TCP message: %+v", msg)
		}
	case <-ctx.Done():
		t.Fatalf("timed out waiting for passive dual-stack TCP message")
	}
}
