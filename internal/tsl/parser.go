package tsl

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"unicode/utf16"
)

// TSL UMD v5.0 constants as defined by TSL Products Protocol V5.0 specification.
const (
	ProtocolVersionV5 = 0x00

	// Stream framing bytes (used by TCP and occasionally emitted over UDP by some switchers).
	DLE = 0xFE
	STX = 0x02

	// FLAGS byte bitmasks.
	FlagUTF16    = 1 << 0 // 0 = ASCII, 1 = UTF-16LE
	FlagSControl = 1 << 1 // 0 = DMSG display messages, 1 = SCONTROL screen control

	// CONTROL 16-bit field bitmasks.
	ControlFlagControlData = 1 << 15 // 1 = non-text control data payload, 0 = display text

	// Broadcast index wildcard.
	BroadcastIndex uint16 = 0xFFFF
)

// LampColor represents the 2-bit tally lamp state in TSL UMD v5.0.
type LampColor uint8

const (
	LampOff   LampColor = 0
	LampRed   LampColor = 1
	LampGreen LampColor = 2
	LampAmber LampColor = 3
)

func (c LampColor) String() string {
	switch c {
	case LampOff:
		return "OFF"
	case LampRed:
		return "RED"
	case LampGreen:
		return "GREEN"
	case LampAmber:
		return "AMBER"
	default:
		return "UNKNOWN"
	}
}

// DMSG represents a single Display Message block within a TSL 5.0 packet.
type DMSG struct {
	Index         uint16    `json:"index"`
	RawControl    uint16    `json:"raw_control"`
	RHTally       LampColor `json:"rh_tally"`       // Bits 0..1: Right-Hand Tally
	TextTally     LampColor `json:"text_tally"`     // Bits 2..3: Text Tally
	LHTally       LampColor `json:"lh_tally"`       // Bits 4..5: Left-Hand Tally
	Brightness    uint8     `json:"brightness"`     // Bits 6..7: Brightness (0..3)
	IsControlData bool      `json:"is_control_data"` // Bit 15: 1 = control data, 0 = display text
	Text          string    `json:"text"`
	HasText       bool      `json:"has_text"`
}

// Packet represents a decoded TSL UMD v5.0 message.
type Packet struct {
	PBC        uint16 `json:"pbc"`
	Version    uint8  `json:"version"`
	Flags      uint8  `json:"flags"`
	Screen     uint16 `json:"screen"`
	IsUTF16    bool   `json:"is_utf16"`
	IsSControl bool   `json:"is_scontrol"`
	WasFramed  bool   `json:"was_framed"` // True if DLE/STX framing was stripped
	Messages   []DMSG `json:"messages"`
}

var (
	ErrEmptyPacket     = errors.New("tsl5: empty packet")
	ErrPacketTooShort  = errors.New("tsl5: packet shorter than 6-byte header")
	ErrTruncatedDMSG   = errors.New("tsl5: truncated DMSG header")
	ErrTruncatedString = errors.New("tsl5: truncated DMSG text payload")
)

// ParsePackets decodes one or more TSL 5.0 packets from a single UDP datagram.
// It transparently handles both standard raw UDP TSL 5.0 datagrams and DLE/STX
// byte-stuffed frames emitted by certain software switchers/gateways.
func ParsePackets(raw []byte) ([]Packet, error) {
	if len(raw) == 0 {
		return nil, ErrEmptyPacket
	}

	// Check if packet starts with DLE (0xFE) + STX (0x02) framing.
	if len(raw) >= 8 && raw[0] == DLE && raw[1] == STX {
		frames := splitAndUnstuffDLEFrames(raw)
		if len(frames) > 0 {
			var out []Packet
			var firstErr error
			for _, f := range frames {
				pkt, err := parseSingleUnframed(f, true)
				if err != nil {
					if firstErr == nil {
						firstErr = err
					}
					continue
				}
				out = append(out, pkt)
			}
			if len(out) > 0 {
				return out, nil
			}
			if firstErr != nil {
				return nil, firstErr
			}
		}
	}

	// Standard raw UDP datagram (may contain one packet, or back-to-back packets).
	var out []Packet
	offset := 0
	for offset < len(raw) {
		rem := raw[offset:]
		if len(rem) < 6 {
			if len(out) > 0 {
				// Ignore trailing padding bytes (some hardware pads UDP payloads to even/4-byte boundary).
				break
			}
			return nil, ErrPacketTooShort
		}

		pbc := binary.LittleEndian.Uint16(rem[0:2])
		// Per TSL v5.0 spec, PBC is the byte count FOLLOWING the 2-byte PBC field (total = pbc + 2).
		// However, a few non-compliant senders set PBC = total length including PBC.
		// Handle both gracefully!
		var sliceLen int
		switch {
		case int(pbc)+2 <= len(rem) && pbc >= 4:
			sliceLen = int(pbc) + 2
		case int(pbc) == len(rem) && pbc >= 6:
			sliceLen = int(pbc)
		default:
			if len(out) > 0 {
				break
			}
			return nil, fmt.Errorf("tsl5: invalid PBC=%d for remaining payload=%d bytes", pbc, len(rem))
		}

		pkt, err := parseSingleUnframed(rem[:sliceLen], false)
		if err != nil {
			if len(out) > 0 {
				break
			}
			return nil, err
		}
		out = append(out, pkt)
		offset += sliceLen
	}

	if len(out) == 0 {
		return nil, ErrPacketTooShort
	}
	return out, nil
}

func parseSingleUnframed(buf []byte, wasFramed bool) (Packet, error) {
	if len(buf) < 6 {
		return Packet{}, ErrPacketTooShort
	}

	pbc := binary.LittleEndian.Uint16(buf[0:2])
	ver := buf[2]
	flags := buf[3]
	screen := binary.LittleEndian.Uint16(buf[4:6])

	pkt := Packet{
		PBC:        pbc,
		Version:    ver,
		Flags:      flags,
		Screen:     screen,
		IsUTF16:    (flags & FlagUTF16) != 0,
		IsSControl: (flags & FlagSControl) != 0,
		WasFramed:  wasFramed,
	}

	// Screen control messages (SCONTROL) do not carry DMSG tally blocks.
	if pkt.IsSControl {
		return pkt, nil
	}

	payload := buf[6:]
	pos := 0
	for pos < len(payload) {
		// Need at least 6 bytes for a DMSG header: INDEX(2) + CONTROL(2) + LENGTH(2)
		if len(payload)-pos < 6 {
			// If we already parsed at least one valid DMSG and remaining bytes are zero padding, stop cleanly.
			if len(pkt.Messages) > 0 && isAllZero(payload[pos:]) {
				break
			}
			return pkt, ErrTruncatedDMSG
		}

		idx := binary.LittleEndian.Uint16(payload[pos : pos+2])
		ctrl := binary.LittleEndian.Uint16(payload[pos+2 : pos+4])
		textByteLen := int(binary.LittleEndian.Uint16(payload[pos+4 : pos+6]))
		pos += 6

		// Compatibility: TSL 5.0 defines LENGTH in bytes, but some UTF-16 senders populate
		// character count instead of byte count. Detect safely when odd or when remaining bytes
		// cannot form another 6-byte DMSG header.
		if pkt.IsUTF16 && textByteLen > 0 && textByteLen*2 <= len(payload)-pos {
			remAfter := len(payload) - (pos + textByteLen)
			if textByteLen%2 == 1 || (remAfter > 0 && remAfter < 6 && !isAllZero(payload[pos+textByteLen:])) {
				textByteLen *= 2
			}
		}

		if len(payload)-pos < textByteLen {
			return pkt, fmt.Errorf("%w: need %d bytes, have %d", ErrTruncatedString, textByteLen, len(payload)-pos)
		}

		rawText := payload[pos : pos+textByteLen]
		pos += textByteLen

		isCtrlData := (ctrl & ControlFlagControlData) != 0
		msg := DMSG{
			Index:         idx,
			RawControl:    ctrl,
			RHTally:       LampColor(ctrl & 0x03),
			TextTally:     LampColor((ctrl >> 2) & 0x03),
			LHTally:       LampColor((ctrl >> 4) & 0x03),
			Brightness:    uint8((ctrl >> 6) & 0x03),
			IsControlData: isCtrlData,
		}

		if !isCtrlData {
			msg.HasText = textByteLen > 0
			if textByteLen > 0 {
				msg.Text = decodeTSLText(rawText, pkt.IsUTF16)
			}
		}

		pkt.Messages = append(pkt.Messages, msg)
	}

	return pkt, nil
}

func decodeTSLText(b []byte, isUTF16 bool) string {
	if !isUTF16 {
		// Trim trailing NULs while preserving visible text.
		b = bytes.TrimRight(b, "\x00")
		return string(b)
	}
	// UTF-16LE decoding.
	n := len(b) / 2
	if n == 0 {
		return ""
	}
	u16 := make([]uint16, n)
	for i := 0; i < n; i++ {
		u16[i] = binary.LittleEndian.Uint16(b[i*2 : i*2+2])
	}
	// Trim trailing NUL code units.
	for len(u16) > 0 && u16[len(u16)-1] == 0 {
		u16 = u16[:len(u16)-1]
	}
	return string(utf16.Decode(u16))
}

// splitAndUnstuffDLEFrames extracts unstuffed payloads from DLE/STX framed streams.
// Per TSL v5.0: Packet starts with 0xFE 0x02; literal 0xFE inside data is stuffed as 0xFE 0xFE.
func splitAndUnstuffDLEFrames(data []byte) [][]byte {
	var frames [][]byte
	var cur []byte
	inFrame := false

	for i := 0; i < len(data); i++ {
		if data[i] == DLE {
			if i+1 >= len(data) {
				break
			}
			next := data[i+1]
			switch next {
			case STX:
				if inFrame && len(cur) > 0 {
					frames = append(frames, cur)
				}
				cur = make([]byte, 0, len(data)-i)
				inFrame = true
				i++ // consume STX
			case DLE:
				if inFrame {
					cur = append(cur, DLE)
				}
				i++ // consume stuffed DLE
			default:
				if inFrame {
					cur = append(cur, data[i])
				}
			}
		} else if inFrame {
			cur = append(cur, data[i])
		}
	}
	if inFrame && len(cur) > 0 {
		frames = append(frames, cur)
	}
	return frames
}

func isAllZero(b []byte) bool {
	for _, v := range b {
		if v != 0 {
			return false
		}
	}
	return true
}

// MarshalPacket encodes a Packet into raw UDP TSL 5.0 wire format (or DLE/STX framed if withDLE=true).
// Used for unit testing, diagnostics, and built-in TSL simulation.
func MarshalPacket(screen uint16, isUTF16 bool, msgs []DMSG, withDLE bool) []byte {
	var body bytes.Buffer
	flags := uint8(0)
	if isUTF16 {
		flags |= FlagUTF16
	}
	body.WriteByte(ProtocolVersionV5)
	body.WriteByte(flags)
	_ = binary.Write(&body, binary.LittleEndian, screen)

	for _, m := range msgs {
		_ = binary.Write(&body, binary.LittleEndian, m.Index)
		ctrl := (uint16(m.RHTally)&0x03) |
			((uint16(m.TextTally) & 0x03) << 2) |
			((uint16(m.LHTally) & 0x03) << 4) |
			((uint16(m.Brightness) & 0x03) << 6)
		if m.IsControlData {
			ctrl |= ControlFlagControlData
		}
		_ = binary.Write(&body, binary.LittleEndian, ctrl)

		var txtBytes []byte
		if !m.IsControlData && len(m.Text) > 0 {
			if isUTF16 {
				u16 := utf16.Encode([]rune(m.Text))
				txtBytes = make([]byte, len(u16)*2)
				for i, cp := range u16 {
					binary.LittleEndian.PutUint16(txtBytes[i*2:], cp)
				}
			} else {
				txtBytes = []byte(m.Text)
			}
		}
		_ = binary.Write(&body, binary.LittleEndian, uint16(len(txtBytes)))
		body.Write(txtBytes)
	}

	pbc := uint16(body.Len())
	raw := make([]byte, 2+body.Len())
	binary.LittleEndian.PutUint16(raw[0:2], pbc)
	copy(raw[2:], body.Bytes())

	if !withDLE {
		return raw
	}

	var framed bytes.Buffer
	framed.WriteByte(DLE)
	framed.WriteByte(STX)
	for _, b := range raw {
		if b == DLE {
			framed.WriteByte(DLE)
			framed.WriteByte(DLE)
		} else {
			framed.WriteByte(b)
		}
	}
	return framed.Bytes()
}

// MarshalSControlKeepalive builds a minimal valid 6-byte TSL 5.0 SCONTROL frame (0 bytes of DMSG)
// used in udp_client mode to register/keepalive source UDP address with UDP Listener servers.
func MarshalSControlKeepalive() []byte {
	raw := make([]byte, 6)
	binary.LittleEndian.PutUint16(raw[0:2], 4) // PBC = 4 bytes following PBC
	raw[2] = ProtocolVersionV5
	raw[3] = FlagSControl
	binary.LittleEndian.PutUint16(raw[4:6], 0)
	return raw
}

// StreamDecoder reassembles continuous TCP byte streams into discrete TSL 5.0 Packets with zero
// 1-packet lag. It handles arbitrary TCP chunk boundaries (half-packets and sticky multi-packets)
// for both standard DLE/STX (0xFE 0x02) byte-stuffed TCP streams and raw PBC-prefixed streams.
type StreamDecoder struct {
	buf []byte
}

const maxStreamBufferBytes = 65536

// Push appends incoming TCP stream bytes and returns all complete TSL 5.0 Packets decoded so far.
func (d *StreamDecoder) Push(chunk []byte) ([]Packet, int) {
	if len(chunk) > 0 {
		d.buf = append(d.buf, chunk...)
	}
	if len(d.buf) > maxStreamBufferBytes {
		// Guard against corrupted runaway streams.
		d.buf = d.buf[len(d.buf)-4096:]
	}

	var out []Packet
	errCount := 0

	for len(d.buf) > 0 {
		// Case 1: Standard TSL 5.0 TCP DLE/STX (0xFE 0x02) byte-stuffed framing.
		if d.buf[0] == DLE {
			if len(d.buf) < 2 {
				break // Wait for second byte after 0xFE
			}
			if d.buf[1] != STX {
				// Stray byte; resync to next potential frame start.
				d.buf = d.buf[1:]
				errCount++
				continue
			}

			unstuffed, consumed, complete, resetAt := tryExtractOneDLEFrame(d.buf)
			if resetAt > 0 {
				// Encountered a nested unescaped DLE+STX before previous PBC finished (truncated prior frame).
				d.buf = d.buf[resetAt:]
				errCount++
				continue
			}
			if !complete {
				break // Need more TCP bytes
			}
			d.buf = d.buf[consumed:]
			pkt, err := parseSingleUnframed(unstuffed, true)
			if err != nil {
				errCount++
				continue
			}
			out = append(out, pkt)
			continue
		}

		// Case 2: Raw unframed TSL 5.0 over TCP (PBC length-prefixed).
		if len(d.buf) < 6 {
			break
		}
		pbc := int(binary.LittleEndian.Uint16(d.buf[0:2]))
		if pbc < 4 || pbc > 4096 || d.buf[2] != ProtocolVersionV5 || (d.buf[3]&0xFC) != 0 {
			// Resync one byte forward.
			d.buf = d.buf[1:]
			errCount++
			continue
		}
		frameLen := 2 + pbc
		if len(d.buf) < frameLen {
			break // Wait for remaining TCP bytes of this frame!
		}
		rawFrame := d.buf[:frameLen]
		d.buf = d.buf[frameLen:]
		pkt, err := parseSingleUnframed(rawFrame, false)
		if err != nil {
			errCount++
			continue
		}
		out = append(out, pkt)
	}

	return out, errCount
}

// tryExtractOneDLEFrame scans buf starting at buf[0..1] == {DLE, STX}, unstuffing DLE+DLE -> DLE
// and reading the unstuffed PBC length on the fly so the frame completes the instant (2 + PBC)
// unstuffed bytes have arrived — zero waiting for a subsequent frame!
func tryExtractOneDLEFrame(buf []byte) (unstuffed []byte, rawConsumed int, complete bool, resetAt int) {
	// buf[0] == DLE, buf[1] == STX
	needUnstuffed := -1 // Unknown until we have 2 unstuffed bytes for PBC
	i := 2
	for i < len(buf) {
		b := buf[i]
		if b == DLE {
			if i+1 >= len(buf) {
				// Trailing single 0xFE at end of TCP read buffer: wait for next byte!
				return nil, 0, false, 0
			}
			next := buf[i+1]
			if next == DLE {
				unstuffed = append(unstuffed, DLE)
				i += 2
			} else if next == STX {
				// New frame started before previous frame completed -> resync to i!
				return nil, 0, false, i
			} else {
				// Invalid escape sequence; treat literal and advance
				unstuffed = append(unstuffed, b)
				i++
			}
		} else {
			unstuffed = append(unstuffed, b)
			i++
		}

		if needUnstuffed == -1 && len(unstuffed) >= 2 {
			pbc := int(binary.LittleEndian.Uint16(unstuffed[0:2]))
			if pbc < 4 || pbc > 4096 {
				// Corrupted PBC; skip initial DLE+STX so caller resyncs
				return nil, 2, true, 0
			}
			needUnstuffed = 2 + pbc
		}
		if needUnstuffed > 0 && len(unstuffed) >= needUnstuffed {
			return unstuffed[:needUnstuffed], i, true, 0
		}
	}
	return nil, 0, false, 0
}
