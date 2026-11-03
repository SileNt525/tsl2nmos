package nmos

import (
	"crypto/sha1"
	"fmt"
	"time"
)

// NamespaceUUID is a fixed RFC 4122 namespace UUID dedicated to tsl2nmos deterministic resource generation.
var NamespaceUUID = [16]byte{
	0x6b, 0xa7, 0xb8, 0x10,
	0x9d, 0xad,
	0x11, 0xd1,
	0x80, 0xb4,
	0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8,
}

// DeterministicUUIDv5 generates a stable RFC 4122 Version 5 UUID from nodeSeed and resourceKey.
// This guarantees that Node, Device, Source, Flow, and Sender UUIDs remain identical across
// process restarts, OS reboots, and unrelated channel additions.
func DeterministicUUIDv5(nodeSeed, resourceKey string) string {
	h := sha1.New()
	h.Write(NamespaceUUID[:])
	h.Write([]byte(nodeSeed))
	h.Write([]byte("::"))
	h.Write([]byte(resourceKey))
	sum := h.Sum(nil)

	var u [16]byte
	copy(u[:], sum[:16])
	// Set version to 5 (0101xxxx)
	u[6] = (u[6] & 0x0f) | 0x50
	// Set variant to RFC 4122 (10xxxxxx)
	u[8] = (u[8] & 0x3f) | 0x80

	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		u[0:4], u[4:6], u[6:8], u[8:10], u[10:16])
}

// CurrentTAIOffsetSeconds is the TAI - UTC leap second offset (37 seconds per IERS).
const CurrentTAIOffsetSeconds = 37

// TAITimestampNow returns the current time formatted as an NMOS TAI timestamp "<seconds>:<nanoseconds>".
func TAITimestampNow() string {
	return FormatTAITimestamp(time.Now())
}

// FormatTAITimestamp formats a time.Time into NMOS "<seconds>:<nanoseconds>" TAI string.
func FormatTAITimestamp(t time.Time) string {
	sec := t.Unix() + CurrentTAIOffsetSeconds
	nsec := t.Nanosecond()
	if nsec < 0 {
		nsec = 0
	}
	return fmt.Sprintf("%d:%d", sec, nsec)
}
