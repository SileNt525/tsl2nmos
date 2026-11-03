//go:build !windows

package nmos

import (
	"syscall"

	"golang.org/x/sys/unix"
)

func setSocketReuseAddr(c syscall.RawConn) error {
	var err error
	cErr := c.Control(func(fd uintptr) {
		_ = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
		_ = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, unix.SO_REUSEPORT, 1)
	})
	if cErr != nil {
		return cErr
	}
	return err
}
