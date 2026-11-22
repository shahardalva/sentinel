//go:build !windows

package ratelimit

import "syscall"

var (
	errReset   error = syscall.ECONNRESET
	errAborted error = syscall.ECONNABORTED
	errPipe    error = syscall.EPIPE
)
