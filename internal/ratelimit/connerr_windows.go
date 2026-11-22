package ratelimit

import "syscall"

// Winsock's codes: syscall.ECONNRESET and friends are invented values on
// Windows that no socket call returns.
var (
	errReset   error = syscall.WSAECONNRESET
	errAborted error = syscall.WSAECONNABORTED
	errPipe    error = syscall.EPIPE
)
