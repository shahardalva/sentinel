package ratelimit

import "syscall"

// connErrnos are the system errors that mean an open connection was
// closed or reset under a call. Winsock's codes: syscall.ECONNRESET and
// friends are invented values on Windows that no socket call returns, and
// so is EPIPE. A read on a reset socket can also give
// ERROR_NETNAME_DELETED, which Go's own poller treats as a reset.
var connErrnos = []error{
	syscall.WSAECONNRESET,
	syscall.WSAECONNABORTED,
	syscall.Errno(10058), // WSAESHUTDOWN, not defined in syscall
	syscall.ERROR_NETNAME_DELETED,
}
