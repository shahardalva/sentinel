//go:build !windows

package ratelimit

import "syscall"

// connErrnos are the system errors that mean an open connection was
// closed or reset under a call.
var connErrnos = []error{syscall.ECONNRESET, syscall.ECONNABORTED, syscall.EPIPE}
