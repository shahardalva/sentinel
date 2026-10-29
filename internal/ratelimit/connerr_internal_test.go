package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"syscall"
	"testing"
)

func TestIsConnError(t *testing.T) {
	read := func(err error) error {
		return &net.OpError{Op: "read", Net: "tcp", Err: os.NewSyscallError("read", err)}
	}
	write := func(err error) error {
		return &net.OpError{Op: "write", Net: "tcp", Err: os.NewSyscallError("write", err)}
	}
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"EOF", io.EOF, true},
		{"unexpected EOF", io.ErrUnexpectedEOF, true},
		{"wrapped EOF", fmt.Errorf("tls: %w", io.EOF), true},
		{"closed", &net.OpError{Op: "read", Net: "tcp", Err: net.ErrClosed}, true},
		{"reset on read", read(errReset), true},
		{"aborted on read", read(errAborted), true},
		{"broken pipe on write", write(errPipe), true},

		// What an outage looks like: these count.
		{"timeout", context.DeadlineExceeded, false},
		{"dial refused", &net.OpError{Op: "dial", Net: "tcp", Err: os.NewSyscallError("connect", errRefused)}, false},
		{"reset while dialing", &net.OpError{Op: "dial", Net: "tcp", Err: os.NewSyscallError("connect", errReset)}, false},
		{"redis error", errors.New("ERR something"), false},
		{"nil", nil, false},
	}
	for _, tt := range tests {
		if got := isConnError(tt.err); got != tt.want {
			t.Errorf("%s: isConnError(%v) = %v, want %v", tt.name, tt.err, got, tt.want)
		}
	}
}

var errRefused error = syscall.ECONNREFUSED
