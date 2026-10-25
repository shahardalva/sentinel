package upstream

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"testing/iotest"
	"time"
)

// closeRecorder records whether it was closed.
type closeRecorder struct {
	io.Reader
	closed bool
}

func (c *closeRecorder) Close() error { c.closed = true; return nil }

func TestReadErrorBody(t *testing.T) {
	tests := []struct {
		name    string
		size    int
		wantErr error
	}{
		{"empty", 0, nil},
		{"small", 300, nil},
		{"at the limit", MaxErrorBody, nil},
		{"one byte over", MaxErrorBody + 1, ErrErrorBodyTooLarge},
		{"far over", 10 * MaxErrorBody, ErrErrorBodyTooLarge},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := &closeRecorder{Reader: strings.NewReader(strings.Repeat("x", tt.size))}
			b, err := ReadErrorBody(t.Context(), body)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if tt.wantErr == nil && len(b) != tt.size {
				t.Errorf("read %d bytes, want %d", len(b), tt.size)
			}
			if tt.wantErr != nil && b != nil {
				t.Errorf("returned %d bytes with an error", len(b))
			}
			if !body.closed {
				t.Error("body not closed")
			}
		})
	}
}

func TestReadErrorBody_ReadError(t *testing.T) {
	body := &closeRecorder{Reader: iotest.ErrReader(errors.New("connection reset"))}
	if _, err := ReadErrorBody(t.Context(), body); err == nil || errors.Is(err, ErrErrorBodyTooLarge) {
		t.Errorf("err = %v, want the read error", err)
	}
	if !body.closed {
		t.Error("body not closed")
	}
}

// stuckBody blocks every Read until it is closed, like a connection that
// cancelling the request doesn't close.
func stuckBody() io.ReadCloser {
	pr, _ := io.Pipe()
	return pr
}

func TestReadErrorBody_ContextEndsRead(t *testing.T) {
	cause := errors.New("request over")
	ctx, cancel := context.WithCancelCause(t.Context())
	time.AfterFunc(50*time.Millisecond, func() { cancel(cause) })

	errc := make(chan error, 1)
	go func() {
		_, err := ReadErrorBody(ctx, stuckBody())
		errc <- err
	}()
	select {
	case err := <-errc:
		if !errors.Is(err, cause) {
			t.Errorf("err = %v, want the context's cause", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("read still blocked 5s after its context ended")
	}
}

func TestReadErrorBody_TimeoutCause(t *testing.T) {
	// A context already past the timeout stands in for 10s of waiting; the
	// slow case end to end is TestGateway_SlowErrorBody in internal/proxy.
	ctx, cancel := context.WithTimeoutCause(t.Context(), time.Millisecond, ErrErrorBodyTimeout)
	defer cancel()
	if _, err := ReadErrorBody(ctx, stuckBody()); !errors.Is(err, ErrErrorBodyTimeout) {
		t.Errorf("err = %v, want ErrErrorBodyTimeout", err)
	}
}

func TestRedactAccountIDs(t *testing.T) {
	tests := []struct {
		name, in, want string
	}{
		{
			"OpenAI organization",
			"Rate limit reached for gpt-5 in organization org-AbC123dEf456GhI789 on tokens per min (TPM).",
			"Rate limit reached for gpt-5 in organization org-[redacted] on tokens per min (TPM).",
		},
		{"OpenAI project", "project proj_q1W2e3R4t5Y6 is over quota", "project proj_[redacted] is over quota"},
		{
			"UUID",
			"This request would exceed your organization's (a1b2c3d4-E5F6-7890-abcd-ef0123456789) rate limit.",
			"This request would exceed your organization's ([redacted]) rate limit.",
		},
		{"two in a row", "two: org-aaaaaaaa1,org-bbbbbbbb2", "two: org-[redacted],org-[redacted]"},
		{"start of string", "org-AbC123dEf456 over quota", "org-[redacted] over quota"},

		// Case-insensitive.
		{"upper-case organization", "ORG-ABCDEFGH12", "org-[redacted]"},
		{"upper-case project", "PROJ_ABCDEFGH12", "proj_[redacted]"},
		{"upper-case UUID", "A1B2C3D4-E5F6-7890-ABCD-EF0123456789", "[redacted]"},

		// After "_" or a digit, where \b would not match.
		{"after underscore", "user_org-AbC123dEf456", "user_org-[redacted]"},
		{"after digit", "1org-AbC123dEf456", "1org-[redacted]"},
		{"project after underscore", "my_proj_AbC123dEf456", "my_proj_[redacted]"},
		{"UUID after underscore", "key_a1b2c3d4-e5f6-7890-abcd-ef0123456789", "key_[redacted]"},

		// Key shapes.
		{"OpenAI key", "Incorrect API key provided: sk-abcdefgh12345678.", "Incorrect API key provided: sk-[redacted]."},
		{"OpenAI project key", "Incorrect API key provided: sk-proj-abcd1234wxyz5678.", "Incorrect API key provided: sk-[redacted]."},
		{"Anthropic key", "key sk-ant-api03-AbC_dEf-123456 revoked", "key sk-[redacted] revoked"},
		{"Anthropic workspace", "workspace wrkspc_01AbCdEfGhIjKl has no access", "workspace wrkspc_[redacted] has no access"},
		{"Anthropic API key ID", "apikey_01AbCdEfGhIj is disabled", "apikey_[redacted] is disabled"},
		{"key after underscore", "x_sk-abcdefgh12345678", "x_sk-[redacted]"},

		// UUIDs without hyphens.
		{"32 hex", "account a1b2c3d4e5f67890abcdef0123456789 suspended", "account [redacted] suspended"},
		{"32 hex upper-case", "A1B2C3D4E5F67890ABCDEF0123456789", "[redacted]"},

		// Request IDs are kept, whatever their form.
		{"Anthropic request ID", "request req_011CUabcdEFGhijkLMNop failed", "request req_011CUabcdEFGhijkLMNop failed"},
		{"OpenAI request ID", "see req_0123456789abcdef0123456789abcdef", "see req_0123456789abcdef0123456789abcdef"},
		{"upper-case request ID", "REQ_0123456789ABCDEF0123456789ABCDEF", "REQ_0123456789ABCDEF0123456789ABCDEF"},

		// Negatives: words, short values, ordinary numbers.
		{"too short", "org-1 and org-abc and sk-abc are too short", "org-1 and org-abc and sk-abc are too short"},
		{"after a letter", "cyborg-abcdefghij is a word", "cyborg-abcdefghij is a word"},
		{"task, not a key", "task-abcdefghij12 failed", "task-abcdefghij12 failed"},
		{"risk, not a key", "risk-assessment-model is not available", "risk-assessment-model is not available"},
		{"31 hex", "a1b2c3d4e5f67890abcdef012345678", "a1b2c3d4e5f67890abcdef012345678"},
		{"numbers", "max_tokens: 99999999 > 64000", "max_tokens: 99999999 > 64000"},
		{"model name", "model claude-sonnet-5-5 not found", "model claude-sonnet-5-5 not found"},
		{"empty", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := RedactAccountIDs(tt.in); got != tt.want {
				t.Errorf("RedactAccountIDs(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}
