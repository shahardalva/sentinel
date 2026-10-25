package upstream

import (
	"errors"
	"io"
	"strings"
	"testing"
	"testing/iotest"
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
			b, err := ReadErrorBody(body)
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
	if _, err := ReadErrorBody(body); err == nil || errors.Is(err, ErrErrorBodyTooLarge) {
		t.Errorf("err = %v, want the read error", err)
	}
	if !body.closed {
		t.Error("body not closed")
	}
}

func TestRedactAccountIDs(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{
			"Rate limit reached for gpt-5 in organization org-AbC123dEf456GhI789 on tokens per min (TPM).",
			"Rate limit reached for gpt-5 in organization org-[redacted] on tokens per min (TPM).",
		},
		{"project proj_q1W2e3R4t5Y6 is over quota", "project proj_[redacted] is over quota"},
		{
			"This request would exceed your organization's (a1b2c3d4-E5F6-7890-abcd-ef0123456789) rate limit.",
			"This request would exceed your organization's ([redacted]) rate limit.",
		},
		{"org-1 and org-abc are too short", "org-1 and org-abc are too short"},
		{"cyborg-abcdefghij is a word", "cyborg-abcdefghij is a word"},
		{"two: org-aaaaaaaa1, org-bbbbbbbb2", "two: org-[redacted], org-[redacted]"},
		// Request IDs are kept, whatever their form.
		{"request req_011CUabcdEFGhijkLMNop failed", "request req_011CUabcdEFGhijkLMNop failed"},
		{"see req_0123456789abcdef0123456789abcdef", "see req_0123456789abcdef0123456789abcdef"},
		{"max_tokens: 99999999 > 64000", "max_tokens: 99999999 > 64000"},
		{"", ""},
	}
	for _, tt := range tests {
		if got := RedactAccountIDs(tt.in); got != tt.want {
			t.Errorf("RedactAccountIDs(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}
