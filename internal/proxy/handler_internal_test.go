package proxy

import (
	"bytes"
	"io"
	"strings"
	"testing"
	"testing/iotest"
)

func TestReadBody(t *testing.T) {
	body := strings.Repeat("x", 100_000)
	tests := []struct {
		name          string
		contentLength int64
	}{
		{"known length", int64(len(body))},
		{"unknown length", -1},
		{"declared length over limit", 1 << 30},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// OneByteReader forces many reads, so any growth would happen.
			got, err := readBody(iotest.OneByteReader(strings.NewReader(body)), tt.contentLength, DefaultMaxBodyBytes)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != body {
				t.Errorf("read %d bytes, want %d", len(got), len(body))
			}
		})
	}
}

func TestReadBody_KnownLengthAllocatesOnce(t *testing.T) {
	body := bytes.Repeat([]byte("x"), 1<<20)
	allocs := testing.AllocsPerRun(10, func() {
		if _, err := readBody(bytes.NewReader(body), int64(len(body)), DefaultMaxBodyBytes); err != nil {
			t.Fatal(err)
		}
	})
	// One for the buffer, one for the bytes.Buffer header.
	if allocs > 2 {
		t.Errorf("allocs = %v, want <= 2", allocs)
	}
	// io.ReadAll on the same body grows its buffer repeatedly; this confirms
	// the comparison above is meaningful.
	baseline := testing.AllocsPerRun(10, func() { io.ReadAll(bytes.NewReader(body)) })
	if baseline <= allocs {
		t.Errorf("io.ReadAll allocs = %v, not more than readBody's %v", baseline, allocs)
	}
}
