package proxy

import (
	"bytes"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/iotest"
	"time"
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
			got, err := readBody(iotest.OneByteReader(strings.NewReader(body)), tt.contentLength)
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
	body := bytes.Repeat([]byte("x"), initialBodyBuf)
	allocs := testing.AllocsPerRun(10, func() {
		if _, err := readBody(bytes.NewReader(body), int64(len(body))); err != nil {
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

func TestReadBody_DeclaredLengthNotTrusted(t *testing.T) {
	// A client declares 32 MiB and sends two bytes.
	got, err := readBody(strings.NewReader("{}"), DefaultMaxBodyBytes)
	if err != nil {
		t.Fatal(err)
	}
	if c := cap(got); c > initialBodyBuf+bytes.MinRead {
		t.Errorf("buffer cap = %d, want at most %d", c, initialBodyBuf+bytes.MinRead)
	}
}

func TestRequireKey_ClearsReadDeadline(t *testing.T) {
	old := authReadTimeout
	authReadTimeout = 50 * time.Millisecond
	defer func() { authReadTimeout = old }()

	got := make(chan string, 1)
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := io.ReadAll(r.Body)
		if err != nil {
			got <- "error: " + err.Error()
			return
		}
		got <- string(b)
	})
	srv := httptest.NewServer(RequireKey([]string{"k"}, slog.New(slog.DiscardHandler), next))
	defer srv.Close()

	// The body arrives after the auth deadline would have passed.
	pr, pw := io.Pipe()
	go func() {
		time.Sleep(150 * time.Millisecond)
		io.WriteString(pw, "late")
		pw.Close()
	}()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, srv.URL, pr)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Api-Key", "k")
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	if g := <-got; g != "late" {
		t.Errorf("next read %q, want %q; the auth deadline leaked into next", g, "late")
	}
}
