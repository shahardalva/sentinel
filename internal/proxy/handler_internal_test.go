package proxy

import (
	"bytes"
	"errors"
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

// brokenClient is a ResponseWriter whose connection has gone away.
type brokenClient struct{ http.ResponseWriter }

func (brokenClient) Write([]byte) (int, error) { return 0, errors.New("broken pipe") }

func TestCopyFlush_AttributesFailures(t *testing.T) {
	err := copyFlush(brokenClient{httptest.NewRecorder()}, strings.NewReader("data"), time.Second)
	if _, ok := errors.AsType[*clientWriteError](err); !ok {
		t.Errorf("client write failure: err = %v, want *clientWriteError", err)
	}
	err = copyFlush(httptest.NewRecorder(), iotest.ErrReader(errors.New("upstream reset")), time.Second)
	if _, ok := errors.AsType[*clientWriteError](err); ok || err == nil {
		t.Errorf("upstream read failure: err = %v, want a non-client error", err)
	}
}

func TestLogLevel_Stream(t *testing.T) {
	failure := errors.New("boom")
	if got := logLevel(http.StatusOK, phaseStream, failure, false); got != slog.LevelError {
		t.Errorf("upstream-side stream failure: level = %v, want ERROR", got)
	}
	if got := logLevel(http.StatusOK, phaseStream, failure, true); got != slog.LevelWarn {
		t.Errorf("client-side stream failure: level = %v, want WARN", got)
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
