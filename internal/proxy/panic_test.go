package proxy_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/shahardalva/sentinel/internal/provider/anthropic"
	"github.com/shahardalva/sentinel/internal/proxy"
)

var errBoom = errors.New("provider bug")

// panicProvider panics in Forward, or, with inStream, returns a 200 whose
// body panics on the first Read.
type panicProvider struct{ inStream bool }

func (panicProvider) Name() string { return "stub" }
func (panicProvider) FormatError(status int, kind proxy.ErrorKind, msg string) (string, []byte) {
	return anthropic.FormatError(status, kind, msg)
}
func (p panicProvider) Forward(context.Context, *proxy.Request) (*proxy.Response, error) {
	if !p.inStream {
		panic(errBoom)
	}
	return &proxy.Response{Status: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(panicReader{})}, nil
}

type panicReader struct{}

func (panicReader) Read([]byte) (int, error) { panic(errBoom) }

// servePanicking runs h on one request and returns what it panicked with.
func servePanicking(h http.Handler) (recovered any) {
	defer func() { recovered = recover() }()
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(reqBody)))
	return nil
}

// A provider that panics is a server failure: recorded and logged as one,
// and the panic still reaches net/http with its own value.
func TestHandler_ProviderPanic(t *testing.T) {
	tests := []struct {
		name       string
		provider   panicProvider
		wantStatus int
		wantPhase  string
		wantStages proxy.Stages
	}{
		// Nothing was sent, so the gateway's failure is a 500.
		{"in Forward", panicProvider{}, 500, proxy.PhaseUpstream, proxy.StageReadBody | proxy.StageUpstreamTTFB},
		// The 200 is already on the wire; like any broken stream it keeps it.
		{"in the stream", panicProvider{inStream: true}, 200, proxy.PhaseStream, proxy.StageReadBody | proxy.StageUpstreamTTFB | proxy.StageStream},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := &fakeRecorder{}
			var logs bytes.Buffer
			h := proxy.NewHandler(tt.provider, slog.New(slog.NewJSONHandler(&logs, nil)), proxy.Options{Recorder: rec})

			if got := servePanicking(h); got != errBoom {
				t.Fatalf("panic value = %v, want the provider's own (%v)", got, errBoom)
			}
			info := rec.only(t)
			if info.Status != tt.wantStatus || !info.Failed || info.Phase != tt.wantPhase || info.Ran != tt.wantStages {
				t.Errorf("RequestInfo = %+v; want status %d, failed, phase %s, stages %b", info, tt.wantStatus, tt.wantPhase, tt.wantStages)
			}
			line := logLine(t, &logs)
			if line["status"] != float64(tt.wantStatus) || line["level"] != "ERROR" || line["phase"] != tt.wantPhase {
				t.Errorf("log line = %v; want status %d at ERROR, phase %s", line, tt.wantStatus, tt.wantPhase)
			}
			if e, _ := line["error"].(string); !strings.Contains(e, "panic") || !strings.Contains(e, errBoom.Error()) {
				t.Errorf("log error = %q, want it to name the panic", e)
			}
			// The stack is the panic site's, not the re-panic's.
			site := "panicProvider.Forward"
			if tt.provider.inStream {
				site = "panicReader.Read"
			}
			if s, _ := line["stack"].(string); !strings.Contains(s, site) {
				t.Errorf("log stack doesn't show %s:\n%s", site, s)
			}
		})
	}
}

// Through a real server, net/http still recovers the re-raised panic: the
// client's connection is dropped and the server keeps serving.
func TestGateway_ProviderPanicOverHTTP(t *testing.T) {
	rec := &fakeRecorder{}
	var calls int
	mux := http.NewServeMux()
	mux.Handle("POST /panic", proxy.NewHandler(panicProvider{}, slog.New(slog.DiscardHandler), proxy.Options{Recorder: rec}))
	mux.HandleFunc("GET /ok", func(w http.ResponseWriter, r *http.Request) { calls++ })
	srv := httptest.NewUnstartedServer(mux)
	srv.Config.ErrorLog = log.New(io.Discard, "", 0) // net/http logs the panic
	srv.Start()
	defer srv.Close()

	if resp, err := http.Post(srv.URL+"/panic", "application/json", strings.NewReader(reqBody)); err == nil {
		resp.Body.Close()
		t.Fatalf("status %d, want the connection dropped", resp.StatusCode)
	}
	if info := rec.only(t); info.Status != 500 || !info.Failed {
		t.Errorf("RequestInfo = %+v, want a failed 500", info)
	}
	resp, err := http.Get(srv.URL + "/ok")
	if err != nil {
		t.Fatalf("server not serving after the panic: %v", err)
	}
	resp.Body.Close()
	if calls != 1 {
		t.Errorf("next request handled %d times, want 1", calls)
	}
}

// A panic with no Recorder still logs a 500 and re-panics.
func TestHandler_ProviderPanicNoRecorder(t *testing.T) {
	var logs bytes.Buffer
	h := proxy.NewHandler(panicProvider{}, slog.New(slog.NewJSONHandler(&logs, nil)), proxy.Options{})
	if got := servePanicking(h); got != errBoom {
		t.Fatalf("panic value = %v, want %v", got, errBoom)
	}
	if line := logLine(t, &logs); line["status"] != float64(500) {
		t.Errorf("log status = %v, want 500", line["status"])
	}
}
