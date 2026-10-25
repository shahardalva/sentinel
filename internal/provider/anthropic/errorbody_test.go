package anthropic

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/shahardalva/sentinel/internal/proxy"
)

// forwardTo returns what Forward makes of upstream's response.
func forwardTo(t *testing.T, upstream http.HandlerFunc) *proxy.Response {
	t.Helper()
	srv := httptest.NewServer(upstream)
	t.Cleanup(srv.Close)
	resp, err := mustNew(t, Config{BaseURL: srv.URL}).Forward(t.Context(), &proxy.Request{Header: http.Header{}})
	if err != nil {
		t.Fatalf("Forward: %v", err)
	}
	t.Cleanup(func() { resp.Body.Close() }) // before srv.Close, which waits for it
	return resp
}

// respond answers with status, a Content-Type and body.
func respond(status int, contentType, body string, header map[string]string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		for k, v := range header {
			w.Header().Set(k, v)
		}
		w.Header().Set("Content-Type", contentType)
		w.WriteHeader(status)
		io.WriteString(w, body)
	}
}

// wireError is the Messages API's error shape, as a client decodes it.
type wireError struct {
	Type  string `json:"type"`
	Error struct {
		Type    string `json:"type"`
		Message string `json:"message"`
	} `json:"error"`
	RequestID string `json:"request_id"`
}

func decodeError(t *testing.T, b []byte) wireError {
	t.Helper()
	var e wireError
	if err := json.Unmarshal(b, &e); err != nil {
		t.Fatalf("decode %q: %v", b, err)
	}
	return e
}

func TestForward_RedactsErrorBody(t *testing.T) {
	const (
		uuid    = "a1b2c3d4-e5f6-7890-abcd-ef0123456789"
		org     = "org-AbC123dEf456GhI789"
		project = "proj_q1W2e3R4t5Y6"
	)
	body := `{"type":"error","error":{"type":"rate_limit_error","message":"This request would exceed your organization's (` + uuid +
		`) rate limit of 50 requests per minute (` + org + `, ` + project + `)."},"request_id":"req_011CUabcdEFGhijk"}`
	resp := forwardTo(t, respond(http.StatusTooManyRequests, "application/json", body, map[string]string{"Retry-After": "20"}))

	if resp.Status != http.StatusTooManyRequests {
		t.Errorf("status = %d, want 429", resp.Status)
	}
	if ra := resp.Header.Get("Retry-After"); ra != "20" {
		t.Errorf("Retry-After = %q, want 20", ra)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q", ct)
	}
	b, _ := io.ReadAll(resp.Body)
	for _, id := range []string{uuid, org, project} {
		if strings.Contains(string(b), id) {
			t.Errorf("body %q still names %s", b, id)
		}
	}
	e := decodeError(t, b)
	want := "This request would exceed your organization's ([redacted]) rate limit of 50 requests per minute (org-[redacted], proj_[redacted])."
	if e.Type != "error" || e.Error.Type != "rate_limit_error" || e.Error.Message != want {
		t.Errorf("error = %+v, want type rate_limit_error and message %q", e, want)
	}
	if e.RequestID != "req_011CUabcdEFGhijk" {
		t.Errorf("request_id = %q, want it kept", e.RequestID)
	}
}

func TestForward_PassesUsefulErrorMessage(t *testing.T) {
	// Already in the form the provider re-encodes to, so nothing may change.
	body := `{"error":{"message":"max_tokens: 100000 > 64000, which is the maximum allowed number of output tokens for claude-sonnet-5-5","type":"invalid_request_error"},"request_id":"req_011CUxyz","type":"error"}` + "\n"
	resp := forwardTo(t, respond(http.StatusBadRequest, "application/json", body, nil))

	if resp.Status != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.Status)
	}
	if b, _ := io.ReadAll(resp.Body); string(b) != body {
		t.Errorf("body = %q, want %q", b, body)
	}
}

func TestForward_ReplacesUnreadableErrorBody(t *testing.T) {
	huge := `{"type":"error","error":{"type":"api_error","message":"` + strings.Repeat("x", 70<<10) + `"}}`
	tests := []struct {
		name        string
		status      int
		contentType string
		body        string
	}{
		{"html", http.StatusInternalServerError, "text/html", "<html><body><h1>500 Internal Server Error</h1></body></html>"},
		{"over the bound", http.StatusBadRequest, "application/json", huge},
		{"empty", http.StatusBadGateway, "text/plain", ""},
		{"another shape", http.StatusNotFound, "application/json", `{"error":"model not found"}`},
		{"error without a type", http.StatusBadRequest, "application/json", `{"type":"error","error":{"message":"x"}}`},
		{"trailing data", http.StatusBadRequest, "application/json", `{"type":"error","error":{"type":"api_error","message":"x"}} <html>`},
		{"overloaded, not JSON", 529, "text/plain", "Overloaded"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := forwardTo(t, respond(tt.status, tt.contentType, tt.body, nil))

			if resp.Status != tt.status {
				t.Errorf("status = %d, want %d", resp.Status, tt.status)
			}
			if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
				t.Errorf("Content-Type = %q, want application/json", ct)
			}
			_, want := FormatError(tt.status, proxy.ErrAPI, "upstream error")
			if b, _ := io.ReadAll(resp.Body); string(b) != string(want) {
				t.Errorf("body = %.200q, want %q", b, want)
			}
		})
	}
}

func TestForward_ErrorBodyReadFails(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Declares more than it sends; net/http then closes the connection.
		w.Header().Set("Content-Length", "100")
		w.WriteHeader(http.StatusInternalServerError)
		io.WriteString(w, `{"type":`)
	}))
	defer srv.Close()

	_, err := mustNew(t, Config{BaseURL: srv.URL}).Forward(t.Context(), &proxy.Request{Header: http.Header{}})
	if err == nil || !strings.HasPrefix(err.Error(), "anthropic: read error body: ") {
		t.Errorf("err = %v, want a read error", err)
	}
}

func TestForward_SuccessBodyUntouched(t *testing.T) {
	const (
		first  = "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"usage\":{\"org\":\"org-AbC123dEf456GhI789\"}}}\n\n"
		second = "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"
	)
	next := make(chan struct{})
	resp := forwardTo(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, first)
		w.(http.Flusher).Flush()
		select {
		case <-next:
		case <-r.Context().Done():
			return
		}
		io.WriteString(w, second)
	})

	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("Content-Type = %q", ct)
	}
	// The first event is readable while the upstream holds back the second,
	// so the body is passed on as it arrives, not read whole first.
	got := make([]byte, len(first))
	if _, err := io.ReadFull(resp.Body, got); err != nil {
		t.Fatalf("read first event: %v", err)
	}
	close(next)
	rest, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read rest: %v", err)
	}
	// Byte for byte, identifiers included: a 2xx body is never rewritten.
	if string(got)+string(rest) != first+second {
		t.Errorf("body = %q, want %q", string(got)+string(rest), first+second)
	}
}

func TestForward_InformationalStatus(t *testing.T) {
	// Answers 101 and holds the connection: the body is then the raw
	// connection, which cancelling the request would not close.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, bw, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		defer conn.Close()
		bw.WriteString("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: x\r\n\r\n")
		bw.Flush()
		conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		io.Copy(io.Discard, conn)
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	errc := make(chan error, 1)
	go func() {
		resp, err := mustNew(t, Config{BaseURL: srv.URL}).Forward(ctx, &proxy.Request{Header: http.Header{}})
		if err == nil {
			resp.Body.Close()
		}
		errc <- err
	}()
	select {
	case err := <-errc:
		if err == nil || !strings.HasPrefix(err.Error(), "anthropic: upstream sent an informational status") {
			t.Errorf("err = %v, want the informational status error", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Forward still running 3s after its deadline")
	}
}
