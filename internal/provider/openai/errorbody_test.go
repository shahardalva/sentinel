package openai

import (
	"context"
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

func TestForward_RedactsErrorBody(t *testing.T) {
	const (
		org     = "org-AbC123dEf456GhI789"
		project = "proj_q1W2e3R4t5Y6"
		uuid    = "a1b2c3d4-e5f6-7890-abcd-ef0123456789"
	)
	body := `{"error":{"message":"Rate limit reached for gpt-5 in organization ` + org + ` on tokens per min (TPM): Limit 30000, Used 29000, Requested 2000. Project ` +
		project + `, key ` + uuid + `. Request ID req_0123456789abcdef.","type":"tokens","param":null,"code":"rate_limit_exceeded","internal":{"org":"` + org + `"}}}`
	resp := forwardTo(t, respond(http.StatusTooManyRequests, "application/json; charset=utf-8", body, map[string]string{
		"Retry-After":                  "2",
		"Retry-After-Ms":               "1500",
		"X-Ratelimit-Remaining-Tokens": "0",
	}))

	if resp.Status != http.StatusTooManyRequests {
		t.Errorf("status = %d, want 429", resp.Status)
	}
	for k, v := range map[string]string{"Retry-After": "2", "Retry-After-Ms": "1500", "X-Ratelimit-Remaining-Tokens": "0", "Content-Type": "application/json"} {
		if got := resp.Header.Get(k); got != v {
			t.Errorf("%s = %q, want %q", k, got, v)
		}
	}
	// Same type, param and code; message redacted, request ID kept; the
	// unknown "internal" field dropped.
	want := `{"error":{"message":"Rate limit reached for gpt-5 in organization org-[redacted] on tokens per min (TPM): Limit 30000, Used 29000, Requested 2000. Project proj_[redacted], key [redacted]. Request ID req_0123456789abcdef.","type":"tokens","param":null,"code":"rate_limit_exceeded"}}` + "\n"
	if b, _ := io.ReadAll(resp.Body); string(b) != want {
		t.Errorf("body = %q, want %q", b, want)
	}
}

func TestForward_PassesUsefulErrorMessage(t *testing.T) {
	tests := []struct {
		name, body, want string
	}{
		{
			name: "OpenAI",
			body: `{"error":{"message":"Invalid 'max_tokens': integer below minimum value. Expected a value >= 1, but got 0 instead.","type":"invalid_request_error","param":"max_tokens","code":"integer_below_min_value"}}`,
			want: `{"error":{"message":"Invalid 'max_tokens': integer below minimum value. Expected a value >= 1, but got 0 instead.","type":"invalid_request_error","param":"max_tokens","code":"integer_below_min_value"}}`,
		},
		{
			// A compatible server with a numeric code and no param.
			name: "numeric code",
			body: `{"error":{"message":"model \"llama9\" not found","type":"not_found_error","code":404}}`,
			want: `{"error":{"message":"model \"llama9\" not found","type":"not_found_error","param":null,"code":404}}`,
		},
		{
			// Beyond float64's exact range: the digits must survive.
			name: "large numeric code",
			body: `{"error":{"message":"quota exceeded","type":"quota","param":null,"code":12345678901234567}}`,
			want: `{"error":{"message":"quota exceeded","type":"quota","param":null,"code":12345678901234567}}`,
		},
		{
			// Neither a string, number nor null: no client expects it.
			name: "object code",
			body: `{"error":{"message":"bad","type":"invalid_request_error","param":["a"],"code":{"x":1}}}`,
			want: `{"error":{"message":"bad","type":"invalid_request_error","param":null,"code":null}}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := forwardTo(t, respond(http.StatusBadRequest, "application/json", tt.body, nil))
			if resp.Status != http.StatusBadRequest {
				t.Errorf("status = %d, want 400", resp.Status)
			}
			if b, _ := io.ReadAll(resp.Body); string(b) != tt.want+"\n" {
				t.Errorf("body = %q, want %q", b, tt.want+"\n")
			}
		})
	}
}

func TestForward_ReplacesUnreadableErrorBody(t *testing.T) {
	huge := `{"error":{"message":"` + strings.Repeat("x", 70<<10) + `","type":"server_error"}}`
	tests := []struct {
		name        string
		status      int
		contentType string
		body        string
	}{
		{"html", http.StatusInternalServerError, "text/html", "<html><body><h1>500 Internal Server Error</h1></body></html>"},
		{"over the bound", http.StatusBadRequest, "application/json", huge},
		{"empty", http.StatusBadGateway, "text/plain", ""},
		{"error as a string", http.StatusNotFound, "application/json", `{"error":"model not found"}`},
		{"error without a message", http.StatusBadRequest, "application/json", `{"error":{"type":"invalid_request_error"}}`},
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
		io.WriteString(w, `{"error":`)
	}))
	defer srv.Close()

	_, err := mustNew(t, Config{BaseURL: srv.URL}).Forward(t.Context(), &proxy.Request{Header: http.Header{}})
	if err == nil || !strings.HasPrefix(err.Error(), "openai: read error body: ") {
		t.Errorf("err = %v, want a read error", err)
	}
}

func TestForward_SuccessBodyUntouched(t *testing.T) {
	const (
		first  = "data: {\"id\":\"chatcmpl-1\",\"system_fingerprint\":\"org-AbC123dEf456GhI789\",\"choices\":[{\"delta\":{\"content\":\"Hi\"}}]}\n\n"
		second = "data: [DONE]\n\n"
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
	// The first chunk is readable while the upstream holds back the second,
	// so the body is passed on as it arrives, not read whole first.
	got := make([]byte, len(first))
	if _, err := io.ReadFull(resp.Body, got); err != nil {
		t.Fatalf("read first chunk: %v", err)
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
		if err == nil || !strings.HasPrefix(err.Error(), "openai: upstream sent an informational status") {
			t.Errorf("err = %v, want the informational status error", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Forward still running 3s after its deadline")
	}
}
