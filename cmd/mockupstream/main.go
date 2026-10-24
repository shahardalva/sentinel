// Command mockupstream imitates Anthropic's POST /v1/messages and OpenAI's
// POST /v1/chat/completions for load tests. It returns a fixed response
// after a fixed delay, so a load test through the gateway measures the
// gateway's own overhead rather than the model's.
package main

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"
)

const maxBodyBytes = 32 << 20

func main() {
	addr := flag.String("addr", "127.0.0.1:9090", "listen address")
	delay := flag.Duration("delay", 200*time.Millisecond, "delay before a non-streaming response and before the first stream event")
	eventDelay := flag.Duration("event-delay", 10*time.Millisecond, "delay between stream events")
	tlsCert := flag.String("tls-cert", "", "PEM certificate file; serve HTTPS (requires -tls-key)")
	tlsKey := flag.String("tls-key", "", "PEM private key file; serve HTTPS (requires -tls-cert)")
	http1 := flag.Bool("http1", false, "over HTTPS, offer only HTTP/1.1, matching the plain-http connection model")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	fail := func(err error) {
		logger.Error("mockupstream exited", "error", err)
		os.Exit(1)
	}
	srv := &http.Server{
		Handler:           newHandler(*delay, *eventDelay),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	// Check everything that can fail before logging that we're listening.
	useTLS, err := tlsEnabled(*tlsCert, *tlsKey)
	if err != nil {
		fail(err)
	}
	if useTLS {
		cert, err := tls.LoadX509KeyPair(*tlsCert, *tlsKey)
		if err != nil {
			fail(err)
		}
		srv.TLSConfig = &tls.Config{Certificates: []tls.Certificate{cert}}
	}
	if *http1 {
		// A non-nil, empty map stops ServeTLS from enabling HTTP/2.
		srv.TLSNextProto = map[string]func(*http.Server, *tls.Conn, http.Handler){}
	}
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		fail(err)
	}
	logger.Info("listening", "addr", ln.Addr().String(), "tls", useTLS, "http1_only", *http1,
		"delay", delay.String(), "event_delay", eventDelay.String())
	fail(serve(srv, ln, useTLS))
}

// tlsEnabled reports whether to serve HTTPS: true when certFile and keyFile
// are both set, false when neither is, and an error when only one is.
func tlsEnabled(certFile, keyFile string) (bool, error) {
	if (certFile == "") != (keyFile == "") {
		return false, errors.New("-tls-cert and -tls-key must be set together")
	}
	return certFile != "", nil
}

// serve runs srv on ln, over HTTPS with srv.TLSConfig's certificate when
// useTLS is set.
func serve(srv *http.Server, ln net.Listener, useTLS bool) error {
	if useTLS {
		return srv.ServeTLS(ln, "", "")
	}
	return srv.Serve(ln)
}

func newHandler(delay, eventDelay time.Duration) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/messages", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Model  string `json:"model"`
			Stream bool   `json:"stream"`
		}
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
		if err != nil || json.Unmarshal(body, &req) != nil {
			writeError(w, http.StatusBadRequest, "invalid_request_error", "request body must be JSON")
			return
		}
		if req.Model == "" {
			req.Model = "mock-model"
		}
		if !sleep(r, delay) {
			return
		}
		if req.Stream {
			stream(w, r, req.Model, eventDelay)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Request-Id", "req_mock")
		json.NewEncoder(w).Encode(message(req.Model))
	})
	mux.HandleFunc("POST /v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Model  string `json:"model"`
			Stream bool   `json:"stream"`
		}
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
		if err != nil || json.Unmarshal(body, &req) != nil {
			writeOpenAIError(w, http.StatusBadRequest, "invalid_request_error", "request body must be JSON")
			return
		}
		if req.Model == "" {
			req.Model = "mock-model"
		}
		if !sleep(r, delay) {
			return
		}
		if req.Stream {
			streamChat(w, r, req.Model, eventDelay)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Request-Id", "req_mock")
		json.NewEncoder(w).Encode(chatCompletion(req.Model))
	})
	return mux
}

// sleep waits for d, or returns false if the client goes away first.
func sleep(r *http.Request, d time.Duration) bool {
	if d <= 0 {
		return true
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-r.Context().Done():
		return false
	}
}

var replyChunks = []string{"Hello", " from", " the", " mock", " upstream."}

func message(model string) map[string]any {
	return map[string]any{
		"id":            "msg_mock",
		"type":          "message",
		"role":          "assistant",
		"model":         model,
		"content":       []map[string]any{{"type": "text", "text": "Hello from the mock upstream."}},
		"stop_reason":   "end_turn",
		"stop_sequence": nil,
		"usage":         map[string]int{"input_tokens": 10, "output_tokens": 7},
	}
}

type event struct {
	name string
	data any
}

// streamEvents is the Anthropic SSE sequence for a single text block.
func streamEvents(model string) []event {
	start := message(model)
	start["content"] = []any{}
	start["stop_reason"] = nil
	start["usage"] = map[string]int{"input_tokens": 10, "output_tokens": 1}
	events := []event{
		{"message_start", map[string]any{"type": "message_start", "message": start}},
		{"content_block_start", map[string]any{"type": "content_block_start", "index": 0,
			"content_block": map[string]any{"type": "text", "text": ""}}},
	}
	for _, c := range replyChunks {
		events = append(events, event{"content_block_delta", map[string]any{"type": "content_block_delta", "index": 0,
			"delta": map[string]any{"type": "text_delta", "text": c}}})
	}
	return append(events,
		event{"content_block_stop", map[string]any{"type": "content_block_stop", "index": 0}},
		event{"message_delta", map[string]any{"type": "message_delta",
			"delta": map[string]any{"stop_reason": "end_turn", "stop_sequence": nil},
			"usage": map[string]int{"output_tokens": 7}}},
		event{"message_stop", map[string]any{"type": "message_stop"}},
	)
}

func stream(w http.ResponseWriter, r *http.Request, model string, eventDelay time.Duration) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Request-Id", "req_mock")
	rc := http.NewResponseController(w)
	for i, ev := range streamEvents(model) {
		if i > 0 && !sleep(r, eventDelay) {
			return
		}
		data, _ := json.Marshal(ev.data)
		if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", ev.name, data); err != nil {
			return
		}
		if err := rc.Flush(); err != nil {
			return
		}
	}
}

// chatCompletion is OpenAI's non-streaming Chat Completions response.
func chatCompletion(model string) map[string]any {
	return map[string]any{
		"id":      "chatcmpl-mock",
		"object":  "chat.completion",
		"created": time.Now().Unix(),
		"model":   model,
		"choices": []map[string]any{{
			"index":         0,
			"message":       map[string]any{"role": "assistant", "content": "Hello from the mock upstream."},
			"finish_reason": "stop",
		}},
		"usage": map[string]int{"prompt_tokens": 10, "completion_tokens": 7, "total_tokens": 17},
	}
}

// chatChunks is the OpenAI stream for the reply: one chunk per text piece,
// the first carrying the role, then an empty delta with finish_reason.
func chatChunks(model string) []map[string]any {
	created := time.Now().Unix()
	chunk := func(delta map[string]any, finish any) map[string]any {
		return map[string]any{
			"id":      "chatcmpl-mock",
			"object":  "chat.completion.chunk",
			"created": created,
			"model":   model,
			"choices": []map[string]any{{"index": 0, "delta": delta, "finish_reason": finish}},
		}
	}
	var chunks []map[string]any
	for i, c := range replyChunks {
		delta := map[string]any{"content": c}
		if i == 0 {
			delta["role"] = "assistant"
		}
		chunks = append(chunks, chunk(delta, nil))
	}
	return append(chunks, chunk(map[string]any{}, "stop"))
}

func streamChat(w http.ResponseWriter, r *http.Request, model string, eventDelay time.Duration) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Request-Id", "req_mock")
	rc := http.NewResponseController(w)
	send := func(i int, data []byte) bool {
		if i > 0 && !sleep(r, eventDelay) {
			return false
		}
		if _, err := fmt.Fprintf(w, "data: %s\n\n", data); err != nil {
			return false
		}
		return rc.Flush() == nil
	}
	chunks := chatChunks(model)
	for i, c := range chunks {
		data, _ := json.Marshal(c)
		if !send(i, data) {
			return
		}
	}
	send(len(chunks), []byte("[DONE]"))
}

func writeOpenAIError(w http.ResponseWriter, status int, typ, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{"message": msg, "type": typ, "param": nil, "code": nil},
	})
}

func writeError(w http.ResponseWriter, status int, typ, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]any{
		"type":  "error",
		"error": map[string]string{"type": typ, "message": msg},
	})
}
