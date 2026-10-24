package main

import (
	"bufio"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/shahardalva/sentinel/internal/devcert"
)

func post(t *testing.T, url, body string) *http.Response {
	t.Helper()
	return postPath(t, url, "/v1/messages", body)
}

func postPath(t *testing.T, url, path, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(url+path, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func TestNonStreamResponse(t *testing.T) {
	const delay = 50 * time.Millisecond
	srv := httptest.NewServer(newHandler(delay, 0))
	defer srv.Close()

	start := time.Now()
	resp := post(t, srv.URL, `{"model":"claude-test","max_tokens":16,"messages":[{"role":"user","content":"hi"}]}`)
	if elapsed := time.Since(start); elapsed < delay {
		t.Errorf("responded after %v, want at least %v", elapsed, delay)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q", ct)
	}

	var msg struct {
		ID         string `json:"id"`
		Type       string `json:"type"`
		Role       string `json:"role"`
		Model      string `json:"model"`
		StopReason string `json:"stop_reason"`
		Content    []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
		Usage struct {
			InputTokens  int `json:"input_tokens"`
			OutputTokens int `json:"output_tokens"`
		} `json:"usage"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&msg); err != nil {
		t.Fatal(err)
	}
	if msg.Type != "message" || msg.Role != "assistant" || msg.ID == "" {
		t.Errorf("type/role/id = %q/%q/%q", msg.Type, msg.Role, msg.ID)
	}
	if msg.Model != "claude-test" {
		t.Errorf("model = %q, want the request's model echoed", msg.Model)
	}
	if msg.StopReason != "end_turn" {
		t.Errorf("stop_reason = %q", msg.StopReason)
	}
	if len(msg.Content) != 1 || msg.Content[0].Type != "text" || msg.Content[0].Text == "" {
		t.Errorf("content = %+v", msg.Content)
	}
	if msg.Usage.OutputTokens == 0 {
		t.Errorf("usage = %+v", msg.Usage)
	}
}

func TestStreamEventSequence(t *testing.T) {
	srv := httptest.NewServer(newHandler(0, time.Millisecond))
	defer srv.Close()

	resp := post(t, srv.URL, `{"model":"claude-test","stream":true}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("Content-Type = %q", ct)
	}

	var names []string
	var text strings.Builder
	sc := bufio.NewScanner(resp.Body)
	var name string
	for sc.Scan() {
		line := sc.Text()
		switch {
		case strings.HasPrefix(line, "event: "):
			name = strings.TrimPrefix(line, "event: ")
			names = append(names, name)
		case strings.HasPrefix(line, "data: "):
			var data struct {
				Type  string `json:"type"`
				Delta struct {
					Text string `json:"text"`
				} `json:"delta"`
			}
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &data); err != nil {
				t.Fatalf("event %s: bad data: %v", name, err)
			}
			if data.Type != name {
				t.Errorf("data type %q under event %q", data.Type, name)
			}
			text.WriteString(data.Delta.Text)
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}

	want := []string{"message_start", "content_block_start"}
	for range replyChunks {
		want = append(want, "content_block_delta")
	}
	want = append(want, "content_block_stop", "message_delta", "message_stop")
	if strings.Join(names, ",") != strings.Join(want, ",") {
		t.Errorf("events = %v\nwant     %v", names, want)
	}
	if got := text.String(); got != strings.Join(replyChunks, "") {
		t.Errorf("streamed text = %q", got)
	}
}

func TestChatCompletion(t *testing.T) {
	const delay = 50 * time.Millisecond
	srv := httptest.NewServer(newHandler(delay, 0))
	defer srv.Close()

	start := time.Now()
	resp := postPath(t, srv.URL, "/v1/chat/completions", `{"model":"gpt-test","messages":[{"role":"user","content":"hi"}]}`)
	if elapsed := time.Since(start); elapsed < delay {
		t.Errorf("responded after %v, want at least %v", elapsed, delay)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q", ct)
	}
	if id := resp.Header.Get("X-Request-Id"); id != "req_mock" {
		t.Errorf("X-Request-Id = %q, want req_mock", id)
	}

	var c struct {
		ID      string `json:"id"`
		Object  string `json:"object"`
		Created int64  `json:"created"`
		Model   string `json:"model"`
		Choices []struct {
			Index   int `json:"index"`
			Message struct {
				Role    string `json:"role"`
				Content string `json:"content"`
			} `json:"message"`
			FinishReason string `json:"finish_reason"`
		} `json:"choices"`
		Usage struct {
			PromptTokens     int `json:"prompt_tokens"`
			CompletionTokens int `json:"completion_tokens"`
			TotalTokens      int `json:"total_tokens"`
		} `json:"usage"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&c); err != nil {
		t.Fatal(err)
	}
	if c.Object != "chat.completion" || c.ID == "" || c.Created == 0 {
		t.Errorf("object/id/created = %q/%q/%d", c.Object, c.ID, c.Created)
	}
	if c.Model != "gpt-test" {
		t.Errorf("model = %q, want the request's model echoed", c.Model)
	}
	if len(c.Choices) != 1 || c.Choices[0].Message.Role != "assistant" ||
		c.Choices[0].Message.Content != strings.Join(replyChunks, "") || c.Choices[0].FinishReason != "stop" {
		t.Errorf("choices = %+v", c.Choices)
	}
	if c.Usage.TotalTokens != c.Usage.PromptTokens+c.Usage.CompletionTokens || c.Usage.CompletionTokens == 0 {
		t.Errorf("usage = %+v", c.Usage)
	}
}

func TestChatCompletionStream(t *testing.T) {
	srv := httptest.NewServer(newHandler(0, time.Millisecond))
	defer srv.Close()

	resp := postPath(t, srv.URL, "/v1/chat/completions", `{"model":"gpt-test","stream":true}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("Content-Type = %q", ct)
	}
	if id := resp.Header.Get("X-Request-Id"); id != "req_mock" {
		t.Errorf("X-Request-Id = %q, want req_mock", id)
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}

	// Every event is a data line and a blank line, with no event: names.
	events := strings.Split(strings.TrimSuffix(string(raw), "\n\n"), "\n\n")
	if want := len(replyChunks) + 2; len(events) != want {
		t.Fatalf("got %d events, want %d:\n%s", len(events), want, raw)
	}
	if last := events[len(events)-1]; last != "data: [DONE]" {
		t.Errorf("last event = %q, want data: [DONE]", last)
	}
	var text strings.Builder
	var finishes []string
	for i, ev := range events[:len(events)-1] {
		data, ok := strings.CutPrefix(ev, "data: ")
		if !ok || strings.Contains(data, "\n") {
			t.Fatalf("event %d = %q, want a single data line", i, ev)
		}
		var chunk struct {
			Object  string `json:"object"`
			Model   string `json:"model"`
			Choices []struct {
				Delta struct {
					Role    string `json:"role"`
					Content string `json:"content"`
				} `json:"delta"`
				FinishReason *string `json:"finish_reason"`
			} `json:"choices"`
		}
		if err := json.Unmarshal([]byte(data), &chunk); err != nil {
			t.Fatalf("event %d: bad data: %v", i, err)
		}
		if chunk.Object != "chat.completion.chunk" || chunk.Model != "gpt-test" || len(chunk.Choices) != 1 {
			t.Fatalf("event %d = %s", i, data)
		}
		choice := chunk.Choices[0]
		if i == 0 && choice.Delta.Role != "assistant" {
			t.Errorf("first chunk role = %q, want assistant", choice.Delta.Role)
		}
		text.WriteString(choice.Delta.Content)
		if choice.FinishReason != nil {
			finishes = append(finishes, fmt.Sprintf("%d:%s", i, *choice.FinishReason))
		}
	}
	if got := text.String(); got != strings.Join(replyChunks, "") {
		t.Errorf("streamed text = %q", got)
	}
	if want := fmt.Sprintf("%d:stop", len(replyChunks)); len(finishes) != 1 || finishes[0] != want {
		t.Errorf("finish reasons = %q, want only %q on the final chunk", finishes, want)
	}
}

func TestRejectsWrongMethodPathAndBody(t *testing.T) {
	srv := httptest.NewServer(newHandler(0, 0))
	defer srv.Close()

	tests := []struct {
		method, path, body string
		want               int
	}{
		{http.MethodGet, "/v1/messages", "", http.StatusMethodNotAllowed},
		{http.MethodPost, "/v1/complete", "{}", http.StatusNotFound},
		{http.MethodPost, "/", "{}", http.StatusNotFound},
		{http.MethodPost, "/v1/messages", "not json", http.StatusBadRequest},
		{http.MethodGet, "/v1/chat/completions", "", http.StatusMethodNotAllowed},
		{http.MethodPost, "/v1/chat/completions", "not json", http.StatusBadRequest},
		{http.MethodPost, "/chat/completions", "{}", http.StatusNotFound},
	}
	for _, tt := range tests {
		req, err := http.NewRequest(tt.method, srv.URL+tt.path, strings.NewReader(tt.body))
		if err != nil {
			t.Fatal(err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != tt.want {
			t.Errorf("%s %s: status = %d, want %d", tt.method, tt.path, resp.StatusCode, tt.want)
		}
	}
}

func TestTLSEnabled(t *testing.T) {
	tests := []struct {
		cert, key string
		want      bool
		wantErr   bool
	}{
		{"", "", false, false},
		{"server.pem", "server-key.pem", true, false},
		{"server.pem", "", false, true},
		{"", "server-key.pem", false, true},
	}
	for _, tt := range tests {
		got, err := tlsEnabled(tt.cert, tt.key)
		if got != tt.want || (err != nil) != tt.wantErr {
			t.Errorf("tlsEnabled(%q, %q) = %v, %v; want %v, error %v", tt.cert, tt.key, got, err, tt.want, tt.wantErr)
		}
	}
}

func TestServe_TLS(t *testing.T) {
	b, err := devcert.Generate([]string{"127.0.0.1"}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := tls.X509KeyPair(b.ServerCert, b.ServerKey)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(b.CACert) {
		t.Fatal("bad CA PEM")
	}

	for _, http1 := range []bool{false, true} {
		srv := &http.Server{Handler: newHandler(0, 0), TLSConfig: &tls.Config{Certificates: []tls.Certificate{cert}}}
		if http1 {
			srv.TLSNextProto = map[string]func(*http.Server, *tls.Conn, http.Handler){}
		}
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- serve(srv, ln, true) }()

		client := &http.Client{Transport: &http.Transport{
			TLSClientConfig:   &tls.Config{RootCAs: roots},
			ForceAttemptHTTP2: true,
		}}
		resp, err := client.Post("https://"+ln.Addr().String()+"/v1/messages", "application/json", strings.NewReader(`{}`))
		if err != nil {
			t.Fatalf("http1=%v: POST: %v", http1, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK || resp.TLS == nil {
			t.Errorf("http1=%v: status %d, TLS %v; want 200 over TLS", http1, resp.StatusCode, resp.TLS != nil)
		}
		if want := map[bool]int{false: 2, true: 1}[http1]; resp.ProtoMajor != want {
			t.Errorf("http1=%v: protocol %s, want HTTP/%d", http1, resp.Proto, want)
		}

		srv.Close()
		if err := <-done; !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("http1=%v: serve returned %v, want ErrServerClosed", http1, err)
		}
	}
}
