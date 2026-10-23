package main

import (
	"bufio"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func post(t *testing.T, url, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(url+"/v1/messages", "application/json", strings.NewReader(body))
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

func TestServe_RequiresCertAndKeyTogether(t *testing.T) {
	for _, files := range [][2]string{{"server.pem", ""}, {"", "server-key.pem"}} {
		srv := &http.Server{Addr: "127.0.0.1:0", Handler: newHandler(0, 0)}
		err := serve(srv, files[0], files[1])
		if err == nil || !strings.Contains(err.Error(), "must be set together") {
			t.Errorf("serve(%q, %q) = %v, want error about setting both", files[0], files[1], err)
		}
	}
}
