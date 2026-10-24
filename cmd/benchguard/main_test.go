package main

import (
	"strings"
	"testing"
)

const mock = "https://mockupstream:9090"

// enabled is the gateway's "upstream enabled" line for name at baseURL.
func enabled(name, baseURL string) string {
	return `{"time":"2026-10-24T10:00:00Z","level":"INFO","msg":"upstream enabled","upstream":"` + name +
		`","protocol":"openai","base_url":"` + baseURL + `"}`
}

const listening = `{"time":"2026-10-24T10:00:00Z","level":"INFO","msg":"listening","addr":"0.0.0.0:8081"}`

func TestCheck(t *testing.T) {
	tests := []struct {
		name    string
		lines   []string
		wantErr string // empty for success; else a substring of the error
	}{
		{
			name: "every upstream on the mock",
			lines: []string{
				"some non-JSON line\r",
				enabled("anthropic", mock),
				enabled("openai", mock+"/v1") + "\r", // Docker Desktop may add \r
				enabled("mockcompat", mock+"/v1"),
				`{"level":"INFO","msg":"rate limiting enabled","redis":"redis:6379"}`,
				listening,
			},
		},
		{name: "one upstream on a public API", lines: []string{enabled("anthropic", mock), enabled("openai", "https://api.openai.com/v1"), listening}, wantErr: `upstream "openai" is not on the mock`},
		{name: "host that only starts like the mock", lines: []string{enabled("xai", mock+".example.com/v1"), listening}, wantErr: `upstream "xai"`},
		{name: "longer port", lines: []string{enabled("xai", mock+"1/v1"), listening}, wantErr: `upstream "xai"`},
		{name: "userinfo trick", lines: []string{enabled("xai", mock+"@api.x.ai/v1"), listening}, wantErr: `upstream "xai"`},
		{name: "plain http", lines: []string{enabled("xai", "http://mockupstream:9090/v1"), listening}, wantErr: `upstream "xai"`},
		{name: "no base_url field", lines: []string{`{"msg":"upstream enabled","upstream":"xai"}`, listening}, wantErr: `upstream "xai"`},
		{name: "escaped slash still on the mock", lines: []string{enabled("openai", mock+`/v1`), listening}},
		{name: "no upstream lines", lines: []string{listening}, wantErr: `no "upstream enabled" line`},
		{name: "never listening", lines: []string{enabled("anthropic", mock), `{"level":"ERROR","msg":"gateway exited","error":"OPENAI_API_KEY is not set"}`}, wantErr: `did not log "listening"`},
		{name: "empty log", wantErr: `did not log "listening"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := check(strings.NewReader(strings.Join(tt.lines, "\n")), mock)
			switch {
			case tt.wantErr == "" && err != nil:
				t.Fatalf("check = %v, want nil", err)
			case tt.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErr)):
				t.Fatalf("check = %v, want an error containing %q", err, tt.wantErr)
			}
			// The error names the upstream, never the base URL it resolved.
			if err != nil && (strings.Contains(err.Error(), "api.") || strings.Contains(err.Error(), "example.com")) {
				t.Errorf("error %q repeats a base URL", err)
			}
		})
	}
}

func TestCheck_NeedsMock(t *testing.T) {
	if err := check(strings.NewReader(enabled("anthropic", "https://api.anthropic.com")+"\n"+listening), ""); err == nil {
		t.Fatal("check with no -mock = nil, want an error")
	}
}
