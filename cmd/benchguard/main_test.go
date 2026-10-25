package main

import (
	"strings"
	"testing"
)

const mock = "https://mockupstream:9090"

// enabled is the gateway's "upstream enabled" line for name at baseURL,
// which the gateway logs as scheme://host[:port].
func enabled(name, baseURL string) string {
	return `{"time":"2026-10-24T10:00:00Z","level":"INFO","msg":"upstream enabled","upstream":"` + name +
		`","protocol":"openai","base_url":"` + baseURL + `"}`
}

const listening = `{"time":"2026-10-24T10:00:00Z","level":"INFO","msg":"listening","addr":"0.0.0.0:8081"}`

// The metrics listener's startup lines, logged before "listening".
const (
	metricsListening = `{"time":"2026-10-24T10:00:00Z","level":"INFO","msg":"metrics listening","addr":"[::]:9091","path":"/metrics"}`
	metricsDisabled  = `{"time":"2026-10-24T10:00:00Z","level":"INFO","msg":"metrics disabled: SENTINEL_METRICS_ADDR is off"}`
)

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
				enabled("openai", mock) + "\r", // Docker Desktop may add \r
				enabled("mockcompat", mock),
				`{"level":"INFO","msg":"rate limiting enabled","redis":"redis:6379"}`,
				listening,
			},
		},
		{
			name: "with the metrics listener",
			lines: []string{
				enabled("anthropic", mock),
				enabled("openai", mock),
				`{"level":"INFO","msg":"rate limiting enabled","redis":"redis:6379"}`,
				metricsListening,
				listening,
			},
		},
		{name: "metrics off", lines: []string{enabled("anthropic", mock), metricsDisabled, listening}},
		{name: "escaped slashes still the mock", lines: []string{enabled("openai", `https:\/\/mockupstream:9090`), listening}},

		// Not the mock, or not exactly.
		{name: "one upstream on a public API", lines: []string{enabled("anthropic", mock), enabled("openai", "https://api.openai.com"), listening}, wantErr: `upstream "openai" is not on the mock`},
		{name: "a path", lines: []string{enabled("xai", mock+"/v1"), listening}, wantErr: `upstream "xai" is not on the mock`},
		{name: "a trailing slash", lines: []string{enabled("xai", mock+"/"), listening}, wantErr: `upstream "xai" is not on the mock`},
		{name: "host that only starts like the mock", lines: []string{enabled("xai", mock+".example.com"), listening}, wantErr: `upstream "xai"`},
		{name: "longer port", lines: []string{enabled("xai", mock+"1"), listening}, wantErr: `upstream "xai"`},
		{name: "userinfo trick", lines: []string{enabled("xai", mock+"@api.x.ai"), listening}, wantErr: `upstream "xai"`},
		{name: "plain http", lines: []string{enabled("xai", "http://mockupstream:9090"), listening}, wantErr: `upstream "xai"`},
		{name: "different case", lines: []string{enabled("xai", "https://MockUpstream:9090"), listening}, wantErr: `upstream "xai"`},

		// Lines that can't be read as one "upstream enabled" line.
		{name: "no base_url field", lines: []string{`{"msg":"upstream enabled","upstream":"xai"}`, listening}, wantErr: `upstream "xai": 0 base_url fields`},
		{
			name:    "base_url twice",
			lines:   []string{`{"msg":"upstream enabled","upstream":"xai","base_url":"https://api.x.ai","base_url":"` + mock + `"}`, listening},
			wantErr: `upstream "xai": 2 base_url fields`,
		},
		{
			name:    "base_url in another case",
			lines:   []string{`{"msg":"upstream enabled","upstream":"xai","base_url":"https://api.x.ai","BASE_URL":"` + mock + `"}`, listening},
			wantErr: `upstream "xai": 2 base_url fields`,
		},
		{name: "base_url not a string", lines: []string{`{"msg":"upstream enabled","upstream":"xai","base_url":null}`, listening}, wantErr: `base_url is not a string`},
		{name: "truncated line", lines: []string{enabled("anthropic", mock), `{"msg":"upstream enabled","upstream":"xai","base_url":"https://api`, listening}, wantErr: `line 2 mentions "upstream enabled"`},
		{name: "msg twice", lines: []string{`{"msg":"x","MSG":"upstream enabled","upstream":"xai","base_url":"https://api.x.ai"}`, listening}, wantErr: `line 1 mentions "upstream enabled"`},
		{name: "data after the object", lines: []string{enabled("xai", mock) + `{"base_url":"https://api.x.ai"}`, listening}, wantErr: `line 1 mentions "upstream enabled"`},
		{name: "not JSON", lines: []string{`level=INFO msg="upstream enabled" upstream=xai base_url=https://api.x.ai`, listening}, wantErr: `line 1 mentions`},

		// Missing lines.
		{name: "no upstream lines", lines: []string{listening}, wantErr: `no "upstream enabled" line`},
		// "metrics listening" is not the API's "listening".
		{name: "only the metrics listener", lines: []string{enabled("anthropic", mock), metricsListening}, wantErr: `did not log "listening"`},
		{name: "metrics line among public upstreams", lines: []string{enabled("openai", "https://api.openai.com"), metricsListening, listening}, wantErr: `upstream "openai" is not on the mock`},
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
