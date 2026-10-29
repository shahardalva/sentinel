package main

import (
	"os"
	"strings"
	"testing"
)

// fixture returns the Prometheus text in testdata/name.prom.
func fixture(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile("testdata/" + name + ".prom")
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// allowedValue returns the clean fixture with the allowed series' value
// replaced by v.
func allowedValue(t *testing.T, v string) string {
	t.Helper()
	clean := fixture(t, "clean")
	s := strings.Replace(clean, `outcome="allowed"} 47959`, `outcome="allowed"} `+v, 1)
	if s == clean {
		t.Fatal("clean fixture has no allowed series of 47959")
	}
	return s
}

// summary is k6's --summary-export JSON with http_reqs as count.
func summary(count string) string {
	return `{"root_group":{"name":"","path":"","checks":{}},"metrics":{"http_reqs":{"count":` + count +
		`,"rate":456.78},"iterations":{"count":` + count + `,"rate":456.78}}}`
}

// output is what bench/k6/gateway-verdict.js prints: the main run's
// summary and gateway-bench's /metrics, between markers.
func output(summaryJSON, metrics string) string {
	return markSummary + "\n" + summaryJSON + "\n" + markMetrics + "\n" + metrics + "\n" + markEnd + "\n"
}

// The gateway-bench startup lines that matter here.
const (
	logOK = `{"time":"2026-10-25T10:00:00Z","level":"INFO","msg":"rate limiting enabled","redis":"redis:6379","rpm":1000000,"fail_mode":"open"}
{"time":"2026-10-25T10:00:00Z","level":"INFO","msg":"metrics listening","addr":"[::]:9464","path":"/metrics"}
{"time":"2026-10-25T10:00:00Z","level":"INFO","msg":"listening","addr":"0.0.0.0:8081"}
`
	logRedisDown = `{"time":"2026-10-25T10:00:00Z","level":"INFO","msg":"rate limiting enabled","redis":"redis:6379","rpm":1000000,"fail_mode":"open"}
{"time":"2026-10-25T10:00:00Z","level":"WARN","msg":"redis not reachable at startup","redis":"redis:6379","fail_mode":"open","error":"dial tcp: i/o timeout"}
{"time":"2026-10-25T10:00:00Z","level":"INFO","msg":"listening","addr":"0.0.0.0:8081"}
`
)

func TestJudge(t *testing.T) {
	tests := []struct {
		name     string
		log      string
		k6       string
		want     string   // the clean verdict; empty for a rejection
		wantErrs []string // substrings of the rejection
		notErrs  []string // reasons the rejection must not give
	}{
		{
			name: "clean run",
			log:  logOK,
			k6:   output(summary("47959"), fixture(t, "clean")),
			want: "bench clean: 47,959 requests, 47,959 allowed, 0 fail_open",
		},
		{
			name: "labels in a different order",
			log:  logOK,
			k6:   output(summary("47959"), fixture(t, "reordered_clean")),
			want: "bench clean: 47,959 requests, 47,959 allowed, 0 fail_open",
		},
		{
			name: "CRLF line endings and k6 text around the markers",
			log:  strings.ReplaceAll(logOK, "\n", "\r\n"),
			k6:   "\r\n          /\\      Grafana   /‾‾/\r\n" + strings.ReplaceAll(output(summary("47959"), fixture(t, "clean")), "\n", "\r\n") + "running (00m00.1s)\r\n",
			want: "bench clean: 47,959 requests, 47,959 allowed, 0 fail_open",
		},
		{
			name: "http_reqs as values.count",
			log:  logOK,
			k6:   output(`{"metrics":{"http_reqs":{"type":"counter","contains":"default","values":{"count":47959,"rate":456.78}}}}`, fixture(t, "clean")),
			want: "bench clean: 47,959 requests, 47,959 allowed, 0 fail_open",
		},

		// Rule 1: no fail_open.
		{
			name:     "fail_open, the issue's run",
			log:      logOK,
			k6:       output(summary("46881"), fixture(t, "fail_open")),
			wantErrs: []string{"4,058 fail_open", "42,823 allowed, but k6 sent 46,881 requests", "(46,881 requests, 42,823 allowed, 4,058 fail_open)"},
		},
		{
			name:     "fail_open alone",
			log:      logOK,
			k6:       output(summary("42823"), fixture(t, "fail_open")),
			wantErrs: []string{"4,058 fail_open"},
			notErrs:  []string{"but k6 sent"},
		},
		{
			name:     "fail_open with labels in a different order",
			log:      logOK,
			k6:       output(summary("47957"), fixture(t, "reordered")),
			wantErrs: []string{"2 fail_open"},
			notErrs:  []string{"but k6 sent"},
		},

		// Rule 2: allowed equals the requests k6 sent.
		{
			name:     "fewer allowed than requests",
			log:      logOK,
			k6:       output(summary("47959"), fixture(t, "allowed_short")),
			wantErrs: []string{"47,950 allowed, but k6 sent 47,959 requests"},
			notErrs:  []string{" fail_open: "}, // the fail_open reason, not the counts' "0 fail_open)"
		},
		{
			name:     "more allowed than requests",
			log:      logOK,
			k6:       output(summary("47958"), fixture(t, "clean")),
			wantErrs: []string{"47,959 allowed, but k6 sent 47,958 requests"},
		},
		{
			name:     "no decisions metric",
			log:      logOK,
			k6:       output(summary("47959"), fixture(t, "no_decisions")),
			wantErrs: []string{"no sentinel_ratelimit_decisions_total", "no proof"},
		},
		{
			name:     "empty scrape",
			log:      logOK,
			k6:       output(summary("47959"), ""),
			wantErrs: []string{"no sentinel_ratelimit_decisions_total"},
		},
		// Rule 3: no other outcome. Allowed equals the requests, so only
		// this rule can reject.
		{
			name:     "limited",
			log:      logOK,
			k6:       output(summary("47959"), fixture(t, "limited")),
			wantErrs: []string{"3 limited: every request should be allowed by Redis"},
			notErrs:  []string{"but k6 sent", " fail_open: "},
		},
		{
			name:     "fail_closed",
			log:      logOK,
			k6:       output(summary("47959"), fixture(t, "fail_closed")),
			wantErrs: []string{"3 fail_closed: every request should be allowed by Redis"},
			notErrs:  []string{"but k6 sent", " fail_open: "},
		},
		{
			name:     "not_checked",
			log:      logOK,
			k6:       output(summary("47959"), fixture(t, "not_checked")),
			wantErrs: []string{"3 not_checked: every request should be allowed by Redis"},
			notErrs:  []string{"but k6 sent", " fail_open: "},
		},

		// Counters must be whole, finite numbers.
		{name: "allowed a fraction", log: logOK, k6: output(summary("47959"), allowedValue(t, "47959.5")), wantErrs: []string{"is 47959.5, not a count"}},
		{name: "allowed NaN", log: logOK, k6: output(summary("47959"), allowedValue(t, "NaN")), wantErrs: []string{"is NaN, not a count"}},
		{name: "allowed +Inf", log: logOK, k6: output(summary("47959"), allowedValue(t, "+Inf")), wantErrs: []string{"is +Inf, not a count"}},

		// Rule 4: Redis answered at startup, and the log is gateway-bench's.
		{
			name:     "redis not reachable at startup",
			log:      logRedisDown,
			k6:       output(summary("47959"), fixture(t, "clean")),
			wantErrs: []string{`gateway-bench logged "redis not reachable at startup" (error: dial tcp: i/o timeout)`},
		},
		{
			name:     "redis not reachable at startup, no error field",
			log:      strings.Replace(logRedisDown, `,"error":"dial tcp: i/o timeout"`, "", 1),
			k6:       output(summary("47959"), fixture(t, "clean")),
			wantErrs: []string{`gateway-bench logged "redis not reachable at startup"`},
			notErrs:  []string{"(error:"},
		},
		{
			name: "redis not reachable at startup and no metrics",
			log:  logRedisDown,
			k6:   markSummary + "\n" + summary("47959") + "\n",
			wantErrs: []string{
				`gateway-bench logged "redis not reachable at startup" (error: dial tcp: i/o timeout)`,
				"no gateway-bench metrics",
			},
		},
		{
			name:     "redis not reachable at startup and no summary",
			log:      logRedisDown,
			k6:       "",
			wantErrs: []string{`gateway-bench logged "redis not reachable at startup"`, "no k6 summary"},
		},
		{
			name:     "redis not reachable and no listening line",
			log:      strings.Replace(logRedisDown, `"msg":"listening"`, `"msg":"starting"`, 1),
			k6:       output(summary("47959"), fixture(t, "clean")),
			wantErrs: []string{`gateway-bench logged "redis not reachable at startup"`, `no "listening" line`},
		},
		{
			name:     "empty log",
			log:      "",
			k6:       output(summary("47959"), fixture(t, "clean")),
			wantErrs: []string{`gateway-bench log has no "listening" line`},
		},
		{
			// "metrics listening" binds first and must never count.
			name:     "only metrics listening",
			log:      strings.Replace(logOK, `{"time":"2026-10-25T10:00:00Z","level":"INFO","msg":"listening","addr":"0.0.0.0:8081"}`+"\n", "", 1),
			k6:       output(summary("47959"), fixture(t, "clean")),
			wantErrs: []string{`gateway-bench log has no "listening" line`},
		},

		// Output that can't be read is no proof either.
		{name: "no k6 output", log: logOK, k6: "", wantErrs: []string{"no k6 summary"}},
		{
			name:     "no metrics section",
			log:      logOK,
			k6:       markSummary + "\n" + summary("47959") + "\n",
			wantErrs: []string{"no gateway-bench metrics"},
		},
		{
			name:     "no end marker",
			log:      logOK,
			k6:       strings.TrimSuffix(output(summary("47959"), fixture(t, "clean")), markEnd+"\n"),
			wantErrs: []string{"truncated"},
		},
		{
			name:     "a section twice",
			log:      logOK,
			k6:       output(summary("47959"), fixture(t, "clean")) + output(summary("1"), fixture(t, "clean")),
			wantErrs: []string{"more than once"},
		},
		{name: "summary not JSON", log: logOK, k6: output("http_reqs: 47959", fixture(t, "clean")), wantErrs: []string{"k6 summary"}},
		{name: "no http_reqs", log: logOK, k6: output(`{"metrics":{"iterations":{"count":47959}}}`, fixture(t, "clean")), wantErrs: []string{"no http_reqs count"}},
		{name: "http_reqs not a whole number", log: logOK, k6: output(summary("47959.5"), fixture(t, "clean")), wantErrs: []string{"no http_reqs count"}},
		{name: "no requests", log: logOK, k6: output(summary("0"), fixture(t, "clean")), wantErrs: []string{"k6 sent no requests"}},
		{name: "metrics not Prometheus text", log: logOK, k6: output(summary("47959"), "<html>502 Bad Gateway</html>"), wantErrs: []string{"gateway-bench metrics"}},
		{
			name:     "decisions not a counter",
			log:      logOK,
			k6:       output(summary("47959"), strings.Replace(fixture(t, "clean"), "decisions_total counter", "decisions_total gauge", 1)),
			wantErrs: []string{"not a counter"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := judge(strings.NewReader(tt.log), strings.NewReader(tt.k6))
			if tt.want != "" {
				if err != nil || got != tt.want {
					t.Fatalf("judge = %q, %v; want %q", got, err, tt.want)
				}
				return
			}
			if err == nil {
				t.Fatalf("judge = %q, nil; want a rejection", got)
			}
			for _, s := range tt.wantErrs {
				if !strings.Contains(err.Error(), s) {
					t.Errorf("rejection %q does not contain %q", err, s)
				}
			}
			for _, s := range tt.notErrs {
				if strings.Contains(err.Error(), s) {
					t.Errorf("rejection %q contains %q", err, s)
				}
			}
		})
	}
}

func TestCommas(t *testing.T) {
	for n, want := range map[int64]string{0: "0", 7: "7", 999: "999", 1000: "1,000", 47959: "47,959", 1234567: "1,234,567", -4058: "-4,058"} {
		if got := commas(n); got != want {
			t.Errorf("commas(%d) = %q, want %q", n, got, want)
		}
	}
}
