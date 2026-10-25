package telemetry

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/shahardalva/sentinel/internal/provider/anthropic"
	"github.com/shahardalva/sentinel/internal/proxy"
)

type allowLimiter struct{}

func (allowLimiter) Allow(context.Context, string) (proxy.RateLimit, error) {
	return proxy.RateLimit{Allowed: true}, nil
}

// seriesKeys names every series in c by instrument and attribute set.
func (c collected) seriesKeys() map[string]bool {
	keys := map[string]bool{}
	for name, d := range c.data {
		switch d := d.(type) {
		case metricdata.Sum[int64]:
			for _, dp := range d.DataPoints {
				keys[name+"{"+key(dp.Attributes)+"}"] = true
			}
		case metricdata.Histogram[float64]:
			for _, dp := range d.DataPoints {
				keys[name+"{"+key(dp.Attributes)+"}"] = true
			}
		case metricdata.Gauge[int64]:
			for _, dp := range d.DataPoints {
				keys[name+"{"+key(dp.Attributes)+"}"] = true
			}
		}
	}
	return keys
}

// A client choosing a new key or model for every request must not create a
// series: those values never become labels.
func TestCardinality_ClientValuesAddNoSeries(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"id":"msg_1","type":"message"}`)
	}))
	defer upstream.Close()
	p, err := anthropic.New(anthropic.Config{APIKey: "upstream-key", BaseURL: upstream.URL})
	if err != nil {
		t.Fatal(err)
	}

	const n = 1000
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("client-key-%04d", i)
	}
	m, reader := newTestMetrics(t, Config{Upstreams: []Upstream{{"anthropic", p.Name()}}, Admission: proxy.NewAdmission(8)})
	logger := slog.New(slog.DiscardHandler)
	gw := proxy.NewAuth(keys, logger, m).Require(p, proxy.NewHandler(p, logger, proxy.Options{
		Upstream: "anthropic", Recorder: m, RateLimiter: allowLimiter{},
	}))
	send := func(i int, key string) {
		t.Helper()
		body := fmt.Sprintf(`{"model":"model-%04d","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}`, i)
		req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(body))
		req.Header.Set("X-Api-Key", key)
		w := httptest.NewRecorder()
		gw.ServeHTTP(w, req)
		want := http.StatusOK
		if key != keys[i] {
			want = http.StatusUnauthorized
		}
		if w.Code != want {
			t.Fatalf("request %d: status %d, want %d", i, w.Code, want)
		}
	}

	// One accepted and one rejected request create every series these
	// paths have.
	send(0, keys[0])
	send(0, "bad-key-0000")
	before := collect(t, reader).seriesKeys()

	for i := 1; i < n; i++ {
		send(i, keys[i])
		send(i, fmt.Sprintf("bad-key-%04d", i))
	}
	got := collect(t, reader)
	after := got.seriesKeys()
	if len(after) != len(before) {
		t.Errorf("series grew from %d to %d after %d requests with distinct keys and models", len(before), len(after), 2*(n-1))
	}
	for k := range after {
		if !before[k] {
			t.Errorf("new series %s", k)
		}
		if strings.Contains(k, "key-") || strings.Contains(k, "model-") {
			t.Errorf("series %s carries a client value", k)
		}
	}
	if v := got.sums(t, "sentinel.requests")["protocol=anthropic,status_class=2xx,upstream=anthropic"]; v != n {
		t.Errorf("sentinel.requests 2xx = %d, want %d", v, n)
	}
	if v := got.sums(t, "sentinel.auth.rejections")[""]; v != n {
		t.Errorf("sentinel.auth.rejections = %d, want %d", v, n)
	}
}
