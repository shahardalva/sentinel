package proxy

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// After the interval the next rejection is logged again, with the count of
// the ones held back.
func TestAuth_WarnCountsSuppressed(t *testing.T) {
	defer func(d time.Duration) { authWarnInterval = d }(authWarnInterval)
	authWarnInterval = 100 * time.Millisecond

	var logs bytes.Buffer
	h := NewAuth([]string{"k"}, slog.New(slog.NewJSONHandler(&logs, nil)), nil).Require(plainErrors, http.NotFoundHandler())
	reject := func() {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/v1/messages", nil))
		if w.Code != http.StatusUnauthorized {
			t.Fatalf("status %d, want 401", w.Code)
		}
	}
	for range 4 {
		reject()
	}
	time.Sleep(authWarnInterval + 20*time.Millisecond)
	reject()

	var suppressed []float64
	for line := range strings.SplitSeq(strings.TrimSpace(logs.String()), "\n") {
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatal(err)
		}
		if m["msg"] != "unauthorized" || m["level"] != "WARN" {
			t.Fatalf("unexpected line %s", line)
		}
		suppressed = append(suppressed, m["suppressed"].(float64))
	}
	if len(suppressed) != 2 || suppressed[0] != 0 || suppressed[1] != 3 {
		t.Errorf("suppressed per line = %v, want [0 3]", suppressed)
	}
}

func TestLogThrottle(t *testing.T) {
	var th logThrottle
	now := time.Unix(1000, 0)
	if ok, n := th.allow(now, time.Second); !ok || n != 0 {
		t.Fatalf("first = %v, %d; want true, 0", ok, n)
	}
	for range 5 {
		if ok, _ := th.allow(now.Add(500*time.Millisecond), time.Second); ok {
			t.Fatal("allowed within the interval")
		}
	}
	if ok, n := th.allow(now.Add(time.Second), time.Second); !ok || n != 5 {
		t.Errorf("after the interval = %v, %d; want true, 5", ok, n)
	}
}
