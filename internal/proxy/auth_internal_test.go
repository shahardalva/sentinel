package proxy

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// After the interval the next rejection is logged again, with the count of
// the ones held back. The clock is injected, so no sleep.
func TestAuth_WarnCountsSuppressed(t *testing.T) {
	var logs bytes.Buffer
	a := NewAuth([]string{"k"}, slog.New(slog.NewJSONHandler(&logs, nil)), nil)
	now := throttleBase.Add(time.Hour)
	a.now = func() time.Time { return now }
	h := a.Require(plainErrors, http.NotFoundHandler())
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
	now = now.Add(authWarnInterval - time.Nanosecond)
	reject() // still inside the interval
	now = now.Add(time.Nanosecond)
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
	if len(suppressed) != 2 || suppressed[0] != 0 || suppressed[1] != 4 {
		t.Errorf("suppressed per line = %v, want [0 4]", suppressed)
	}
}

func TestLogThrottle(t *testing.T) {
	var th logThrottle
	// The very first line passes at once, even at the base itself: zero
	// means "never logged", not "logged at the base".
	if ok, n := th.allow(throttleBase, time.Second); !ok || n != 0 {
		t.Fatalf("first = %v, %d; want true, 0", ok, n)
	}
	for range 5 {
		if ok, _ := th.allow(throttleBase.Add(500*time.Millisecond), time.Second); ok {
			t.Fatal("allowed within the interval")
		}
	}
	if ok, n := th.allow(throttleBase.Add(time.Second), time.Second); !ok || n != 5 {
		t.Errorf("after the interval = %v, %d; want true, 5", ok, n)
	}
}

// Under contention every call is either let through or counted, once: the
// lines let through plus every suppressed count reported plus the count
// still held at the end equal the calls made.
func TestLogThrottle_Concurrent(t *testing.T) {
	const (
		goroutines = 32
		calls      = 500
		interval   = time.Millisecond
	)
	var th logThrottle
	var allowed, reported, total int64
	var mu sync.Mutex
	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Go(func() {
			var a, r int64
			for i := range calls {
				// Times spread over many intervals, interleaved across
				// goroutines, so lines are let through all along.
				at := throttleBase.Add(time.Duration(i*goroutines+g) * interval / 10)
				if ok, n := th.allow(at, interval); ok {
					a++
					r += n
				}
			}
			mu.Lock()
			allowed += a
			reported += r
			total += calls
			mu.Unlock()
		})
	}
	wg.Wait()
	held := th.skipped.Load() // the last batch, never logged
	if allowed+reported+held != total {
		t.Errorf("allowed %d + reported %d + held %d = %d, want %d calls", allowed, reported, held, allowed+reported+held, total)
	}
	if allowed == 0 || allowed == total {
		t.Errorf("allowed %d of %d; want the throttle to let some through and hold some back", allowed, total)
	}

	// All at one instant: exactly one gets through.
	var once logThrottle
	var got sync.WaitGroup
	var through int64
	at := throttleBase.Add(time.Hour)
	for range goroutines {
		got.Go(func() {
			if ok, _ := once.allow(at, time.Second); ok {
				mu.Lock()
				through++
				mu.Unlock()
			}
		})
	}
	got.Wait()
	if through != 1 || once.skipped.Load() != goroutines-1 {
		t.Errorf("at one instant: %d through, %d held; want 1 and %d", through, once.skipped.Load(), goroutines-1)
	}
}
