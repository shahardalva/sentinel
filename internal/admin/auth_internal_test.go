package admin

import (
	"bytes"
	"crypto/sha256"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestReject_ThrottlesAndCounts(t *testing.T) {
	var logs bytes.Buffer
	now := time.Now()
	h := &handler{
		token:  sha256.Sum256([]byte("t")),
		logger: slog.New(slog.NewTextHandler(&logs, nil)),
		now:    func() time.Time { return now },
	}
	next := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("next ran without the token")
	})
	guard := h.requireToken(next)
	hit := func() {
		guard.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/admin/tenants", nil))
	}
	for range 4 {
		hit()
	}
	if n := strings.Count(logs.String(), "admin unauthorized"); n != 1 {
		t.Fatalf("%d warnings within the interval, want 1:\n%s", n, &logs)
	}
	now = now.Add(authWarnInterval)
	hit()
	if n := strings.Count(logs.String(), "admin unauthorized"); n != 2 {
		t.Fatalf("%d warnings after the interval, want 2", n)
	}
	if !strings.Contains(logs.String(), "suppressed=3") {
		t.Errorf("second warning lacks suppressed=3:\n%s", &logs)
	}
}
