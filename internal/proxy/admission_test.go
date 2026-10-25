package proxy_test

import (
	"log/slog"
	"net/http"
	"testing"

	"github.com/shahardalva/sentinel/internal/proxy"
)

func TestAdmission(t *testing.T) {
	a := proxy.NewAdmission(2)
	if !a.TryAcquire() || !a.TryAcquire() {
		t.Fatal("TryAcquire failed below the cap")
	}
	if a.TryAcquire() {
		t.Fatal("TryAcquire succeeded at the cap; want it to shed, not wait")
	}
	a.Release()
	if !a.TryAcquire() {
		t.Error("TryAcquire failed after Release")
	}
}

func TestAdmission_InUseAndLimit(t *testing.T) {
	a := proxy.NewAdmission(3)
	check := func(want int) {
		t.Helper()
		if a.InUse() != want || a.Limit() != 3 {
			t.Errorf("InUse, Limit = %d, %d; want %d, 3", a.InUse(), a.Limit(), want)
		}
	}
	check(0)
	a.TryAcquire()
	a.TryAcquire()
	check(2)
	a.Release()
	check(1)
	if l := proxy.NewAdmission(0).Limit(); l != proxy.DefaultMaxConcurrent {
		t.Errorf("default Limit = %d, want %d", l, proxy.DefaultMaxConcurrent)
	}
}

func TestNewAdmission_Default(t *testing.T) {
	for _, n := range []int{0, -1} {
		a := proxy.NewAdmission(n)
		for i := range proxy.DefaultMaxConcurrent {
			if !a.TryAcquire() {
				t.Fatalf("NewAdmission(%d): TryAcquire %d failed, want DefaultMaxConcurrent slots", n, i+1)
			}
		}
		if a.TryAcquire() {
			t.Errorf("NewAdmission(%d): more than DefaultMaxConcurrent slots", n)
		}
	}
}

// mustPanic fails unless f panics.
func mustPanic(t *testing.T, name string, f func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Errorf("%s did not panic", name)
		}
	}()
	f()
}

func TestAdmission_ReleaseWithoutAcquirePanics(t *testing.T) {
	a := proxy.NewAdmission(1)
	mustPanic(t, "Release with no slot held", a.Release)

	// A matched pair still works afterwards.
	if !a.TryAcquire() {
		t.Fatal("TryAcquire failed")
	}
	a.Release()
}

func TestNewHandler_ZeroAdmissionPanics(t *testing.T) {
	mustPanic(t, "NewHandler with a zero-value Admission", func() {
		proxy.NewHandler(stubProvider{}, slog.New(slog.DiscardHandler), proxy.Options{Admission: &proxy.Admission{}})
	})
}

func TestRequireKey_NilFormatterPanics(t *testing.T) {
	mustPanic(t, "RequireKey with a nil ErrorFormatter", func() {
		proxy.RequireKey([]string{"k"}, nil, slog.New(slog.DiscardHandler), http.NotFoundHandler())
	})
}
