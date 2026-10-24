package proxy_test

import (
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
