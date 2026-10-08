package main

import (
	"log/slog"
	"slices"
	"strings"
	"testing"
)

func TestRun_RequiresEnv(t *testing.T) {
	tests := []struct {
		name, upstreamKey, clientKeys, wantErr string
	}{
		{"no upstream key", "", "k1", "ANTHROPIC_API_KEY"},
		{"no client keys", "sk", "", "SENTINEL_API_KEYS"},
		{"blank client keys", "sk", " , ,", "SENTINEL_API_KEYS"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("ANTHROPIC_API_KEY", tt.upstreamKey)
			t.Setenv("SENTINEL_API_KEYS", tt.clientKeys)
			err := run(slog.New(slog.DiscardHandler))
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("run() = %v, want error mentioning %s", err, tt.wantErr)
			}
		})
	}
}

func TestParseKeys(t *testing.T) {
	got := parseKeys(" a ,b,, ,c ")
	if want := []string{"a", "b", "c"}; !slices.Equal(got, want) {
		t.Errorf("parseKeys = %q, want %q", got, want)
	}
	if got := parseKeys(""); len(got) != 0 {
		t.Errorf("parseKeys(\"\") = %q, want empty", got)
	}
}
