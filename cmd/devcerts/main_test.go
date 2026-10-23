package main

import (
	"crypto/tls"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"testing"
	"time"
)

func TestRun_WritesFiles(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "certs")
	if err := run(dir, []string{"localhost"}, time.Hour); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"ca.pem", "server.pem", "server-key.pem"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "ca-key.pem")); !os.IsNotExist(err) {
		t.Errorf("CA key written to disk (stat err %v), want it discarded", err)
	}
	if _, err := tls.LoadX509KeyPair(filepath.Join(dir, "server.pem"), filepath.Join(dir, "server-key.pem")); err != nil {
		t.Errorf("load server key pair: %v", err)
	}
}

func TestRun_RerunResetsKeyMode(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows ignores Unix permission bits")
	}
	dir := t.TempDir()
	key := filepath.Join(dir, "server-key.pem")
	if err := os.WriteFile(key, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := run(dir, []string{"localhost"}, time.Hour); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(key)
	if err != nil {
		t.Fatal(err)
	}
	if mode := info.Mode().Perm(); mode != 0o600 {
		t.Errorf("server-key.pem mode = %o, want 600", mode)
	}
}

func TestCurrent(t *testing.T) {
	const week = 7 * 24 * time.Hour
	now := time.Now()

	fresh := t.TempDir()
	if err := run(fresh, []string{"localhost"}, 30*24*time.Hour); err != nil {
		t.Fatal(err)
	}
	if ok, why := current(fresh, week, now); !ok {
		t.Errorf("30-day certs: current = false (%s), want true", why)
	}
	if ok, _ := current(fresh, week, now.Add(24*24*time.Hour)); ok {
		t.Error("30-day certs checked 24 days later: current = true, want false")
	}

	expiring := t.TempDir()
	if err := run(expiring, []string{"localhost"}, 6*24*time.Hour); err != nil {
		t.Fatal(err)
	}
	if ok, _ := current(expiring, week, now); ok {
		t.Error("certs expiring in 6 days: current = true, want false")
	}

	for _, name := range []string{"ca.pem", "server.pem", "server-key.pem"} {
		dir := t.TempDir()
		if err := run(dir, []string{"localhost"}, 30*24*time.Hour); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(filepath.Join(dir, name)); err != nil {
			t.Fatal(err)
		}
		if ok, _ := current(dir, week, now); ok {
			t.Errorf("%s missing: current = true, want false", name)
		}
	}

	garbage := t.TempDir()
	if err := run(garbage, []string{"localhost"}, 30*24*time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(garbage, "server.pem"), []byte("not a cert"), 0o644); err != nil {
		t.Fatal(err)
	}
	if ok, _ := current(garbage, week, now); ok {
		t.Error("garbage server.pem: current = true, want false")
	}

	if ok, _ := current(filepath.Join(t.TempDir(), "missing"), week, now); ok {
		t.Error("missing dir: current = true, want false")
	}
}

func TestSplitHosts(t *testing.T) {
	got := splitHosts(" localhost, ,127.0.0.1,mockupstream ")
	if want := []string{"localhost", "127.0.0.1", "mockupstream"}; !slices.Equal(got, want) {
		t.Errorf("splitHosts = %q, want %q", got, want)
	}
}
