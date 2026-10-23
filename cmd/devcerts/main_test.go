package main

import (
	"crypto/tls"
	"os"
	"path/filepath"
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

func TestSplitHosts(t *testing.T) {
	got := splitHosts(" localhost, ,127.0.0.1,mockupstream ")
	if want := []string{"localhost", "127.0.0.1", "mockupstream"}; !slices.Equal(got, want) {
		t.Errorf("splitHosts = %q, want %q", got, want)
	}
}
