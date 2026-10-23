// Command devcerts writes a throwaway CA and a server certificate signed by
// it, so cmd/mockupstream can serve HTTPS and the gateway can trust it via
// SENTINEL_UPSTREAM_CA_FILE. For local development only.
//
// Output, in -dir:
//
//	ca.pem          CA certificate (point SENTINEL_UPSTREAM_CA_FILE here)
//	server.pem      server certificate (mockupstream -tls-cert)
//	server-key.pem  server private key (mockupstream -tls-key)
//
// The CA's private key is never written, so nothing else can be signed by
// this CA. Rerun to get a fresh set.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/shahardalva/sentinel/internal/devcert"
)

func main() {
	dir := flag.String("dir", "certs", "output directory")
	hosts := flag.String("hosts", "localhost,127.0.0.1,mockupstream", "comma-separated DNS names and IP addresses for the server certificate")
	validity := flag.Duration("validity", 30*24*time.Hour, "certificate lifetime")
	flag.Parse()

	if err := run(*dir, splitHosts(*hosts), *validity); err != nil {
		fmt.Fprintln(os.Stderr, "devcerts:", err)
		os.Exit(1)
	}
}

func run(dir string, hosts []string, validity time.Duration) error {
	b, err := devcert.Generate(hosts, validity)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	files := []struct {
		name string
		data []byte
		perm os.FileMode
	}{
		{"ca.pem", b.CACert, 0o644},
		{"server.pem", b.ServerCert, 0o644},
		{"server-key.pem", b.ServerKey, 0o600},
	}
	for _, f := range files {
		path := filepath.Join(dir, f.name)
		if err := os.WriteFile(path, f.data, f.perm); err != nil {
			return err
		}
		fmt.Println("wrote", path)
	}
	fmt.Printf("valid for %s, hosts: %s\n", validity, strings.Join(hosts, ", "))
	return nil
}

func splitHosts(s string) []string {
	var hosts []string
	for h := range strings.SplitSeq(s, ",") {
		if h = strings.TrimSpace(h); h != "" {
			hosts = append(hosts, h)
		}
	}
	return hosts
}
