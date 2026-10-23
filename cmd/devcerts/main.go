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
// this CA. Rerun to get a fresh set. With -renew-within, an existing set is
// kept unless a file is missing or server.pem expires within that window.
package main

import (
	"crypto/x509"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"io/fs"
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
	renewWithin := flag.Duration("renew-within", 0, "keep existing certificates unless a file is missing or server.pem expires within this window (0: always regenerate)")
	flag.Parse()

	if *renewWithin > 0 {
		ok, why := current(*dir, *renewWithin, time.Now())
		if ok {
			fmt.Println("keeping certificates in", *dir+":", why)
			return
		}
		fmt.Println("regenerating certificates:", why)
	}
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
		// WriteFile applies perm only when it creates the file, so remove
		// any old copy first; otherwise a rerun keeps the old mode.
		if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		if err := os.WriteFile(path, f.data, f.perm); err != nil {
			return err
		}
		fmt.Println("wrote", path)
	}
	fmt.Printf("valid for %s, hosts: %s\n", validity, strings.Join(hosts, ", "))
	return nil
}

// current reports whether dir holds a full set of certificates whose server
// certificate is still valid at now+within, and why.
func current(dir string, within time.Duration, now time.Time) (bool, string) {
	for _, name := range []string{"ca.pem", "server.pem", "server-key.pem"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			return false, err.Error()
		}
	}
	data, err := os.ReadFile(filepath.Join(dir, "server.pem"))
	if err != nil {
		return false, err.Error()
	}
	block, _ := pem.Decode(data)
	if block == nil || block.Type != "CERTIFICATE" {
		return false, "server.pem holds no certificate"
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return false, "server.pem: " + err.Error()
	}
	expiry := cert.NotAfter.Format(time.RFC3339)
	if now.Add(within).After(cert.NotAfter) {
		return false, "server.pem expires " + expiry
	}
	return true, "server.pem valid until " + expiry
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
