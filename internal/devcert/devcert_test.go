package devcert

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"testing"
	"time"
)

func parse(t *testing.T, data []byte) *x509.Certificate {
	t.Helper()
	block, _ := pem.Decode(data)
	if block == nil || block.Type != "CERTIFICATE" {
		t.Fatalf("not a PEM certificate: %q", data)
	}
	c, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestGenerate(t *testing.T) {
	b, err := Generate([]string{"localhost", "127.0.0.1", "mockupstream"}, 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tls.X509KeyPair(b.ServerCert, b.ServerKey); err != nil {
		t.Fatalf("server cert and key don't match: %v", err)
	}
	ca, srv := parse(t, b.CACert), parse(t, b.ServerCert)
	if !ca.IsCA || srv.IsCA {
		t.Errorf("IsCA: CA %v, server %v; want true, false", ca.IsCA, srv.IsCA)
	}
	if srv.NotAfter.After(time.Now().Add(25 * time.Hour)) {
		t.Errorf("server cert valid until %v, want about 24h from now", srv.NotAfter)
	}

	roots := x509.NewCertPool()
	roots.AddCert(ca)
	for _, host := range []string{"localhost", "127.0.0.1", "mockupstream"} {
		if _, err := srv.Verify(x509.VerifyOptions{DNSName: host, Roots: roots}); err != nil {
			t.Errorf("verify for %s: %v", host, err)
		}
	}
	if _, err := srv.Verify(x509.VerifyOptions{DNSName: "example.com", Roots: roots}); err == nil {
		t.Error("verify for example.com succeeded, want hostname error")
	}
}

func TestGenerate_RejectsBadInput(t *testing.T) {
	if _, err := Generate(nil, time.Hour); err == nil {
		t.Error("no hosts: want error")
	}
	if _, err := Generate([]string{"localhost"}, 0); err == nil {
		t.Error("zero validity: want error")
	}
}
