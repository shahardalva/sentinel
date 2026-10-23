package devcert

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"strings"
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

func TestGenerate_DistinctCANames(t *testing.T) {
	a, err := Generate([]string{"localhost"}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	b, err := Generate([]string{"localhost"}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	na, nb := parse(t, a.CACert).Subject.CommonName, parse(t, b.CACert).Subject.CommonName
	if na == nb {
		t.Errorf("two CAs share the name %q", na)
	}
	if !strings.HasPrefix(na, "Sentinel dev CA ") {
		t.Errorf("CA name = %q, want prefix %q", na, "Sentinel dev CA ")
	}
}

func TestCAName(t *testing.T) {
	serial := new(big.Int).SetBytes([]byte{0x1a, 0x2b, 0x3c, 0x4d, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x01})
	got := caName(serial, time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC))
	if want := "Sentinel dev CA 2026-10-23 1a2b3c4d"; got != want {
		t.Errorf("caName = %q, want %q", got, want)
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
