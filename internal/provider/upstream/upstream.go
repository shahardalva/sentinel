// Package upstream holds the security-critical plumbing every provider
// shares for talking to its upstream API: which base URLs are acceptable,
// how the HTTP client and its TLS are set up, and which header values may
// be sent. Providers call it instead of carrying their own copies.
//
// It must not import internal/proxy, so it stays usable by any protocol.
// Errors carry no provider prefix; callers add their own.
package upstream

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
)

// ValidateBaseURL checks that raw is scheme://host[/path] with no
// credentials, query, or fragment, and uses https. Plain http is allowed only
// to a loopback host, for local test servers, since the upstream key would
// otherwise cross the network in the clear.
func ValidateBaseURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("base URL: %w", err)
	}
	if u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("base URL %q must be scheme://host[/path] with no credentials, query, or fragment", raw)
	}
	switch {
	case u.Scheme == "https":
		return nil
	case u.Scheme == "http" && IsLoopback(u.Hostname()):
		return nil
	}
	return fmt.Errorf("base URL %q must use https (http is allowed only for loopback hosts)", raw)
}

// IsLoopback reports whether host is "localhost" or a loopback IP address.
func IsLoopback(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// ClientConfig configures NewClient.
type ClientConfig struct {
	// MaxIdleConns sizes the idle connection pool, in total and per host;
	// set it to the gateway's concurrency cap. Must be positive.
	MaxIdleConns int
	// ExtraRootCAs are trusted in addition to the system trust store, never
	// instead of it.
	ExtraRootCAs []*x509.Certificate
}

// NewClient returns an HTTP client for an upstream API. It has no timeout of
// its own; callers bound requests via their context.
func NewClient(cfg ClientConfig) (*http.Client, error) {
	if cfg.MaxIdleConns <= 0 {
		return nil, errors.New("MaxIdleConns must be positive")
	}
	t := http.DefaultTransport.(*http.Transport).Clone()
	// All traffic goes to one host. The default keeps only 2 idle
	// connections per host, so under load most requests would pay for a
	// fresh TLS handshake.
	t.MaxIdleConns = cfg.MaxIdleConns
	t.MaxIdleConnsPerHost = cfg.MaxIdleConns
	tlsCfg, err := tlsConfig(t.TLSClientConfig, cfg.ExtraRootCAs)
	if err != nil {
		return nil, err
	}
	t.TLSClientConfig = tlsCfg
	return &http.Client{Transport: t}, nil
}

// tlsConfig builds on base (which may be nil) rather than replacing it, so
// any defaults the transport carries are kept. It sets a TLS 1.2 floor and,
// when extra CAs are given, a pool of the system roots plus those CAs.
func tlsConfig(base *tls.Config, extraCAs []*x509.Certificate) (*tls.Config, error) {
	c := &tls.Config{}
	if base != nil {
		c = base.Clone()
	}
	if c.MinVersion < tls.VersionTLS12 {
		c.MinVersion = tls.VersionTLS12
	}
	if len(extraCAs) > 0 {
		pool, err := x509.SystemCertPool()
		if err != nil {
			return nil, fmt.Errorf("load system trust store: %w", err)
		}
		for _, ca := range extraCAs {
			pool.AddCert(ca)
		}
		c.RootCAs = pool
	}
	return c, nil
}

// HasControlChar reports whether s contains an ASCII control character other
// than horizontal tab, which header values may not. Control characters (CR
// and LF above all) would make the HTTP client refuse the request.
func HasControlChar(s string) bool {
	return strings.ContainsFunc(s, func(r rune) bool {
		return (r < ' ' && r != '\t') || r == 0x7f
	})
}
