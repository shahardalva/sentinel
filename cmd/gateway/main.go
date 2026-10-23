// Command gateway runs the Sentinel HTTP gateway.
package main

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/shahardalva/sentinel/internal/provider/anthropic"
	"github.com/shahardalva/sentinel/internal/proxy"
)

const (
	defaultAddr     = "127.0.0.1:8081" // loopback only; set SENTINEL_ADDR to expose
	shutdownTimeout = 30 * time.Second
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("gateway exited", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	apiKey := os.Getenv("ANTHROPIC_API_KEY")
	if apiKey == "" {
		return errors.New("ANTHROPIC_API_KEY is not set")
	}
	clientKeys := parseKeys(os.Getenv("SENTINEL_API_KEYS"))
	if len(clientKeys) == 0 {
		return errors.New("SENTINEL_API_KEYS is not set")
	}
	addr := envOr("SENTINEL_ADDR", defaultAddr)
	maxConcurrent, err := parseMaxConcurrent(os.Getenv("SENTINEL_MAX_CONCURRENT"))
	if err != nil {
		return fmt.Errorf("SENTINEL_MAX_CONCURRENT: %w", err)
	}

	var rootCAs *x509.CertPool
	if caFile := os.Getenv("SENTINEL_UPSTREAM_CA_FILE"); caFile != "" {
		if rootCAs, err = loadCAFile(caFile); err != nil {
			return fmt.Errorf("SENTINEL_UPSTREAM_CA_FILE: %w", err)
		}
		logger.Info("trusting extra upstream CAs", "file", caFile)
	}

	upstream, err := anthropic.New(anthropic.Config{
		APIKey:       apiKey,
		BaseURL:      os.Getenv("ANTHROPIC_BASE_URL"),
		MaxIdleConns: maxConcurrent,
		RootCAs:      rootCAs,
	})
	if err != nil {
		return fmt.Errorf("ANTHROPIC_BASE_URL: %w", err)
	}

	mux := http.NewServeMux()
	messages := proxy.NewHandler(upstream, logger, proxy.Options{MaxConcurrent: maxConcurrent})
	mux.Handle("POST /v1/messages", proxy.RequireKey(clientKeys, logger, messages))

	// No ReadTimeout or WriteTimeout: they cap a whole request, and streams
	// can legitimately run for minutes. The handler sets its own per-request
	// body-read and per-chunk write deadlines instead.
	srv := &http.Server{
		Addr:              addr,
		Handler:           closeUnmatched(mux),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelError),
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		logger.Info("listening", "addr", addr)
		errCh <- srv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}

	// Give in-flight requests, including streams, a grace period to finish,
	// then cut whatever is still open.
	logger.Info("shutting down", "grace_period", shutdownTimeout.String())
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Warn("grace period expired; closing open connections", "error", err)
		_ = srv.Close()
	}
	return nil
}

// closeUnmatched closes the connection after a 404 or 405 from mux. Those
// responses leave the body unread, and net/http would otherwise drain it
// with no deadline (see proxy.RejectDrainTimeout).
func closeUnmatched(mux *http.ServeMux) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, pattern := mux.Handler(r); pattern == "" {
			_ = http.NewResponseController(w).SetReadDeadline(time.Now().Add(proxy.RejectDrainTimeout))
			w.Header().Set("Connection", "close")
		}
		mux.ServeHTTP(w, r)
	})
}

// parseKeys splits a comma-separated key list, dropping blank entries.
func parseKeys(s string) []string {
	var keys []string
	for k := range strings.SplitSeq(s, ",") {
		if k = strings.TrimSpace(k); k != "" {
			keys = append(keys, k)
		}
	}
	return keys
}

// parseMaxConcurrent reads the concurrency cap. Empty means
// proxy.DefaultMaxConcurrent; anything else must be a positive integer.
func parseMaxConcurrent(s string) (int, error) {
	if s == "" {
		return proxy.DefaultMaxConcurrent, nil
	}
	n, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%q is not a positive integer", s)
	}
	return n, nil
}

// loadCAFile returns the system trust store plus the PEM certificates in
// path. It fails if the file can't be read or holds no valid certificate.
func loadCAFile(path string) (*x509.CertPool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	pool, err := x509.SystemCertPool()
	if err != nil {
		return nil, fmt.Errorf("load system trust store: %w", err)
	}
	if !pool.AppendCertsFromPEM(data) {
		return nil, fmt.Errorf("%s contains no valid PEM certificates", path)
	}
	return pool, nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
