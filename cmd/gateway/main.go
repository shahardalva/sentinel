// Command gateway runs the Sentinel HTTP gateway.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
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

	upstream := anthropic.New(anthropic.Config{
		APIKey:  apiKey,
		BaseURL: os.Getenv("ANTHROPIC_BASE_URL"),
	})

	mux := http.NewServeMux()
	messages := proxy.NewHandler(upstream, logger, proxy.Options{})
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

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
