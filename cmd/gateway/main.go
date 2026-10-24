// Command gateway runs the Sentinel HTTP gateway.
package main

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/shahardalva/sentinel/internal/provider/anthropic"
	"github.com/shahardalva/sentinel/internal/provider/openai"
	"github.com/shahardalva/sentinel/internal/proxy"
	"github.com/shahardalva/sentinel/internal/ratelimit"
)

const (
	defaultAddr     = "127.0.0.1:8081" // loopback only; set SENTINEL_ADDR to expose
	shutdownTimeout = 30 * time.Second
	defaultRateRPM  = 60
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("gateway exited", "error", err)
		os.Exit(1)
	}
}

// upstreamSpec is an upstream the gateway can enable by naming it in
// SENTINEL_UPSTREAMS.
type upstreamSpec struct {
	name       string // as listed in SENTINEL_UPSTREAMS
	label      string // for log messages
	keyVar     string // the upstream key's variable; required when enabled
	baseURLVar string
	defaultURL string
	route      string // the mux pattern its handler serves
	newFunc    func(apiKey, baseURL string, maxIdleConns int, extraCAs []*x509.Certificate) (proxy.Provider, error)
}

// upstreamSpecs are the upstreams SENTINEL_UPSTREAMS may name.
var upstreamSpecs = []upstreamSpec{
	{
		name: "anthropic", label: "Anthropic",
		keyVar: "ANTHROPIC_API_KEY", baseURLVar: "ANTHROPIC_BASE_URL", defaultURL: anthropic.DefaultBaseURL,
		route: "POST /v1/messages",
		newFunc: func(apiKey, baseURL string, maxIdleConns int, extraCAs []*x509.Certificate) (proxy.Provider, error) {
			p, err := anthropic.New(anthropic.Config{APIKey: apiKey, BaseURL: baseURL, MaxIdleConns: maxIdleConns, ExtraRootCAs: extraCAs})
			if err != nil {
				return nil, err
			}
			return p, nil
		},
	},
	{
		name: "openai", label: "OpenAI",
		keyVar: "OPENAI_API_KEY", baseURLVar: "OPENAI_BASE_URL", defaultURL: openai.DefaultBaseURL,
		route: "POST /v1/chat/completions",
		newFunc: func(apiKey, baseURL string, maxIdleConns int, extraCAs []*x509.Certificate) (proxy.Provider, error) {
			p, err := openai.New(openai.Config{APIKey: apiKey, BaseURL: baseURL, MaxIdleConns: maxIdleConns, ExtraRootCAs: extraCAs})
			if err != nil {
				return nil, err
			}
			return p, nil
		},
	},
}

const defaultUpstreams = "anthropic"

// enabledUpstream is an upstream listed in SENTINEL_UPSTREAMS, ready to serve.
type enabledUpstream struct {
	spec     *upstreamSpec
	baseURL  string // in use: the variable's value, or the default when unset
	provider proxy.Provider
}

// config is the gateway's validated startup configuration.
type config struct {
	clientKeys    []string
	addr          string
	maxConcurrent int
	caFile        string // SENTINEL_UPSTREAM_CA_FILE; empty if unset
	extraCAs      []*x509.Certificate
	upstreams     []enabledUpstream // in SENTINEL_UPSTREAMS order
	// ignored are the upstreams not in SENTINEL_UPSTREAMS whose key
	// variable is set anyway. The key is not used.
	ignored []*upstreamSpec
	// Rate limiting; redis is nil when SENTINEL_REDIS_URL is unset.
	redis          *redis.Client
	rateRPM        int
	rateFailClosed bool
}

// loadConfig reads and validates the gateway's environment variables via
// getenv. Each error starts with the name of the variable at fault.
func loadConfig(getenv func(string) string) (config, error) {
	specs, err := parseUpstreams(getenv("SENTINEL_UPSTREAMS"))
	if err != nil {
		return config{}, fmt.Errorf("SENTINEL_UPSTREAMS: %w", err)
	}
	for _, s := range specs {
		if getenv(s.keyVar) == "" {
			return config{}, fmt.Errorf("%s is not set; it is required while SENTINEL_UPSTREAMS lists %s", s.keyVar, s.name)
		}
	}
	var ignored []*upstreamSpec
	for i := range upstreamSpecs {
		if s := &upstreamSpecs[i]; !slices.Contains(specs, s) && getenv(s.keyVar) != "" {
			ignored = append(ignored, s)
		}
	}
	clientKeys := parseKeys(getenv("SENTINEL_API_KEYS"))
	if len(clientKeys) == 0 {
		return config{}, errors.New("SENTINEL_API_KEYS is not set")
	}
	addr := getenv("SENTINEL_ADDR")
	if addr == "" {
		addr = defaultAddr
	}
	maxConcurrent, err := parseMaxConcurrent(getenv("SENTINEL_MAX_CONCURRENT"))
	if err != nil {
		return config{}, fmt.Errorf("SENTINEL_MAX_CONCURRENT: %w", err)
	}

	caFile := getenv("SENTINEL_UPSTREAM_CA_FILE")
	var extraCAs []*x509.Certificate
	if caFile != "" {
		if extraCAs, err = loadCAFile(caFile); err != nil {
			return config{}, fmt.Errorf("SENTINEL_UPSTREAM_CA_FILE: %w", err)
		}
	}

	// The extra CAs apply to every enabled upstream.
	var upstreams []enabledUpstream
	for _, s := range specs {
		baseURL := getenv(s.baseURLVar)
		p, err := s.newFunc(getenv(s.keyVar), baseURL, maxConcurrent, extraCAs)
		if err != nil {
			return config{}, fmt.Errorf("%s: %w", s.baseURLVar, err)
		}
		if baseURL == "" {
			baseURL = s.defaultURL
		}
		upstreams = append(upstreams, enabledUpstream{spec: s, baseURL: baseURL, provider: p})
	}

	rateRPM, err := parsePositive(getenv("SENTINEL_RATE_LIMIT_RPM"), defaultRateRPM)
	if err != nil {
		return config{}, fmt.Errorf("SENTINEL_RATE_LIMIT_RPM: %w", err)
	}
	failClosed, err := parseFailMode(getenv("SENTINEL_RATE_LIMIT_FAIL_MODE"))
	if err != nil {
		return config{}, fmt.Errorf("SENTINEL_RATE_LIMIT_FAIL_MODE: %w", err)
	}
	var rdb *redis.Client
	if u := getenv("SENTINEL_REDIS_URL"); u != "" {
		// The URL may hold a password, so errors don't repeat it.
		if rdb, err = ratelimit.NewClient(u, ratelimit.DefaultTimeout, maxConcurrent); err != nil {
			return config{}, errors.New("SENTINEL_REDIS_URL: not a valid redis:// or rediss:// URL")
		}
	}

	return config{
		clientKeys:     clientKeys,
		addr:           addr,
		maxConcurrent:  maxConcurrent,
		caFile:         caFile,
		extraCAs:       extraCAs,
		upstreams:      upstreams,
		ignored:        ignored,
		redis:          rdb,
		rateRPM:        rateRPM,
		rateFailClosed: failClosed,
	}, nil
}

func run(logger *slog.Logger) error {
	cfg, err := loadConfig(os.Getenv)
	if err != nil {
		return err
	}
	logUpstreams(logger, cfg)
	if cfg.caFile != "" {
		logCAs(logger, cfg.caFile, cfg.extraCAs, cfg.upstreams)
	}

	var opts proxy.Options
	if cfg.redis == nil {
		logger.Info("rate limiting disabled: SENTINEL_REDIS_URL is not set")
	} else {
		defer cfg.redis.Close()
		ratelimit.RouteClientLogs(logger)
		limiter, err := ratelimit.New(ratelimit.Config{Client: cfg.redis, PerMinute: cfg.rateRPM, Logger: logger})
		if err != nil {
			return err
		}
		opts.RateLimiter = limiter
		opts.RateLimitFailClosed = cfg.rateFailClosed
		logRateLimit(logger, cfg)
	}

	mux := newMux(cfg, logger, opts)

	// No ReadTimeout or WriteTimeout: they cap a whole request, and streams
	// can legitimately run for minutes. The handler sets its own per-request
	// body-read and per-chunk write deadlines instead.
	srv := &http.Server{
		Addr:              cfg.addr,
		Handler:           closeUnmatched(mux),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelError),
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		logger.Info("listening", "addr", cfg.addr)
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

// newMux registers one route per enabled upstream, each behind the same
// client keys. Every handler gets the same opts, so they share its rate
// limiter, and one Admission: when opts has none, newMux creates it with
// cfg.maxConcurrent slots. One cap for the whole gateway means every route
// and upstream draws on the same slots, so the memory bound in ADR 0001
// holds however many there are.
func newMux(cfg config, logger *slog.Logger, opts proxy.Options) *http.ServeMux {
	if opts.Admission == nil {
		opts.Admission = proxy.NewAdmission(cfg.maxConcurrent)
	}
	mux := http.NewServeMux()
	for _, u := range cfg.upstreams {
		h := proxy.NewHandler(u.provider, logger, opts)
		mux.Handle(u.spec.route, proxy.RequireKey(cfg.clientKeys, u.provider, logger, h))
	}
	return mux
}

// parseUpstreams reads SENTINEL_UPSTREAMS, a comma-separated list of
// upstream names. Empty means defaultUpstreams. A name that is unknown or
// listed twice is an error, as is a list with no names in it.
func parseUpstreams(s string) ([]*upstreamSpec, error) {
	if s == "" {
		s = defaultUpstreams
	}
	var specs []*upstreamSpec
	for name := range strings.SplitSeq(s, ",") {
		if name = strings.TrimSpace(name); name == "" {
			continue
		}
		i := slices.IndexFunc(upstreamSpecs, func(u upstreamSpec) bool { return u.name == name })
		if i < 0 {
			return nil, fmt.Errorf("unknown upstream %q; known upstreams are %s", name, knownUpstreams())
		}
		if slices.Contains(specs, &upstreamSpecs[i]) {
			return nil, fmt.Errorf("upstream %q is listed more than once", name)
		}
		specs = append(specs, &upstreamSpecs[i])
	}
	if len(specs) == 0 {
		return nil, fmt.Errorf("%q lists no upstreams; known upstreams are %s", s, knownUpstreams())
	}
	return specs, nil
}

func knownUpstreams() string {
	var names []string
	for _, u := range upstreamSpecs {
		names = append(names, u.name)
	}
	return strings.Join(names, ", ")
}

// logUpstreams records each enabled upstream and warns about upstream keys
// that are set but unused. It never logs a key.
func logUpstreams(logger *slog.Logger, cfg config) {
	for _, u := range cfg.upstreams {
		logger.Info("upstream enabled", "upstream", u.spec.name, "base_url", u.baseURL)
	}
	for _, s := range cfg.ignored {
		logger.Warn(fmt.Sprintf("%s is set but %s is not in SENTINEL_UPSTREAMS; ignoring", s.keyVar, s.name))
	}
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
	return parsePositive(s, proxy.DefaultMaxConcurrent)
}

// parsePositive reads a positive integer, or returns def when s is empty.
func parsePositive(s string, def int) (int, error) {
	if s == "" {
		return def, nil
	}
	n, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%q is not a positive integer", s)
	}
	return n, nil
}

// parseFailMode reads SENTINEL_RATE_LIMIT_FAIL_MODE and reports whether it
// is "closed". Empty means "open".
func parseFailMode(s string) (closed bool, err error) {
	switch strings.TrimSpace(s) {
	case "", "open":
		return false, nil
	case "closed":
		return true, nil
	}
	return false, fmt.Errorf("%q is neither \"open\" nor \"closed\"", s)
}

// logRateLimit records the rate-limit settings and checks that Redis
// answers. An unreachable Redis is only a warning: it may still be starting,
// and the fail mode decides what requests get until it is up.
func logRateLimit(logger *slog.Logger, cfg config) {
	mode := "open"
	if cfg.rateFailClosed {
		mode = "closed"
	}
	// Addr only: the URL may carry a password.
	logger.Info("rate limiting enabled", "redis", cfg.redis.Options().Addr,
		"rpm", cfg.rateRPM, "fail_mode", mode)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := cfg.redis.Ping(ctx).Err(); err != nil {
		logger.Warn("redis not reachable at startup", "redis", cfg.redis.Options().Addr,
			"fail_mode", mode, "error", err)
	}
}

// loadCAFile returns the certificates in the PEM file at path. Every PEM
// block must be a CERTIFICATE that parses, and there must be at least one;
// a stray private key or a corrupt block fails rather than being skipped.
func loadCAFile(path string) ([]*x509.Certificate, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var certs []*x509.Certificate
	for n := 1; ; n++ {
		var block *pem.Block
		if block, data = pem.Decode(data); block == nil {
			break
		}
		if block.Type != "CERTIFICATE" {
			return nil, fmt.Errorf("%s: PEM block %d is %q, want CERTIFICATE", path, n, block.Type)
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("%s: PEM block %d: %w", path, n, err)
		}
		certs = append(certs, cert)
	}
	if len(certs) == 0 {
		return nil, fmt.Errorf("%s contains no PEM certificates", path)
	}
	return certs, nil
}

// logCAs records the extra upstream CAs and warns about setups that widen
// trust more than intended, including once for each enabled upstream left
// on its public default host.
func logCAs(logger *slog.Logger, file string, cas []*x509.Certificate, upstreams []enabledUpstream) {
	logger.Info("trusting extra upstream CAs", "file", file, "certs", len(cas))
	for _, c := range cas {
		if !c.IsCA {
			// Go trusts any certificate in the root pool, CA or not.
			logger.Warn("upstream CA file holds a non-CA certificate; it is trusted as a root anyway",
				"file", file, "subject", c.Subject.String())
		}
	}
	for _, u := range upstreams {
		if onDefaultHost(u) {
			logger.Warn(fmt.Sprintf("extra upstream CAs are trusted for the default %s API; "+
				"anyone holding one of their keys could impersonate it and read %s", u.spec.label, u.spec.keyVar),
				"file", file, "upstream", u.spec.name, "base_url", u.baseURL)
		}
	}
}

// onDefaultHost reports whether u talks to its public default host, such
// as api.anthropic.com, whatever the path.
func onDefaultHost(u enabledUpstream) bool {
	got, err := url.Parse(u.baseURL)
	if err != nil {
		return false
	}
	def, err := url.Parse(u.spec.defaultURL)
	return err == nil && strings.EqualFold(got.Hostname(), def.Hostname())
}
