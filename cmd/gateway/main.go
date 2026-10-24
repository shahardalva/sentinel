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
	"regexp"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/shahardalva/sentinel/internal/provider/anthropic"
	"github.com/shahardalva/sentinel/internal/provider/openai"
	"github.com/shahardalva/sentinel/internal/provider/upstream"
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

// protocol is a wire protocol an upstream speaks. It fixes the route's path
// and the Provider that forwards to the upstream.
type protocol struct {
	name    string // as in SENTINEL_UPSTREAM_<NAME>_PROTOCOL
	path    string // the route's path, after any /<name> prefix
	newFunc func(apiKey, baseURL string, maxIdleConns int, extraCAs []*x509.Certificate) (proxy.Provider, error)
}

var (
	anthropicProtocol = protocol{
		name: "anthropic", path: "/v1/messages",
		newFunc: func(apiKey, baseURL string, maxIdleConns int, extraCAs []*x509.Certificate) (proxy.Provider, error) {
			p, err := anthropic.New(anthropic.Config{APIKey: apiKey, BaseURL: baseURL, MaxIdleConns: maxIdleConns, ExtraRootCAs: extraCAs})
			if err != nil {
				return nil, err
			}
			return p, nil
		},
	}
	openaiProtocol = protocol{
		name: "openai", path: "/v1/chat/completions",
		newFunc: func(apiKey, baseURL string, maxIdleConns int, extraCAs []*x509.Certificate) (proxy.Provider, error) {
			p, err := openai.New(openai.Config{APIKey: apiKey, BaseURL: baseURL, MaxIdleConns: maxIdleConns, ExtraRootCAs: extraCAs})
			if err != nil {
				return nil, err
			}
			return p, nil
		},
	}
	protocols = []*protocol{&anthropicProtocol, &openaiProtocol}
)

// upstreamSpec is an upstream the gateway can enable by naming it in
// SENTINEL_UPSTREAMS: a built-in one from upstreamSpecs, or a custom one
// configured by SENTINEL_UPSTREAM_<NAME>_* variables.
type upstreamSpec struct {
	name       string // as listed in SENTINEL_UPSTREAMS
	protocol   *protocol
	keyVar     string // the upstream key's variable; required when enabled
	baseURLVar string
	// Built-in upstreams only. A built-in upstream also serves its
	// protocol's unprefixed route, and its base URL has a default.
	builtin    bool
	label      string // for log messages
	defaultURL string
}

// upstreamSpecs are the built-in upstreams.
var upstreamSpecs = []upstreamSpec{
	{
		name: "anthropic", protocol: &anthropicProtocol, builtin: true, label: "Anthropic",
		keyVar: "ANTHROPIC_API_KEY", baseURLVar: "ANTHROPIC_BASE_URL", defaultURL: anthropic.DefaultBaseURL,
	},
	{
		name: "openai", protocol: &openaiProtocol, builtin: true, label: "OpenAI",
		keyVar: "OPENAI_API_KEY", baseURLVar: "OPENAI_BASE_URL", defaultURL: openai.DefaultBaseURL,
	},
}

// routes returns the mux patterns s's handler serves: /<name> followed by
// its protocol's path, and for a built-in upstream the path on its own too.
func (s *upstreamSpec) routes() []string {
	prefixed := "POST /" + s.name + s.protocol.path
	if s.builtin {
		return []string{"POST " + s.protocol.path, prefixed}
	}
	return []string{prefixed}
}

const defaultUpstreams = "anthropic"

// customVarPrefix starts the variables of a custom upstream:
// SENTINEL_UPSTREAM_<NAME>_PROTOCOL, _BASE_URL and _API_KEY, with NAME
// upper-cased. Names have no underscores, so the mapping is one to one.
const customVarPrefix = "SENTINEL_UPSTREAM_"

var customVarFields = []string{"PROTOCOL", "BASE_URL", "API_KEY"}

// upstreamName is the rule for names in SENTINEL_UPSTREAMS.
var upstreamName = regexp.MustCompile(`^[a-z][a-z0-9]{0,31}$`)

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
	// ignored are the built-in upstreams not in SENTINEL_UPSTREAMS whose key
	// variable is set anyway. The key is not used.
	ignored []*upstreamSpec
	// ignoredCustom are the names, sorted, of custom upstreams not in
	// SENTINEL_UPSTREAMS whose SENTINEL_UPSTREAM_<NAME>_* variables are set
	// anyway. They are neither validated nor used.
	ignoredCustom []string
	// Rate limiting; redis is nil when SENTINEL_REDIS_URL is unset.
	redis          *redis.Client
	rateRPM        int
	rateFailClosed bool
}

// loadConfig reads and validates the gateway's environment variables via
// getenv, and looks through environ for custom upstream variables. Each
// error starts with the name of the variable at fault.
func loadConfig(getenv func(string) string, environ func() []string) (config, error) {
	names, err := parseUpstreams(getenv("SENTINEL_UPSTREAMS"))
	if err != nil {
		return config{}, fmt.Errorf("SENTINEL_UPSTREAMS: %w", err)
	}
	ignoredCustom, err := scanUpstreamVars(environ(), names)
	if err != nil {
		return config{}, err
	}
	var specs []*upstreamSpec
	for _, name := range names {
		s, err := resolveUpstream(name, getenv)
		if err != nil {
			return config{}, err
		}
		if err := checkUpstreamKey(getenv(s.keyVar)); err != nil {
			return config{}, fmt.Errorf("%s %w; it is required while SENTINEL_UPSTREAMS lists %s", s.keyVar, err, s.name)
		}
		specs = append(specs, s)
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
		p, err := s.protocol.newFunc(getenv(s.keyVar), baseURL, maxConcurrent, extraCAs)
		if err != nil {
			return config{}, providerError(s, err)
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
		ignoredCustom:  ignoredCustom,
		redis:          rdb,
		rateRPM:        rateRPM,
		rateFailClosed: failClosed,
	}, nil
}

func run(logger *slog.Logger) error {
	cfg, err := loadConfig(os.Getenv, os.Environ)
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

// newMux registers the routes of every enabled upstream (see
// upstreamSpec.routes), each behind the same client keys. Every handler
// gets the same opts, so they share its rate limiter, and one Admission:
// when opts has none, newMux creates it with cfg.maxConcurrent slots. One
// cap for the whole gateway means every route and upstream draws on the
// same slots, so the memory bound in ADR 0001 holds however many there are.
func newMux(cfg config, logger *slog.Logger, opts proxy.Options) *http.ServeMux {
	if opts.Admission == nil {
		opts.Admission = proxy.NewAdmission(cfg.maxConcurrent)
	}
	mux := http.NewServeMux()
	for _, u := range cfg.upstreams {
		o := opts
		o.Upstream = u.spec.name
		h := proxy.RequireKey(cfg.clientKeys, u.provider, logger, proxy.NewHandler(u.provider, logger, o))
		for _, route := range u.spec.routes() {
			mux.Handle(route, h)
		}
	}
	return mux
}

// checkUpstreamKey rejects an upstream key that is empty, has leading or
// trailing whitespace, or holds a control character, which the HTTP client
// would refuse to send, so every request would fail as a 502. The error
// completes "<VAR> ..." and never includes the key.
func checkUpstreamKey(key string) error {
	switch {
	case key == "":
		return errors.New("is not set")
	case strings.TrimSpace(key) != key:
		return errors.New("has leading or trailing whitespace")
	case upstream.HasControlChar(key):
		return errors.New("contains a control character")
	}
	return nil
}

// providerError attributes an error from s.protocol.newFunc: a rejected base URL to
// s's base URL variable, anything else (the trust store, the client) to the
// upstream itself.
func providerError(s *upstreamSpec, err error) error {
	if _, ok := errors.AsType[*upstream.BaseURLError](err); ok {
		return fmt.Errorf("%s: %w", s.baseURLVar, err)
	}
	return fmt.Errorf("upstream %s: %w", s.name, err)
}

// parseUpstreams reads SENTINEL_UPSTREAMS, a comma-separated list of
// upstream names. Empty means defaultUpstreams. A name must match
// upstreamName and must not be "v1"; a name that doesn't, or is listed
// twice, is an error, as is a list with no names in it. Whether a name is
// built-in or custom is resolveUpstream's business.
func parseUpstreams(s string) ([]string, error) {
	if s == "" {
		s = defaultUpstreams
	}
	var names []string
	for name := range strings.SplitSeq(s, ",") {
		if name = strings.TrimSpace(name); name == "" {
			continue
		}
		switch {
		case !upstreamName.MatchString(name):
			return nil, fmt.Errorf("upstream name %q must be a lowercase letter followed by up to 31 lowercase letters or digits", name)
		case name == "v1":
			return nil, errors.New(`upstream name "v1" is reserved: its routes would start /v1/v1/`)
		case slices.Contains(names, name):
			return nil, fmt.Errorf("upstream %q is listed more than once", name)
		}
		names = append(names, name)
	}
	if len(names) == 0 {
		return nil, fmt.Errorf("%q lists no upstreams", s)
	}
	return names, nil
}

// resolveUpstream returns the spec for a name from SENTINEL_UPSTREAMS: the
// built-in upstream of that name, or else a custom one read from its
// SENTINEL_UPSTREAM_<NAME>_* variables. A custom upstream has no defaults,
// so its protocol and base URL must be set; resolveUpstream checks they are
// present and leaves the base URL itself to the provider. Errors name the
// variable at fault, never its value.
func resolveUpstream(name string, getenv func(string) string) (*upstreamSpec, error) {
	if i := slices.IndexFunc(upstreamSpecs, func(u upstreamSpec) bool { return u.name == name }); i >= 0 {
		return &upstreamSpecs[i], nil
	}
	prefix := customVarPrefix + strings.ToUpper(name) + "_"
	protoVar := prefix + "PROTOCOL"
	raw := strings.TrimSpace(getenv(protoVar))
	if raw == "" {
		return nil, fmt.Errorf("%s is not set; it is required while SENTINEL_UPSTREAMS lists %s", protoVar, name)
	}
	i := slices.IndexFunc(protocols, func(p *protocol) bool { return p.name == raw })
	if i < 0 {
		return nil, fmt.Errorf(`%s must be "anthropic" or "openai"`, protoVar)
	}
	s := &upstreamSpec{name: name, protocol: protocols[i], keyVar: prefix + "API_KEY", baseURLVar: prefix + "BASE_URL"}
	if getenv(s.baseURLVar) == "" {
		return nil, fmt.Errorf("%s is not set; it is required while SENTINEL_UPSTREAMS lists %s", s.baseURLVar, name)
	}
	return s, nil
}

// scanUpstreamVars looks through environ ("KEY=value" entries) for
// SENTINEL_UPSTREAM_<NAME>_* variables with a value. One that names a
// built-in upstream is an error, since those are configured by their own
// variables. Otherwise it returns the names, lower-cased and sorted, of
// custom upstreams with a PROTOCOL, BASE_URL or API_KEY variable set that
// are not in listed. Only variable names reach the error, never values.
func scanUpstreamVars(environ []string, listed []string) ([]string, error) {
	var unlisted []string
	for _, kv := range environ {
		key, val, _ := strings.Cut(kv, "=")
		rest, ok := strings.CutPrefix(key, customVarPrefix)
		if !ok || val == "" {
			continue
		}
		upper, field, ok := strings.Cut(rest, "_")
		if !ok {
			continue
		}
		name := strings.ToLower(upper)
		if i := slices.IndexFunc(upstreamSpecs, func(u upstreamSpec) bool { return u.name == name }); i >= 0 {
			s := &upstreamSpecs[i]
			return nil, fmt.Errorf("%s: %s is a built-in upstream with a fixed protocol; set %s and %s instead",
				key, name, s.keyVar, s.baseURLVar)
		}
		// SENTINEL_UPSTREAM_CA_FILE, say, is not an upstream's.
		if !slices.Contains(customVarFields, field) || upper != strings.ToUpper(name) || !upstreamName.MatchString(name) {
			continue
		}
		if !slices.Contains(listed, name) && !slices.Contains(unlisted, name) {
			unlisted = append(unlisted, name)
		}
	}
	slices.Sort(unlisted)
	return unlisted, nil
}

// logUpstreams records each enabled upstream and warns about upstream
// variables that are set but unused. It never logs a key.
func logUpstreams(logger *slog.Logger, cfg config) {
	for _, u := range cfg.upstreams {
		logger.Info("upstream enabled", "upstream", u.spec.name, "protocol", u.spec.protocol.name, "base_url", u.baseURL)
	}
	for _, s := range cfg.ignored {
		logger.Warn(fmt.Sprintf("%s is set but %s is not in SENTINEL_UPSTREAMS; ignoring", s.keyVar, s.name))
	}
	for _, name := range cfg.ignoredCustom {
		logger.Warn(fmt.Sprintf("%s%s_* is set but %s is not in SENTINEL_UPSTREAMS; ignoring",
			customVarPrefix, strings.ToUpper(name), name))
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

// onDefaultHost reports whether u is a built-in upstream talking to its
// public default host, such as api.anthropic.com, whatever the path. A
// trailing dot (the fully qualified form, api.anthropic.com.) names the
// same host. A custom upstream has no default, so it is never on one.
func onDefaultHost(u enabledUpstream) bool {
	if !u.spec.builtin {
		return false
	}
	got, err := url.Parse(u.baseURL)
	if err != nil {
		return false
	}
	def, err := url.Parse(u.spec.defaultURL)
	return err == nil && strings.EqualFold(strings.TrimSuffix(got.Hostname(), "."), def.Hostname())
}
