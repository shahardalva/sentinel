// Command gateway runs the Sentinel HTTP gateway.
package main

import (
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/shahardalva/sentinel/internal/admin"
	"github.com/shahardalva/sentinel/internal/keys"
	"github.com/shahardalva/sentinel/internal/provider/anthropic"
	"github.com/shahardalva/sentinel/internal/provider/openai"
	"github.com/shahardalva/sentinel/internal/provider/upstream"
	"github.com/shahardalva/sentinel/internal/proxy"
	"github.com/shahardalva/sentinel/internal/ratelimit"
	"github.com/shahardalva/sentinel/internal/store"
	"github.com/shahardalva/sentinel/internal/telemetry"
)

const (
	defaultAddr     = "127.0.0.1:8081" // loopback only; set SENTINEL_ADDR to expose
	shutdownTimeout = 30 * time.Second
	defaultRateRPM  = 60
	// Loopback only, and never the API port: metrics are for the operator,
	// not for clients. 9464 is the conventional port of OpenTelemetry's
	// Prometheus exporter; 9091 would clash with the Pushgateway's.
	defaultMetricsAddr = "127.0.0.1:9464"
	metricsOff         = "off"
	// A scrape takes milliseconds; nothing on the metrics port streams.
	metricsShutdownTimeout = 5 * time.Second
	// Loopback only, like metrics, and the port after it. Used only with a
	// key database: the admin API manages it.
	defaultAdminAddr = "127.0.0.1:9465"
	// defaultKeysRefresh is how often the key set is fetched from the
	// database. A revoked key stops working within it plus up to two fetch
	// timeouts (min(refresh/2, 10s) each): at most 2x the refresh, 50 s
	// with the defaults (ADR 0005). minKeysRefresh keeps the fetch
	// timeout, half the interval below 20 s, long enough for a round trip
	// to a database over a network, and keeps a typo such as 30ms from
	// querying it 30 times a second per gateway.
	defaultKeysRefresh = 30 * time.Second
	minKeysRefresh     = time.Second
	// defaultKeysMaxStale is how long the gateway serves its last good key
	// set while the database can't be reached, before it answers 503 to
	// every request (ADR 0005): the longest a revoked key keeps working
	// through an outage. 10 minutes rides out a database failover or a
	// short network outage, 20 refreshes at the default interval.
	defaultKeysMaxStale = 10 * time.Minute
)

// Deadlines for the key database and the admin API; vars so tests can
// shorten them.
//
// storeOpenTimeout bounds store.Open: a TCP connect, TLS if configured,
// authentication and a ping. Without it a database behind a firewall that
// drops packets would hold startup for the OS connect timeout, minutes.
// 10 s is far beyond a healthy connect, even across regions or to a
// database just started, and matches the longest default key fetch
// timeout.
//
// storeMigrateTimeout bounds Migrate. The migrations so far are a few
// CREATE TABLE statements that take milliseconds; most of the time is
// waiting on the migration lock while other gateways starting at once
// migrate in turn. 30 s covers that with room to spare. A migration that
// needs longer (a backfill on a big table) should be run on purpose, not
// on a gateway's startup, and the deadline rolls it back whole rather
// than leaving startup hanging.
//
// adminShutdownTimeout bounds the admin server's graceful shutdown, for a
// client to get the answer to a request in flight: an admin request is one
// store call of at most admin.StoreTimeout (5 s), and nothing on the admin
// port streams. Shorter than shutdownTimeout, which is for streams; it
// runs alongside the API's drain, so it adds nothing to it.
//
// adminDrainTimeout bounds the wait for admin handlers once their
// connections are cut. All that is left of a handler then is its store
// call, which admin.StoreTimeout bounds (the body read fails at once on
// the closed connection), and its audit line; the extra second is for
// those. A handler past it is stuck in a store call that ignores its
// context.
//
// keysStopTimeout bounds the wait for the refresh loop at shutdown. A
// fetch honors its context, so it returns at once when cancelled.
var (
	storeOpenTimeout     = 10 * time.Second
	storeMigrateTimeout  = 30 * time.Second
	adminShutdownTimeout = 5 * time.Second
	keysStopTimeout      = 5 * time.Second
	adminDrainTimeout    = admin.StoreTimeout + time.Second
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
	// clientKeys are the keys from SENTINEL_API_KEYS; nil when the keys
	// come from the database.
	clientKeys []string
	// databaseURL is SENTINEL_DATABASE_URL, empty when unset. It may hold
	// a password: never log it or put it in an error.
	databaseURL string
	keysRefresh time.Duration // with a database only
	// keysMaxStale is how old the key set may get while refreshes fail
	// before every request gets a 503; 0 never. With a database only.
	keysMaxStale time.Duration
	adminAddr    string // empty without a database or when "off"
	// adminToken is SENTINEL_ADMIN_TOKEN, set when adminAddr is. Never log
	// it.
	adminToken string
	// keyWarnings say which variables of the key database or the admin
	// API are set but not used, and why. Never their values.
	keyWarnings   []string
	addr          string
	metricsAddr   string // empty when SENTINEL_METRICS_ADDR is "off"
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
	// unknownVars maps a listed custom upstream to its set variables with
	// an unknown suffix, such as SENTINEL_UPSTREAM_XAI_APIKEY. Not used.
	unknownVars map[string][]string
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
	found, err := scanUpstreamVars(environ(), names)
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
	kc, err := loadKeyConfig(getenv)
	if err != nil {
		return config{}, err
	}
	addr := getenv("SENTINEL_ADDR")
	if addr == "" {
		addr = defaultAddr
	}
	metricsAddr, err := parseMetricsAddr(getenv("SENTINEL_METRICS_ADDR"), addr)
	if err != nil {
		return config{}, fmt.Errorf("SENTINEL_METRICS_ADDR: %w", err)
	}
	if err := loadAdminConfig(getenv, &kc, addr, metricsAddr); err != nil {
		return config{}, err
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
		clientKeys:     kc.clientKeys,
		databaseURL:    kc.databaseURL,
		keysRefresh:    kc.keysRefresh,
		keysMaxStale:   kc.keysMaxStale,
		adminAddr:      kc.adminAddr,
		adminToken:     kc.adminToken,
		keyWarnings:    kc.keyWarnings,
		addr:           addr,
		metricsAddr:    metricsAddr,
		maxConcurrent:  maxConcurrent,
		caFile:         caFile,
		extraCAs:       extraCAs,
		upstreams:      upstreams,
		ignored:        ignored,
		ignoredCustom:  found.unlisted,
		unknownVars:    found.unknown,
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
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return serve(ctx, logger, cfg)
}

// serve runs the gateway for cfg until ctx is done, then shuts it down.
func serve(ctx context.Context, logger *slog.Logger, cfg config) error {
	logUpstreams(logger, cfg)
	if cfg.caFile != "" {
		logCAs(logger, cfg.caFile, cfg.extraCAs, cfg.upstreams)
	}

	// Created here rather than in newMux, so the metrics can watch it.
	opts := proxy.Options{Admission: proxy.NewAdmission(cfg.maxConcurrent)}
	var breakerOpen func() bool
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
		breakerOpen = limiter.BreakerOpen
		logRateLimit(logger, cfg)
	}

	var metrics *telemetry.Metrics
	if cfg.metricsAddr == "" {
		logger.Info("metrics disabled: SENTINEL_METRICS_ADDR is off")
	} else {
		var err error
		if metrics, err = newMetrics(cfg, opts.Admission, breakerOpen); err != nil {
			return fmt.Errorf("metrics: %w", err)
		}
		defer metrics.Shutdown(context.Background())
		opts.Recorder = metrics
	}

	src, db, stopKeys, err := startKeys(ctx, logger, cfg, opts.Recorder)
	if err != nil {
		if startupInterrupted(ctx, err) {
			// A signal while startup waited on the database: a stop that
			// was asked for, not a failure, so no error exit.
			logger.Info("startup interrupted", "error", err)
			return nil
		}
		return err
	}
	// storeIdle turns false if an admin handler is still running at the
	// end; the store is then left open (see stopAdmin).
	storeIdle := true
	defer func() { stopKeys(storeIdle) }()

	mux := newMux(cfg, logger, opts, src)

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

	// The metrics listener binds first, so "listening", which make
	// bench-gateway waits for, still means the whole gateway is up. A bind
	// failure stops startup; once serving, a metrics failure only loses
	// metrics: it is logged and the API keeps serving.
	var metricsSrv *http.Server
	if metrics != nil {
		mln, err := listenMetrics(logger, cfg.metricsAddr)
		if err != nil {
			return fmt.Errorf("SENTINEL_METRICS_ADDR: %w", err)
		}
		metricsSrv = newMetricsServer(metrics.Handler(), logger)
		go func() {
			if err := metricsSrv.Serve(mln); !errors.Is(err, http.ErrServerClosed) {
				logger.Error("metrics server stopped; the API keeps serving without metrics", "error", err)
			}
		}()
		defer metricsSrv.Close()
	}

	// After metrics and before the API, for the same reason, and with the
	// same rule: a bind failure stops startup, a later failure only loses
	// the admin API. Keys keep refreshing and the API keeps serving; an
	// operator who can't issue or revoke a key for a while is better off
	// than every client losing the gateway.
	var stopAdminAPI func() bool // nil without an admin API
	switch {
	case db == nil:
	case cfg.adminAddr == "":
		logger.Info("admin API disabled: SENTINEL_ADMIN_ADDR is off")
	default:
		aln, err := listenAdmin(logger, cfg.adminAddr)
		if err != nil {
			return fmt.Errorf("SENTINEL_ADMIN_ADDR: %w", err)
		}
		api := admin.New(admin.Config{Store: db, Token: cfg.adminToken, Logger: logger})
		adminSrv := newAdminServer(api, logger)
		go func() {
			if err := adminSrv.Serve(aln); !errors.Is(err, http.ErrServerClosed) {
				logger.Error("admin server stopped; the API keeps serving without the admin API", "error", err)
			}
		}()
		// Every return from here stops the admin API, and waits for its
		// handlers, before the deferred stopKeys closes the store they use.
		// At shutdown it starts at once (below); this then waits for it.
		stopAdminAPI = sync.OnceValue(func() bool { return stopAdmin(logger, adminSrv, api) })
		defer func() { storeIdle = stopAdminAPI() }()
	}

	ln, err := listen(logger, cfg.addr)
	if err != nil {
		return err
	}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ln) }()

	select {
	case err := <-errCh:
		_ = srv.Close()
		return err
	case <-ctx.Done():
	}

	// Give in-flight requests, including streams, a grace period to finish,
	// then cut whatever is still open.
	logger.Info("shutting down", "grace_period", shutdownTimeout.String())
	// The admin API stops alongside the API's drain, not after it: no
	// change starts once shutdown has begun. The deferred stopAdminAPI
	// waits for it before the store is closed.
	if stopAdminAPI != nil {
		go stopAdminAPI()
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Warn("grace period expired; closing open connections", "error", err)
		_ = srv.Close()
	}
	// After the API, so a scrape during the grace period still answers.
	// The deferred Close cuts whatever outlives this.
	if metricsSrv != nil {
		mctx, mcancel := context.WithTimeout(context.Background(), metricsShutdownTimeout)
		defer mcancel()
		_ = metricsSrv.Shutdown(mctx)
	}
	return nil
}

// listen binds addr and only then logs "listening", so the line means the
// port is open. make bench-gateway waits for it.
func listen(logger *slog.Logger, addr string) (net.Listener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	logger.Info("listening", "addr", ln.Addr().String())
	return ln, nil
}

// listenTCP is net.Listen for the metrics and admin listeners; a var so
// tests can make them fail.
var listenTCP = net.Listen

// listenMetrics binds the metrics address and only then logs "metrics
// listening". It never logs "listening", which make bench-gateway takes to
// mean the API port is open.
func listenMetrics(logger *slog.Logger, addr string) (net.Listener, error) {
	ln, err := listenTCP("tcp", addr)
	if err != nil {
		return nil, err
	}
	logger.Info("metrics listening", "addr", ln.Addr().String(), "path", "/metrics")
	return ln, nil
}

// listenAdmin binds the admin address and only then logs "admin
// listening". Like listenMetrics, it never logs "listening".
func listenAdmin(logger *slog.Logger, addr string) (net.Listener, error) {
	ln, err := listenTCP("tcp", addr)
	if err != nil {
		return nil, err
	}
	logger.Info("admin listening", "addr", ln.Addr().String(), "path", "/admin/")
	return ln, nil
}

// newAdminServer serves the admin API, with whole-request timeouts like
// the metrics server: every admin request is short, and its store call
// has 5 s. DisableGeneralOptionsHandler hands "OPTIONS *" to the handler,
// which answers it with its 401 like any other request without the token,
// instead of net/http's own 200.
func newAdminServer(h http.Handler, logger *slog.Logger) *http.Server {
	return &http.Server{
		Handler:                      h,
		DisableGeneralOptionsHandler: true,
		ReadHeaderTimeout:            5 * time.Second,
		ReadTimeout:                  10 * time.Second,
		WriteTimeout:                 15 * time.Second,
		IdleTimeout:                  60 * time.Second,
		ErrorLog:                     slog.NewLogLogger(logger.Handler(), slog.LevelError),
	}
}

// stopAdmin stops the admin API before the key store it uses is closed: it
// stops accepting, gives the requests open adminShutdownTimeout to be
// answered, cuts the connections still open, then waits up to
// adminDrainTimeout for their handlers. A change whose connection was cut
// still runs to its end and writes its audit line, and neither the store's
// Close nor the process's exit may come first. It reports whether every
// handler finished; if not, the store must stay open, since pgxpool.Close
// would wait for the connection the stuck call holds.
func stopAdmin(logger *slog.Logger, srv *http.Server, api *admin.API) bool {
	ctx, cancel := context.WithTimeout(context.Background(), adminShutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		// A change among the requests cut may or may not be applied; if it
		// is, the drain below lets it log so.
		logger.Warn("admin grace period expired; closing open admin connections", "error", err)
		_ = srv.Close()
	}
	dctx, dcancel := context.WithTimeout(context.Background(), adminDrainTimeout)
	defer dcancel()
	if err := api.Drain(dctx); err != nil {
		logger.Warn("admin requests still running; leaving the key store open", "waited", adminDrainTimeout.String())
		return false
	}
	return true
}

// keyStore is what the gateway needs of the key database: the admin API's
// calls, the migrations and the key fetch. *store.Store satisfies it.
type keyStore interface {
	admin.Store
	Migrate(ctx context.Context) error
	SchemaVersion(ctx context.Context) (int, error)
	ActiveDigests(ctx context.Context) ([][sha256.Size]byte, error)
	Close()
}

var _ keyStore = (*store.Store)(nil)

// openStore is store.Open; a var so tests can run the wiring on a fake
// store.
var openStore = func(ctx context.Context, url string) (keyStore, error) {
	s, err := store.Open(ctx, url)
	if err != nil {
		return nil, err
	}
	return s, nil
}

// startKeys returns the source of the client keys. Without a database it
// holds the keys from SENTINEL_API_KEYS, db is nil and stop does nothing.
// With one, startKeys opens and migrates the database, each within its
// deadline, and starts a keys.Refreshing on it, which has loaded the keys
// once when startKeys returns. stop cancels the refresh, waits for it a
// bounded time, and closes the database if closeStore is set; the caller
// clears it while something else may still be using the store.
//
// ctx cancels the startup steps but not the refresh, so keys keep
// refreshing while the API drains its requests at shutdown, until stop.
// Every error is safe to log: the store's are sanitized, and none holds
// the URL.
func startKeys(ctx context.Context, logger *slog.Logger, cfg config, rec proxy.Recorder) (src proxy.KeySource, db keyStore, stop func(closeStore bool), err error) {
	if cfg.databaseURL == "" {
		logger.Info("client keys from SENTINEL_API_KEYS", "keys", len(cfg.clientKeys))
		return keys.NewStatic(cfg.clientKeys, rec), nil, func(bool) {}, nil
	}

	octx, ocancel := context.WithTimeout(ctx, storeOpenTimeout)
	db, err = openStore(octx, cfg.databaseURL)
	ocancel()
	if err != nil {
		return nil, nil, nil, startupError(ctx, "SENTINEL_DATABASE_URL", err, storeOpenTimeout)
	}
	mctx, mcancel := context.WithTimeout(ctx, storeMigrateTimeout)
	err = db.Migrate(mctx)
	var version int
	if err == nil {
		version, err = db.SchemaVersion(mctx)
	}
	mcancel()
	if err != nil {
		db.Close()
		return nil, nil, nil, startupError(ctx, "key store migration", err, storeMigrateTimeout)
	}

	rctx, rcancel := context.WithCancel(context.Background())
	// A signal during the first fetch still stops startup.
	unlink := context.AfterFunc(ctx, rcancel)
	r, err := keys.NewRefreshing(rctx, keys.Config{
		Fetch:    db.ActiveDigests,
		Interval: cfg.keysRefresh,
		MaxStale: cfg.keysMaxStale,
		Logger:   logger,
		Recorder: rec,
	})
	unlink()
	if err != nil {
		rcancel()
		db.Close()
		return nil, nil, nil, fmt.Errorf("key store: %w", err)
	}
	logger.Info("client keys from postgres", "keys", r.Keys().Len(),
		"refresh", cfg.keysRefresh.String(), "max_stale", cfg.keysMaxStale.String(), "schema_version", version)
	stop = func(closeStore bool) {
		rcancel()
		select {
		case <-r.Done():
		case <-time.After(keysStopTimeout):
			// Close waits for every connection in use, the stuck fetch's too.
			logger.Warn("key refresh did not stop; leaving the key store open", "waited", keysStopTimeout.String())
			return
		}
		if closeStore {
			db.Close()
		}
	}
	return r, db, stop, nil
}

// startupInterrupted reports whether startKeys failed because a signal
// cancelled ctx: the error is the cancellation itself, a Canceled or a
// DeadlineExceeded (a step's own deadline that ran out as the signal came),
// and ctx is done. Any other error is a real failure even if a signal
// arrived meanwhile: ErrSchemaTooNew must not exit 0.
func startupInterrupted(ctx context.Context, err error) bool {
	return ctx.Err() != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded))
}

// startupError wraps the error of a startup step on the key database. A
// step that ran out of its own deadline, rather than being cancelled by a
// signal, says which deadline.
func startupError(ctx context.Context, step string, err error, deadline time.Duration) error {
	if ctx.Err() == nil && errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("%s: %w (startup deadline %s)", step, err, deadline)
	}
	return fmt.Errorf("%s: %w", step, err)
}

// newMetrics returns the gateway's metrics, labelled with the enabled
// upstreams, watching admission and, when rate limiting is on, the Redis
// breaker.
func newMetrics(cfg config, admission *proxy.Admission, breakerOpen func() bool) (*telemetry.Metrics, error) {
	tc := telemetry.Config{Admission: admission, BreakerOpen: breakerOpen}
	for _, u := range cfg.upstreams {
		tc.Upstreams = append(tc.Upstreams, telemetry.Upstream{Name: u.spec.name, Protocol: u.provider.Name()})
	}
	return telemetry.New(tc)
}

// newMetricsServer serves metrics on GET /metrics and nothing else. Unlike
// the API server it has whole-request timeouts: a scrape is short.
func newMetricsServer(metrics http.Handler, logger *slog.Logger) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", metrics)
	return &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelError),
	}
}

// newMux registers the routes of every enabled upstream (see
// upstreamSpec.routes), each behind the client keys from src and one
// proxy.Auth, which reports rejections to opts.Recorder. Every handler gets
// the same opts, so they share its rate limiter and Recorder, and one
// Admission:
// when opts has none, newMux creates it with cfg.maxConcurrent slots. One
// cap for the whole gateway means every route and upstream draws on the
// same slots, so the memory bound in ADR 0001 holds however many there are.
func newMux(cfg config, logger *slog.Logger, opts proxy.Options, src proxy.KeySource) *http.ServeMux {
	if opts.Admission == nil {
		opts.Admission = proxy.NewAdmission(cfg.maxConcurrent)
	}
	auth := proxy.NewAuthSource(src, logger, opts.Recorder)
	mux := http.NewServeMux()
	for _, u := range cfg.upstreams {
		o := opts
		o.Upstream = u.spec.name
		h := auth.Require(u.provider, proxy.NewHandler(u.provider, logger, o))
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
// upstreamName and must not be "v1" or "ca"; a name that doesn't, or is listed
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
		case name == "ca":
			return nil, errors.New(`upstream name "ca" is reserved: its variables would share a prefix with SENTINEL_UPSTREAM_CA_FILE`)
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

// upstreamVars is what scanUpstreamVars found besides errors.
type upstreamVars struct {
	// unlisted are the names, lower-cased and sorted, of custom upstreams
	// not in SENTINEL_UPSTREAMS with a PROTOCOL, BASE_URL or API_KEY
	// variable set.
	unlisted []string
	// unknown maps a listed custom upstream to its variables, sorted, whose
	// suffix is none of those three (a typo such as _APIKEY).
	unknown map[string][]string
}

// scanUpstreamVars looks through environ ("KEY=value" entries) for
// SENTINEL_UPSTREAM_<NAME>_* variables with a value. Names are compared
// ignoring case, as Windows compares environment variable names, so a
// variable getenv would find is never missed here. One that names a
// built-in upstream is an error, since those are configured by their own
// variables. Only variable names reach the error, never values.
func scanUpstreamVars(environ []string, listed []string) (upstreamVars, error) {
	var found upstreamVars
	for _, kv := range environ {
		key, val, _ := strings.Cut(kv, "=")
		rest, ok := strings.CutPrefix(strings.ToUpper(key), customVarPrefix)
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
			return upstreamVars{}, fmt.Errorf("%s: %s is a built-in upstream with a fixed protocol; set %s and %s instead",
				key, name, s.keyVar, s.baseURLVar)
		}
		switch {
		case !upstreamName.MatchString(name):
		case slices.Contains(listed, name):
			if !slices.Contains(customVarFields, field) {
				if found.unknown == nil {
					found.unknown = make(map[string][]string)
				}
				found.unknown[name] = append(found.unknown[name], key)
			}
		// SENTINEL_UPSTREAM_CA_FILE, say, is not an upstream's.
		case slices.Contains(customVarFields, field) && !slices.Contains(found.unlisted, name):
			found.unlisted = append(found.unlisted, name)
		}
	}
	slices.Sort(found.unlisted)
	for _, vars := range found.unknown {
		slices.Sort(vars)
	}
	return found, nil
}

// logUpstreams records each enabled upstream and warns about upstream
// variables that are set but unused. It never logs a key.
func logUpstreams(logger *slog.Logger, cfg config) {
	for _, u := range cfg.upstreams {
		logger.Info("upstream enabled", "upstream", u.spec.name, "protocol", u.spec.protocol.name, "base_url", u.origin())
	}
	for _, u := range cfg.upstreams {
		if vars := cfg.unknownVars[u.spec.name]; len(vars) > 0 {
			logger.Warn(fmt.Sprintf("%s set for upstream %s, which reads only %sPROTOCOL, _BASE_URL and _API_KEY; ignoring",
				strings.Join(vars, ", "), u.spec.name, customVarPrefix+strings.ToUpper(u.spec.name)+"_"))
		}
	}
	for _, s := range cfg.ignored {
		logger.Warn(fmt.Sprintf("%s is set but %s is not in SENTINEL_UPSTREAMS; ignoring", s.keyVar, s.name))
	}
	for _, w := range cfg.keyWarnings {
		logger.Warn(w)
	}
	for _, name := range cfg.ignoredCustom {
		logger.Warn(fmt.Sprintf("%s%s_* is set but %s is not in SENTINEL_UPSTREAMS; ignoring",
			customVarPrefix, strings.ToUpper(name), name))
	}
}

// closeUnmatched closes the connection after a 404 or 405 from mux. Those
// responses leave the body unread, and net/http would otherwise drain it
// with no deadline (see proxy.RejectDrainTimeout). A path that is not
// already clean (a double slash, a dot segment, a trailing slash) gets a
// plain-text 400 the same way, before mux sees it: mux would answer it with
// a redirect to the cleaned path, which matches a route, so it would not
// count as unmatched and the drain would have no deadline.
func closeUnmatched(mux *http.ServeMux) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		clean := r.URL.Path == path.Clean(r.URL.Path)
		if _, pattern := mux.Handler(r); !clean || pattern == "" {
			_ = http.NewResponseController(w).SetReadDeadline(time.Now().Add(proxy.RejectDrainTimeout))
			w.Header().Set("Connection", "close")
		}
		if !clean {
			http.Error(w, "request path is not clean", http.StatusBadRequest)
			return
		}
		mux.ServeHTTP(w, r)
	})
}

// parseMetricsAddr reads SENTINEL_METRICS_ADDR. Empty means
// defaultMetricsAddr, and "off" means no metrics, returned as "". Anything
// else must be host:port with a numeric port other than apiAddr's (see
// checkOwnPort).
func parseMetricsAddr(s, apiAddr string) (string, error) {
	switch s = strings.TrimSpace(s); s {
	case "":
		s = defaultMetricsAddr
	case metricsOff:
		return "", nil
	}
	if err := checkOwnPort(s, "metrics", listenerAddr{"SENTINEL_ADDR", apiAddr}); err != nil {
		return "", err
	}
	return s, nil
}

// listenerAddr is another listener's address and the variable it is from.
type listenerAddr struct{ name, addr string }

// checkOwnPort checks that s, the address of the listener called what, is
// host:port with a numeric port, and that the port is none of others',
// whatever either host: 0.0.0.0, [::] and 127.0.0.1 overlap in practice,
// and a client resolving localhost to ::1 would otherwise reach one
// listener on another's port. Ports compare as numbers, so 08081 is 8081,
// and a named port among others, such as :http, is resolved first. Port 0
// (any free port) is exempt, and so is an other's address that doesn't
// parse: it fails at its own bind.
func checkOwnPort(s, what string, others ...listenerAddr) error {
	_, port, err := net.SplitHostPort(s)
	if err != nil {
		return fmt.Errorf("%q is neither host:port nor %q", s, metricsOff)
	}
	n, err := strconv.ParseUint(port, 10, 16)
	if err != nil {
		return fmt.Errorf("%q has no numeric port", s)
	}
	if n == 0 {
		return nil
	}
	for _, o := range others {
		_, oport, err := net.SplitHostPort(o.addr)
		if err != nil {
			continue
		}
		if p, err := net.LookupPort("tcp", oport); err == nil && uint64(p) == n {
			return fmt.Errorf("%q has %s's port %d; %s needs a port of its own, never the API's or another listener's", s, o.name, n, what)
		}
	}
	return nil
}

// loadKeyConfig reads where the client keys come from:
// SENTINEL_API_KEYS, or the database at SENTINEL_DATABASE_URL, never both,
// and SENTINEL_KEYS_REFRESH and SENTINEL_KEYS_MAX_STALE for the database. The returned config has only
// those fields. No error holds the URL, which may carry a password.
func loadKeyConfig(getenv func(string) string) (config, error) {
	var kc config
	rawURL := getenv("SENTINEL_DATABASE_URL")
	rawKeys := getenv("SENTINEL_API_KEYS")
	rawRefresh := getenv("SENTINEL_KEYS_REFRESH")
	rawMaxStale := getenv("SENTINEL_KEYS_MAX_STALE")
	if rawURL == "" {
		clientKeys, err := keys.Parse(rawKeys)
		switch {
		case err == nil:
		case rawKeys == "":
			return config{}, errors.New("SENTINEL_API_KEYS is not set; it is required unless SENTINEL_DATABASE_URL is set")
		case errors.Is(err, keys.ErrNoKeys):
			return config{}, errors.New("SENTINEL_API_KEYS has no keys, only commas or spaces; it needs at least one unless SENTINEL_DATABASE_URL is set")
		default:
			return config{}, fmt.Errorf("SENTINEL_API_KEYS: %w", err)
		}
		kc.clientKeys = clientKeys
		if rawRefresh != "" {
			kc.keyWarnings = append(kc.keyWarnings, "SENTINEL_KEYS_REFRESH is set but SENTINEL_DATABASE_URL is not; ignoring")
		}
		if rawMaxStale != "" {
			kc.keyWarnings = append(kc.keyWarnings, "SENTINEL_KEYS_MAX_STALE is set but SENTINEL_DATABASE_URL is not; ignoring")
		}
		return kc, nil
	}
	// libpq and pgx read an empty connection string as "all defaults",
	// from PG* variables and ~/.pgpass, which a blank value shouldn't mean.
	// Surrounding whitespace, from a .env file say, is dropped.
	if kc.databaseURL = strings.TrimSpace(rawURL); kc.databaseURL == "" {
		return config{}, errors.New("SENTINEL_DATABASE_URL is blank; unset it to take keys from SENTINEL_API_KEYS")
	}
	if rawKeys != "" {
		return config{}, errors.New("SENTINEL_API_KEYS must be empty while SENTINEL_DATABASE_URL is set: " +
			"the client keys come from the database, and the gateway never merges the two")
	}
	refresh, err := parseKeysRefresh(rawRefresh)
	if err != nil {
		return config{}, fmt.Errorf("SENTINEL_KEYS_REFRESH: %w", err)
	}
	maxStale, err := parseKeysMaxStale(rawMaxStale, refresh)
	if err != nil {
		return config{}, fmt.Errorf("SENTINEL_KEYS_MAX_STALE: %w", err)
	}
	kc.keysRefresh, kc.keysMaxStale = refresh, maxStale
	return kc, nil
}

// parseKeysRefresh reads SENTINEL_KEYS_REFRESH, a Go duration such as 30s
// or 1m. Empty means defaultKeysRefresh; anything else must be at least
// minKeysRefresh.
func parseKeysRefresh(s string) (time.Duration, error) {
	if s == "" {
		return defaultKeysRefresh, nil
	}
	d, err := time.ParseDuration(strings.TrimSpace(s))
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("%q is not a positive duration such as 30s", s)
	}
	if d < minKeysRefresh {
		return 0, fmt.Errorf("%q is under the minimum of %s", s, minKeysRefresh)
	}
	return d, nil
}

// parseKeysMaxStale reads SENTINEL_KEYS_MAX_STALE, a Go duration such as
// 10m, or 0 to never expire the key set. Empty means defaultKeysMaxStale.
// Anything else must be at least three times refresh. The fetch timeout
// is at most half of refresh, so a successful refresh comes up to 1.5 x
// refresh after the last, and after one failed refresh the next success
// lands by 3 x refresh. With 2 x, a single failed refresh would put every
// request on a 503. The default is held to the same rule, so a long
// refresh needs the variable set rather than a limit quietly raised.
func parseKeysMaxStale(s string, refresh time.Duration) (time.Duration, error) {
	d := defaultKeysMaxStale
	if s != "" {
		var err error
		if d, err = time.ParseDuration(strings.TrimSpace(s)); err != nil {
			return 0, fmt.Errorf("%q is not a duration such as 10m, or 0 to never expire the key set", s)
		}
		if d < 0 {
			return 0, fmt.Errorf("%q is negative", s)
		}
	}
	if d == 0 || d >= 3*refresh {
		return d, nil
	}
	if s == "" {
		return 0, fmt.Errorf("the default of %s is under three times SENTINEL_KEYS_REFRESH (%s); set SENTINEL_KEYS_MAX_STALE to at least %s, or 0 to never expire the key set", d, refresh, 3*refresh)
	}
	return 0, fmt.Errorf("%q is under three times SENTINEL_KEYS_REFRESH (%s), so one failed refresh would answer every request with a 503", s, refresh)
}

// loadAdminConfig reads SENTINEL_ADMIN_ADDR and SENTINEL_ADMIN_TOKEN into
// kc, which loadKeyConfig filled. The admin API exists only with a
// database: by default on defaultAdminAddr, "off" disables it, and any
// other address must be host:port on a port of its own (checkOwnPort);
// one off loopback is warned about. Without a database, an address other
// than "off" is an error. The token is required while the admin API is
// on; no error holds it.
func loadAdminConfig(getenv func(string) string, kc *config, apiAddr, metricsAddr string) error {
	addr := strings.TrimSpace(getenv("SENTINEL_ADMIN_ADDR"))
	token := getenv("SENTINEL_ADMIN_TOKEN")
	if kc.databaseURL == "" {
		if addr != "" && addr != metricsOff {
			return errors.New("SENTINEL_ADMIN_ADDR is set but SENTINEL_DATABASE_URL is not; " +
				"the admin API manages the keys in the database")
		}
		if token != "" {
			kc.keyWarnings = append(kc.keyWarnings, "SENTINEL_ADMIN_TOKEN is set but SENTINEL_DATABASE_URL is not; ignoring")
		}
		return nil
	}
	switch addr {
	case "":
		addr = defaultAdminAddr
	case metricsOff:
		if token != "" {
			kc.keyWarnings = append(kc.keyWarnings, "SENTINEL_ADMIN_TOKEN is set but SENTINEL_ADMIN_ADDR is off; ignoring")
		}
		return nil
	}
	others := []listenerAddr{{"SENTINEL_ADDR", apiAddr}}
	if metricsAddr != "" {
		others = append(others, listenerAddr{"SENTINEL_METRICS_ADDR", metricsAddr})
	}
	if err := checkOwnPort(addr, "the admin API", others...); err != nil {
		return fmt.Errorf("SENTINEL_ADMIN_ADDR: %w", err)
	}
	if token == "" {
		return fmt.Errorf("SENTINEL_ADMIN_TOKEN is not set; it is required while the admin API is on (%s), "+
			"or set SENTINEL_ADMIN_ADDR=off", addr)
	}
	if err := admin.ValidateToken(token); err != nil {
		return fmt.Errorf("SENTINEL_ADMIN_TOKEN: %w", err)
	}
	if !isLoopbackAddr(addr) {
		kc.keyWarnings = append(kc.keyWarnings, fmt.Sprintf("SENTINEL_ADMIN_ADDR %q is not a loopback address; "+
			"the admin API is plain HTTP, so the admin token and newly created keys cross the network unencrypted "+
			"unless only loopback can reach the port", addr))
	}
	kc.adminAddr, kc.adminToken = addr, token
	return nil
}

// isLoopbackAddr reports whether host:port s listens on loopback only: a
// loopback IP or "localhost". An empty host, an unspecified address and
// any other hostname are not.
func isLoopbackAddr(s string) bool {
	host, _, err := net.SplitHostPort(s)
	if err != nil {
		return false
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
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

// redisWarmUpTimeout bounds the startup warm-up of the Redis pool: two of
// ratelimit.PoolDialTimeout and some, so a dial can be retried once.
var redisWarmUpTimeout = 5 * time.Second

// logRateLimit records the rate-limit settings, checks that Redis answers
// and warms its pool, so the first requests don't have to set up
// connections within their 50 ms. An unreachable Redis or an incomplete
// warm-up is only a warning: Redis may still be starting, and the fail mode
// decides what requests get until it is up. A failed ping skips the warm-up,
// which could only wait out its timeout.
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
		return
	}

	ctx, cancel = context.WithTimeout(context.Background(), redisWarmUpTimeout)
	defer cancel()
	want := cfg.redis.Options().MinIdleConns
	if ready, err := ratelimit.WarmUp(ctx, cfg.redis); err != nil {
		logger.Warn("redis pool warm-up incomplete", "redis", cfg.redis.Options().Addr,
			"ready", ready, "want", want, "fail_mode", mode, "error", err)
	} else {
		logger.Info("redis pool warm", "ready", ready)
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
// trust more than intended, including once for each enabled upstream,
// built-in or custom, on a public default host.
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
		if def := publicDefault(u); def != nil {
			logger.Warn(fmt.Sprintf("extra upstream CAs are trusted for the default %s API; "+
				"anyone holding one of their keys could impersonate it and read %s", def.label, u.spec.keyVar),
				"file", file, "upstream", u.spec.name, "base_url", u.origin())
		}
	}
}

// publicDefault returns the built-in upstream whose public default host,
// such as api.anthropic.com, u talks to, whatever the path, or nil. Custom
// upstreams count too: one pointed at api.openai.com is as exposed to an
// extra CA as the built-in. Case and a trailing dot (the fully qualified
// form, api.anthropic.com.) don't matter.
func publicDefault(u enabledUpstream) *upstreamSpec {
	got, err := url.Parse(u.baseURL)
	if err != nil {
		return nil
	}
	host := strings.TrimSuffix(got.Hostname(), ".")
	for i := range upstreamSpecs {
		def, err := url.Parse(upstreamSpecs[i].defaultURL)
		if err == nil && strings.EqualFold(host, def.Hostname()) {
			return &upstreamSpecs[i]
		}
	}
	return nil
}

// origin returns u's base URL as scheme://host[:port], for logs. The path
// is left out: a base URL's path may carry a key, and validation rejects
// only userinfo and queries.
func (u enabledUpstream) origin() string {
	p, err := url.Parse(u.baseURL)
	if err != nil {
		return ""
	}
	return p.Scheme + "://" + p.Host
}
