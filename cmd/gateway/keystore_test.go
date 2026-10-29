package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/shahardalva/sentinel/internal/keys"
	"github.com/shahardalva/sentinel/internal/proxy"
	"github.com/shahardalva/sentinel/internal/store"
)

// staticKeys is the key source serve builds from SENTINEL_API_KEYS.
func staticKeys(cfg config) proxy.KeySource { return keys.NewStatic(cfg.clientKeys, nil) }

// Distinctive secrets, so a leak into an error or a log is easy to spot.
const (
	leakUser   = "gwuser_q7x"
	leakPass   = "gwpass_z9k"
	leakHost   = "gwhost-m4v.invalid"
	leakDB     = "gwdb_r2t"
	testToken  = "admin-token-for-tests-only-0123456789"
	shortToken = "tok_w5j_short"
)

var leakURL = "postgres://" + leakUser + ":" + leakPass + "@" + leakHost + ":5432/" + leakDB + "?sslmode=disable"

// with returns base plus extra, extra winning.
func with(base, extra map[string]string) map[string]string {
	m := maps.Clone(base)
	maps.Copy(m, extra)
	return m
}

// dbBase is a valid configuration with the keys in a database.
var dbBase = map[string]string{
	"ANTHROPIC_API_KEY":     "sk",
	"SENTINEL_DATABASE_URL": leakURL,
	"SENTINEL_ADMIN_TOKEN":  testToken,
}

func TestLoadConfig_KeySource(t *testing.T) {
	cfg, err := loadConfig(env(map[string]string{"ANTHROPIC_API_KEY": "sk", "SENTINEL_API_KEYS": "k1,k2"}))
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(cfg.clientKeys, []string{"k1", "k2"}) || cfg.databaseURL != "" || cfg.adminAddr != "" ||
		cfg.adminToken != "" || cfg.keysRefresh != 0 || cfg.keyWarnings != nil {
		t.Errorf("env keys: clientKeys %q, adminAddr %q, refresh %s, warnings %q",
			cfg.clientKeys, cfg.adminAddr, cfg.keysRefresh, cfg.keyWarnings)
	}

	cfg, err = loadConfig(env(dbBase))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.clientKeys != nil || cfg.databaseURL != leakURL || cfg.keysRefresh != defaultKeysRefresh ||
		cfg.adminAddr != defaultAdminAddr || cfg.adminToken != testToken || cfg.keyWarnings != nil {
		t.Errorf("database keys: clientKeys %q, refresh %s, adminAddr %q, warnings %q",
			cfg.clientKeys, cfg.keysRefresh, cfg.adminAddr, cfg.keyWarnings)
	}
	if defaultKeysRefresh != 30*time.Second || defaultAdminAddr != "127.0.0.1:9465" {
		t.Errorf("defaults: refresh %s, admin %s", defaultKeysRefresh, defaultAdminAddr)
	}

	for name, vars := range map[string]map[string]string{
		"neither":               {"ANTHROPIC_API_KEY": "sk"},
		"both":                  with(dbBase, map[string]string{"SENTINEL_API_KEYS": "k1"}),
		"both, blank env keys":  with(dbBase, map[string]string{"SENTINEL_API_KEYS": " , "}),
		"blank database URL":    with(dbBase, map[string]string{"SENTINEL_DATABASE_URL": "  "}),
		"blank URL, env keys":   {"ANTHROPIC_API_KEY": "sk", "SENTINEL_API_KEYS": "k1", "SENTINEL_DATABASE_URL": " "},
		"database, unparseable": with(dbBase, map[string]string{"SENTINEL_KEYS_REFRESH": "soon"}),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := loadConfig(env(vars)); err == nil {
				t.Error("loadConfig succeeded")
			}
		})
	}
	wantConfigErr(t, map[string]string{"ANTHROPIC_API_KEY": "sk"}, "SENTINEL_API_KEYS")
	wantConfigErr(t, with(dbBase, map[string]string{"SENTINEL_API_KEYS": "k1"}), "SENTINEL_API_KEYS")
	wantConfigErr(t, with(dbBase, map[string]string{"SENTINEL_DATABASE_URL": "\t"}), "SENTINEL_DATABASE_URL")

	// Surrounding whitespace, as a .env line can leave, is dropped.
	cfg, err = loadConfig(env(with(dbBase, map[string]string{"SENTINEL_DATABASE_URL": "  " + leakURL + " \n"})))
	if err != nil || cfg.databaseURL != leakURL {
		t.Errorf("padded URL: stored %d bytes, %v; want the trimmed URL", len(cfg.databaseURL), err)
	}
}

func TestParseKeysRefresh(t *testing.T) {
	for in, want := range map[string]time.Duration{
		"":      30 * time.Second,
		"30s":   30 * time.Second,
		"1s":    time.Second,
		"1m30s": 90 * time.Second,
		" 2m ":  2 * time.Minute,
		"1h":    time.Hour,
	} {
		if got, err := parseKeysRefresh(in); err != nil || got != want {
			t.Errorf("parseKeysRefresh(%q) = %s, %v; want %s", in, got, err, want)
		}
	}
	for _, in := range []string{"0", "0s", "-30s", "30", "soon", "999ms", "1ns", "30 s"} {
		if got, err := parseKeysRefresh(in); err == nil {
			t.Errorf("parseKeysRefresh(%q) = %s, want an error", in, got)
		}
	}
}

func TestLoadConfig_KeysRefresh(t *testing.T) {
	cfg, err := loadConfig(env(with(dbBase, map[string]string{"SENTINEL_KEYS_REFRESH": "2m"})))
	if err != nil || cfg.keysRefresh != 2*time.Minute {
		t.Errorf("refresh 2m: %s, %v", cfg.keysRefresh, err)
	}
	for _, v := range []string{"0s", "-1m", "fast", "500ms"} {
		wantConfigErr(t, with(dbBase, map[string]string{"SENTINEL_KEYS_REFRESH": v}), "SENTINEL_KEYS_REFRESH")
	}
	// Without a database it does nothing, so it is only warned about.
	cfg, err = loadConfig(env(map[string]string{"ANTHROPIC_API_KEY": "sk", "SENTINEL_API_KEYS": "k", "SENTINEL_KEYS_REFRESH": "5m"}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.keysRefresh != 0 || len(cfg.keyWarnings) != 1 || !strings.HasPrefix(cfg.keyWarnings[0], "SENTINEL_KEYS_REFRESH is set") {
		t.Errorf("refresh without a database: %s, warnings %q", cfg.keysRefresh, cfg.keyWarnings)
	}
}

func TestParseKeysMaxStale(t *testing.T) {
	refresh := 30 * time.Second
	for in, want := range map[string]time.Duration{
		"":     10 * time.Minute,
		"0":    0,
		"0s":   0,
		"90s":  90 * time.Second, // three times the refresh
		" 1h ": time.Hour,
	} {
		if got, err := parseKeysMaxStale(in, refresh); err != nil || got != want {
			t.Errorf("parseKeysMaxStale(%q, 30s) = %s, %v; want %s", in, got, err, want)
		}
	}
	// Under three times the refresh, a single failed refresh could put the
	// gateway on 503s.
	for _, in := range []string{"-1m", "-0.5s", "10", "soon", "10 m", "29s", "30s", "59s", "1m", "89s"} {
		if got, err := parseKeysMaxStale(in, refresh); err == nil {
			t.Errorf("parseKeysMaxStale(%q, 30s) = %s, want an error", in, got)
		}
	}
	if _, err := parseKeysMaxStale("-1m", refresh); err == nil || !strings.Contains(err.Error(), "negative") {
		t.Errorf("parseKeysMaxStale(-1m) = %v, want it called negative", err)
	}
	// The default too: a refresh over 3m20s needs a max stale set.
	if got, err := parseKeysMaxStale("", 4*time.Minute); err == nil || !strings.Contains(err.Error(), "set SENTINEL_KEYS_MAX_STALE") {
		t.Errorf("default with a 4m refresh = %s, %v; want an error asking to set it", got, err)
	}
	if got, err := parseKeysMaxStale("", 200*time.Second); err != nil || got != 10*time.Minute {
		t.Errorf("default with a 3m20s refresh = %s, %v; want 10m, exactly three times", got, err)
	}
	if got, err := parseKeysMaxStale("0", 6*time.Minute); err != nil || got != 0 {
		t.Errorf("0 with a 6m refresh = %s, %v; want 0", got, err)
	}
}

func TestLoadConfig_KeysMaxStale(t *testing.T) {
	cfg, err := loadConfig(env(dbBase))
	if err != nil || cfg.keysMaxStale != 10*time.Minute {
		t.Errorf("default: %s, %v; want 10m", cfg.keysMaxStale, err)
	}
	cfg, err = loadConfig(env(with(dbBase, map[string]string{"SENTINEL_KEYS_MAX_STALE": "0"})))
	if err != nil || cfg.keysMaxStale != 0 {
		t.Errorf("0: %s, %v; want 0, never expire", cfg.keysMaxStale, err)
	}
	for _, v := range []string{"-1m", "45s", "never"} {
		wantConfigErr(t, with(dbBase, map[string]string{"SENTINEL_KEYS_MAX_STALE": v}), "SENTINEL_KEYS_MAX_STALE")
	}
	wantConfigErr(t, with(dbBase, map[string]string{"SENTINEL_KEYS_REFRESH": "10m"}), "SENTINEL_KEYS_MAX_STALE")
	// Without a database it does nothing, so it is only warned about.
	cfg, err = loadConfig(env(map[string]string{"ANTHROPIC_API_KEY": "sk", "SENTINEL_API_KEYS": "k", "SENTINEL_KEYS_MAX_STALE": "5m"}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.keysMaxStale != 0 || len(cfg.keyWarnings) != 1 || !strings.HasPrefix(cfg.keyWarnings[0], "SENTINEL_KEYS_MAX_STALE is set") {
		t.Errorf("max stale without a database: %s, warnings %q", cfg.keysMaxStale, cfg.keyWarnings)
	}
}

// While the key store can't be reached for longer than the max stale, the
// API answers 503 to every key, and serves again once it is back.
func TestServe_StaleKeysAre503(t *testing.T) {
	f := newFakeStore()
	var down atomic.Bool
	f.fetch = func(context.Context) ([][sha256.Size]byte, error) {
		if down.Load() {
			return nil, &store.Error{Op: "active digests"}
		}
		return [][sha256.Size]byte{sha256.Sum256([]byte("k"))}, nil
	}
	useStore(t, f, nil)
	r := startServeWith(t, dbVars(t, "postgres://fake"), func(cfg *config) {
		cfg.keysRefresh, cfg.keysMaxStale = 50*time.Millisecond, 300*time.Millisecond
	})
	waitStatus(t, r, "k", http.StatusOK)
	down.Store(true)
	waitStatus(t, r, "k", http.StatusServiceUnavailable)
	if code := apiStatus(t, r, "wrong"); code != http.StatusServiceUnavailable {
		t.Errorf("wrong key while stale: %d, want 503", code)
	}
	if v := staleGauge(t, r); v != "1" {
		t.Errorf("sentinel_auth_keys_stale while stale = %s, want 1", v)
	}
	down.Store(false)
	waitStatus(t, r, "k", http.StatusOK)
	if v := staleGauge(t, r); v != "0" {
		t.Errorf("sentinel_auth_keys_stale after recovery = %s, want 0", v)
	}
}

// staleGauge scrapes r's metrics and returns the value on the
// sentinel_auth_keys_stale line, failing unless there is exactly one.
func staleGauge(t *testing.T, r *running) string {
	t.Helper()
	_, body := get(t, http.MethodGet, "http://"+r.metrics+"/metrics", nil)
	var values []string
	for line := range strings.SplitSeq(body, "\n") {
		name, rest, _ := strings.Cut(line, "{")
		if name != "sentinel_auth_keys_stale" {
			continue
		}
		_, value, ok := strings.Cut(rest, "} ")
		if !ok {
			t.Fatalf("unreadable line %q", line)
		}
		values = append(values, value)
	}
	if len(values) != 1 {
		t.Fatalf("scrape has %d sentinel_auth_keys_stale lines, want 1", len(values))
	}
	return values[0]
}

func TestLoadConfig_AdminAddr(t *testing.T) {
	ok := []struct{ admin, api, metrics, want string }{
		{admin: "", want: "127.0.0.1:9465"},
		{admin: " 0.0.0.0:9465 ", want: "0.0.0.0:9465"},
		{admin: "[::1]:7000", want: "[::1]:7000"},
		{admin: ":9500", want: ":9500"},
		{admin: "127.0.0.1:0", api: "127.0.0.1:0", metrics: "127.0.0.1:0", want: "127.0.0.1:0"},
		// Metrics off frees their port.
		{admin: "127.0.0.1:9464", metrics: "off", want: "127.0.0.1:9464"},
		{admin: "off", want: ""},
	}
	for _, tt := range ok {
		vars := with(dbBase, map[string]string{"SENTINEL_ADMIN_ADDR": tt.admin, "SENTINEL_ADDR": tt.api, "SENTINEL_METRICS_ADDR": tt.metrics})
		cfg, err := loadConfig(env(vars))
		if err != nil || cfg.adminAddr != tt.want {
			t.Errorf("admin %q, api %q, metrics %q: adminAddr %q, %v; want %q", tt.admin, tt.api, tt.metrics, cfg.adminAddr, err, tt.want)
		}
	}
	bad := []struct{ admin, api, metrics string }{
		{admin: "localhost"},
		{admin: "OFF"},
		{admin: "127.0.0.1:admin"},
		{admin: "127.0.0.1:70000"},
		{admin: "http://127.0.0.1:9465"},
		// The API's port, on any host and in any spelling.
		{admin: "127.0.0.1:8081"},
		{admin: "0.0.0.0:08081"},
		{admin: "[::1]:9000", api: "0.0.0.0:9000"},
		{admin: "127.0.0.1:80", api: ":http"},
		// The metrics port, default or set.
		{admin: "127.0.0.1:9464"},
		{admin: "0.0.0.0:9464", metrics: "127.0.0.1:9464"},
		{admin: "127.0.0.1:9600", metrics: "[::]:9600"},
		// The default admin port, taken by metrics or the API.
		{admin: "", metrics: "0.0.0.0:9465"},
		{admin: "", api: "0.0.0.0:9465"},
	}
	for _, tt := range bad {
		vars := with(dbBase, map[string]string{"SENTINEL_ADMIN_ADDR": tt.admin, "SENTINEL_ADDR": tt.api, "SENTINEL_METRICS_ADDR": tt.metrics})
		_, err := loadConfig(env(vars))
		if err == nil || !strings.HasPrefix(err.Error(), "SENTINEL_ADMIN_ADDR") {
			t.Errorf("admin %q, api %q, metrics %q: %v, want an SENTINEL_ADMIN_ADDR error", tt.admin, tt.api, tt.metrics, err)
		}
	}

	// Without a database there is no admin API: an address is an error,
	// "off" says what is already so.
	noDB := map[string]string{"ANTHROPIC_API_KEY": "sk", "SENTINEL_API_KEYS": "k"}
	wantConfigErr(t, with(noDB, map[string]string{"SENTINEL_ADMIN_ADDR": "127.0.0.1:9465"}), "SENTINEL_ADMIN_ADDR")
	for _, v := range []string{"", "off", " off "} {
		if cfg, err := loadConfig(env(with(noDB, map[string]string{"SENTINEL_ADMIN_ADDR": v}))); err != nil || cfg.adminAddr != "" {
			t.Errorf("SENTINEL_ADMIN_ADDR=%q without a database: %q, %v", v, cfg.adminAddr, err)
		}
	}
}

func TestLoadConfig_AdminToken(t *testing.T) {
	for name, tok := range map[string]string{
		"unset":       "",
		"short":       shortToken,
		"31 chars":    strings.Repeat("a", 31),
		"inner space": "admin token for tests only 0123456789",
		"trailing nl": testToken + "\n",
		"non-ASCII":   testToken + "é",
	} {
		t.Run(name, func(t *testing.T) {
			wantConfigErr(t, with(dbBase, map[string]string{"SENTINEL_ADMIN_TOKEN": tok}), "SENTINEL_ADMIN_TOKEN")
		})
	}
	cfg, err := loadConfig(env(with(dbBase, map[string]string{"SENTINEL_ADMIN_TOKEN": strings.Repeat("x", 32)})))
	if err != nil || cfg.adminToken != strings.Repeat("x", 32) {
		t.Errorf("32-character token: %v", err)
	}

	// With the admin API off no token is needed, and one set is warned
	// about, as it is without a database.
	cfg, err = loadConfig(env(with(dbBase, map[string]string{"SENTINEL_ADMIN_ADDR": "off", "SENTINEL_ADMIN_TOKEN": ""})))
	if err != nil || cfg.adminToken != "" || cfg.keyWarnings != nil {
		t.Errorf("admin off, no token: %q, %v", cfg.keyWarnings, err)
	}
	cfg, err = loadConfig(env(with(dbBase, map[string]string{"SENTINEL_ADMIN_ADDR": "off"})))
	if err != nil || cfg.adminToken != "" || len(cfg.keyWarnings) != 1 || !strings.Contains(cfg.keyWarnings[0], "SENTINEL_ADMIN_ADDR is off") {
		t.Errorf("admin off with a token: %q, %v", cfg.keyWarnings, err)
	}
	cfg, err = loadConfig(env(map[string]string{"ANTHROPIC_API_KEY": "sk", "SENTINEL_API_KEYS": "k", "SENTINEL_ADMIN_TOKEN": testToken}))
	if err != nil || cfg.adminToken != "" || len(cfg.keyWarnings) != 1 ||
		!strings.Contains(cfg.keyWarnings[0], "SENTINEL_DATABASE_URL is not") || strings.Contains(cfg.keyWarnings[0], testToken) {
		t.Errorf("token without a database: %q, %v", cfg.keyWarnings, err)
	}
}

// No loadConfig error holds the database URL or any part of it, or the
// admin token, whichever variable is at fault.
func TestLoadConfig_KeyErrorsHoldNoSecrets(t *testing.T) {
	secrets := []string{leakUser, leakPass, leakHost, leakDB, testToken, shortToken}
	for name, extra := range map[string]map[string]string{
		"env keys too":        {"SENTINEL_API_KEYS": "k1"},
		"bad refresh":         {"SENTINEL_KEYS_REFRESH": "-1s"},
		"admin on API port":   {"SENTINEL_ADMIN_ADDR": "127.0.0.1:8081"},
		"admin on metrics":    {"SENTINEL_ADMIN_ADDR": "127.0.0.1:9464"},
		"bad admin addr":      {"SENTINEL_ADMIN_ADDR": "nowhere"},
		"no token":            {"SENTINEL_ADMIN_TOKEN": ""},
		"short token":         {"SENTINEL_ADMIN_TOKEN": shortToken},
		"token with a space":  {"SENTINEL_ADMIN_TOKEN": testToken + " x"},
		"bad metrics addr":    {"SENTINEL_METRICS_ADDR": "nowhere"},
		"no upstream key":     {"ANTHROPIC_API_KEY": ""},
		"bad redis URL":       {"SENTINEL_REDIS_URL": "http://" + leakHost},
		"blank database URL":  {"SENTINEL_DATABASE_URL": " "},
		"admin, no database":  {"SENTINEL_DATABASE_URL": "", "SENTINEL_API_KEYS": "k", "SENTINEL_ADMIN_ADDR": "127.0.0.1:9465"},
		"no keys, no URL":     {"SENTINEL_DATABASE_URL": ""},
		"URL as the API keys": {"SENTINEL_API_KEYS": leakURL},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := loadConfig(env(with(dbBase, extra)))
			if err == nil {
				t.Fatal("loadConfig succeeded")
			}
			for _, s := range secrets {
				if strings.Contains(err.Error(), s) {
					t.Errorf("error %q holds %q", err, s)
				}
			}
		})
	}
}

func TestLogUpstreams_KeyWarnings(t *testing.T) {
	cfg, err := loadConfig(env(map[string]string{
		"ANTHROPIC_API_KEY": "sk", "SENTINEL_API_KEYS": "k",
		"SENTINEL_KEYS_REFRESH": "1m", "SENTINEL_ADMIN_TOKEN": testToken,
	}))
	if err != nil {
		t.Fatal(err)
	}
	logs := &syncBuffer{}
	logUpstreams(slog.New(slog.NewJSONHandler(logs, nil)), cfg)
	var warned []string
	for _, line := range logs.lines(t) {
		if line["level"] == "WARN" {
			warned = append(warned, line["msg"].(string))
		}
	}
	if len(warned) != 2 || !strings.HasPrefix(warned[0], "SENTINEL_KEYS_REFRESH ") || !strings.HasPrefix(warned[1], "SENTINEL_ADMIN_TOKEN ") {
		t.Errorf("warnings %q, want one for each of SENTINEL_KEYS_REFRESH and SENTINEL_ADMIN_TOKEN", warned)
	}
	if strings.Contains(strings.Join(warned, " "), testToken) {
		t.Error("a warning holds the token")
	}
}

// fakeStore is an in-memory keyStore, to run serve's database path without
// Postgres. Its hooks, when set, replace a method.
type fakeStore struct {
	mu       sync.Mutex
	n        int
	tenants  map[string]*store.Tenant
	keys     map[string]*fakeKey
	migrated int
	closed   bool
	// afterClose counts calls made after Close, which must be none.
	afterClose int

	migrate      func(ctx context.Context) error
	fetch        func(ctx context.Context) ([][sha256.Size]byte, error)
	createTenant func(ctx context.Context) // runs before CreateTenant, unlocked

	// events records what happened, in order, for tests of the shutdown
	// order: "close" here, the rest from the hooks.
	events []string
}

// event appends to f's event log.
func (f *fakeStore) event(e string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, e)
}

// eventLog returns a copy of f's event log.
func (f *fakeStore) eventLog() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.events)
}

type fakeKey struct {
	store.Key
	digest [sha256.Size]byte
}

var _ keyStore = (*fakeStore)(nil)

func newFakeStore() *fakeStore {
	return &fakeStore{tenants: map[string]*store.Tenant{}, keys: map[string]*fakeKey{}}
}

// call locks f and counts a call after Close.
func (f *fakeStore) call() func() {
	f.mu.Lock()
	if f.closed {
		f.afterClose++
	}
	return f.mu.Unlock
}

func (f *fakeStore) id() string {
	f.n++
	return fmt.Sprintf("00000000-0000-4000-8000-%012d", f.n)
}

func (f *fakeStore) CreateTenant(ctx context.Context, name string) (store.Tenant, error) {
	f.mu.Lock()
	hook := f.createTenant
	f.mu.Unlock()
	if hook != nil {
		hook(ctx)
	}
	defer f.call()()
	t := &store.Tenant{ID: f.id(), Name: name, CreatedAt: time.Now()}
	f.tenants[t.ID] = t
	return *t, nil
}

func (f *fakeStore) ListTenants(context.Context) ([]store.Tenant, error) {
	defer f.call()()
	var ts []store.Tenant
	for _, t := range f.tenants {
		ts = append(ts, *t)
	}
	return ts, nil
}

func (f *fakeStore) DisableTenant(_ context.Context, id string) error {
	defer f.call()()
	t, ok := f.tenants[id]
	if !ok {
		return store.ErrNotFound
	}
	now := time.Now()
	t.DisabledAt = &now
	return nil
}

func (f *fakeStore) CreateKey(_ context.Context, tenantID string) (store.Key, string, error) {
	defer f.call()()
	if _, ok := f.tenants[tenantID]; !ok {
		return store.Key{}, "", store.ErrNotFound
	}
	key := "snl_" + rand.Text()
	k := &fakeKey{Key: store.Key{ID: f.id(), TenantID: tenantID, Prefix: key[:12], CreatedAt: time.Now()}, digest: sha256.Sum256([]byte(key))}
	f.keys[k.ID] = k
	return k.Key, key, nil
}

func (f *fakeStore) ListKeys(_ context.Context, tenantID string) ([]store.Key, error) {
	defer f.call()()
	var ks []store.Key
	for _, k := range f.keys {
		if k.TenantID == tenantID {
			ks = append(ks, k.Key)
		}
	}
	return ks, nil
}

func (f *fakeStore) RevokeKey(_ context.Context, id string) error {
	defer f.call()()
	k, ok := f.keys[id]
	if !ok {
		return store.ErrNotFound
	}
	now := time.Now()
	k.RevokedAt = &now
	return nil
}

func (f *fakeStore) Migrate(ctx context.Context) error {
	f.mu.Lock()
	f.migrated++
	hook := f.migrate
	f.mu.Unlock()
	if hook != nil {
		return hook(ctx)
	}
	return nil
}

func (f *fakeStore) SchemaVersion(context.Context) (int, error) { return 1, nil }

func (f *fakeStore) ActiveDigests(ctx context.Context) ([][sha256.Size]byte, error) {
	f.mu.Lock()
	hook := f.fetch
	f.mu.Unlock()
	if hook != nil {
		return hook(ctx)
	}
	defer f.call()()
	var ds [][sha256.Size]byte
	for _, k := range f.keys {
		if t := f.tenants[k.TenantID]; k.RevokedAt == nil && t.DisabledAt == nil {
			ds = append(ds, k.digest)
		}
	}
	return ds, nil
}

func (f *fakeStore) Close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	f.events = append(f.events, "close")
}

func (f *fakeStore) state() (migrated int, closed bool, afterClose int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.migrated, f.closed, f.afterClose
}

// useStore makes serve open f, or fail with openErr, and records the URL
// it was given.
func useStore(t *testing.T, f keyStore, openErr error) *string {
	t.Helper()
	var gotURL string
	useOpen(t, func(ctx context.Context, url string) (keyStore, error) {
		gotURL = url
		if openErr != nil {
			return nil, openErr
		}
		return f, nil
	})
	return &gotURL
}

// useOpen makes serve open the key store with open.
func useOpen(t *testing.T, open func(ctx context.Context, url string) (keyStore, error)) {
	t.Helper()
	prev := openStore
	t.Cleanup(func() { openStore = prev })
	openStore = open
}

// okUpstream answers every request with a 200, as a real upstream would a
// valid one.
func okUpstream(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"msg_1","type":"message","content":[]}`)
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

// dbVars are startServe's variables for a gateway on a key database, with
// the admin API on a free port and a working upstream.
func dbVars(t *testing.T, databaseURL string) map[string]string {
	return map[string]string{
		"ANTHROPIC_BASE_URL":    okUpstream(t),
		"SENTINEL_API_KEYS":     "",
		"SENTINEL_DATABASE_URL": databaseURL,
		"SENTINEL_ADMIN_ADDR":   "127.0.0.1:0",
		"SENTINEL_ADMIN_TOKEN":  testToken,
	}
}

// fastRefresh sets a refresh interval under the 1 s minimum, so tests
// don't wait for it.
func fastRefresh(cfg *config) { cfg.keysRefresh = 50 * time.Millisecond }

// adminDo sends an admin request with the token and decodes a JSON answer
// into out, if not nil.
func adminDo(t *testing.T, r *running, method, path string, body string, out any) int {
	t.Helper()
	req, err := http.NewRequest(method, "http://"+r.admin+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+testToken)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if out != nil {
		if err := json.Unmarshal(raw, out); err != nil {
			t.Fatalf("%s %s: %d %s: %v", method, path, resp.StatusCode, raw, err)
		}
	}
	return resp.StatusCode
}

// apiStatus sends a request on the API with key.
func apiStatus(t *testing.T, r *running, key string) int {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, "http://"+r.api+"/v1/messages", strings.NewReader(`{"model":"m","max_tokens":1,"messages":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Api-Key", key)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

// waitStatus retries the API with key until it answers want, which a
// refresh brings about. It fails after 10 s, many refresh intervals.
func waitStatus(t *testing.T, r *running, key string, want int) {
	t.Helper()
	var got int
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		if got = apiStatus(t, r, key); got == want {
			return
		}
	}
	t.Fatalf("API with the key: %d, want %d after a refresh", got, want)
}

// keyLifecycle creates a tenant and a key through the admin API, checks
// the key works on the API once a refresh has fetched it, revokes it, and
// checks it is rejected after the next refresh.
func keyLifecycle(t *testing.T, r *running) {
	t.Helper()
	var tenant struct{ ID string }
	if code := adminDo(t, r, http.MethodPost, "/admin/tenants", `{"name":"acme"}`, &tenant); code != http.StatusCreated || tenant.ID == "" {
		t.Fatalf("create tenant: %d %+v", code, tenant)
	}
	var key struct{ ID, Key string }
	if code := adminDo(t, r, http.MethodPost, "/admin/tenants/"+tenant.ID+"/keys", "", &key); code != http.StatusCreated || !strings.HasPrefix(key.Key, "snl_") {
		t.Fatalf("create key: %d, id %q", code, key.ID)
	}
	waitStatus(t, r, key.Key, http.StatusOK)

	if code := adminDo(t, r, http.MethodDelete, "/admin/keys/"+key.ID, "", nil); code != http.StatusNoContent {
		t.Fatalf("revoke key: %d", code)
	}
	waitStatus(t, r, key.Key, http.StatusUnauthorized)

	// The key reached no log line, and the token none either.
	for _, line := range r.logs.lines(t) {
		raw, _ := json.Marshal(line)
		if s := string(raw); strings.Contains(s, key.Key) || strings.Contains(s, testToken) {
			t.Errorf("log line holds the key or the token: %s", s)
		}
	}
}

// rawRequest writes a request line and headers to addr as is and returns
// the status, for requests net/http's client can't send, such as
// OPTIONS *.
func rawRequest(t *testing.T, addr, head string) int {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.WriteString(conn, head); err != nil {
		t.Fatal(err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

func TestServe_DatabaseKeys(t *testing.T) {
	f := newFakeStore()
	gotURL := useStore(t, f, nil)
	r := startServeWith(t, dbVars(t, leakURL), fastRefresh)

	if *gotURL != leakURL {
		t.Errorf("opened %q, want SENTINEL_DATABASE_URL", *gotURL)
	}
	if migrated, _, _ := f.state(); migrated != 1 {
		t.Errorf("Migrate called %d times, want 1", migrated)
	}
	if r.admin == "" || r.metrics == "" || r.admin == r.api || r.admin == r.metrics {
		t.Fatalf("admin %q, metrics %q, API %q: want three listeners", r.admin, r.metrics, r.api)
	}
	// The admin and metrics listeners bind before the API, so "listening"
	// still comes last.
	if i := slices.Index(r.msgsInOrder, "admin listening"); i < 0 || i > slices.Index(r.msgsInOrder, "listening") ||
		i < slices.Index(r.msgsInOrder, "metrics listening") {
		t.Errorf("startup log order %v: want metrics, then admin, then the API", r.msgsInOrder)
	}
	if !slices.Contains(r.msgsInOrder, "client keys from postgres") {
		t.Errorf("no client keys line in %v", r.msgsInOrder)
	}

	keyLifecycle(t, r)

	// Nothing of the admin API on the API port, nor the reverse.
	if code, _ := get(t, http.MethodGet, "http://"+r.api+"/admin/tenants", http.Header{"Authorization": {"Bearer " + testToken}}); code != http.StatusNotFound {
		t.Errorf("GET /admin/tenants on the API port: %d, want 404", code)
	}
	if code, _ := get(t, http.MethodGet, "http://"+r.admin+"/admin/tenants", nil); code != http.StatusUnauthorized {
		t.Errorf("GET /admin/tenants without the token: %d, want 401", code)
	}
	if code, _ := get(t, http.MethodPost, "http://"+r.admin+"/v1/messages", http.Header{"X-Api-Key": {"k"}}); code != http.StatusUnauthorized {
		t.Errorf("POST /v1/messages on the admin port: %d, want the admin 401", code)
	}
	// OPTIONS * reaches the handler too: its 401, not net/http's 200.
	if code := rawRequest(t, r.admin, "OPTIONS * HTTP/1.1\r\nHost: admin\r\nConnection: close\r\n\r\n"); code != http.StatusUnauthorized {
		t.Errorf("OPTIONS * on the admin port: %d, want 401", code)
	}

	// The key metrics come from the refreshing source.
	_, body := get(t, http.MethodGet, "http://"+r.metrics+"/metrics", nil)
	for _, want := range []string{"sentinel_auth_keys{", "sentinel_auth_key_refresh_age_seconds{"} {
		if !strings.Contains(body, want) {
			t.Errorf("scrape has no %s", want)
		}
	}

	r.stop()
	if err := <-r.done; err != nil {
		t.Errorf("serve after shutdown = %v", err)
	}
	r.done <- nil // for the cleanup
	if _, closed, afterClose := f.state(); !closed || afterClose != 0 {
		t.Errorf("store closed %v, %d calls after Close; want closed after the refresh and admin stopped", closed, afterClose)
	}
	if _, err := net.DialTimeout("tcp", r.admin, time.Second); err == nil {
		t.Error("admin listener still accepting after shutdown")
	}
}

func TestServe_AdminOff(t *testing.T) {
	useStore(t, newFakeStore(), nil)
	vars := dbVars(t, "postgres://fake")
	vars["SENTINEL_ADMIN_ADDR"] = "off"
	delete(vars, "SENTINEL_ADMIN_TOKEN")
	r := startServe(t, vars)
	if r.admin != "" {
		t.Errorf("admin listening on %s with SENTINEL_ADMIN_ADDR=off", r.admin)
	}
	if !slices.Contains(r.msgsInOrder, "admin API disabled: SENTINEL_ADMIN_ADDR is off") {
		t.Errorf("no admin disabled line in %v", r.msgsInOrder)
	}
}

// Without a database nothing is opened and there is no admin listener.
func TestServe_EnvKeysOpenNoStore(t *testing.T) {
	var opened bool
	prev := openStore
	t.Cleanup(func() { openStore = prev })
	openStore = func(context.Context, string) (keyStore, error) { opened = true; return nil, errors.New("unexpected") }
	r := startServe(t, nil)
	if opened || r.admin != "" || !slices.Contains(r.msgsInOrder, "client keys from SENTINEL_API_KEYS") {
		t.Errorf("opened %v, admin %q, log %v", opened, r.admin, r.msgsInOrder)
	}
	if code := apiStatus(t, r, "client-key"); code == http.StatusUnauthorized {
		t.Errorf("API with the env key: %d", code)
	}
}

// serveErr runs serve for vars until it returns, which it must before
// binding anything, and returns its log and error.
func serveErr(t *testing.T, vars map[string]string) (*syncBuffer, error) {
	t.Helper()
	cfg, err := loadConfig(env(with(map[string]string{
		"ANTHROPIC_API_KEY":     "sk",
		"SENTINEL_ADDR":         "127.0.0.1:0",
		"SENTINEL_METRICS_ADDR": "127.0.0.1:0",
		"SENTINEL_ADMIN_ADDR":   "127.0.0.1:0",
		"SENTINEL_ADMIN_TOKEN":  testToken,
	}, vars)))
	if err != nil {
		t.Fatal(err)
	}
	logs := &syncBuffer{}
	done := make(chan error, 1)
	go func() { done <- serve(context.Background(), slog.New(slog.NewJSONHandler(logs, nil)), cfg) }()
	select {
	case err = <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("serve kept running")
	}
	if err == nil {
		t.Fatal("serve = nil, want a startup error")
	}
	for _, line := range logs.lines(t) {
		if m, _ := line["msg"].(string); strings.HasSuffix(m, "listening") {
			t.Errorf("logged %q though startup failed", m)
		}
	}
	return logs, err
}

func TestServe_DatabaseStartupFailures(t *testing.T) {
	defer func(o, m time.Duration) { storeOpenTimeout, storeMigrateTimeout = o, m }(storeOpenTimeout, storeMigrateTimeout)
	storeOpenTimeout, storeMigrateTimeout = 100*time.Millisecond, 100*time.Millisecond
	hang := func(ctx context.Context) error { <-ctx.Done(); return &store.Error{Op: "x"} }
	timedOut := func(ctx context.Context) error {
		<-ctx.Done()
		return fmt.Errorf("store: migrate: timed out: %w", ctx.Err())
	}
	errDB := errors.New("store: migrate: postgres error (SQLSTATE 42501)")

	tests := []struct {
		name    string
		setup   func(f *fakeStore) (openErr error)
		want    string
		closed  bool
		timeout bool
	}{
		{name: "open fails", want: "SENTINEL_DATABASE_URL: store: open", setup: func(*fakeStore) error {
			return &store.Error{Op: "open"}
		}},
		{name: "migrate fails", want: "key store migration: " + errDB.Error(), closed: true, setup: func(f *fakeStore) error {
			f.migrate = func(context.Context) error { return errDB }
			return nil
		}},
		{name: "migrate too new", want: "key store migration: ", closed: true, setup: func(f *fakeStore) error {
			f.migrate = func(context.Context) error { return store.ErrSchemaTooNew }
			return nil
		}},
		{name: "migrate times out", want: "key store migration: ", closed: true, timeout: true, setup: func(f *fakeStore) error {
			f.migrate = timedOut
			return nil
		}},
		{name: "migrate hangs", want: "key store migration: ", closed: true, setup: func(f *fakeStore) error {
			f.migrate = hang
			return nil
		}},
		{name: "first fetch fails", want: "key store: keys: initial fetch", closed: true, setup: func(f *fakeStore) error {
			f.fetch = func(context.Context) ([][sha256.Size]byte, error) { return nil, &store.Error{Op: "active digests"} }
			return nil
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeStore()
			openErr := tt.setup(f)
			useStore(t, f, openErr)
			_, err := serveErr(t, map[string]string{"SENTINEL_DATABASE_URL": leakURL})
			if !strings.HasPrefix(err.Error(), tt.want) {
				t.Errorf("serve = %q, want it to start %q", err, tt.want)
			}
			if got := strings.Contains(err.Error(), "startup deadline"); got != tt.timeout {
				t.Errorf("serve = %q; names its startup deadline: %v, want %v", err, got, tt.timeout)
			}
			if _, closed, _ := f.state(); closed != tt.closed {
				t.Errorf("store closed: %v, want %v", closed, tt.closed)
			}
		})
	}
}

// The real store.Open, on a server that isn't there and on a URL that
// doesn't parse: no part of the URL reaches the error or the log.
func TestServe_DatabaseURLNotLeaked(t *testing.T) {
	for _, u := range []string{
		"postgres://" + leakUser + ":" + leakPass + "@127.0.0.1:1/" + leakDB + "?sslmode=disable",
		"postgres://" + leakUser + ":" + leakPass + "@" + leakHost + ":notaport/" + leakDB,
		"host=" + leakHost + " user=" + leakUser + " password=" + leakPass + " dbname=" + leakDB + " sslmode=bogus",
	} {
		logs, err := serveErr(t, map[string]string{"SENTINEL_DATABASE_URL": u})
		if !strings.HasPrefix(err.Error(), "SENTINEL_DATABASE_URL: store: open: ") {
			t.Errorf("serve = %q, want a store open error", err)
		}
		logs.mu.Lock()
		all := err.Error() + logs.buf.String()
		logs.mu.Unlock()
		for _, s := range []string{leakUser, leakPass, leakHost, leakDB, "127.0.0.1:1", testToken} {
			if strings.Contains(all, s) {
				t.Errorf("error or log holds %q: %s", s, all)
			}
		}
	}
}

func TestServe_AdminBindFailureStopsStartup(t *testing.T) {
	useStore(t, newFakeStore(), nil)
	taken, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer taken.Close()
	// Metrics off: their listener binds before the admin one, by design.
	_, err = serveErr(t, map[string]string{
		"SENTINEL_DATABASE_URL": "postgres://fake", "SENTINEL_ADMIN_ADDR": taken.Addr().String(), "SENTINEL_METRICS_ADDR": "off",
	})
	if !strings.HasPrefix(err.Error(), "SENTINEL_ADMIN_ADDR") {
		t.Errorf("serve = %v, want an error naming SENTINEL_ADMIN_ADDR", err)
	}
}

// An admin server that stops serving is logged, and the API is untouched.
func TestServe_AdminServeErrorKeepsAPI(t *testing.T) {
	useStore(t, newFakeStore(), nil)
	errAccept := errors.New("accept: broken socket")
	defer func(f func(string, string) (net.Listener, error)) { listenTCP = f }(listenTCP)
	listenTCP = func(network, addr string) (net.Listener, error) {
		ln, err := net.Listen(network, addr)
		if err != nil {
			return nil, err
		}
		return failingListener{ln, errAccept}, nil
	}
	vars := dbVars(t, "postgres://fake")
	vars["SENTINEL_METRICS_ADDR"] = "off" // only the admin listener fails
	r := startServe(t, vars)

	var logged map[string]any
	for deadline := time.Now().Add(5 * time.Second); logged == nil && time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		for _, line := range r.logs.lines(t) {
			if line["msg"] == "admin server stopped; the API keeps serving without the admin API" {
				logged = line
			}
		}
	}
	if logged == nil || logged["level"] != "ERROR" {
		t.Fatalf("admin server failure logged as %v, want ERROR", logged)
	}
	select {
	case err := <-r.done:
		t.Fatalf("serve returned %v after an admin failure, want it still serving", err)
	default:
	}
	if code := apiStatus(t, r, "wrong"); code != http.StatusUnauthorized {
		t.Errorf("API after the admin failure: %d, want 401", code)
	}
}

// The whole stack on a real Postgres: serve opens and migrates the
// database, a key made through the admin API works on the API after a
// refresh, and is rejected after it is revoked and a refresh runs. Only
// with SENTINEL_TEST_POSTGRES_URL, in a schema of its own.
func TestServe_PostgresKeyLifecycle(t *testing.T) {
	// 500 ms, not fastRefresh: each fetch gets half of it, and a real
	// fetch, or a reconnect after one timed out, needs more than 25 ms on
	// a loaded CI runner under -race.
	r := startServeWith(t, dbVars(t, testSchemaURL(t)), func(cfg *config) { cfg.keysRefresh = 500 * time.Millisecond })
	keyLifecycle(t, r)
}

// testSchemaURL returns SENTINEL_TEST_POSTGRES_URL for an empty schema of
// its own, dropped when the test ends, or skips the test when it is unset.
func testSchemaURL(t *testing.T) string {
	t.Helper()
	raw := os.Getenv("SENTINEL_TEST_POSTGRES_URL")
	if raw == "" {
		t.Skip("SENTINEL_TEST_POSTGRES_URL not set")
	}
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") {
		// Not the value: it holds a password.
		t.Fatal("SENTINEL_TEST_POSTGRES_URL must be a postgres:// URL")
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, raw)
	if err != nil {
		t.Fatal("connecting to SENTINEL_TEST_POSTGRES_URL failed") // pgx's error names the user and host
	}
	schema := "gw_" + strings.ToLower(rand.Text())
	if _, err := conn.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		conn.Close(ctx)
		t.Fatalf("creating schema %s failed", schema)
	}
	// Registered first, so it runs after startServe's cleanup has stopped
	// the gateway.
	t.Cleanup(func() {
		defer conn.Close(ctx)
		if _, err := conn.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Errorf("dropping test schema %s failed", schema)
		}
	})
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	return u.String()
}
