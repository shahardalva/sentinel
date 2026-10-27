package admin_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/shahardalva/sentinel/internal/admin"
	"github.com/shahardalva/sentinel/internal/store"
)

// *store.Store is what cmd/gateway will pass.
var _ admin.Store = (*store.Store)(nil)

const (
	token    = "test-admin-token-0123456789abcdefghij"
	tenantID = "11111111-2222-3333-4444-555555555555"
	keyID    = "66666666-7777-8888-9999-aaaaaaaaaaaa"
	// plainKey is what the fake store issues; it must appear in the create
	// response and nowhere else.
	plainKey  = "snl_PLAINTEXTKEYplaintextkeyPLAINTEXTKEY0123456"
	keyPrefix = "snl_PLAINTEX"
)

var created = time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC)

// fakeStore records its calls and returns err from every method when set.
type fakeStore struct {
	mu    sync.Mutex
	calls []string
	args  []string
	err   error
}

func (f *fakeStore) record(call, arg string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, call)
	f.args = append(f.args, arg)
	return f.err
}

func (f *fakeStore) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func (f *fakeStore) CreateTenant(_ context.Context, name string) (store.Tenant, error) {
	if err := f.record("CreateTenant", name); err != nil {
		return store.Tenant{}, err
	}
	return store.Tenant{ID: tenantID, Name: name, CreatedAt: created}, nil
}

func (f *fakeStore) ListTenants(context.Context) ([]store.Tenant, error) {
	if err := f.record("ListTenants", ""); err != nil {
		return nil, err
	}
	return []store.Tenant{{ID: tenantID, Name: "acme", CreatedAt: created, DisabledAt: &created}}, nil
}

func (f *fakeStore) DisableTenant(_ context.Context, id string) error {
	return f.record("DisableTenant", id)
}

func (f *fakeStore) CreateKey(_ context.Context, id string) (store.Key, string, error) {
	if err := f.record("CreateKey", id); err != nil {
		return store.Key{}, "", err
	}
	return store.Key{ID: keyID, TenantID: id, Prefix: keyPrefix, CreatedAt: created}, plainKey, nil
}

func (f *fakeStore) ListKeys(_ context.Context, id string) ([]store.Key, error) {
	if err := f.record("ListKeys", id); err != nil {
		return nil, err
	}
	return []store.Key{{ID: keyID, TenantID: id, Prefix: keyPrefix, CreatedAt: created}}, nil
}

func (f *fakeStore) RevokeKey(_ context.Context, id string) error {
	return f.record("RevokeKey", id)
}

// syncBuffer is a log destination safe for concurrent writes.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// lines returns every log record, decoded.
func (b *syncBuffer) lines(t *testing.T) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, l := range strings.Split(strings.TrimSpace(b.String()), "\n") {
		if l == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(l), &m); err != nil {
			t.Fatalf("log line %q: %v", l, err)
		}
		out = append(out, m)
	}
	return out
}

func newAPI(t *testing.T) (http.Handler, *fakeStore, *syncBuffer) {
	t.Helper()
	fs := &fakeStore{}
	logs := &syncBuffer{}
	h := admin.New(admin.Config{
		Store:  fs,
		Token:  token,
		Logger: slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
	})
	return h, fs, logs
}

type route struct {
	method, path, body string
}

// routes holds every route, plus paths and methods that match none, so
// the auth tests cover what the token guards in front of the mux too.
var routes = []route{
	{http.MethodPost, "/admin/tenants", `{"name":"acme"}`},
	{http.MethodGet, "/admin/tenants", ""},
	{http.MethodPost, "/admin/tenants/" + tenantID + "/disable", ""},
	{http.MethodPost, "/admin/tenants/" + tenantID + "/keys", ""},
	{http.MethodGet, "/admin/tenants/" + tenantID + "/keys", ""},
	{http.MethodDelete, "/admin/keys/" + keyID, ""},
	{http.MethodGet, "/admin/nope", ""},
	{http.MethodPut, "/admin/tenants", `{"name":"acme"}`},
	{http.MethodGet, "/admin//tenants", ""},
}

func do(h http.Handler, method, path, body string, header http.Header) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, vs := range header {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func authed() http.Header {
	return http.Header{"Authorization": {"Bearer " + token}}
}

func errorType(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var e struct {
		Error struct {
			Type    string `json:"type"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &e); err != nil {
		t.Fatalf("body %q is not the error shape: %v", rec.Body, err)
	}
	if e.Error.Type == "" || e.Error.Message == "" {
		t.Fatalf("body %q lacks type or message", rec.Body)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q", ct)
	}
	return e.Error.Type
}

var badAuth = map[string]http.Header{
	"none":                 nil,
	"empty header":         {"Authorization": {""}},
	"scheme only":          {"Authorization": {"Bearer"}},
	"empty bearer":         {"Authorization": {"Bearer "}},
	"wrong token":          {"Authorization": {"Bearer wrong-token-wrong-token-wrong-token"}},
	"token prefix":         {"Authorization": {"Bearer " + token[:len(token)-1]}},
	"token plus a char":    {"Authorization": {"Bearer " + token + "x"}},
	"trailing space":       {"Authorization": {"Bearer " + token + " "}},
	"two spaces":           {"Authorization": {"Bearer  " + token}},
	"basic scheme":         {"Authorization": {"Basic " + token}},
	"bare token":           {"Authorization": {token}},
	"x-api-key":            {"X-Api-Key": {token}},
	"two headers":          {"Authorization": {"Bearer " + token, "Bearer " + token}},
	"wrong then right":     {"Authorization": {"Bearer wrong", "Bearer " + token}},
	"bearer-like scheme":   {"Authorization": {"Bearerx " + token}},
	"token in other field": {"Proxy-Authorization": {"Bearer " + token}},
}

func TestAuth_RejectsEveryRouteAlike(t *testing.T) {
	h, fs, logs := newAPI(t)
	var first []byte
	for name, hdr := range badAuth {
		for _, rt := range routes {
			rec := do(h, rt.method, rt.path, rt.body, hdr)
			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("%s, %s %s: status %d, want 401", name, rt.method, rt.path, rec.Code)
			}
			if got := rec.Header().Get("WWW-Authenticate"); got != "Bearer" {
				t.Errorf("%s: WWW-Authenticate = %q", name, got)
			}
			if typ := errorType(t, rec); typ != "authentication_error" {
				t.Errorf("%s: type %q", name, typ)
			}
			if first == nil {
				first = rec.Body.Bytes()
			} else if !bytes.Equal(rec.Body.Bytes(), first) {
				t.Errorf("%s, %s %s: body %q differs from %q", name, rt.method, rt.path, rec.Body, first)
			}
		}
	}
	if n := fs.callCount(); n != 0 {
		t.Errorf("store called %d times without a valid token", n)
	}
	if strings.Contains(logs.String(), token) {
		t.Error("the token reached the log")
	}
}

func TestAuth_AcceptsBearerCaseInsensitively(t *testing.T) {
	h, _, _ := newAPI(t)
	for _, scheme := range []string{"Bearer", "bearer", "BEARER"} {
		rec := do(h, http.MethodGet, "/admin/tenants", "", http.Header{"Authorization": {scheme + " " + token}})
		if rec.Code != http.StatusOK {
			t.Errorf("%s: status %d", scheme, rec.Code)
		}
	}
}

// failReader fails the test if anything reads it.
type failReader struct{ read bool }

func (f *failReader) Read([]byte) (int, error) {
	f.read = true
	return 0, errors.New("body read")
}

func TestAuth_BeforeBodyIsRead(t *testing.T) {
	h, fs, _ := newAPI(t)
	for _, rt := range routes {
		body := &failReader{}
		req := httptest.NewRequest(rt.method, rt.path, body)
		req.Header.Set("Content-Type", "application/json")
		req.ContentLength = 1 << 20
		req.Header.Set("Authorization", "Bearer wrong")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("%s %s: status %d", rt.method, rt.path, rec.Code)
		}
		if body.read {
			t.Errorf("%s %s: body read before the token was checked", rt.method, rt.path)
		}
		if c := rec.Header().Get("Connection"); c != "close" {
			t.Errorf("%s %s: Connection = %q, want close", rt.method, rt.path, c)
		}
	}
	if n := fs.callCount(); n != 0 {
		t.Errorf("store called %d times", n)
	}
}

func TestAuth_WarnIsThrottled(t *testing.T) {
	h, _, logs := newAPI(t)
	for range 5 {
		do(h, http.MethodGet, "/admin/tenants", "", nil)
	}
	warns := 0
	for _, l := range logs.lines(t) {
		if l["msg"] == "admin unauthorized" {
			warns++
			if l["level"] != "WARN" {
				t.Errorf("level %v", l["level"])
			}
		}
	}
	if warns != 1 {
		t.Errorf("%d unauthorized warnings for 5 rejections within the interval, want 1", warns)
	}
}

func TestKeyAppearsOnlyInCreateResponse(t *testing.T) {
	h, _, logs := newAPI(t)
	var createBody string
	for _, rt := range routes {
		rec := do(h, rt.method, rt.path, rt.body, authed())
		if rt.method == http.MethodPost && strings.HasSuffix(rt.path, "/keys") {
			createBody = rec.Body.String()
			continue
		}
		if strings.Contains(rec.Body.String(), plainKey) {
			t.Errorf("%s %s response holds the key: %s", rt.method, rt.path, rec.Body)
		}
	}
	if !strings.Contains(createBody, plainKey) {
		t.Fatalf("create response %q lacks the key", createBody)
	}
	if out := logs.String(); strings.Contains(out, plainKey) || strings.Contains(out, token) {
		t.Errorf("log holds the key or the token:\n%s", out)
	}
	if !strings.Contains(logs.String(), keyPrefix) {
		t.Error("audit log lacks the key prefix")
	}
}

func TestCreateKey_Response(t *testing.T) {
	h, fs, _ := newAPI(t)
	rec := do(h, http.MethodPost, "/admin/tenants/"+tenantID+"/keys", "", authed())
	if rec.Code != http.StatusCreated {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	if cc := rec.Header().Get("Cache-Control"); cc != "no-store" {
		t.Errorf("Cache-Control = %q, want no-store", cc)
	}
	var got map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	want := map[string]any{
		"id": keyID, "tenant_id": tenantID, "prefix": keyPrefix,
		"created_at": created.Format(time.RFC3339), "key": plainKey,
	}
	if len(got) != len(want) {
		t.Errorf("fields %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %v, want %v", k, got[k], v)
		}
	}
	if fs.args[0] != tenantID {
		t.Errorf("CreateKey(%q)", fs.args[0])
	}
}

func TestEveryResponseIsNoStore(t *testing.T) {
	h, _, _ := newAPI(t)
	for _, hdr := range []http.Header{nil, authed()} {
		for _, rt := range routes {
			rec := do(h, rt.method, rt.path, rt.body, hdr)
			if cc := rec.Header().Get("Cache-Control"); cc != "no-store" {
				t.Errorf("%s %s (%d): Cache-Control = %q", rt.method, rt.path, rec.Code, cc)
			}
			if n := rec.Header().Get("X-Content-Type-Options"); n != "nosniff" {
				t.Errorf("%s %s: X-Content-Type-Options = %q", rt.method, rt.path, n)
			}
		}
	}
}

func TestRoutes(t *testing.T) {
	tests := []struct {
		route
		status   int
		call     string
		arg      string
		contains string
	}{
		{route{http.MethodPost, "/admin/tenants", `{"name":"acme"}`}, 201, "CreateTenant", "acme", `"name":"acme"`},
		{route{http.MethodGet, "/admin/tenants", ""}, 200, "ListTenants", "", `"tenants":[{"id":"` + tenantID},
		{route{http.MethodPost, "/admin/tenants/" + tenantID + "/disable", ""}, 204, "DisableTenant", tenantID, ""},
		{route{http.MethodPost, "/admin/tenants/" + tenantID + "/disable", "{}"}, 204, "DisableTenant", tenantID, ""},
		{route{http.MethodPost, "/admin/tenants/" + tenantID + "/keys", "{}"}, 201, "CreateKey", tenantID, `"key":"snl_`},
		{route{http.MethodGet, "/admin/tenants/" + tenantID + "/keys", ""}, 200, "ListKeys", tenantID, `"keys":[{"id":"` + keyID},
		{route{http.MethodDelete, "/admin/keys/" + keyID, ""}, 204, "RevokeKey", keyID, ""},
		{route{http.MethodHead, "/admin/tenants", ""}, 200, "ListTenants", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.method+" "+tt.path, func(t *testing.T) {
			h, fs, _ := newAPI(t)
			rec := do(h, tt.method, tt.path, tt.body, authed())
			if rec.Code != tt.status {
				t.Fatalf("status %d, want %d: %s", rec.Code, tt.status, rec.Body)
			}
			if len(fs.calls) != 1 || fs.calls[0] != tt.call || fs.args[0] != tt.arg {
				t.Errorf("calls %v %v, want %s(%q)", fs.calls, fs.args, tt.call, tt.arg)
			}
			if !strings.Contains(rec.Body.String(), tt.contains) {
				t.Errorf("body %q lacks %q", rec.Body, tt.contains)
			}
			if tt.status != 204 && tt.method != http.MethodHead {
				if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
					t.Errorf("Content-Type = %q", ct)
				}
			}
		})
	}
}

func TestListKeys_HasNoKeyField(t *testing.T) {
	h, _, _ := newAPI(t)
	rec := do(h, http.MethodGet, "/admin/tenants/"+tenantID+"/keys", "", authed())
	var got struct {
		Keys []map[string]any `json:"keys"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Keys) != 1 {
		t.Fatalf("keys %v", got.Keys)
	}
	if _, ok := got.Keys[0]["key"]; ok {
		t.Errorf("listed key has a key field: %v", got.Keys[0])
	}
	if _, ok := got.Keys[0]["revoked_at"]; !ok {
		t.Errorf("listed key lacks revoked_at: %v", got.Keys[0])
	}
}

// emptyStore lists nothing, to check empty lists encode as [].
type emptyStore struct{ fakeStore }

func (*emptyStore) ListTenants(context.Context) ([]store.Tenant, error)   { return nil, nil }
func (*emptyStore) ListKeys(context.Context, string) ([]store.Key, error) { return nil, nil }

func TestEmptyListsAreArrays(t *testing.T) {
	h := admin.New(admin.Config{Store: &emptyStore{}, Token: token, Logger: slog.New(slog.DiscardHandler)})
	for path, want := range map[string]string{
		"/admin/tenants":                       `{"tenants":[]}`,
		"/admin/tenants/" + tenantID + "/keys": `{"keys":[]}`,
	} {
		rec := do(h, http.MethodGet, path, "", authed())
		if got := strings.TrimSpace(rec.Body.String()); got != want {
			t.Errorf("%s: %s, want %s", path, got, want)
		}
	}
}

func TestUnmatched(t *testing.T) {
	tests := []struct {
		method, path string
		status       int
		typ, allow   string
	}{
		{http.MethodGet, "/", 404, "not_found_error", ""},
		{http.MethodGet, "/admin/nope", 404, "not_found_error", ""},
		{http.MethodGet, "/v1/messages", 404, "not_found_error", ""},
		{http.MethodGet, "/admin/tenants/" + tenantID, 404, "not_found_error", ""},
		{http.MethodPut, "/admin/tenants", 405, "method_not_allowed_error", "GET, HEAD, POST"},
		{http.MethodDelete, "/admin/tenants", 405, "method_not_allowed_error", "GET, HEAD, POST"},
		{http.MethodGet, "/admin/tenants/" + tenantID + "/disable", 405, "method_not_allowed_error", "POST"},
		{http.MethodDelete, "/admin/tenants/" + tenantID + "/keys", 405, "method_not_allowed_error", "GET, HEAD, POST"},
		{http.MethodGet, "/admin/keys/" + keyID, 405, "method_not_allowed_error", "DELETE"},
		{http.MethodGet, "/admin/tenants/", 400, "invalid_request_error", ""},
		{http.MethodGet, "/admin//tenants", 400, "invalid_request_error", ""},
		{http.MethodGet, "/admin/x/../tenants", 400, "invalid_request_error", ""},
		{http.MethodDelete, "/admin/keys/%2e%2e", 400, "invalid_request_error", ""},
	}
	for _, tt := range tests {
		t.Run(tt.method+" "+tt.path, func(t *testing.T) {
			h, fs, _ := newAPI(t)
			rec := do(h, tt.method, tt.path, "", authed())
			if rec.Code != tt.status {
				t.Fatalf("status %d, want %d", rec.Code, tt.status)
			}
			if typ := errorType(t, rec); typ != tt.typ {
				t.Errorf("type %q, want %q", typ, tt.typ)
			}
			if got := rec.Header().Get("Allow"); got != tt.allow {
				t.Errorf("Allow = %q, want %q", got, tt.allow)
			}
			if c := rec.Header().Get("Connection"); c != "close" {
				t.Errorf("Connection = %q, want close", c)
			}
			if fs.callCount() != 0 {
				t.Errorf("store called: %v", fs.calls)
			}
		})
	}
}

func TestBodyRules(t *testing.T) {
	tenants := "/admin/tenants"
	keys := "/admin/tenants/" + tenantID + "/keys"
	disable := "/admin/tenants/" + tenantID + "/disable"
	// fits is a valid body of exactly the limit.
	fits := `{"name":"acme"}` + strings.Repeat(" ", 4096-len(`{"name":"acme"}`))
	tests := []struct {
		name, path, ct, body string
		status               int
		typ                  string
	}{
		{"at the limit", tenants, "application/json", fits, 201, ""},
		{"over the limit", tenants, "application/json", fits + " ", 413, "request_too_large_error"},
		{"far over the limit", tenants, "application/json", `{"name":"` + strings.Repeat("a", 1<<20) + `"}`, 413, "request_too_large_error"},
		{"utf-8 charset", tenants, "application/json; charset=UTF-8", `{"name":"acme"}`, 201, ""},
		{"text/plain", tenants, "text/plain", `{"name":"acme"}`, 415, "unsupported_media_type_error"},
		{"form", tenants, "application/x-www-form-urlencoded", `name=acme`, 415, "unsupported_media_type_error"},
		{"no content type", tenants, "", `{"name":"acme"}`, 415, "unsupported_media_type_error"},
		{"other charset", tenants, "application/json; charset=latin1", `{"name":"acme"}`, 415, "unsupported_media_type_error"},
		{"json-like type", tenants, "application/jsonx", `{"name":"acme"}`, 415, "unsupported_media_type_error"},
		{"body on create key needs json", keys, "text/plain", `{}`, 415, "unsupported_media_type_error"},
		{"body on disable needs json", disable, "text/plain", `{}`, 415, "unsupported_media_type_error"},
		{"empty body", tenants, "application/json", "", 400, "invalid_request_error"},
		{"whitespace body", tenants, "application/json", "  \n", 400, "invalid_request_error"},
		{"invalid json", tenants, "application/json", `{"name":`, 400, "invalid_request_error"},
		{"unknown field", tenants, "application/json", `{"name":"acme","admin":true}`, 400, "invalid_request_error"},
		{"two values", tenants, "application/json", `{"name":"a"}{"name":"b"}`, 400, "invalid_request_error"},
		{"trailing garbage", tenants, "application/json", `{"name":"a"} x`, 400, "invalid_request_error"},
		{"array", tenants, "application/json", `[{"name":"a"}]`, 400, "invalid_request_error"},
		{"wrong type", tenants, "application/json", `{"name":7}`, 400, "invalid_request_error"},
		{"unknown field on create key", keys, "application/json", `{"tenant_id":"x"}`, 400, "invalid_request_error"},
		{"unknown field on disable", disable, "application/json", `{"x":1}`, 400, "invalid_request_error"},
		{"two values on create key", keys, "application/json", `{}{}`, 400, "invalid_request_error"},
		{"empty object on create key", keys, "application/json", `{}`, 201, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, fs, _ := newAPI(t)
			req := httptest.NewRequest(http.MethodPost, tt.path, strings.NewReader(tt.body))
			if tt.ct != "" {
				req.Header.Set("Content-Type", tt.ct)
			}
			req.Header.Set("Authorization", "Bearer "+token)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != tt.status {
				t.Fatalf("status %d, want %d: %s", rec.Code, tt.status, rec.Body)
			}
			if tt.typ == "" {
				return
			}
			if typ := errorType(t, rec); typ != tt.typ {
				t.Errorf("type %q, want %q", typ, tt.typ)
			}
			if n := fs.callCount(); n != 0 {
				t.Errorf("store called %d times for a rejected body", n)
			}
		})
	}
}

// secretErr reads like an unsanitized database error, to show none of it
// reaches a client.
var secretErr = errors.New("connect user=alice password=hunter2 host=db.internal")

func TestStoreErrorMapping(t *testing.T) {
	tests := []struct {
		err    error
		status int
		typ    string
	}{
		{store.ErrInvalidName, 400, "invalid_request_error"},
		{store.ErrNotFound, 404, "not_found_error"},
		{store.ErrDuplicateName, 409, "conflict_error"},
		{store.ErrTenantDisabled, 409, "conflict_error"},
		{&store.Error{Op: "create key", Code: "23505", Constraint: "api_keys_digest_key"}, 500, "internal_error"},
		{context.DeadlineExceeded, 500, "internal_error"},
		{secretErr, 500, "internal_error"},
	}
	for _, tt := range tests {
		for _, rt := range routes[:6] {
			t.Run(tt.err.Error()+" "+rt.method+" "+rt.path, func(t *testing.T) {
				h, fs, logs := newAPI(t)
				fs.err = tt.err
				rec := do(h, rt.method, rt.path, rt.body, authed())
				if rec.Code != tt.status {
					t.Fatalf("status %d, want %d", rec.Code, tt.status)
				}
				if typ := errorType(t, rec); typ != tt.typ {
					t.Errorf("type %q, want %q", typ, tt.typ)
				}
				if strings.Contains(rec.Body.String(), tt.err.Error()) || strings.Contains(rec.Body.String(), "store:") {
					t.Errorf("response holds the store's error: %s", rec.Body)
				}
				for _, l := range logs.lines(t) {
					if l["msg"] == "admin" {
						t.Errorf("audit line for a failed change: %v", l)
					}
				}
				if tt.status == 500 {
					if body := rec.Body.String(); strings.Contains(body, "hunter2") || strings.Contains(body, "SQLSTATE") || strings.Contains(body, "alice") {
						t.Errorf("500 leaks: %s", body)
					}
					if !strings.Contains(logs.String(), tt.err.Error()) {
						t.Errorf("log lacks the store's error:\n%s", logs)
					}
				}
			})
		}
	}
}

func TestAuditLines(t *testing.T) {
	tests := []struct {
		route
		action string
		want   map[string]string
	}{
		{routes[0], "create_tenant", map[string]string{"tenant_id": tenantID}},
		{routes[2], "disable_tenant", map[string]string{"tenant_id": tenantID}},
		{routes[3], "create_key", map[string]string{"tenant_id": tenantID, "key_id": keyID, "key_prefix": keyPrefix}},
		{routes[5], "revoke_key", map[string]string{"key_id": keyID}},
		{routes[1], "", nil},
		{routes[4], "", nil},
	}
	for _, tt := range tests {
		t.Run(tt.method+" "+tt.path, func(t *testing.T) {
			h, _, logs := newAPI(t)
			if rec := do(h, tt.method, tt.path, tt.body, authed()); rec.Code >= 300 {
				t.Fatalf("status %d", rec.Code)
			}
			var audits []map[string]any
			for _, l := range logs.lines(t) {
				if l["msg"] == "admin" {
					audits = append(audits, l)
				}
			}
			if tt.action == "" {
				if len(audits) != 0 {
					t.Errorf("audit line for a read: %v", audits)
				}
				return
			}
			if len(audits) != 1 {
				t.Fatalf("%d audit lines, want 1", len(audits))
			}
			a := audits[0]
			if a["level"] != "INFO" || a["action"] != tt.action || a["actor"] != "admin-token" {
				t.Errorf("audit line %v", a)
			}
			for k, v := range tt.want {
				if a[k] != v {
					t.Errorf("%s = %v, want %s", k, a[k], v)
				}
			}
			for k, v := range a {
				if s, _ := v.(string); strings.Contains(s, plainKey) || strings.Contains(s, token) {
					t.Errorf("audit field %s holds a secret", k)
				}
			}
		})
	}
}

func TestValidateToken(t *testing.T) {
	ok := strings.Repeat("a", 32)
	tests := []struct {
		name, token string
		valid       bool
	}{
		{"32 chars", ok, true},
		{"long, mixed", "Zx9-_.~!$&'()*+,;=:@/?" + ok, true},
		{"empty", "", false},
		{"31 chars", ok[:31], false},
		{"space", ok[:16] + " " + ok[16:], false},
		{"leading space", " " + ok, false},
		{"trailing newline", ok + "\n", false},
		{"tab", ok + "\t", false},
		{"nul", ok + "\x00", false},
		{"del", ok + "\x7f", false},
		{"non-ascii", ok + "é", false},
		{"non-breaking space", ok + " ", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := admin.ValidateToken(tt.token)
			if (err == nil) != tt.valid {
				t.Fatalf("ValidateToken = %v, want valid %v", err, tt.valid)
			}
			if err != nil && tt.token != "" && strings.Contains(err.Error(), tt.token) {
				t.Errorf("error %q holds the token", err)
			}
		})
	}
}

func TestNew_Panics(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	short := "short-secret-token"
	tests := map[string]admin.Config{
		"no store":  {Token: token, Logger: logger},
		"no logger": {Store: &fakeStore{}, Token: token},
		"bad token": {Store: &fakeStore{}, Token: short, Logger: logger},
	}
	for name, cfg := range tests {
		t.Run(name, func(t *testing.T) {
			defer func() {
				r := recover()
				if r == nil {
					t.Fatal("no panic")
				}
				if msg, _ := r.(string); strings.Contains(msg, short) || strings.Contains(msg, token) {
					t.Errorf("panic %q holds the token", msg)
				}
			}()
			admin.New(cfg)
		})
	}
}

// TestOverHTTP runs the API on a real server, where net/http adds its own
// behavior: an unread body on a 401, and the 413's connection close.
func TestOverHTTP(t *testing.T) {
	h, fs, _ := newAPI(t)
	srv := httptest.NewServer(h)
	defer srv.Close()

	post := func(auth, body string) *http.Response {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, srv.URL+"/admin/tenants", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		return resp
	}
	if resp := post("", `{"name":"acme"}`); resp.StatusCode != 401 || resp.Header.Get("WWW-Authenticate") != "Bearer" {
		t.Errorf("no token: %d %v", resp.StatusCode, resp.Header)
	}
	if resp := post("Bearer "+token, strings.Repeat(" ", 8<<10)); resp.StatusCode != 413 {
		t.Errorf("big body: %d", resp.StatusCode)
	}
	if resp := post("Bearer "+token, `{"name":"acme"}`); resp.StatusCode != 201 {
		t.Errorf("valid: %d", resp.StatusCode)
	}
	if n := fs.callCount(); n != 1 {
		t.Errorf("store called %d times, want 1", n)
	}
}
