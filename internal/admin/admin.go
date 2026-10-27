// Package admin is the HTTP API that manages tenants and their client keys
// in the key store: create and list tenants, disable one, issue, list and
// revoke keys. Every request needs the admin token as a bearer token. It is
// meant for its own listener, never the API port (ADR 0005).
//
// The plaintext key leaves the store once, in the response to a key's
// creation; it is never logged and no other response holds it. Every
// successful change is logged at INFO as "admin", without the key or the
// token.
package admin

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/shahardalva/sentinel/internal/proxy"
	"github.com/shahardalva/sentinel/internal/store"
)

// maxBodyBytes bounds a request body. The largest valid one is a tenant
// name of at most 200 bytes, escaped.
const maxBodyBytes = 4 << 10

// actor names who made a change in the audit log. There is one shared
// token, so it says no more than that.
const actor = "admin-token"

// Store is what the admin API needs of the key store; *store.Store
// satisfies it. Its errors must be safe to log.
type Store interface {
	CreateTenant(ctx context.Context, name string) (store.Tenant, error)
	ListTenants(ctx context.Context) ([]store.Tenant, error)
	DisableTenant(ctx context.Context, id string) error
	CreateKey(ctx context.Context, tenantID string) (store.Key, string, error)
	ListKeys(ctx context.Context, tenantID string) ([]store.Key, error)
	RevokeKey(ctx context.Context, id string) error
}

// Config configures New.
type Config struct {
	Store Store
	// Token is the admin token clients send as "Authorization: Bearer
	// <token>". It must pass ValidateToken. Only its SHA-256 digest is
	// kept.
	Token  string
	Logger *slog.Logger
}

type handler struct {
	store  Store
	token  [sha256.Size]byte
	logger *slog.Logger
	warn   proxy.LogThrottle
	now    func() time.Time // time.Now; tests replace it
}

// New returns the admin API. Its routes, JSON in and out:
//
//	POST   /admin/tenants              {"name"}  201 tenant
//	GET    /admin/tenants                        200 {"tenants": [...]}
//	POST   /admin/tenants/{id}/disable           204
//	POST   /admin/tenants/{id}/keys              201 key with "key", once
//	GET    /admin/tenants/{id}/keys              200 {"keys": [...]}
//	DELETE /admin/keys/{id}                      204
//
// The token is checked first, for every path, so without it every request
// gets the same 401 and nothing else is learned. Errors are
// {"error":{"type","message"}}. New panics if cfg has no Store or Logger
// or its Token fails ValidateToken; validate the token at startup first.
func New(cfg Config) http.Handler {
	if cfg.Store == nil || cfg.Logger == nil {
		panic("admin: New needs a Store and a Logger")
	}
	if err := ValidateToken(cfg.Token); err != nil {
		panic("admin: " + err.Error())
	}
	h := &handler{
		store:  cfg.Store,
		token:  sha256.Sum256([]byte(cfg.Token)),
		logger: cfg.Logger,
		now:    time.Now,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /admin/tenants", h.createTenant)
	mux.HandleFunc("GET /admin/tenants", h.listTenants)
	mux.HandleFunc("POST /admin/tenants/{id}/disable", h.disableTenant)
	mux.HandleFunc("POST /admin/tenants/{id}/keys", h.createKey)
	mux.HandleFunc("GET /admin/tenants/{id}/keys", h.listKeys)
	mux.HandleFunc("DELETE /admin/keys/{id}", h.revokeKey)
	// The method-less patterns catch every other method on a route, so
	// the 405 is JSON like the rest; "/" catches every other path.
	mux.Handle("/admin/tenants", methodNotAllowed("GET, HEAD, POST"))
	mux.Handle("/admin/tenants/{id}/disable", methodNotAllowed("POST"))
	mux.Handle("/admin/tenants/{id}/keys", methodNotAllowed("GET, HEAD, POST"))
	mux.Handle("/admin/keys/{id}", methodNotAllowed("DELETE"))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		rejectEarly(w, http.StatusNotFound, errNotFound, "no such route")
	})
	return commonHeaders(h.requireToken(requireCleanPath(mux)))
}

func (h *handler) createTenant(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Name string `json:"name"`
	}
	if !decodeBody(w, r, &in, true) {
		return
	}
	t, err := h.store.CreateTenant(r.Context(), in.Name)
	if err != nil {
		h.storeError(w, r, "create_tenant", err)
		return
	}
	h.audit(r, "create_tenant", slog.String("tenant_id", t.ID))
	writeJSON(w, http.StatusCreated, tenantJSON(t))
}

func (h *handler) listTenants(w http.ResponseWriter, r *http.Request) {
	ts, err := h.store.ListTenants(r.Context())
	if err != nil {
		h.storeError(w, r, "list_tenants", err)
		return
	}
	out := struct {
		Tenants []tenant `json:"tenants"`
	}{Tenants: make([]tenant, 0, len(ts))}
	for _, t := range ts {
		out.Tenants = append(out.Tenants, tenantJSON(t))
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *handler) disableTenant(w http.ResponseWriter, r *http.Request) {
	if !decodeBody(w, r, &struct{}{}, false) {
		return
	}
	id := r.PathValue("id")
	if err := h.store.DisableTenant(r.Context(), id); err != nil {
		h.storeError(w, r, "disable_tenant", err)
		return
	}
	h.audit(r, "disable_tenant", slog.String("tenant_id", id))
	w.WriteHeader(http.StatusNoContent)
}

func (h *handler) createKey(w http.ResponseWriter, r *http.Request) {
	if !decodeBody(w, r, &struct{}{}, false) {
		return
	}
	k, key, err := h.store.CreateKey(r.Context(), r.PathValue("id"))
	if err != nil {
		h.storeError(w, r, "create_key", err)
		return
	}
	h.audit(r, "create_key",
		slog.String("tenant_id", k.TenantID),
		slog.String("key_id", k.ID),
		slog.String("key_prefix", k.Prefix),
	)
	// The only response that holds a key. commonHeaders already set
	// Cache-Control: no-store; set again so this one can't lose it.
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusCreated, struct {
		ID        string    `json:"id"`
		TenantID  string    `json:"tenant_id"`
		Prefix    string    `json:"prefix"`
		CreatedAt time.Time `json:"created_at"`
		Key       string    `json:"key"`
	}{k.ID, k.TenantID, k.Prefix, k.CreatedAt, key})
}

func (h *handler) listKeys(w http.ResponseWriter, r *http.Request) {
	ks, err := h.store.ListKeys(r.Context(), r.PathValue("id"))
	if err != nil {
		h.storeError(w, r, "list_keys", err)
		return
	}
	out := struct {
		Keys []keyMeta `json:"keys"`
	}{Keys: make([]keyMeta, 0, len(ks))}
	for _, k := range ks {
		out.Keys = append(out.Keys, keyMeta{k.ID, k.TenantID, k.Prefix, k.CreatedAt, k.RevokedAt})
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *handler) revokeKey(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := h.store.RevokeKey(r.Context(), id); err != nil {
		h.storeError(w, r, "revoke_key", err)
		return
	}
	h.audit(r, "revoke_key", slog.String("key_id", id))
	w.WriteHeader(http.StatusNoContent)
}

// tenant is a tenant as the API shows it.
type tenant struct {
	ID         string     `json:"id"`
	Name       string     `json:"name"`
	CreatedAt  time.Time  `json:"created_at"`
	DisabledAt *time.Time `json:"disabled_at"`
}

func tenantJSON(t store.Tenant) tenant {
	return tenant{t.ID, t.Name, t.CreatedAt, t.DisabledAt}
}

// keyMeta is a key's metadata as the API lists it; it has no field for the
// key itself.
type keyMeta struct {
	ID        string     `json:"id"`
	TenantID  string     `json:"tenant_id"`
	Prefix    string     `json:"prefix"`
	CreatedAt time.Time  `json:"created_at"`
	RevokedAt *time.Time `json:"revoked_at"`
}

// audit logs a successful change. attrs name what changed; never pass the
// key or the token.
func (h *handler) audit(r *http.Request, action string, attrs ...slog.Attr) {
	attrs = append([]slog.Attr{
		slog.String("action", action),
		slog.String("actor", actor),
		slog.String("remote_addr", r.RemoteAddr),
	}, attrs...)
	h.logger.LogAttrs(r.Context(), slog.LevelInfo, "admin", attrs...)
}

// storeError answers a store error. The typed errors map to their status;
// anything else is a 500 whose body says nothing about the error, which
// goes to the log only (the store's errors are sanitized).
func (h *handler) storeError(w http.ResponseWriter, r *http.Request, action string, err error) {
	switch {
	case errors.Is(err, store.ErrInvalidName):
		writeError(w, http.StatusBadRequest, errInvalidRequest, "invalid tenant name")
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, errNotFound, "not found")
	case errors.Is(err, store.ErrDuplicateName):
		writeError(w, http.StatusConflict, errConflict, "a tenant with that name exists")
	case errors.Is(err, store.ErrTenantDisabled):
		writeError(w, http.StatusConflict, errConflict, "tenant is disabled")
	default:
		h.logger.LogAttrs(r.Context(), slog.LevelError, "admin store error",
			slog.String("action", action),
			slog.String("error", err.Error()),
		)
		writeError(w, http.StatusInternalServerError, errInternal, "internal error")
	}
}

// decodeBody reads a JSON body into v: at most maxBodyBytes (413), sent
// as application/json (415), one JSON value with no unknown fields (400).
// An empty body is a 400 if required, else leaves v alone. It reports
// whether to go on; if not, it has answered.
func decodeBody(w http.ResponseWriter, r *http.Request, v any, required bool) bool {
	if r.ContentLength != 0 && !isJSON(r.Header.Get("Content-Type")) {
		rejectEarly(w, http.StatusUnsupportedMediaType, errUnsupportedMediaType, "body must be application/json")
		return false
	}
	b, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			// MaxBytesReader has told the server to close the connection.
			writeError(w, http.StatusRequestEntityTooLarge, errTooLarge, "body is larger than 4 KiB")
			return false
		}
		writeError(w, http.StatusBadRequest, errInvalidRequest, "cannot read body")
		return false
	}
	if len(bytes.TrimSpace(b)) == 0 {
		if required {
			writeError(w, http.StatusBadRequest, errInvalidRequest, "body is required")
			return false
		}
		return true
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		writeError(w, http.StatusBadRequest, errInvalidRequest, "body must be a JSON object with only the documented fields")
		return false
	}
	if err := dec.Decode(&json.RawMessage{}); err != io.EOF {
		writeError(w, http.StatusBadRequest, errInvalidRequest, "body must be exactly one JSON value")
		return false
	}
	return true
}

// isJSON reports whether a Content-Type is application/json, with no
// charset or a UTF-8 one.
func isJSON(ct string) bool {
	mt, params, err := mime.ParseMediaType(ct)
	if err != nil || mt != "application/json" {
		return false
	}
	cs, ok := params["charset"]
	return !ok || strings.EqualFold(cs, "utf-8")
}

// commonHeaders sets the headers every admin response carries: none is to
// be cached or sniffed as anything but what it says.
func commonHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		next.ServeHTTP(w, r)
	})
}

// requireCleanPath answers a path that isn't already clean (a double
// slash, a dot segment, a trailing slash) with a 400, as the gateway does.
// ServeMux would redirect it to the cleaned path instead.
func requireCleanPath(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != path.Clean(r.URL.Path) {
			rejectEarly(w, http.StatusBadRequest, errInvalidRequest, "request path is not clean")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func methodNotAllowed(allow string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Allow", allow)
		rejectEarly(w, http.StatusMethodNotAllowed, errMethodNotAllowed, "method not allowed")
	})
}

// Error types, the "type" of an error response.
const (
	errAuthentication       = "authentication_error"
	errInvalidRequest       = "invalid_request_error"
	errNotFound             = "not_found_error"
	errMethodNotAllowed     = "method_not_allowed_error"
	errConflict             = "conflict_error"
	errTooLarge             = "request_too_large_error"
	errUnsupportedMediaType = "unsupported_media_type_error"
	errInternal             = "internal_error"
)

// rejectEarly answers a request whose body is left unread: Connection:
// close, and a read deadline so net/http's drain of the body can't hang
// (see proxy.RejectDrainTimeout).
func rejectEarly(w http.ResponseWriter, status int, typ, msg string) {
	_ = http.NewResponseController(w).SetReadDeadline(time.Now().Add(proxy.RejectDrainTimeout))
	w.Header().Set("Connection", "close")
	writeError(w, status, typ, msg)
}

func writeError(w http.ResponseWriter, status int, typ, msg string) {
	type detail struct {
		Type    string `json:"type"`
		Message string `json:"message"`
	}
	writeJSON(w, status, struct {
		Error detail `json:"error"`
	}{detail{typ, msg}})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		// Only the types above are encoded; none can fail.
		status, b = http.StatusInternalServerError, []byte(`{"error":{"type":"internal_error","message":"internal error"}}`)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(append(b, '\n'))
}
