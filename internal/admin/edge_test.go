package admin_test

import (
	"bufio"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestPreflight_Unauthenticated(t *testing.T) {
	h, fs, _ := newAPI(t)
	hdr := http.Header{
		"Origin":                         {"http://evil.example"},
		"Access-Control-Request-Method":  {"POST"},
		"Access-Control-Request-Headers": {"authorization, content-type"},
	}
	rec := do(h, http.MethodOptions, "/admin/tenants", "", hdr)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status %d, want 401", rec.Code)
	}
	for k := range rec.Header() {
		if strings.HasPrefix(k, "Access-Control-") {
			t.Errorf("CORS header %s on a preflight", k)
		}
	}
	if fs.callCount() != 0 {
		t.Errorf("store called: %v", fs.calls)
	}
}

func TestEscapedIDs_ReachStoreAsValues(t *testing.T) {
	// ServeMux matches the escaped path and unescapes a wildcard's segment,
	// so these are IDs, not path syntax. The store finds none of them.
	tests := map[string]string{
		"/admin/keys/a%2Fb":    "a/b",
		"/admin/keys/a%2F":     "a/",
		"/admin/keys/%2e%2e":   "..",
		"/admin/keys/%2F..%2F": "/../",
	}
	for p, want := range tests {
		t.Run(p, func(t *testing.T) {
			h, fs, _ := newAPI(t)
			rec := do(h, http.MethodDelete, p, "", authed())
			if rec.Code != http.StatusNoContent {
				t.Fatalf("status %d: %s", rec.Code, rec.Body)
			}
			if len(fs.args) != 1 || fs.args[0] != want {
				t.Errorf("RevokeKey(%q), want %q", fs.args, want)
			}
		})
	}
}

func TestJSONFieldMatching(t *testing.T) {
	tests := []struct {
		name, body string
		status     int
		arg        string
	}{
		// encoding/json: names match ignoring case, the last duplicate wins.
		{"upper case", `{"NAME":"up"}`, 201, "up"},
		{"mixed case", `{"Name":"mixed"}`, 201, "mixed"},
		{"duplicate", `{"name":"a","name":"bbbb"}`, 201, "bbbb"},
		{"case-variant duplicate", `{"name":"a","Name":"bbbb"}`, 201, "bbbb"},
		// No name reaches the store as "", which refuses it.
		{"null", `null`, 400, ""},
		{"empty object", `{}`, 400, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, fs, _ := newAPI(t)
			rec := do(h, http.MethodPost, "/admin/tenants", tt.body, authed())
			if rec.Code != tt.status {
				t.Fatalf("status %d, want %d: %s", rec.Code, tt.status, rec.Body)
			}
			if len(fs.args) != 1 || fs.args[0] != tt.arg {
				t.Errorf("CreateTenant(%q), want %q", fs.args, tt.arg)
			}
		})
	}
}

func TestEmptyChunkedBody(t *testing.T) {
	h, fs, _ := newAPI(t)
	req := httptest.NewRequest(http.MethodPost, "/admin/tenants/"+tenantID+"/keys", strings.NewReader(""))
	req.ContentLength = -1 // chunked: length unknown until read
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	if fs.callCount() != 1 {
		t.Errorf("calls %v", fs.calls)
	}
}

func TestDelete_WithBody(t *testing.T) {
	tests := []struct {
		name, body string
		status     int
	}{
		{"json", `{}`, 400},
		{"text", "revoke", 400},
		{"too big", strings.Repeat("x", 5000), 413},
		{"whitespace only", " \n", 204},
		{"none", "", 204},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, fs, _ := newAPI(t)
			rec := do(h, http.MethodDelete, "/admin/keys/"+keyID, tt.body, authed())
			if rec.Code != tt.status {
				t.Fatalf("status %d, want %d: %s", rec.Code, tt.status, rec.Body)
			}
			if called := fs.callCount() == 1; called != (tt.status == 204) {
				t.Errorf("store calls %v for status %d", fs.calls, tt.status)
			}
		})
	}
}

// rawRequest sends req as written on a new connection to addr and returns
// a reader of the response, so a test controls the bytes net/http parses.
func rawRequest(t *testing.T, addr, req string) (net.Conn, *bufio.Reader) {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := c.Write([]byte(req)); err != nil {
		t.Fatal(err)
	}
	return c, bufio.NewReader(c)
}

func statusLine(t *testing.T, r *bufio.Reader) string {
	t.Helper()
	l, err := r.ReadString('\n')
	if err != nil {
		t.Fatalf("reading status line: %v", err)
	}
	return strings.TrimSpace(l)
}
