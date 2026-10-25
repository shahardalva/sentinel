package upstream

import (
	"context"
	"errors"
	"io"
	"regexp"
	"strings"
	"time"
)

// MaxErrorBody bounds how much of an upstream error body is read. Real
// provider errors are a few hundred bytes; anything this large is not one.
const MaxErrorBody = 64 << 10

// ErrorBodyTimeout bounds reading an upstream error body. The request's
// header timeout is still running then, but it may have minutes left; a
// real error body arrives with its headers.
const ErrorBodyTimeout = 10 * time.Second

var (
	// ErrErrorBodyTooLarge is returned by ReadErrorBody for a body over
	// MaxErrorBody.
	ErrErrorBodyTooLarge = errors.New("upstream error body over the limit")
	// ErrErrorBodyTimeout is returned by ReadErrorBody when the body takes
	// longer than ErrorBodyTimeout.
	ErrErrorBodyTimeout = errors.New("upstream error body not received in time")
)

// ReadErrorBody reads body to the end, at most MaxErrorBody bytes, and closes
// it. It returns ErrErrorBodyTooLarge, and no bytes, for a longer body, and
// ErrErrorBodyTimeout after ErrorBodyTimeout. When ctx ends first, it
// returns ctx's cause.
//
// The body is closed as soon as ctx ends or the timeout passes, so the read
// can't outlive its request even when cancelling the request doesn't stop
// it, as with the raw connection a 101 response hands over.
func ReadErrorBody(ctx context.Context, body io.ReadCloser) ([]byte, error) {
	ctx, cancel := context.WithTimeoutCause(ctx, ErrorBodyTimeout, ErrErrorBodyTimeout)
	defer cancel()
	stop := context.AfterFunc(ctx, func() { body.Close() })
	defer stop()
	defer body.Close()
	b, err := io.ReadAll(io.LimitReader(body, MaxErrorBody+1))
	if err != nil {
		// A read cut short by the close above reports a closed body; the
		// cause says why.
		if cause := context.Cause(ctx); cause != nil {
			return nil, cause
		}
		return nil, err
	}
	if len(b) > MaxErrorBody {
		return nil, ErrErrorBodyTooLarge
	}
	return b, nil
}

// Account identifier patterns, all case-insensitive. Each prefix must not
// follow a letter, so "cyborg-..." and "task-..." are left alone, but it may
// follow "_" or a digit, unlike with \b. Group 1 is that preceding
// character, which the replacement keeps.
var (
	// API keys: OpenAI sk-... and sk-proj-..., Anthropic sk-ant-....
	apiKey = regexp.MustCompile(`(?i)(^|[^a-z])sk-[a-z0-9_-]{8,}`)
	// OpenAI organization and project IDs.
	orgID     = regexp.MustCompile(`(?i)(^|[^a-z])org-[a-z0-9]{8,}`)
	projectID = regexp.MustCompile(`(?i)(^|[^a-z])proj_[a-z0-9]{8,}`)
	// Anthropic workspace and API key IDs.
	anthropicID = regexp.MustCompile(`(?i)(^|[^a-z])(wrkspc|apikey)_[a-z0-9]{8,}`)
	// Any UUID: Anthropic names organizations by UUID, and other providers
	// use them for accounts and keys too.
	uuid = regexp.MustCompile(`(?i)[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`)
	// A UUID without hyphens: a run of 32 or more hex digits. A request ID
	// (req_...) is matched whole by the first alternative and kept, since
	// OpenAI's are req_ and 32 hex digits.
	hexRun = regexp.MustCompile(`(?i)req_[a-z0-9]+|[0-9a-f]{32,}`)
)

// RedactAccountIDs replaces the account identifiers and keys an upstream
// error message may hold, which would tell any client whose account the
// gateway uses: API keys become sk-[redacted], OpenAI organization IDs
// org-[redacted], project IDs proj_[redacted], Anthropic workspace and API
// key IDs wrkspc_[redacted] and apikey_[redacted], and any UUID, with or
// without hyphens, [redacted]. Request IDs (req_...) are kept; they identify
// one request, and providers' support asks for them. Matching is by pattern
// only; an identifier in another form passes.
func RedactAccountIDs(s string) string {
	s = apiKey.ReplaceAllString(s, "${1}sk-[redacted]")
	s = orgID.ReplaceAllString(s, "${1}org-[redacted]")
	s = projectID.ReplaceAllString(s, "${1}proj_[redacted]")
	s = anthropicID.ReplaceAllString(s, "${1}${2}_[redacted]")
	s = uuid.ReplaceAllLiteralString(s, "[redacted]")
	return hexRun.ReplaceAllStringFunc(s, func(m string) string {
		if len(m) > 4 && strings.EqualFold(m[:4], "req_") {
			return m
		}
		return "[redacted]"
	})
}
