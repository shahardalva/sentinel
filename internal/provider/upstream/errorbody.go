package upstream

import (
	"errors"
	"io"
	"regexp"
)

// MaxErrorBody bounds how much of an upstream error body is read. Real
// provider errors are a few hundred bytes; anything this large is not one.
const MaxErrorBody = 64 << 10

// ErrErrorBodyTooLarge is returned by ReadErrorBody for a body over
// MaxErrorBody.
var ErrErrorBodyTooLarge = errors.New("upstream error body over the limit")

// ReadErrorBody reads body to the end, at most MaxErrorBody bytes, and closes
// it. It returns ErrErrorBodyTooLarge, and no bytes, for a longer body. It
// has no deadline of its own; the request's context bounds the read.
func ReadErrorBody(body io.ReadCloser) ([]byte, error) {
	defer body.Close()
	b, err := io.ReadAll(io.LimitReader(body, MaxErrorBody+1))
	if err != nil {
		return nil, err
	}
	if len(b) > MaxErrorBody {
		return nil, ErrErrorBodyTooLarge
	}
	return b, nil
}

var (
	// OpenAI organization and project IDs. \b keeps "cyborg-..." intact.
	orgID     = regexp.MustCompile(`\borg-[A-Za-z0-9]{8,}`)
	projectID = regexp.MustCompile(`\bproj_[A-Za-z0-9]{8,}`)
	// Any UUID: Anthropic names organizations by UUID, and other providers
	// use them for accounts and keys too.
	uuid = regexp.MustCompile(`[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}`)
)

// RedactAccountIDs replaces the account identifiers an upstream error
// message may hold, which would tell any client whose account the gateway
// uses: OpenAI organization IDs become org-[redacted], project IDs
// proj_[redacted], and any UUID [redacted]. Request IDs (req_...) are kept;
// they identify one request, and providers' support asks for them.
func RedactAccountIDs(s string) string {
	s = orgID.ReplaceAllLiteralString(s, "org-[redacted]")
	s = projectID.ReplaceAllLiteralString(s, "proj_[redacted]")
	return uuid.ReplaceAllLiteralString(s, "[redacted]")
}
