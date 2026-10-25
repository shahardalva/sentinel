package proxy

import (
	"context"
	"sync/atomic"
	"time"
)

// Recorder receives a summary of every request, for metrics. The core
// calls it and knows nothing about how it records; internal/telemetry
// implements it with OpenTelemetry. Implementations must be safe for
// concurrent use and cheap: they run on the request's goroutine.
type Recorder interface {
	// RecordRequest is called once per request a Handler serves, after its
	// log line is written.
	RecordRequest(ctx context.Context, info RequestInfo)
	// RecordAuthRejection is called once per request rejected by an Auth
	// given this Recorder (NewAuth(keys, logger, rec).Require). RequireKey
	// passes none. Nothing about the presented key is passed on.
	RecordAuthRejection(ctx context.Context)
}

// RequestInfo summarizes one request for a Recorder. Every field is from a
// closed set or a duration; nothing the client chose (the key, the model,
// the path) is in it.
type RequestInfo struct {
	Upstream string // Options.Upstream, from configuration
	Protocol string // the Provider's Name()
	Status   int    // as logged, including 499 for a client that went away
	// Phase is where the request failed, one of the Phase constants, when
	// Failed is true. A failed stream keeps the status already sent.
	Phase  string
	Failed bool

	RateLimit RateLimitOutcome
	// Ran says which stages started; the duration of one that didn't is
	// zero. Total is the whole request, as in the log's latency_ms.
	Ran               Stages
	RateLimitDuration time.Duration
	ReadBody          time.Duration
	UpstreamTTFB      time.Duration
	Stream            time.Duration
	Total             time.Duration
}

// Request phases, as logged. A failure's phase says where it happened.
const (
	PhaseRateLimit = "rate_limit"
	PhaseReadBody  = "read_body"
	PhaseAdmit     = "admit" // waiting for a concurrency slot
	PhaseUpstream  = "upstream"
	PhaseStream    = "stream"
)

// Stages is a set of timed request stages.
type Stages uint8

const (
	StageRateLimit    Stages = 1 << iota // the rate limiter call
	StageReadBody                        // reading the client's body
	StageUpstreamTTFB                    // Forward, until the response headers
	StageStream                          // relaying the response body
)

// Has reports whether every stage in t is in s.
func (s Stages) Has(t Stages) bool { return s&t == t }

// RateLimitOutcome is what the rate limit check decided for a request.
type RateLimitOutcome uint8

const (
	// RateLimitNotChecked: no limiter, no client identity, a rejection
	// before the check, or a client that left during it.
	RateLimitNotChecked RateLimitOutcome = iota
	RateLimitAllowed
	RateLimitLimited    // answered 429
	RateLimitFailOpen   // the limiter failed; the request went ahead
	RateLimitFailClosed // the limiter failed; answered 503
)

var rateLimitOutcomes = [...]string{
	RateLimitNotChecked: "not_checked",
	RateLimitAllowed:    "allowed",
	RateLimitLimited:    "limited",
	RateLimitFailOpen:   "fail_open",
	RateLimitFailClosed: "fail_closed",
}

func (o RateLimitOutcome) String() string {
	if int(o) < len(rateLimitOutcomes) {
		return rateLimitOutcomes[o]
	}
	return "unknown"
}

// throttleBase is the origin of logThrottle's clock. Offsets from it use
// Go's monotonic reading, so a wall-clock step back (NTP, a resumed VM)
// can't hold a throttle shut until the wall clock catches up.
var throttleBase = time.Now()

// logThrottle lets a repeated log line through at most once per interval
// and counts the ones it holds back, so a flood (a Redis outage, a client
// hammering with bad keys) can't fill the log. Each line let through
// reports the count held back since the previous one; the count after the
// last line of a flood is never logged, so a metric, not the log, carries
// totals.
type logThrottle struct {
	// last is the offset from throttleBase of the last line let through,
	// plus one, so that the zero value means none yet.
	last    atomic.Int64
	skipped atomic.Int64
}

// allow reports whether to log at now, and if so how many lines were held
// back since the last one. now must not be before throttleBase; callers
// pass time.Now(), tests a time derived from throttleBase.
func (t *logThrottle) allow(now time.Time, interval time.Duration) (ok bool, suppressed int64) {
	n := int64(now.Sub(throttleBase)) + 1
	last := t.last.Load()
	if (last != 0 && n-last < int64(interval)) || !t.last.CompareAndSwap(last, n) {
		t.skipped.Add(1)
		return false, 0
	}
	return true, t.skipped.Swap(0)
}
