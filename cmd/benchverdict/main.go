// Command benchverdict decides whether a make bench-gateway run was a clean
// measurement, after k6 has finished. It reads gateway-bench's log on stdin
// and, from the file named by -k6, the output of bench/k6/gateway-verdict.js:
// the main k6 run's --summary-export JSON and gateway-bench's own /metrics,
// between markers.
//
//	docker logs <container> 2>&1 | go run ./cmd/benchverdict -k6 <file>
//
// gateway-bench is recreated for every run, so its counters cover that run
// alone. The run is clean only if no request was fail_open, the requests
// Redis allowed equal the requests k6 sent, no request had another outcome,
// and gateway-bench didn't log "redis not reachable at startup". It prints
// one line either way and exits 1 on a rejection.
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"slices"
	"strconv"
	"strings"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

// The lines gateway-verdict.js prints around its two sections.
const (
	markSummary = "# benchverdict: k6 summary"
	markMetrics = "# benchverdict: gateway-bench metrics"
	markEnd     = "# benchverdict: end"
)

const (
	decisionsName = "sentinel_ratelimit_decisions_total"
	redisDownMsg  = "redis not reachable at startup"
)

func main() {
	k6 := flag.String("k6", "", "file holding the output of bench/k6/gateway-verdict.js")
	flag.Parse()
	if *k6 == "" {
		fmt.Fprintln(os.Stderr, "benchverdict needs -k6")
		os.Exit(2)
	}
	f, err := os.Open(*k6)
	if err != nil {
		fmt.Fprintf(os.Stderr, "bench rejected: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()
	line, err := judge(os.Stdin, f)
	if err != nil {
		fmt.Fprintf(os.Stderr, "bench rejected: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(line)
}

// judge returns the clean verdict line, or an error listing every reason
// the run is rejected. Output it can't read is a rejection: no metrics
// means no proof.
func judge(log, k6out io.Reader) (string, error) {
	var problems []string
	logProblem, err := checkLog(log)
	if err != nil {
		return "", err
	}
	reject := func(err error) (string, error) {
		return "", errors.New(strings.Join(append(problems, err.Error()), "; "))
	}

	summaryJSON, metrics, err := sections(k6out)
	if err != nil {
		return reject(err)
	}
	requests, err := httpReqs(summaryJSON)
	if err != nil {
		return reject(err)
	}
	decisions, err := parseDecisions(metrics)
	if err != nil {
		return reject(err)
	}

	allowed, failOpen := decisions["allowed"], decisions["fail_open"]
	if failOpen != 0 {
		problems = append(problems, commas(failOpen)+" fail_open: those requests skipped Redis or got no answer from it")
	}
	if allowed != requests {
		problems = append(problems, fmt.Sprintf("%s allowed, but k6 sent %s requests: not every request went through Redis exactly once",
			commas(allowed), commas(requests)))
	}
	var others []string
	for outcome, n := range decisions {
		if outcome != "allowed" && outcome != "fail_open" && n != 0 {
			others = append(others, commas(n)+" "+outcome)
		}
	}
	if len(others) > 0 {
		slices.Sort(others)
		problems = append(problems, strings.Join(others, ", ")+": every request should be allowed by Redis")
	}
	if logProblem != "" {
		problems = append(problems, logProblem)
	}

	counts := fmt.Sprintf("%s requests, %s allowed, %s fail_open", commas(requests), commas(allowed), commas(failOpen))
	if len(problems) > 0 {
		return "", fmt.Errorf("%s (%s)", strings.Join(problems, "; "), counts)
	}
	return "bench clean: " + counts, nil
}

// checkLog returns why gateway-bench's log makes the run unclean, or "".
// It must hold the "listening" line, so an empty or wrong log can't pass.
func checkLog(r io.Reader) (string, error) {
	var listening, redisDown bool
	sc := bufio.NewScanner(r)
	sc.Buffer(nil, 1<<20)
	for sc.Scan() {
		line := sc.Bytes()
		listening = listening || bytes.Contains(line, []byte(`"msg":"listening"`))
		redisDown = redisDown || bytes.Contains(line, []byte(redisDownMsg))
	}
	if err := sc.Err(); err != nil {
		return "", fmt.Errorf("read gateway-bench log: %w", err)
	}
	switch {
	case redisDown:
		return `gateway-bench logged "` + redisDownMsg + `"`, nil
	case !listening:
		return `gateway-bench log has no "listening" line`, nil
	}
	return "", nil
}

// sections returns the summary and metrics sections of gateway-verdict.js's
// output. Lines outside the markers (k6's banner, if any) are skipped.
func sections(r io.Reader) (summary, metrics string, err error) {
	var parts [2]strings.Builder
	var seen [2]bool
	cur, ended := -1, false
	sc := bufio.NewScanner(r)
	sc.Buffer(nil, 1<<20)
	for sc.Scan() {
		line := strings.TrimSuffix(sc.Text(), "\r") // Docker Desktop may add \r
		next := -1
		switch line {
		case markSummary:
			next = 0
		case markMetrics:
			next = 1
		case markEnd:
			cur, ended = -1, true
			continue
		}
		if next >= 0 {
			if seen[next] {
				return "", "", fmt.Errorf("k6 output has %q more than once", line)
			}
			seen[next], cur = true, next
			continue
		}
		if cur >= 0 {
			parts[cur].WriteString(line)
			parts[cur].WriteByte('\n')
		}
	}
	if err := sc.Err(); err != nil {
		return "", "", fmt.Errorf("read k6 output: %w", err)
	}
	switch {
	case !seen[0]:
		return "", "", errors.New("no k6 summary in the k6 output; see k6's errors above")
	case !seen[1]:
		return "", "", errors.New("no gateway-bench metrics in the k6 output; see k6's errors above")
	case !ended:
		return "", "", errors.New("k6 output truncated: no end marker")
	}
	return parts[0].String(), parts[1].String(), nil
}

// httpReqs returns http_reqs' count from k6's --summary-export JSON, where
// it is metrics.http_reqs.count, or from handleSummary's shape, where it
// is metrics.http_reqs.values.count.
func httpReqs(summaryJSON string) (int64, error) {
	var s struct {
		Metrics map[string]struct {
			Count  json.Number `json:"count"`
			Values struct {
				Count json.Number `json:"count"`
			} `json:"values"`
		} `json:"metrics"`
	}
	if err := json.Unmarshal([]byte(summaryJSON), &s); err != nil {
		return 0, fmt.Errorf("read k6 summary: %w", err)
	}
	m := s.Metrics["http_reqs"]
	count := m.Count
	if count == "" {
		count = m.Values.Count
	}
	n, err := strconv.ParseInt(string(count), 10, 64)
	switch {
	case err != nil || n < 0:
		return 0, errors.New("k6 summary has no http_reqs count")
	case n == 0:
		return 0, errors.New("k6 sent no requests")
	}
	return n, nil
}

// parseDecisions returns sentinel_ratelimit_decisions_total summed by
// outcome, whatever other labels there are or their order.
func parseDecisions(metrics string) (map[string]int64, error) {
	p := expfmt.NewTextParser(model.UTF8Validation)
	families, err := p.TextToMetricFamilies(strings.NewReader(metrics))
	if err != nil {
		return nil, fmt.Errorf("read gateway-bench metrics: %w", err)
	}
	mf := families[decisionsName]
	if mf == nil || len(mf.GetMetric()) == 0 {
		return nil, errors.New("gateway-bench /metrics has no " + decisionsName + ": no proof any request went through Redis")
	}
	if mf.GetType() != dto.MetricType_COUNTER {
		return nil, fmt.Errorf("%s is a %s, not a counter", decisionsName, strings.ToLower(mf.GetType().String()))
	}
	sums := map[string]int64{}
	for _, m := range mf.GetMetric() {
		var outcome string
		for _, l := range m.GetLabel() {
			if l.GetName() == "outcome" {
				outcome = l.GetValue()
			}
		}
		v := m.GetCounter().GetValue()
		if v < 0 || v != math.Trunc(v) || v > math.MaxInt64/2 {
			return nil, fmt.Errorf("%s{outcome=%q} is %v, not a count", decisionsName, outcome, v)
		}
		sums[outcome] += int64(v)
	}
	return sums, nil
}

// commas formats n with a comma between groups of three digits.
func commas(n int64) string {
	s := strconv.FormatInt(n, 10)
	neg := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(s, "-")
	var b strings.Builder
	if neg {
		b.WriteByte('-')
	}
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(c)
	}
	return b.String()
}
