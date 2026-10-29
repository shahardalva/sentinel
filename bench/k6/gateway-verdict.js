// Run by make bench-gateway after messages.js, for cmd/benchverdict. It
// prints the main run's --summary-export JSON and gateway-bench's own
// /metrics between markers, and nothing else on stdout. It runs in the
// k6-gateway container on the compose network, so gateway-bench's metrics
// port stays unpublished.
//
//   BENCH_SUMMARY  the file messages.js wrote through K6_SUMMARY_EXPORT in
//                  the k6 working directory (the k6-out volume), such as
//                  summary-20261029101500-1234.json: a bare name, since Git
//                  Bash rewrites an argument that starts with "/"
//   METRICS_URL    default http://gateway-bench:9464/metrics
import http from 'k6/http';

const SUMMARY = __ENV.BENCH_SUMMARY || '';
if (!/^summary-[0-9-]+\.json$/.test(SUMMARY)) {
  throw new Error(`BENCH_SUMMARY must be a file name such as summary-20261029101500-1234.json, not "${SUMMARY}"`);
}
// The k6 image's working directory. open() resolves a relative path against
// this script's directory, not the working directory.
const summary = open(`/home/k6/${SUMMARY}`);
const METRICS_URL = __ENV.METRICS_URL || 'http://gateway-bench:9464/metrics';

// cmd/benchverdict looks for exactly these lines.
const MARK_SUMMARY = '# benchverdict: k6 summary';
const MARK_METRICS = '# benchverdict: gateway-bench metrics';
const MARK_END = '# benchverdict: end';

export const options = { vus: 1, iterations: 1 };

export function setup() {
  const res = http.get(METRICS_URL, { timeout: '10s' });
  if (res.status !== 200) {
    throw new Error(`GET ${METRICS_URL}: status ${res.status} ${res.error || ''}`);
  }
  return { metrics: res.body };
}

export default function () {}

// Replaces k6's own summary: stdout is only what cmd/benchverdict reads.
export function handleSummary(data) {
  const metrics = data.setup_data && data.setup_data.metrics;
  if (typeof metrics !== 'string') {
    return { stderr: 'gateway-verdict.js: setup returned no metrics\n' };
  }
  return { stdout: `${MARK_SUMMARY}\n${summary.trim()}\n${MARK_METRICS}\n${metrics.trim()}\n${MARK_END}\n` };
}
