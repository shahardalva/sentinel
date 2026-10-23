// Load test for POST /v1/messages at 100, 500 and 1000 req/s, 30s each,
// run one after another. Point TARGET_URL at the mock upstream or at the
// gateway; see bench/README.md.
//
//   TARGET_URL   base URL, default http://127.0.0.1:9090
//   API_KEY      sent as x-api-key, default "bench"
//   STREAM       "1" to send "stream": true
//   DURATION_S   seconds per rate, default 30 (lower it for a smoke run)
import http from 'k6/http';
import { check } from 'k6';

const TARGET_URL = (__ENV.TARGET_URL || 'http://127.0.0.1:9090').replace(/\/+$/, '');
const API_KEY = __ENV.API_KEY || 'bench';
const STREAM = __ENV.STREAM === '1';

const RATES = [100, 500, 1000];
const DURATION_S = Number(__ENV.DURATION_S || 30); // per rate
const GAP_S = 5; // lets in-flight requests drain before the next rate starts

const scenarios = {};
const thresholds = {};
RATES.forEach((rate, i) => {
  const name = `rate_${rate}`;
  scenarios[name] = {
    executor: 'constant-arrival-rate',
    rate,
    timeUnit: '1s',
    duration: `${DURATION_S}s`,
    startTime: `${i * (DURATION_S + GAP_S)}s`,
    // In-flight requests ~= rate * latency; at 200ms that is 0.2 * rate.
    preAllocatedVUs: Math.ceil(rate * 0.4),
    maxVUs: rate * 2,
    gracefulStop: `${GAP_S}s`,
  };
  // k6 tags every request with its scenario. A threshold on each tagged
  // submetric makes the end-of-test summary print its percentiles. Latency
  // counts successful responses only, since fast errors (e.g. the gateway's
  // 503 when it is at its concurrency cap) would pull percentiles down. The
  // latency limits are loose on purpose: this test measures, it does not
  // gate. The failure threshold does fail, so a run with errors is flagged.
  thresholds[`http_req_duration{scenario:${name},expected_response:true}`] = [
    'p(50)<60000',
    'p(95)<60000',
    'p(99)<60000',
  ];
  thresholds[`http_req_failed{scenario:${name}}`] = ['rate<0.01'];
  thresholds[`dropped_iterations{scenario:${name}}`] = ['count>=0'];
});

export const options = {
  scenarios,
  thresholds,
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
  discardResponseBodies: true,
};

const body = JSON.stringify({
  model: 'claude-bench',
  max_tokens: 16,
  stream: STREAM,
  messages: [{ role: 'user', content: 'Say hello.' }],
});

const params = {
  headers: {
    'content-type': 'application/json',
    'x-api-key': API_KEY,
    'anthropic-version': '2023-06-01',
  },
};

export default function () {
  const res = http.post(`${TARGET_URL}/v1/messages`, body, params);
  check(res, { 'status is 200': (r) => r.status === 200 });
}
