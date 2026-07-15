// k6 dirty-traffic load test for eSPX tracker hot path.
// Mix: ~35% valid, ~15% fraud, ~20% invalid payload, ~15% DDoS, ~15% edge abuse.
//
// Env (set by run_dirty_load.sh):
//   TRACKER_BASES  comma-separated tracker URLs (default direct :8181-8184)
//   EDGE_URL       optional nginx edge URL for rate-limit / circuit-breaker abuse
//   RATE           target RPS (default 5000)
//   DURATION       test duration (default 5m)

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';
import exec from 'k6/execution';

const rate = parseInt(__ENV.RATE || '2000', 10);
const duration = __ENV.DURATION || '5m';
const edgeURL = __ENV.EDGE_URL || '';
const oversizeBytes = parseInt(__ENV.OVERSIZE_BYTES || '65536', 10);
const preAllocVUs = parseInt(__ENV.PREALLOC_VUS || '200', 10);
const maxVUs = parseInt(__ENV.MAX_VUS || '800', 10);

const trackerBases = (__ENV.TRACKER_BASES || 'http://127.0.0.1:8181,http://127.0.0.1:8182')
  .split(',')
  .map((s) => s.trim())
  .filter(Boolean);

const trafficValid = new Counter('traffic_valid');
const trafficFraud = new Counter('traffic_fraud');
const trafficInvalid = new Counter('traffic_invalid');
const trafficDdos = new Counter('traffic_ddos');
const trafficEdge = new Counter('traffic_edge');
const acceptRate = new Rate('accepted_2xx');
const serverErrorRate = new Rate('server_5xx');
const clientErrorRate = new Rate('client_4xx');
const trackLatency = new Trend('track_latency_ms', true);

export const options = {
  scenarios: {
    dirty_mix: {
      executor: 'constant-arrival-rate',
      rate: rate,
      timeUnit: '1s',
      duration: duration,
      preAllocatedVUs: preAllocVUs,
      maxVUs: maxVUs,
    },
  },
  discardResponseBodies: true,
  thresholds: {
    // Dirty traffic intentionally yields high 4xx; track tail latency on valid subset only.
    track_latency_ms: ['p(99)<300'],
    http_req_duration: ['p(99)<500'],
  },
  tags: { test: 'dirty_traffic' },
};

function pickTracker() {
  const idx = exec.scenario.iterationInTest % trackerBases.length;
  return trackerBases[idx];
}

function campaignID(vu, iter) {
  const n = ((vu * 997 + iter) % 100) + 1;
  return `00000000-0000-0000-0000-${n.toString(16).padStart(12, '0')}`;
}

function validBody(vu, iter) {
  return JSON.stringify({
    campaign_id: campaignID(vu, iter),
    user_id: `u-${vu}-${iter}`,
    type: iter % 3 === 0 ? 'click' : 'impression',
    click_id: `clk-${vu}-${iter}-${__ITER}`,
    payload: { slot: 'top', cpm: 1.25 },
  });
}

function fraudBody(vu, iter) {
  return JSON.stringify({
    campaign_id: campaignID(vu, iter),
    user_id: `fraud-bot-${vu}`,
    type: 'click',
    click_id: `fclk-${vu}-${iter}`,
    payload: { bot: true },
  });
}

function fraudIP(iter) {
  // MockGeoProvider flags .66 and .77 as datacenter/anonymous.
  return iter % 2 === 0 ? '203.0.113.66' : '198.51.100.77';
}

function classify(res) {
  if (res.status >= 200 && res.status < 300) acceptRate.add(1);
  else acceptRate.add(0);
  if (res.status >= 400 && res.status < 500) clientErrorRate.add(1);
  else clientErrorRate.add(0);
  if (res.status >= 500) serverErrorRate.add(1);
  else serverErrorRate.add(0);
}

export default function () {
  const vu = __VU;
  const iter = __ITER;
  const roll = Math.random() * 100;
  const base = pickTracker();
  const params = { headers: { Connection: 'keep-alive' }, timeout: '10s' };

  if (roll < 35) {
    // Valid traffic: unique click_id per request, rotate campaigns.
    trafficValid.add(1);
    const res = http.post(`${base}/track`, validBody(vu, iter), {
      ...params,
      headers: { ...params.headers, 'Content-Type': 'application/json' },
    });
    trackLatency.add(res.timings.duration);
    classify(res);
    return;
  }

  if (roll < 50) {
    // Fraud: datacenter IP via trusted X-Forwarded-For (k6 from localhost).
    trafficFraud.add(1);
    const res = http.post(`${base}/track`, fraudBody(vu, iter), {
      ...params,
      headers: {
        ...params.headers,
        'Content-Type': 'application/json',
        'X-Forwarded-For': fraudIP(iter),
      },
    });
    trackLatency.add(res.timings.duration);
    classify(res);
    return;
  }

  if (roll < 70) {
    // Invalid payloads: malformed JSON, bad protobuf, unknown campaign, oversize.
    trafficInvalid.add(1);
    const kind = iter % 4;
    let res;
    if (kind === 0) {
      res = http.post(`${base}/track`, '{not-json', {
        ...params,
        headers: { ...params.headers, 'Content-Type': 'application/json' },
      });
    } else if (kind === 1) {
      res = http.post(`${base}/track`, '\xff\xee\xdd\xcc\xbb', {
        ...params,
        headers: { ...params.headers, 'Content-Type': 'application/x-protobuf' },
      });
    } else if (kind === 2) {
      res = http.post(
        `${base}/track`,
        JSON.stringify({
          campaign_id: 'ffffffff-ffff-ffff-ffff-ffffffffffff',
          user_id: 'ghost',
          type: 'impression',
          click_id: `bad-${iter}`,
        }),
        { ...params, headers: { ...params.headers, 'Content-Type': 'application/json' } },
      );
    } else {
      const big = 'x'.repeat(oversizeBytes);
      res = http.post(`${base}/track`, big, {
        ...params,
        headers: { ...params.headers, 'Content-Type': 'application/json' },
      });
    }
    classify(res);
    return;
  }

  if (roll < 85) {
    // DDoS patterns: wrong method, wrong path, duplicate storm, health flood.
    trafficDdos.add(1);
    const kind = iter % 5;
    let res;
    if (kind === 0) {
      res = http.get(`${base}/track`, params);
    } else if (kind === 1) {
      res = http.get(`${base}/health`, params);
    } else if (kind === 2) {
      res = http.get(`${base}/metrics`, params);
    } else if (kind === 3) {
      // Duplicate click_id storm → 409 dedup.
      const dup = JSON.stringify({
        campaign_id: campaignID(1, 1),
        user_id: 'ddos-dup',
        type: 'click',
        click_id: 'dup-fixed-id',
        payload: {},
      });
      res = http.post(`${base}/track`, dup, {
        ...params,
        headers: { ...params.headers, 'Content-Type': 'application/json' },
      });
    } else {
      res = http.post(`${base}/admin/boom`, '{}', {
        ...params,
        headers: { ...params.headers, 'Content-Type': 'application/json' },
      });
    }
    classify(res);
    return;
  }

  // Edge abuse: nginx rate-limit, circuit breaker pressure (if EDGE_URL set).
  trafficEdge.add(1);
  if (edgeURL) {
    const res = http.post(`${edgeURL}/track`, validBody(vu, iter), {
      ...params,
      headers: { ...params.headers, 'Content-Type': 'application/json' },
    });
    classify(res);
  } else {
    // Fallback: rapid POST without Content-Length header emulation (empty body).
    const res = http.post(`${base}/track`, null, {
      ...params,
      headers: { ...params.headers, 'Content-Type': 'application/json' },
    });
    classify(res);
  }
}

export function setup() {
  for (const base of trackerBases) {
    const res = http.get(`${base}/health`);
    check(res, { 'tracker healthy': (r) => r.status === 200 });
  }
  return { started: new Date().toISOString(), rate, duration, trackers: trackerBases };
}

export function teardown(data) {
  console.log(`dirty_traffic done: rate=${data.rate} duration=${data.duration} trackers=${data.trackers.join(',')}`);
}
