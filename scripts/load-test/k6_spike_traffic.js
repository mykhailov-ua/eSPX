// k6 10× spike profile for eSPX control-cohort load validation (EDGE.md Part III §7).
// Ramp 1×→10× over 10s, hold 30s, ramp down 10s. Uses dirty-traffic mix from k6_dirty_traffic.js.
//
// Env:
//   TRACKER_BASES   comma-separated tracker URLs
//   BASE_RATE       steady RPS before spike (default 200)
//   SPIKE_MULT      multiplier at peak (default 10)
//   RAMP_UP         ramp-up duration (default 10s)
//   HOLD            hold at peak (default 30s)
//   RAMP_DOWN       ramp-down duration (default 10s)
//   CONTROL_CAMPAIGN optional pinned campaign UUID for valid subset metrics

import http from 'k6/http';
import { check } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';
import exec from 'k6/execution';

const baseRate = parseInt(__ENV.BASE_RATE || '200', 10);
const spikeMult = parseInt(__ENV.SPIKE_MULT || '10', 10);
const rampUp = __ENV.RAMP_UP || '10s';
const hold = __ENV.HOLD || '30s';
const rampDown = __ENV.RAMP_DOWN || '10s';
const controlCampaign = __ENV.CONTROL_CAMPAIGN || '';

const trackerBases = (__ENV.TRACKER_BASES || 'http://127.0.0.1:8181,http://127.0.0.1:8182')
  .split(',')
  .map((s) => s.trim())
  .filter(Boolean);

const trafficValid = new Counter('spike_valid');
const acceptRate = new Rate('spike_accepted_2xx');
const controlLatency = new Trend('control_cohort_latency_ms', true);

export const options = {
  scenarios: {
    spike_ramp: {
      executor: 'ramping-arrival-rate',
      startRate: baseRate,
      timeUnit: '1s',
      preAllocatedVUs: 400,
      maxVUs: 2000,
      stages: [
        { duration: rampUp, target: baseRate * spikeMult },
        { duration: hold, target: baseRate * spikeMult },
        { duration: rampDown, target: baseRate },
      ],
    },
  },
  discardResponseBodies: true,
  thresholds: {
    control_cohort_latency_ms: ['p(99)<80'],
    spike_accepted_2xx: ['rate>0.5'],
  },
  tags: { test: 'shard_load_spike' },
};

function pickTracker() {
  const idx = exec.scenario.iterationInTest % trackerBases.length;
  return trackerBases[idx];
}

function validPayload() {
  const id = `${exec.vu.idInTest}-${exec.scenario.iterationInTest}`;
  const campaign = controlCampaign || '00000000-0000-4000-8000-000000000001';
  return JSON.stringify({
    campaign_id: campaign,
    type: 'impression',
    click_id: `spike-${id}`,
    user_id: `control-${id}`,
    payload: { load: 'spike' },
  });
}

export default function () {
  const url = `${pickTracker()}/track`;
  const res = http.post(url, validPayload(), {
    headers: { 'Content-Type': 'application/json' },
    tags: { name: 'spike_track' },
  });
  const ok = res.status >= 200 && res.status < 300;
  acceptRate.add(ok);
  if (ok) {
    trafficValid.add(1);
    controlLatency.add(res.timings.duration);
  }
  check(res, { 'status 2xx': (r) => r.status >= 200 && r.status < 300 });
}
