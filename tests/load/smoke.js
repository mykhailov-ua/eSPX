import { check, sleep } from 'k6';
import http from 'k6/http';

export const options = {
  vus: 1, // Single virtual user for smoke test
  duration: '10s',
  thresholds: {
    http_req_duration: ['p(99)<50'], // 99% of requests must complete below 50ms
  },
};

const BASE_URL = __ENV.BASE_URL || 'http://app:8085';
const CAMPAIGN_ID = '00000000-0000-0000-0000-000000000001'; // Default test campaign

export default function () {
  const url = `${BASE_URL}/track`;
  const payload = JSON.stringify({
    campaign_id: CAMPAIGN_ID,
    type: 'click',
    click_id: `smoke-${Date.now()}-${Math.random()}`,
    payload: { source: 'k6-smoke' }
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
    },
  };

  const res = http.post(url, payload, params);

  check(res, {
    'is status 202': (r) => r.status === 202,
    'has request_id': (r) => r.json().request_id !== undefined,
  });

  sleep(1);
}
