import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
    stages: [
        { target: 100, duration: '1m' },      // Ramp up
        { target: 1000, duration: '5m' },     // High load
        { target: 2000, duration: '2m' },     // Peak load
        { target: 0, duration: '30s' },       // Ramp down
    ],
    thresholds: {
        http_req_duration: ['p(95)<100'],     // 95% of requests must complete below 100ms
        http_req_failed: ['rate<0.01'],       // Error rate must be less than 1%
    },
};

const BASE_URL = __ENV.BASE_URL || 'http://app:8085';

export default function () {
    const payload = JSON.stringify({
        campaign_id: '00000000-0000-0000-0000-000000000001',
        type: 'impression',
        click_id: `clk_${Math.random().toString(36).substring(7)}`,
        payload: { source: 'k6-stress-test' },
    });

    const params = {
        headers: {
            'Content-Type': 'application/json',
        },
    };

    const res = http.post(`${BASE_URL}/track`, payload, params);

    check(res, {
        'status is 202': (r) => r.status === 202,
    });

    sleep(0.1);
}
