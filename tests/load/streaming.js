import { check } from 'k6';
import { http, baseURL, headers, chatPayload } from './common.js';

export const options = { vus: 10, duration: '1m', thresholds: { http_req_failed: ['rate<0.01'] } };

export default function () {
  const response = http.post(`${baseURL}/v1/chat/completions`, chatPayload(true), { headers: headers(`stream-${__VU}-${__ITER}`), timeout: '60s' });
  check(response, { 'SSE status': (r) => r.status === 200, 'SSE terminates': (r) => r.body.includes('data: [DONE]') });
}
