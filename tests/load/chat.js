import { check } from 'k6';
import { http, baseURL, headers, chatPayload } from './common.js';

export const options = {
  scenarios: { chat: { executor: 'constant-arrival-rate', rate: 20, timeUnit: '1s', duration: '2m', preAllocatedVUs: 20, maxVUs: 100 } },
  thresholds: { http_req_failed: ['rate<0.01'], http_req_duration: ['p(95)<1000'] },
};

export default function () {
  const response = http.post(`${baseURL}/v1/chat/completions`, chatPayload(false), { headers: headers(`chat-${__VU}-${__ITER}`) });
  check(response, { 'chat completed': (r) => r.status === 200, 'usage returned': (r) => r.json('usage.total_tokens') > 0 });
}
