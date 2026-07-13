import { check } from 'k6';
import { http, baseURL, headers, chatPayload } from './common.js';

export const options = { vus: 10, duration: '2m' };

export default function () {
  const response = http.post(`${baseURL}/v1/chat/completions`, chatPayload(false), { headers: headers(`failover-${__VU}-${__ITER}`), timeout: '30s' });
  check(response, { 'request resolved': (r) => r.status === 200 || r.status === 503 });
}
