import { check, sleep } from 'k6';
import { http, baseURL, headers, chatPayload } from './common.js';

export const options = { vus: 20, iterations: 200 };
const sharedKey = `duplicate-${Date.now()}`;

export default function () {
  const response = http.post(`${baseURL}/v1/chat/completions`, chatPayload(false), { headers: headers(sharedKey) });
  check(response, { 'same resource returned': (r) => r.status === 200 || r.status === 202 });
  sleep(0.05);
}
