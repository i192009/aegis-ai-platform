import { check } from 'k6';
import { http, baseURL, headers, chatPayload } from './common.js';

export const options = { scenarios: { burst: { executor: 'shared-iterations', vus: 100, iterations: 1000, maxDuration: '30s' } } };

export default function () {
  const response = http.post(`${baseURL}/v1/chat/completions`, chatPayload(false), { headers: headers(`rate-${__VU}-${__ITER}`) });
  check(response, { 'admitted or deliberately limited': (r) => r.status === 200 || r.status === 429 });
}
