import { check } from 'k6';
import { http, evaluationURL, headers } from './common.js';

export const options = { vus: 10, duration: '1m' };

export default function () {
  const requestID = __ENV.SOURCE_REQUEST_ID;
  if (!requestID) throw new Error('SOURCE_REQUEST_ID is required');
  const payload = JSON.stringify({ request_id: requestID, job_type: 'response-evaluation-suite', parameters: { max_total_latency_ms: 5000 } });
  const response = http.post(`${evaluationURL}/v1/evaluations`, payload, { headers: headers(`evaluation-${__VU}-${__ITER}`) });
  check(response, { 'evaluation accepted': (r) => r.status === 202 });
}
