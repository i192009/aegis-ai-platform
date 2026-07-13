import http from 'k6/http';

export const baseURL = __ENV.BASE_URL || 'http://localhost:8080';
export const evaluationURL = __ENV.EVALUATION_URL || 'http://localhost:8083';
export const apiKey = __ENV.AEGIS_API_KEY || 'aegis_devkey0000_0123456789abcdefghijklmnopqrstuvwxyzABCDEFG';

export function headers(idempotencyKey) {
  return {
    'Content-Type': 'application/json',
    'X-API-Key': apiKey,
    'Idempotency-Key': idempotencyKey,
    'X-Correlation-ID': `k6-${__VU}-${__ITER}`,
  };
}

export function chatPayload(stream = false) {
  return JSON.stringify({
    model: 'aegis-small',
    messages: [{ role: 'user', content: `bounded load request ${__VU}-${__ITER}` }],
    max_tokens: 128,
    stream,
  });
}

export { http };
