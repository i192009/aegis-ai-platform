// Package ratelimit implements atomic Redis-backed distributed admission control.
package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
)

// Limits apply to one tenant or API-key bucket.
type Limits struct {
	RequestsPerMinute int64
	TokensPerMinute   int64
	MaxConcurrent     int64
}

// Decision explains an admission result without exposing Redis details.
type Decision struct {
	Allowed    bool
	Reason     string
	RetryAfter time.Duration
}

// Permit owns one distributed concurrency slot and must be released once.
type Permit struct {
	client   redis.UniversalClient
	key      string
	released atomic.Bool
}

// Limiter uses a fixed one-minute window and a TTL-protected concurrency counter.
type Limiter struct {
	client redis.UniversalClient
	prefix string
	allow  *redis.Script
	finish *redis.Script
}

var allowScript = redis.NewScript(`
local requests = tonumber(redis.call('GET', KEYS[1]) or '0')
local tokens = tonumber(redis.call('GET', KEYS[2]) or '0')
local concurrent = tonumber(redis.call('GET', KEYS[3]) or '0')
if requests + 1 > tonumber(ARGV[1]) then return {0, 1} end
if tokens + tonumber(ARGV[4]) > tonumber(ARGV[2]) then return {0, 2} end
if concurrent + 1 > tonumber(ARGV[3]) then return {0, 3} end
redis.call('INCR', KEYS[1])
redis.call('EXPIRE', KEYS[1], 60)
redis.call('INCRBY', KEYS[2], ARGV[4])
redis.call('EXPIRE', KEYS[2], 60)
redis.call('INCR', KEYS[3])
redis.call('EXPIRE', KEYS[3], tonumber(ARGV[5]))
return {1, 0}
`)

var finishScript = redis.NewScript(`
local value = tonumber(redis.call('GET', KEYS[1]) or '0')
if value <= 1 then
  redis.call('DEL', KEYS[1])
  return 0
end
return redis.call('DECR', KEYS[1])
`)

// New creates a distributed limiter.
func New(client redis.UniversalClient, prefix string) *Limiter {
	if prefix == "" {
		prefix = "aegis:limit"
	}
	return &Limiter{client: client, prefix: prefix, allow: allowScript, finish: finishScript}
}

// Allow atomically checks and consumes request, token, and concurrency capacity.
func (limiter *Limiter) Allow(ctx context.Context, bucket string, limits Limits, estimatedTokens int64, concurrencyTTL time.Duration) (Decision, *Permit, error) {
	ctx, span := otel.Tracer("aegis/redis").Start(ctx, "redis.rate_limit")
	defer span.End()
	if bucket == "" || limits.RequestsPerMinute <= 0 || limits.TokensPerMinute <= 0 || limits.MaxConcurrent <= 0 || estimatedTokens < 0 {
		return Decision{}, nil, errors.New("invalid rate-limit input")
	}
	window := time.Now().UTC().Unix() / 60
	base := fmt.Sprintf("%s:%s:%d", limiter.prefix, bucket, window)
	concurrentKey := fmt.Sprintf("%s:%s:concurrent", limiter.prefix, bucket)
	result, err := limiter.allow.Run(ctx, limiter.client, []string{base + ":requests", base + ":tokens", concurrentKey}, limits.RequestsPerMinute, limits.TokensPerMinute, limits.MaxConcurrent, estimatedTokens, int64(concurrencyTTL.Seconds())).Int64Slice()
	if err != nil {
		return Decision{}, nil, fmt.Errorf("execute rate-limit script: %w", err)
	}
	if len(result) != 2 {
		return Decision{}, nil, errors.New("unexpected rate-limit script response")
	}
	if result[0] == 1 {
		return Decision{Allowed: true}, &Permit{client: limiter.client, key: concurrentKey}, nil
	}
	reason := map[int64]string{1: "requests_per_minute", 2: "tokens_per_minute", 3: "maximum_concurrency"}[result[1]]
	return Decision{Allowed: false, Reason: reason, RetryAfter: time.Minute}, nil, nil
}

// Release frees the distributed concurrency slot. Repeated calls are safe within one process.
func (permit *Permit) Release(ctx context.Context) error {
	if permit == nil || !permit.released.CompareAndSwap(false, true) {
		return nil
	}
	if _, err := finishScript.Run(ctx, permit.client, []string{permit.key}).Result(); err != nil {
		return fmt.Errorf("release concurrency permit: %w", err)
	}
	return nil
}
