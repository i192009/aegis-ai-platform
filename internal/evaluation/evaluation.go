// Package evaluation contains transparent deterministic response evaluators.
package evaluation

import (
	"fmt"
	"net"
	"regexp"
	"strings"
	"time"
)

// Input contains only data needed by deterministic evaluators.
type Input struct {
	Response           string
	RequestedFormat    string
	ProviderLatency    time.Duration
	TotalLatency       time.Duration
	PromptTokens       int64
	CompletionTokens   int64
	CostMicroUSD       int64
	MaxProviderLatency time.Duration
	MaxTotalLatency    time.Duration
	MaxTokens          int64
	MaxCostMicroUSD    int64
	BlockedTerms       []string
}

// Finding is a safe, masked observation.
type Finding struct {
	Code     string `json:"code"`
	Severity string `json:"severity"`
	Message  string `json:"message"`
	Masked   string `json:"masked,omitempty"`
}

// Result is returned by every evaluator.
type Result struct {
	Name       string        `json:"evaluator_name"`
	Version    string        `json:"evaluator_version"`
	ScoreMilli int           `json:"score_milli"`
	Status     string        `json:"status"`
	Findings   []Finding     `json:"findings"`
	Duration   time.Duration `json:"execution_duration"`
}

// Evaluator is deterministic and local.
type Evaluator interface {
	Evaluate(Input) Result
}

// PII detects common patterns and masks every finding. It is intentionally not claimed as perfect.
type PII struct{}

var piiPatterns = []struct {
	code    string
	pattern *regexp.Regexp
}{
	{"email", regexp.MustCompile(`(?i)\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b`)},
	{"phone", regexp.MustCompile(`\b(?:\+?[0-9][0-9 ()-]{7,}[0-9])\b`)},
	{"payment_card_like", regexp.MustCompile(`\b(?:[0-9][ -]*?){13,19}\b`)},
	{"national_identifier_like", regexp.MustCompile(`\b[A-Z]{2}[0-9]{6}[A-Z]?\b`)},
	{"ip_address", regexp.MustCompile(`\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b`)},
}

func (PII) Evaluate(input Input) Result {
	started := time.Now()
	findings := make([]Finding, 0)
	for _, candidate := range piiPatterns {
		for _, match := range candidate.pattern.FindAllString(input.Response, -1) {
			if candidate.code == "ip_address" && net.ParseIP(match) == nil {
				continue
			}
			findings = append(findings, Finding{Code: candidate.code, Severity: "high", Message: "possible sensitive value detected", Masked: mask(match)})
		}
	}
	status, score := "pass", 1000
	if len(findings) > 0 {
		status, score = "fail", max(0, 1000-len(findings)*250)
	}
	return Result{Name: "pii_detector", Version: "1.0.0", ScoreMilli: score, Status: status, Findings: findings, Duration: time.Since(started)}
}

// Quality scores empty, short, repetitive, malformed, and requested-format output.
type Quality struct{}

func (Quality) Evaluate(input Input) Result {
	started := time.Now()
	score := 1000
	findings := make([]Finding, 0)
	trimmed := strings.TrimSpace(input.Response)
	if trimmed == "" {
		findings = append(findings, Finding{Code: "empty_response", Severity: "high", Message: "response is empty"})
		score = 0
	} else {
		if len(trimmed) < 20 {
			findings = append(findings, Finding{Code: "too_short", Severity: "medium", Message: "response may not be useful"})
			score -= 250
		}
		words := strings.Fields(strings.ToLower(trimmed))
		if repeatedRatio(words) > 0.6 && len(words) >= 8 {
			findings = append(findings, Finding{Code: "repetition", Severity: "medium", Message: "response contains excessive repetition"})
			score -= 300
		}
		if input.RequestedFormat == "json" && !(strings.HasPrefix(trimmed, "{") && strings.HasSuffix(trimmed, "}")) {
			findings = append(findings, Finding{Code: "format_mismatch", Severity: "medium", Message: "response does not match requested JSON format"})
			score -= 300
		}
	}
	status := "pass"
	if score < 700 {
		status = "fail"
	}
	return Result{Name: "quality", Version: "1.0.0", ScoreMilli: max(0, score), Status: status, Findings: findings, Duration: time.Since(started)}
}

// Safety detects configured terms without making a broad safety guarantee.
type Safety struct{}

func (Safety) Evaluate(input Input) Result {
	started := time.Now()
	lower := strings.ToLower(input.Response)
	findings := make([]Finding, 0)
	for _, term := range input.BlockedTerms {
		term = strings.TrimSpace(strings.ToLower(term))
		if term != "" && strings.Contains(lower, term) {
			findings = append(findings, Finding{Code: "blocked_term", Severity: "high", Message: "configured blocked pattern detected", Masked: mask(term)})
		}
	}
	status, score := "pass", 1000
	if len(findings) > 0 {
		status, score = "fail", 0
	}
	return Result{Name: "safety", Version: "1.0.0", ScoreMilli: score, Status: status, Findings: findings, Duration: time.Since(started)}
}

// Performance checks explicit latency, token, and cost thresholds.
type Performance struct{}

func (Performance) Evaluate(input Input) Result {
	started := time.Now()
	findings := make([]Finding, 0)
	checks := []struct {
		exceeded bool
		code     string
		message  string
	}{
		{input.MaxProviderLatency > 0 && input.ProviderLatency > input.MaxProviderLatency, "provider_latency", "provider latency exceeded threshold"},
		{input.MaxTotalLatency > 0 && input.TotalLatency > input.MaxTotalLatency, "total_latency", "total latency exceeded threshold"},
		{input.MaxTokens > 0 && input.PromptTokens+input.CompletionTokens > input.MaxTokens, "token_count", "token count exceeded threshold"},
		{input.MaxCostMicroUSD > 0 && input.CostMicroUSD > input.MaxCostMicroUSD, "cost", "cost exceeded threshold"},
	}
	for _, check := range checks {
		if check.exceeded {
			findings = append(findings, Finding{Code: check.code, Severity: "medium", Message: check.message})
		}
	}
	status := "pass"
	score := max(0, 1000-len(findings)*250)
	if len(findings) > 0 {
		status = "fail"
	}
	return Result{Name: "performance", Version: "1.0.0", ScoreMilli: score, Status: status, Findings: findings, Duration: time.Since(started)}
}

func mask(value string) string {
	runes := []rune(value)
	if len(runes) <= 4 {
		return strings.Repeat("*", len(runes))
	}
	return fmt.Sprintf("%c%s%c", runes[0], strings.Repeat("*", len(runes)-2), runes[len(runes)-1])
}

func repeatedRatio(words []string) float64 {
	if len(words) == 0 {
		return 0
	}
	counts := make(map[string]int)
	maximum := 0
	for _, word := range words {
		counts[word]++
		maximum = max(maximum, counts[word])
	}
	return float64(maximum) / float64(len(words))
}
