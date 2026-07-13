package evaluation

import (
	"strings"
	"testing"
	"time"
)

func TestPIIMasksFindings(t *testing.T) {
	result := (PII{}).Evaluate(Input{Response: "Contact alice@example.com or 192.168.1.1"})
	if result.Status != "fail" || len(result.Findings) != 2 {
		t.Fatalf("PII result = %+v", result)
	}
	for _, finding := range result.Findings {
		if strings.Contains(finding.Masked, "alice") || strings.Contains(finding.Masked, "192.168") {
			t.Fatalf("finding was not masked: %+v", finding)
		}
	}
}

func TestQualityAndPerformance(t *testing.T) {
	quality := (Quality{}).Evaluate(Input{Response: "short"})
	if quality.ScoreMilli >= 1000 {
		t.Fatal("short response was not penalized")
	}
	performance := (Performance{}).Evaluate(Input{ProviderLatency: 2 * time.Second, MaxProviderLatency: time.Second})
	if performance.Status != "fail" {
		t.Fatal("latency violation passed")
	}
}
