package observability

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics contains low-cardinality collectors shared across service phases.
type Metrics struct {
	HTTPRequests         *prometheus.CounterVec
	HTTPDuration         *prometheus.HistogramVec
	ActiveRequests       prometheus.Gauge
	StreamingConnections prometheus.Gauge
	ProviderRequests     *prometheus.CounterVec
	ProviderLatency      *prometheus.HistogramVec
	ProviderErrors       *prometheus.CounterVec
	CircuitState         *prometheus.GaugeVec
	Retries              *prometheus.CounterVec
	RateLimitRejections  *prometheus.CounterVec
	BudgetRejections     prometheus.Counter
	KafkaPublishFailures prometheus.Counter
	KafkaConsumerLag     *prometheus.GaugeVec
	RabbitJobs           *prometheus.CounterVec
	EvaluationDuration   *prometheus.HistogramVec
	DeadLetters          prometheus.Counter
	OutboxBacklog        prometheus.Gauge
	DatabaseErrors       *prometheus.CounterVec
}

// NewMetrics registers all required collectors in one service registry.
func NewMetrics(service string, registerer prometheus.Registerer) *Metrics {
	constant := prometheus.Labels{"service": service}
	metrics := &Metrics{
		HTTPRequests:         prometheus.NewCounterVec(prometheus.CounterOpts{Name: "aegis_http_requests_total", Help: "HTTP requests by method, route, and status.", ConstLabels: constant}, []string{"method", "route", "status"}),
		HTTPDuration:         prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "aegis_http_request_duration_seconds", Help: "HTTP request duration.", ConstLabels: constant, Buckets: prometheus.DefBuckets}, []string{"method", "route"}),
		ActiveRequests:       prometheus.NewGauge(prometheus.GaugeOpts{Name: "aegis_active_requests", Help: "Requests currently executing.", ConstLabels: constant}),
		StreamingConnections: prometheus.NewGauge(prometheus.GaugeOpts{Name: "aegis_streaming_connections", Help: "Active streaming connections.", ConstLabels: constant}),
		ProviderRequests:     prometheus.NewCounterVec(prometheus.CounterOpts{Name: "aegis_provider_requests_total", Help: "Provider attempts by bounded outcome.", ConstLabels: constant}, []string{"provider", "outcome"}),
		ProviderLatency:      prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "aegis_provider_latency_seconds", Help: "Provider attempt latency.", ConstLabels: constant}, []string{"provider"}),
		ProviderErrors:       prometheus.NewCounterVec(prometheus.CounterOpts{Name: "aegis_provider_errors_total", Help: "Provider errors by category.", ConstLabels: constant}, []string{"provider", "category"}),
		CircuitState:         prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "aegis_provider_circuit_state", Help: "Circuit state: closed=0, half-open=1, open=2.", ConstLabels: constant}, []string{"provider"}),
		Retries:              prometheus.NewCounterVec(prometheus.CounterOpts{Name: "aegis_retries_total", Help: "Retry attempts by operation.", ConstLabels: constant}, []string{"operation"}),
		RateLimitRejections:  prometheus.NewCounterVec(prometheus.CounterOpts{Name: "aegis_rate_limit_rejections_total", Help: "Distributed rate-limit rejections.", ConstLabels: constant}, []string{"reason"}),
		BudgetRejections:     prometheus.NewCounter(prometheus.CounterOpts{Name: "aegis_budget_rejections_total", Help: "Budget admission rejections.", ConstLabels: constant}),
		KafkaPublishFailures: prometheus.NewCounter(prometheus.CounterOpts{Name: "aegis_kafka_publish_failures_total", Help: "Kafka publication failures.", ConstLabels: constant}),
		KafkaConsumerLag:     prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "aegis_kafka_consumer_lag", Help: "Kafka consumer lag where reported.", ConstLabels: constant}, []string{"topic", "partition"}),
		RabbitJobs:           prometheus.NewCounterVec(prometheus.CounterOpts{Name: "aegis_rabbitmq_jobs_total", Help: "RabbitMQ jobs by outcome.", ConstLabels: constant}, []string{"outcome"}),
		EvaluationDuration:   prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "aegis_evaluation_duration_seconds", Help: "Evaluation execution duration.", ConstLabels: constant}, []string{"evaluator"}),
		DeadLetters:          prometheus.NewCounter(prometheus.CounterOpts{Name: "aegis_dead_letters_total", Help: "Jobs routed to dead letter.", ConstLabels: constant}),
		OutboxBacklog:        prometheus.NewGauge(prometheus.GaugeOpts{Name: "aegis_outbox_backlog", Help: "Unpublished outbox rows.", ConstLabels: constant}),
		DatabaseErrors:       prometheus.NewCounterVec(prometheus.CounterOpts{Name: "aegis_database_errors_total", Help: "Database errors by operation.", ConstLabels: constant}, []string{"operation"}),
	}
	registerer.MustRegister(
		metrics.HTTPRequests, metrics.HTTPDuration, metrics.ActiveRequests, metrics.StreamingConnections,
		metrics.ProviderRequests, metrics.ProviderLatency, metrics.ProviderErrors, metrics.CircuitState,
		metrics.Retries, metrics.RateLimitRejections, metrics.BudgetRejections, metrics.KafkaPublishFailures,
		metrics.KafkaConsumerLag, metrics.RabbitJobs, metrics.EvaluationDuration, metrics.DeadLetters,
		metrics.OutboxBacklog, metrics.DatabaseErrors,
	)
	return metrics
}

// HTTPMiddleware records route templates rather than raw paths.
func (metrics *Metrics) HTTPMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		metrics.ActiveRequests.Inc()
		defer metrics.ActiveRequests.Dec()
		recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(recorder, r)
		route := r.Pattern
		if route == "" {
			route = "unmatched"
		}
		metrics.HTTPRequests.WithLabelValues(r.Method, route, strconv.Itoa(recorder.status)).Inc()
		metrics.HTTPDuration.WithLabelValues(r.Method, route).Observe(time.Since(started).Seconds())
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (recorder *statusRecorder) WriteHeader(status int) {
	recorder.status = status
	recorder.ResponseWriter.WriteHeader(status)
}

func (recorder *statusRecorder) Flush() {
	if flusher, ok := recorder.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}
