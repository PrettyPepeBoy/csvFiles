package filer

import (
	"github.com/prometheus/client_golang/prometheus"
	"time"
)

var (
	metricFilerErrors       prometheus.Counter
	metricFilerResponseTime *prometheus.HistogramVec
)

func RegisterFilerMetrics() {
	metricFilerErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "dmitrii_search_decision",
		Subsystem: "csv_filer",
		Name:      "filer_errors",
		Help:      "shows how many and where errors occurred",
	})

	metricFilerResponseTime = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dmitrii_search_decision",
			Subsystem: "csv_filer",
			Name:      "filer_response_time",
			Help:      "shows how long it takes to send response to user",
			Buckets:   []float64{0.1, 0.25, 0.5, 1},
		}, []string{"endpoint", "method"})

	prometheus.MustRegister(metricFilerErrors)
	prometheus.MustRegister(metricFilerResponseTime)
}

func timeMetric(endpoint string, method string, t time.Time) {
	metricFilerResponseTime.With(prometheus.Labels{endpoint: endpoint, method: method}).Observe(time.Since(t).Seconds())
}
