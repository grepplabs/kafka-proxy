package proxy

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	proxyConnectionsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "proxy_connections_total",
			Help: "Total number of created connections"},
		[]string{"broker"})

	proxyRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "proxy_requests_total",
			Help: "Total number of requests sent"},
		[]string{"broker", "api_key", "api_version"})

	proxyRequestsBytes = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "proxy_requests_bytes",
			Help: "Size of outgoing requests"},
		[]string{"broker"})

	proxyResponsesBytes = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "proxy_responses_bytes",
			Help: "Size of incoming responses"},
		[]string{"broker"})

	proxyOpenedConnections = prometheus.NewDesc(
		"proxy_opened_connections",
		"Number of opened connections",
		[]string{"broker"}, nil,
	)
)

func init() {
	prometheus.MustRegister(proxyConnectionsTotal)
	prometheus.MustRegister(proxyRequestsTotal)
	prometheus.MustRegister(proxyRequestsBytes)
	prometheus.MustRegister(proxyResponsesBytes)
}

type proxyCollector struct {
	connSet *ConnSet
}

func NewCollector(connSet *ConnSet) prometheus.Collector {
	return &proxyCollector{connSet: connSet}
}

func (p *proxyCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- proxyOpenedConnections
}

func (p *proxyCollector) Collect(ch chan<- prometheus.Metric) {

	brokerToCount := p.connSet.Count()
	for broker, count := range brokerToCount {
		ch <- prometheus.MustNewConstMetric(proxyOpenedConnections, prometheus.GaugeValue, float64(count), broker)
	}
}
