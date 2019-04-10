package main

import (
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/log"
	"github.com/prometheus/common/version"
	"gopkg.in/alecthomas/kingpin.v2"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"sync"
	"time"
)

const (
	namespace = "fluentd"
)

var (
	fluentdUp               = prometheus.NewDesc(prometheus.BuildFQName(namespace, "", "up"), "Was the last scrape of fluentd succesful.", nil, nil)
	bufferStatusLabelsNames = []string{"plugin_id", "plugin_category", "type", "worker_id"}
	outputStatusLabelsNames = []string{"plugin_id", "type", "worker_id"}
)

func newBufferMetric(metricName string, docString string, constLabels prometheus.Labels) *prometheus.Desc {
	return prometheus.NewDesc(prometheus.BuildFQName(namespace, "status", metricName), docString, bufferStatusLabelsNames, constLabels)
}

func newOutputMetric(metricName string, docString string, constLabels prometheus.Labels) *prometheus.Desc {
	return prometheus.NewDesc(prometheus.BuildFQName(namespace, "output_status", metricName), docString, outputStatusLabelsNames, constLabels)
}

var (
	metrics = map[string]*prometheus.Desc{
		"fluentd_status_buffer_queue_length":        newBufferMetric("buffer_queue_length", "Current buffer queue length.", nil),
		"fluentd_status_buffer_total_bytes":         newBufferMetric("buffer_total_bytes", "Current total size of queued buffers.", nil),
		"fluentd_status_retry_count":                newBufferMetric("retry_count", "Current retry counts.", nil),
		"fluentd_output_status_buffer_queue_length": newOutputMetric("buffer_queue_length", "Current buffer queue length.", nil),
		"fluentd_output_status_buffer_total_bytes":  newOutputMetric("buffer_total_bytes", "Current total size of queued buffers.", nil),
		"fluentd_output_status_retry_count":         newOutputMetric("retry_count", "Current retry counts.", nil),
		"fluentd_output_status_num_errors":          newOutputMetric("num_errors", "Current number of errors.", nil),
		"fluentd_output_status_emit_count":          newOutputMetric("emit_count", "Current emit counts.", nil),
		"fluentd_output_status_emit_records":        newOutputMetric("emit_records", "Current emit records.", nil),
		"fluentd_output_status_write_count":         newOutputMetric("write_count", "Current write counts.", nil),
		"fluentd_output_status_rollback_count":      newOutputMetric("rollback_count", "Current rollback counts.", nil),
		"fluentd_output_status_retry_wait":          newOutputMetric("retry_wait", "Current retry wait", nil),
	}
)

type Fetchers = []func() (io.ReadCloser, error)

type Exporter struct {
	URI      string
	mutex    sync.RWMutex
	fetchers Fetchers
	up       prometheus.Gauge
}

func NewExporter(uri string, startingPort int, workers int) (*Exporter, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}

	fetchers := make(Fetchers, 0, workers)

	for workerId := 0; workerId < workers; workerId++ {
		u.Host = fmt.Sprintf("%s:%d", u.Hostname(), startingPort+workerId)
		u.Path = "/metrics"
		fetchers = append(fetchers, fetchHTTP(u.String(), 5*time.Second))
	}

	return &Exporter{
		URI:      uri,
		fetchers: fetchers,
		up: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "up",
			Help:      "Was the last scraping of fluentd successful.",
		}),
	}, nil
}

func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	ch <- fluentdUp
	for _, v := range metrics {
		ch <- v
	}
}

func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	up := e.scrape(ch)

	ch <- prometheus.MustNewConstMetric(fluentdUp, prometheus.GaugeValue, up)
}

func fetchHTTP(uri string, timeout time.Duration) func() (io.ReadCloser, error) {
	transport := &http.Transport{}
	client := http.Client{
		Timeout:   timeout,
		Transport: transport,
	}

	return func() (io.ReadCloser, error) {
		resp, err := client.Get(uri)
		if err != nil {
			return nil, err
		}
		if !(resp.StatusCode >= 200 && resp.StatusCode < 300) {
			resp.Body.Close()
			return nil, fmt.Errorf("HTTP status %d", resp.StatusCode)
		}
		return resp.Body, nil
	}
}

func buildLabelValues(metric *dto.Metric, workerId int) []string {
	labels := metric.GetLabel()
	sort.Slice(labels, func(i, j int) bool {
		return labels[i].GetName() < labels[j].GetName()
	})
	labelValues := make([]string, 0, len(labels)+1)
	for _, v := range labels {
		labelValues = append(labelValues, v.GetValue())
	}
	labelValues = append(labelValues, strconv.Itoa(workerId))
	return labelValues
}

func buildLabelNames(metric *dto.Metric) []string {
	labels := metric.GetLabel()
	sort.Slice(labels, func(i, j int) bool {
		return labels[i].GetName() < labels[j].GetName()
	})
	labelNames := make([]string, 0, len(labels)+1)
	for _, v := range labels {
		labelNames = append(labelNames, v.GetName())
	}
	labelNames = append(labelNames, "worker_id")
	return labelNames
}

func (e *Exporter) scrape(ch chan<- prometheus.Metric) (up float64) {
	// TODO: use a worker pool
	for workerId, fetcher := range e.fetchers {
		body, err := fetcher()
		if err != nil {
			log.Errorf("Can't scrape worker #%d: %v", workerId, err)
			return 0
		}
		textParser := expfmt.TextParser{}
		metricFamilies, err := textParser.TextToMetricFamilies(body)
		if err != nil {
			log.Errorf("Can't parse worker #%d: %v", workerId, err)
		}

		for k, v := range metricFamilies {
			switch v.GetType() {
			case dto.MetricType_COUNTER:
				for _, metric := range v.GetMetric() {
					desc, ok := metrics[k]
					if !ok {
						desc = prometheus.NewDesc(k, v.GetHelp(), buildLabelNames(metric), nil)
					}
					ch <- prometheus.MustNewConstMetric(desc, prometheus.CounterValue, metric.GetCounter().GetValue(), buildLabelValues(metric, workerId)...)
				}
			case dto.MetricType_GAUGE:
				for _, metric := range v.GetMetric() {
					desc, ok := metrics[k]
					if !ok {
						desc = prometheus.NewDesc(k, v.GetHelp(), buildLabelNames(metric), nil)
					}
					ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, metric.GetGauge().GetValue(), buildLabelValues(metric, workerId)...)
				}
			}
		}

		defer body.Close()
	}
	return 1
}

func main() {
	var (
		listenAddress       = kingpin.Flag("web.listen-address", "Address to listen on for web interface and telemetry.").Default(":9101").String()
		metricsPath         = kingpin.Flag("web.telemetry-path", "Path under which to expose metrics.").Default("/metrics").String()
		fluentdScrapeURI    = kingpin.Flag("fluentd.scrape-uri", "URI on which to scrape Fluentd without the port").Default("http://127.0.0.1").String()
		fluentdStartingPort = kingpin.Flag("fluentd.starting-port", "Starting port of workers").Default("24231").Int()
		fluentdWorkers      = kingpin.Flag("fluentd.workers", "Number of workers").Default("1").Int()
	)

	log.AddFlags(kingpin.CommandLine)
	kingpin.Version(version.Print("fluentd_exporter"))
	kingpin.HelpFlag.Short('h')
	kingpin.Parse()

	log.Infoln("Starting haproxy_exporter", version.Info())
	log.Infoln("Build context", version.BuildContext())

	exporter, err := NewExporter(*fluentdScrapeURI, *fluentdStartingPort, *fluentdWorkers)
	if err != nil {
		log.Fatal(err)
	}
	prometheus.MustRegister(exporter)
	prometheus.MustRegister(version.NewCollector("fluentd_exporter"))

	log.Infoln("Listening on", *listenAddress)
	http.Handle(*metricsPath, promhttp.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
             <head><title>Fluentd Exporter</title></head>
             <body>
             <h1>Fluentd Exporter</h1>
             <p><a href='` + *metricsPath + `'>Metrics</a></p>
             </body>
             </html>`))
	})
	log.Fatal(http.ListenAndServe(*listenAddress, nil))

}
