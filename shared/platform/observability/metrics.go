package observability

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Registry stores service metrics in Prometheus text format.
type Registry struct {
	mu         sync.Mutex
	counters   map[string]*Counter
	gauges     map[string]*Gauge
	histograms map[string]*Histogram
}

// NewRegistry returns an empty metrics registry.
func NewRegistry() *Registry {
	return &Registry{
		counters:   map[string]*Counter{},
		gauges:     map[string]*Gauge{},
		histograms: map[string]*Histogram{},
	}
}

// Counter returns or creates a counter with the supplied name.
func (r *Registry) Counter(name, help string) *Counter {
	r.mu.Lock()
	defer r.mu.Unlock()
	if metric, ok := r.counters[name]; ok {
		return metric
	}
	metric := &Counter{name: name, help: help}
	r.counters[name] = metric
	return metric
}

// Gauge returns or creates a gauge with the supplied name.
func (r *Registry) Gauge(name, help string) *Gauge {
	r.mu.Lock()
	defer r.mu.Unlock()
	if metric, ok := r.gauges[name]; ok {
		return metric
	}
	metric := &Gauge{name: name, help: help}
	r.gauges[name] = metric
	return metric
}

// Histogram returns or creates a histogram with the supplied name.
func (r *Registry) Histogram(name, help string, buckets []float64) *Histogram {
	r.mu.Lock()
	defer r.mu.Unlock()
	if metric, ok := r.histograms[name]; ok {
		return metric
	}
	sorted := append([]float64(nil), buckets...)
	sort.Float64s(sorted)
	metric := &Histogram{name: name, help: help, buckets: sorted}
	r.histograms[name] = metric
	return metric
}

// Render serializes the registry into Prometheus text format.
func (r *Registry) Render() string {
	r.mu.Lock()
	defer r.mu.Unlock()

	var builder strings.Builder
	for _, name := range sortedCounterNames(r.counters) {
		metric := r.counters[name]
		fmt.Fprintf(&builder, "# HELP %s %s\n", metric.name, metric.help)
		fmt.Fprintf(&builder, "# TYPE %s counter\n", metric.name)
		fmt.Fprintf(&builder, "%s %s\n", metric.name, strconv.FormatFloat(metric.Value(), 'f', -1, 64))
	}
	for _, name := range sortedGaugeNames(r.gauges) {
		metric := r.gauges[name]
		fmt.Fprintf(&builder, "# HELP %s %s\n", metric.name, metric.help)
		fmt.Fprintf(&builder, "# TYPE %s gauge\n", metric.name)
		fmt.Fprintf(&builder, "%s %s\n", metric.name, strconv.FormatFloat(metric.Value(), 'f', -1, 64))
	}
	for _, name := range sortedHistogramNames(r.histograms) {
		metric := r.histograms[name]
		fmt.Fprintf(&builder, "# HELP %s %s\n", metric.name, metric.help)
		fmt.Fprintf(&builder, "# TYPE %s histogram\n", metric.name)
		for _, line := range metric.renderLines() {
			builder.WriteString(line)
			builder.WriteByte('\n')
		}
	}
	return builder.String()
}

// Counter tracks monotonically increasing values.
type Counter struct {
	mu    sync.Mutex
	name  string
	help  string
	value float64
}

// Inc increments the counter by one.
func (c *Counter) Inc() {
	c.Add(1)
}

// Add increments the counter by the supplied value.
func (c *Counter) Add(delta float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.value += delta
}

// Value returns the current value.
func (c *Counter) Value() float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.value
}

// Gauge tracks an instantaneous value.
type Gauge struct {
	mu    sync.Mutex
	name  string
	help  string
	value float64
}

// Set updates the gauge.
func (g *Gauge) Set(value float64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.value = value
}

// Add increments the gauge.
func (g *Gauge) Add(delta float64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.value += delta
}

// Value returns the current gauge value.
func (g *Gauge) Value() float64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.value
}

// Histogram captures observation counts over a fixed bucket set.
type Histogram struct {
	mu      sync.Mutex
	name    string
	help    string
	buckets []float64
	counts  []uint64
	sum     float64
	count   uint64
}

// Observe records a new value.
func (h *Histogram) Observe(value float64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.sum += value
	h.count++
	for i, bucket := range h.buckets {
		if value <= bucket {
			h.counts[i]++
		}
	}
}

func (h *Histogram) renderLines() []string {
	h.mu.Lock()
	defer h.mu.Unlock()

	lines := make([]string, 0, len(h.buckets)+3)
	for i, bucket := range h.buckets {
		lines = append(lines, fmt.Sprintf("%s_bucket{le=\"%s\"} %d", h.name, strconv.FormatFloat(bucket, 'f', -1, 64), h.counts[i]))
	}
	lines = append(lines, fmt.Sprintf("%s_bucket{le=\"+Inf\"} %d", h.name, h.count))
	lines = append(lines, fmt.Sprintf("%s_sum %s", h.name, strconv.FormatFloat(h.sum, 'f', -1, 64)))
	lines = append(lines, fmt.Sprintf("%s_count %d", h.name, h.count))
	return lines
}

func sortedCounterNames(m map[string]*Counter) []string {
	names := make([]string, 0, len(m))
	for name := range m {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func sortedGaugeNames(m map[string]*Gauge) []string {
	names := make([]string, 0, len(m))
	for name := range m {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func sortedHistogramNames(m map[string]*Histogram) []string {
	names := make([]string, 0, len(m))
	for name := range m {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
