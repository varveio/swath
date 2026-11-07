// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package metrics is the replay server's meter registry and its JSON
// snapshot, the schema-2 payload of GET /metrics.
//
// A meter is declared once as a Def (name, kind, tag keys) and
// instantiated per tag-value combination on first use, so adding a meter
// is adding a Def. A snapshot lists meters by name and then by tags, the
// same order every time, so two scrapes diff cleanly.
//
// Considered: expvar (no tags, no timers, its own JSON shape) and the
// Prometheus and OpenTelemetry clients (a module each, and neither renders
// the schema-2 envelope the bench scripts and dashboards read).
package metrics

import (
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Kind is a meter's type, as the snapshot names it.
type Kind int

// The kinds.
const (
	Counter Kind = iota
	Timer
	Distribution
	Gauge
)

func (k Kind) String() string {
	return [...]string{"counter", "timer", "distribution", "gauge"}[k]
}

// Def declares a meter: its name, kind and tag keys (values are given in
// this order when the meter is used).
type Def struct {
	Name string
	Kind Kind
	Tags []string
	// NoPercentiles marks a timer that publishes no percentiles: its
	// p50_ms and p99_ms are null, as on the Java server's timers built
	// without published percentiles (the one-off index load and sort).
	NoPercentiles bool
}

// Meter is one instantiated meter.
type Meter struct {
	reg   *Registry
	def   Def
	tags  []string // values, in def.Tags order
	sort  string
	count atomic.Int64
	// timers: the sum in nanoseconds over the meter's life; the max and
	// percentiles over the window (window.go).
	sum atomic.Int64
	// Under mu: distribution totals, the timer window, a gauge's or
	// function counter's source.
	mu      sync.Mutex
	fsum    float64
	fmax    float64
	window  *timeWindow
	gauge   func() float64
	counter func() int64
}

// Add adds n to a counter.
func (m *Meter) Add(n int64) { m.count.Add(n) }

// Inc adds one to a counter.
func (m *Meter) Inc() { m.count.Add(1) }

// Record records one timer sample.
func (m *Meter) Record(d time.Duration) {
	m.count.Add(1)
	m.sum.Add(int64(d))
	now := m.reg.now()
	m.mu.Lock()
	if m.window == nil {
		m.window = newTimeWindow(now)
	}
	m.window.record(now, int64(d))
	m.mu.Unlock()
}

// Observe records one distribution sample.
func (m *Meter) Observe(v float64) {
	m.mu.Lock()
	m.count.Add(1)
	m.fsum += v
	m.fmax = max(m.fmax, v)
	m.mu.Unlock()
}

// Count is a counter's value (a function counter's included) or a
// timer's or distribution's sample count.
func (m *Meter) Count() int64 {
	m.mu.Lock()
	fn := m.counter
	m.mu.Unlock()
	n := m.count.Load()
	if fn != nil {
		n += fn()
	}
	return n
}

// Registry holds the meters.
type Registry struct {
	mu     sync.Mutex
	meters map[string]*Meter
	// clock is the time timer windows rotate on; nil is time.Now.
	clock func() time.Time
}

func (r *Registry) now() time.Time {
	if r.clock != nil {
		return r.clock()
	}
	return time.Now()
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry { return &Registry{meters: map[string]*Meter{}} }

// Get returns the meter for d with the given tag values, creating it on
// first use.
func (r *Registry) Get(d Def, tagValues ...string) *Meter {
	if len(tagValues) != len(d.Tags) {
		panic(fmt.Sprintf("metrics: %s takes tags %v, got %d values", d.Name, d.Tags, len(tagValues)))
	}
	id := d.Name + "\x00" + strings.Join(tagValues, "\x00")
	r.mu.Lock()
	defer r.mu.Unlock()
	m := r.meters[id]
	if m == nil {
		m = &Meter{reg: r, def: d, tags: slices.Clone(tagValues)}
		m.sort = sortKey(m)
		r.meters[id] = m
	}
	return m
}

// Find returns the meter if it exists, nil otherwise.
func (r *Registry) Find(d Def, tagValues ...string) *Meter {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.meters[d.Name+"\x00"+strings.Join(tagValues, "\x00")]
}

// SetGauge registers a gauge read from fn at every snapshot.
func (r *Registry) SetGauge(d Def, fn func() float64, tagValues ...string) {
	m := r.Get(d, tagValues...)
	m.mu.Lock()
	m.gauge = fn
	m.mu.Unlock()
}

// SetCounter registers a counter read from fn at every snapshot, for a
// count another package keeps (the store's and its descriptor cache's).
func (r *Registry) SetCounter(d Def, fn func() int64, tagValues ...string) {
	m := r.Get(d, tagValues...)
	m.mu.Lock()
	m.counter = fn
	m.mu.Unlock()
}

// sortedTags returns the meter's tags sorted by key.
func (m *Meter) sortedTags() [][2]string {
	t := make([][2]string, len(m.tags))
	for i, v := range m.tags {
		t[i] = [2]string{m.def.Tags[i], v}
	}
	slices.SortFunc(t, func(a, b [2]string) int { return strings.Compare(a[0], b[0]) })
	return t
}

// sortKey orders meters of one name by their tag list spelled
// "[tag(k=v), tag(k=v)]", the order the Java server's snapshot uses.
func sortKey(m *Meter) string {
	var b strings.Builder
	b.WriteByte('[')
	for i, kv := range m.sortedTags() {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString("tag(" + kv[0] + "=" + kv[1] + ")")
	}
	b.WriteByte(']')
	return b.String()
}

// AppendMeters appends the snapshot's meters array, "[...]".
func (r *Registry) AppendMeters(dst []byte) []byte {
	r.mu.Lock()
	ms := make([]*Meter, 0, len(r.meters))
	for _, m := range r.meters {
		ms = append(ms, m)
	}
	r.mu.Unlock()
	slices.SortFunc(ms, func(a, b *Meter) int {
		if c := strings.Compare(a.def.Name, b.def.Name); c != 0 {
			return c
		}
		return strings.Compare(a.sort, b.sort)
	})
	dst = append(dst, '[')
	for i, m := range ms {
		if i > 0 {
			dst = append(dst, ',')
		}
		dst = m.appendJSON(dst)
	}
	return append(dst, ']')
}

func (m *Meter) appendJSON(dst []byte) []byte {
	dst = append(dst, `{"name":`...)
	dst = AppendString(dst, m.def.Name)
	dst = append(dst, `,"type":`...)
	dst = AppendString(dst, m.def.Kind.String())
	dst = append(dst, `,"tags":{`...)
	for i, kv := range m.sortedTags() {
		if i > 0 {
			dst = append(dst, ',')
		}
		dst = AppendString(dst, kv[0])
		dst = append(dst, ':')
		dst = AppendString(dst, kv[1])
	}
	dst = append(dst, '}')
	switch m.def.Kind {
	case Counter:
		dst = number(dst, "count", float64(m.Count()))
	case Timer:
		n := m.count.Load()
		sum := float64(m.sum.Load()) / 1e6
		dst = append(dst, `,"count":`...)
		dst = strconv.AppendInt(dst, n, 10)
		dst = number(dst, "sum_ms", sum)
		mean := 0.0
		if n > 0 {
			mean = sum / float64(n)
		}
		dst = number(dst, "mean_ms", mean)
		var maxv, p50, p99 int64
		m.mu.Lock()
		if m.window != nil {
			maxv, p50, p99 = m.window.read(m.reg.now())
		}
		m.mu.Unlock()
		dst = number(dst, "max_ms", float64(maxv)/1e6)
		if m.def.NoPercentiles {
			dst = append(dst, `,"p50_ms":null,"p99_ms":null`...)
		} else {
			dst = number(dst, "p50_ms", float64(p50)/1e6)
			dst = number(dst, "p99_ms", float64(p99)/1e6)
		}
	case Distribution:
		m.mu.Lock()
		n, total, mx := m.count.Load(), m.fsum, m.fmax
		m.mu.Unlock()
		dst = append(dst, `,"count":`...)
		dst = strconv.AppendInt(dst, n, 10)
		dst = number(dst, "total", total)
		mean := 0.0
		if n > 0 {
			mean = total / float64(n)
		}
		dst = number(dst, "mean", mean)
		dst = number(dst, "max", mx)
	case Gauge:
		m.mu.Lock()
		fn := m.gauge
		m.mu.Unlock()
		v := math.NaN()
		if fn != nil {
			v = fn()
		}
		dst = number(dst, "value", v)
	}
	return append(dst, '}')
}

// number appends ,"field":v with six decimals, or null for a value that is
// not a number.
func number(dst []byte, field string, v float64) []byte {
	dst = append(dst, ',', '"')
	dst = append(dst, field...)
	dst = append(dst, '"', ':')
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return append(dst, "null"...)
	}
	return strconv.AppendFloat(dst, v, 'f', 6, 64)
}

// AppendString appends s as a JSON string: '"', '\\', newline, CR and tab
// escaped by name, other control characters as \u00XX, everything else as
// it is.
func AppendString(dst []byte, s string) []byte {
	dst = append(dst, '"')
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case c == '"':
			dst = append(dst, `\"`...)
		case c == '\\':
			dst = append(dst, `\\`...)
		case c == '\n':
			dst = append(dst, `\n`...)
		case c == '\r':
			dst = append(dst, `\r`...)
		case c == '\t':
			dst = append(dst, `\t`...)
		case c < 0x20:
			dst = append(dst, fmt.Sprintf(`\u%04x`, c)...)
		default:
			dst = append(dst, c)
		}
	}
	return append(dst, '"')
}
