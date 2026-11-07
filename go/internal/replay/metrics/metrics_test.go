// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestSnapshotIsStableAndTyped(t *testing.T) {
	r := NewRegistry()
	r.Get(ProtocolObjects, "s3", "page").Add(3)
	r.Get(HTTPRequests).Inc()
	r.Get(HTTPRequests).Inc()
	for _, d := range []time.Duration{time.Millisecond, 3 * time.Millisecond, 2 * time.Millisecond} {
		r.Get(RequestLatency, "worker_page").Record(d)
	}
	r.Get(RequestLatency, "pivot_probe")
	r.Get(InjectOverrunMs, "worker_page").Observe(2.5)
	r.SetGauge(ResponseActive, func() float64 { return 4 })
	r.Get(ProtocolHTTPRequests, "s3", "2xx").Inc()
	r.Get(ProtocolHTTPRequests, "s3", "4xx").Inc()

	a, b := string(r.AppendMeters(nil)), string(r.AppendMeters(nil))
	if a != b {
		t.Fatal("two snapshots differ")
	}
	var meters []map[string]any
	if err := json.Unmarshal([]byte(a), &meters); err != nil {
		t.Fatalf("%v: %s", err, a)
	}
	var names []string
	for _, m := range meters {
		names = append(names, m["name"].(string)+fmtTags(m["tags"]))
	}
	want := "swath.replay.http.requests{} swath.replay.inject.overrun.ms{shape=worker_page} " +
		"swath.replay.protocol.http.requests{protocol=s3,status_class=2xx} swath.replay.protocol.http.requests{protocol=s3,status_class=4xx} " +
		"swath.replay.protocol.objects{protocol=s3,shape=page} swath.replay.request.latency{shape=pivot_probe} " +
		"swath.replay.request.latency{shape=worker_page} swath.replay.response.active{}"
	if got := strings.Join(names, " "); got != want {
		t.Errorf("order:\n%s\nwant\n%s", got, want)
	}
	if !strings.Contains(a, `{"name":"swath.replay.http.requests","type":"counter","tags":{},"count":2.000000}`) {
		t.Errorf("counter: %s", a)
	}
	if !strings.Contains(a, `"tags":{"shape":"worker_page"},"count":3,"sum_ms":6.000000,"mean_ms":2.000000,"max_ms":3.000000,"p50_ms":2.031615,"p99_ms":3.000000}`) {
		t.Errorf("timer: %s", a)
	}
	if !strings.Contains(a, `"tags":{"shape":"pivot_probe"},"count":0,"sum_ms":0.000000,"mean_ms":0.000000`) {
		t.Errorf("empty timer: %s", a)
	}
	if !strings.Contains(a, `"type":"gauge","tags":{},"value":4.000000}`) {
		t.Errorf("gauge: %s", a)
	}
}

func fmtTags(v any) string {
	m := v.(map[string]any)
	var keys []string
	for k := range m {
		keys = append(keys, k)
	}
	var parts []string
	for _, k := range keys {
		parts = append(parts, k+"="+m[k].(string))
	}
	slices.Sort(parts)
	return "{" + strings.Join(parts, ",") + "}"
}

func TestAppendString(t *testing.T) {
	got := string(AppendString(nil, "a\"b\\c\n\r\t\x01é"))
	if got != `"a\"b\\c\n\r\t\u0001é"` {
		t.Errorf("%s", got)
	}
}

// probeField returns one field of the first meter named name in a
// snapshot.
func probeField(t *testing.T, r *Registry, name, field string) string {
	t.Helper()
	s := string(r.AppendMeters(nil))
	i := strings.Index(s, `"name":"`+name+`"`)
	if i < 0 {
		t.Fatalf("no %s in %s", name, s)
	}
	j := strings.Index(s[i:], `"`+field+`":`)
	if j < 0 {
		t.Fatalf("no %s.%s in %s", name, field, s)
	}
	rest := s[i+j+len(field)+3:]
	return rest[:strings.IndexAny(rest, ",}")]
}

// TestWindowsFollowTheJavaRegistry pins the two windows of the Java
// server's registry (Micrometer's TimeWindowMax and
// TimeWindowPercentileHistogram under SimpleMeterRegistry: a one-minute
// expiry, three buffers) with a controlled clock, on the probe the two
// servers were compared on: 900 ms at 0 s and 10 ms at 79 s. At 79 s the
// percentiles describe only the 10 ms sample while the maximum is still
// 900 ms; at 121 s the maximum is still 900 ms and the percentiles are
// empty; at 181 s the maximum is 10 ms, at 241 s nothing. Count and sum
// are lifetime totals throughout. A distribution's maximum follows the
// same window; the one-off timers publish no percentiles.
func TestWindowsFollowTheJavaRegistry(t *testing.T) {
	start := time.Unix(1_000_000, 0)
	now := start
	r := NewRegistry()
	r.clock = func() time.Time { return now }
	at := func(s int) { now = start.Add(time.Duration(s) * time.Second) }
	timer := r.Get(RequestLatency, "worker_page")
	dist := r.Get(IndexEntries)
	timer.Record(900 * time.Millisecond)
	dist.Observe(900)
	at(79)
	timer.Record(10 * time.Millisecond)
	dist.Observe(10)
	const name = "swath.replay.request.latency"
	for _, c := range []struct {
		at                                 int
		max, p50, p99, distMax, count, sum string
	}{
		{79, "900.000000", "10.000000", "10.000000", "900.000000", "2", "910.000000"},
		{121, "900.000000", "0.000000", "0.000000", "900.000000", "2", "910.000000"},
		{181, "10.000000", "0.000000", "0.000000", "10.000000", "2", "910.000000"},
		{241, "0.000000", "0.000000", "0.000000", "0.000000", "2", "910.000000"},
	} {
		at(c.at)
		got := []string{probeField(t, r, name, "max_ms"), probeField(t, r, name, "p50_ms"), probeField(t, r, name, "p99_ms"),
			probeField(t, r, "swath.replay.index.entries", "max"), probeField(t, r, name, "count"), probeField(t, r, name, "sum_ms")}
		want := []string{c.max, c.p50, c.p99, c.distMax, c.count, c.sum}
		if !slices.Equal(got, want) {
			t.Errorf("at %d s: max, p50, p99, distribution max, count, sum %v, want %v", c.at, got, want)
		}
	}
	r.Get(IndexLoadLatency, "derived").Record(time.Second)
	if s := string(r.AppendMeters(nil)); !strings.Contains(s, `"tags":{"source":"derived"},"count":1,"sum_ms":1000.000000,"mean_ms":1000.000000,"max_ms":1000.000000,"p50_ms":null,"p99_ms":null}`) {
		t.Errorf("index load: %s", s)
	}
}

// TestWindowRotationRules pins the two rotations' details on their own
// clocks: the maximum rotates once per minute and clears everything after
// three idle minutes, re-anchoring its clock to the step; the histogram
// clears at most three buffers per call and its clock catches up one
// 20-second step per buffer cleared, so a sample recorded after a long
// idle spell is cleared again by the next calls until the clock catches
// up.
func TestWindowRotationRules(t *testing.T) {
	m := newWindowMax(0)
	m.record(0, 5)
	if m.read(59_999) != 5 || m.read(60_000) != 5 || m.cur != 1 || m.last != 60_000 {
		t.Errorf("max after one rotation: %+v", m)
	}
	m2 := newWindowMax(0)
	m2.record(0, 5)
	m2.read(190_000) // 190 s idle: all cleared, the clock re-anchored to 190 - 190 % 60 = 180 s
	if m2.ring != [windowBuffers]float64{} || m2.cur != 0 || m2.last != 180_000 {
		t.Errorf("max after an idle spell: %+v", m2)
	}

	h := newWindowHistogram(0)
	h.record(0, 1)
	h.read(600_000) // ten idle minutes: three buffers cleared, the clock at 60 s
	if h.last != 60_000 || h.ring[h.cur].n != 0 {
		t.Errorf("histogram after an idle spell: last %d", h.last)
	}
	h.record(600_000, 2) // clears three more first, clock at 120 s, then records
	if p50, _ := h.read(600_000); p50 != 0 || h.last != 180_000 {
		t.Errorf("a sample after an idle spell survives the catch-up: p50 %d, last %d", p50, h.last)
	}
}

// TestHistogramBuckets pins the percentile error bound: every value lands
// in a bucket whose upper bound is at most 1/32 above it.
func TestHistogramBuckets(t *testing.T) {
	prev := -1
	for _, v := range []int64{0, 1, 31, 32, 33, 63, 64, 65, 1000, 999_999, 1_000_000, 123_456_789, 1<<43 - 1} {
		b := bucketOf(v)
		up := bucketUpper(b)
		if b < prev || up < v || float64(up-v) > float64(v)/32 {
			t.Errorf("%d: bucket %d upper %d", v, b, up)
		}
		prev = b
	}
	if bucketOf(1<<50) != numBuckets-1 || bucketOf(-5) != 0 {
		t.Error("out-of-range values")
	}
	var h histogram
	for i := range 1000 {
		h.record(int64(i+1) * 1000)
	}
	if p := h.percentile(0.5); p < 500_000 || p > 500_000*33/32 {
		t.Errorf("p50 %d", p)
	}
	if p := h.percentile(0.99); p < 990_000 || p > 990_000*33/32 {
		t.Errorf("p99 %d", p)
	}
}

// TestCounterFunc reads a function counter at every snapshot, added to
// anything counted on the meter itself.
func TestCounterFunc(t *testing.T) {
	r := NewRegistry()
	var n int64 = 5
	r.SetCounter(HandleCacheOpens, func() int64 { return n })
	n = 7
	if s := string(r.AppendMeters(nil)); s != `[{"name":"swath.replay.handle_cache.opens","type":"counter","tags":{},"count":7.000000}]` {
		t.Error(s)
	}
}
