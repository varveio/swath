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

// TestTimerWindowCoversTheLastTwoMinutes pins the timer window: the max
// and percentiles cover samples from the last 80 to 120 seconds, rotating
// every 40 seconds, while count and sum cover the meter's life; the
// one-off timers publish no percentiles.
func TestTimerWindowCoversTheLastTwoMinutes(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	r := NewRegistry()
	r.clock = func() time.Time { return now }
	m := r.Get(RequestLatency, "worker_page")
	m.Record(900 * time.Millisecond)
	field := func(name string) string {
		s := string(r.AppendMeters(nil))
		i := strings.Index(s, `"`+name+`":`)
		if i < 0 {
			t.Fatalf("no %s in %s", name, s)
		}
		rest := s[i+len(name)+3:]
		return rest[:strings.IndexAny(rest, ",}")]
	}
	now = now.Add(79 * time.Second)
	m.Record(10 * time.Millisecond)
	if field("max_ms") != "900.000000" || field("p99_ms") != "900.000000" {
		t.Errorf("at 79 s: max %s p99 %s, want the 900 ms sample", field("max_ms"), field("p99_ms"))
	}
	now = now.Add(40 * time.Second) // 119 s: the buffer read still holds the first sample
	if field("max_ms") != "900.000000" {
		t.Errorf("at 119 s: max %s, want the 900 ms sample", field("max_ms"))
	}
	now = now.Add(2 * time.Second) // 121 s: the buffer read began at 40 s
	if field("max_ms") != "10.000000" || field("count") != "2" || field("sum_ms") != "910.000000" {
		t.Errorf("at 121 s: max %s count %s sum %s", field("max_ms"), field("count"), field("sum_ms"))
	}
	now = now.Add(windowExpiry)
	if field("max_ms") != "0.000000" || field("p50_ms") != "0.000000" || field("count") != "2" {
		t.Errorf("after an idle window: max %s p50 %s count %s", field("max_ms"), field("p50_ms"), field("count"))
	}
	r.Get(IndexLoadLatency, "derived").Record(time.Second)
	if s := string(r.AppendMeters(nil)); !strings.Contains(s, `"tags":{"source":"derived"},"count":1,"sum_ms":1000.000000,"mean_ms":1000.000000,"max_ms":1000.000000,"p50_ms":null,"p99_ms":null}`) {
		t.Errorf("index load: %s", s)
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
