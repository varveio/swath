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
	if !strings.Contains(a, `"tags":{"shape":"worker_page"},"count":3,"sum_ms":6.000000,"mean_ms":2.000000,"max_ms":3.000000,"p50_ms":2.000000,"p99_ms":3.000000}`) {
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
