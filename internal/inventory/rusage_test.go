// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package inventory

import (
	"encoding/json"
	"runtime"
	"testing"
	"time"
)

// TestRusageFieldShape (R11): a platform without the measurements reports
// rusage_available false and leaves the CPU times and peak RSS out, never
// zeros; a measured zero is still reported.
func TestRusageFieldShape(t *testing.T) {
	fields := func(r *Report) (timings, process, resources map[string]any) {
		b, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		var m struct {
			Timings     map[string]any
			Process     map[string]any
			Diagnostics struct{ Resources map[string]any }
		}
		if err := json.Unmarshal(b, &m); err != nil {
			t.Fatal(err)
		}
		return m.Timings, m.Process, m.Diagnostics.Resources
	}
	var r Report
	r.setRusage(0, 0, 0, false)
	tm, proc, res := fields(&r)
	for _, k := range []string{"user_cpu_ms", "sys_cpu_ms"} {
		if v, ok := tm[k]; ok {
			t.Errorf("unavailable: timings.%s = %v, want absent", k, v)
		}
	}
	if v, ok := proc["peak_rss_bytes"]; ok {
		t.Errorf("unavailable: process.peak_rss_bytes = %v, want absent", v)
	}
	if res["rusage_available"] != false {
		t.Errorf("unavailable: diagnostics.resources.rusage_available = %v, want false", res["rusage_available"])
	}
	r.setRusage(0, 1500*time.Millisecond, 4096, true)
	tm, proc, res = fields(&r)
	if tm["user_cpu_ms"] != 0.0 || tm["sys_cpu_ms"] != 1500.0 || proc["peak_rss_bytes"] != 4096.0 || res["rusage_available"] != true {
		t.Errorf("measured: timings %v, process %v, resources %v", tm, proc, res)
	}
	// This platform's own measurement (linux, darwin and windows have one).
	r = Report{}
	r.setRusage(rusage())
	switch runtime.GOOS {
	case "linux", "darwin", "windows":
		if !r.Resources.RusageAvailable || r.Process.PeakRSSBytes == nil || *r.Process.PeakRSSBytes <= 0 {
			t.Errorf("%s: no peak RSS measured: %+v", runtime.GOOS, r.Resources)
		}
	default:
		if r.Resources.RusageAvailable {
			t.Errorf("%s: claims a measurement", runtime.GOOS)
		}
	}
}
