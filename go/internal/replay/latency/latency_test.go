// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package latency

import (
	"math"
	"testing"
	"time"
)

func TestClassify(t *testing.T) {
	for _, c := range []struct {
		delim bool
		max   int64
		want  Shape
	}{
		{true, 1000, StructureProbe},
		{true, 1, StructureProbe}, // a delimiter wins over the probe size
		{false, 1, PivotProbe},
		{false, 0, PivotProbe},
		{false, 2, WorkerPage},
		{false, 1000, WorkerPage},
	} {
		if got := Classify(c.delim, c.max); got != c.want {
			t.Errorf("Classify(%v, %d) = %v, want %v", c.delim, c.max, got, c.want)
		}
	}
}

func mustParse(t *testing.T, spec string, jitter, scale float64) *Profile {
	t.Helper()
	p, err := Parse(spec, jitter, scale)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestDelayByShapeAndPerCommonPrefix(t *testing.T) {
	p := mustParse(t, "prod-commoncrawl", 0, 1)
	if d := p.Delay(StructureProbe, 121, 1); d != (223+55*121)*time.Millisecond {
		t.Errorf("structure probe at 121 prefixes: %v", d)
	}
	if d := p.Delay(StructureProbe, 0, 1); d != 223*time.Millisecond {
		t.Errorf("structure probe at 0 prefixes: %v", d)
	}
	if d := p.Delay(WorkerPage, 0, 1); d != 223*time.Millisecond {
		t.Errorf("worker page: %v", d)
	}
	if d := p.Delay(PivotProbe, 0, 1); d != 121*time.Millisecond {
		t.Errorf("pivot probe: %v", d)
	}
	p = mustParse(t, "structure_probe=1.5s+40ms/cp,pivot_probe=120ms,worker_page=200", 0, 1)
	if p.Delay(StructureProbe, 10, 0) != 1900*time.Millisecond || p.Delay(PivotProbe, 0, 0) != 120*time.Millisecond ||
		p.Delay(WorkerPage, 0, 0) != 200*time.Millisecond {
		t.Errorf("custom spec: %v %v %v", p.Delay(StructureProbe, 10, 0), p.Delay(PivotProbe, 0, 0), p.Delay(WorkerPage, 0, 0))
	}
	p = mustParse(t, "structure=6s", 0, 1)
	if p.Delay(WorkerPage, 0, 0) != 0 {
		t.Error("an unlisted shape was delayed")
	}
	p = mustParse(t, "structure_probe=6.865s+55ms/cp,worker_page=223ms", 0, 1)
	if p.Delay(StructureProbe, 7, 0) != (6865+55*7)*time.Millisecond {
		t.Errorf("unscaled is not exact: %v", p.Delay(StructureProbe, 7, 0))
	}
	var nilProfile *Profile
	if nilProfile.Delay(WorkerPage, 0, 0) != 0 {
		t.Error("a nil profile delayed")
	}
}

func TestScaleDividesEveryTerm(t *testing.T) {
	p := mustParse(t, "prod-commoncrawl", 0, 50)
	if d := p.Delay(StructureProbe, 121, 0); d != time.Duration(223_000_000/50+121*(55_000_000/50)) {
		t.Errorf("scaled structure probe: %v", d)
	}
	if d := p.Delay(WorkerPage, 0, 0); d != time.Duration(223_000_000/50) {
		t.Errorf("scaled page: %v", d)
	}
}

func TestParseRejects(t *testing.T) {
	for _, c := range []struct {
		spec          string
		jitter, scale float64
	}{
		{"prod-commoncrawl", 0, 0}, {"prod-commoncrawl", 0, -50}, {"prod-commoncrawl", 0, math.NaN()},
		{"prod-commoncrawl", 0, math.Inf(1)}, {"", 0, 50}, {"worker_page", 0, 1}, {"bogus_shape=1s", 0, 1},
		{"worker_page=fast", 0, 1}, {"worker_page=100ms+5ms/cp", 0, 1}, {"structure_probe=100ms+5ms", 0, 1},
		{"worker_page=-1s", 0, 1}, {"worker_page=1s", 1, 1}, {"worker_page=1s", math.NaN(), 1},
		{"worker_page=0x10ms", 0, 1}, {"worker_page=infms", 0, 1}, {"worker_page=1_000ms", 0, 1},
	} {
		if _, err := Parse(c.spec, c.jitter, c.scale); err == nil {
			t.Errorf("Parse(%q, %v, %v) accepted", c.spec, c.jitter, c.scale)
		}
	}
	for _, spec := range []string{"", "  "} {
		if p, err := Parse(spec, 0, 1); p != nil || err != nil {
			t.Errorf("blank %q: %v %v", spec, p, err)
		}
	}
	// The number grammar accepts a type suffix and an exponent.
	p := mustParse(t, "page=1.5e3ms,pivot=2dms,structure=.5s", 0, 1)
	if p.Delay(WorkerPage, 0, 0) != 1500*time.Millisecond || p.Delay(PivotProbe, 0, 0) != 2*time.Millisecond ||
		p.Delay(StructureProbe, 0, 0) != 500*time.Millisecond {
		t.Error("number grammar")
	}
}

func TestJitterIsDeterministicAndBounded(t *testing.T) {
	p := mustParse(t, "worker_page=1000ms", 0.2, 1)
	for key := range int64(1000) {
		d := p.Delay(WorkerPage, 0, key*7919)
		if d != p.Delay(WorkerPage, 0, key*7919) {
			t.Fatal("same key, different delay")
		}
		if d < 800*time.Millisecond || d > 1200*time.Millisecond {
			t.Fatalf("key %d: %v outside ±20%%", key, d)
		}
	}
	if p.Delay(WorkerPage, 0, 1) == p.Delay(WorkerPage, 0, 2) {
		t.Error("adjacent keys drew the same jitter")
	}
}

// TestNumberGrammar accepts what the replay's delay grammar accepts: hex
// floats with a binary exponent, a type suffix, control characters around
// the number, digits from other BMP scripts in a bare millisecond count.
func TestNumberGrammar(t *testing.T) {
	for spec, want := range map[string]time.Duration{
		"worker_page=0x1p0s":           time.Second,
		"worker_page=0X1.8P1MS":        3 * time.Millisecond,
		"worker_page=1.5e3ms":          1500 * time.Millisecond,
		"worker_page=2dms":             2 * time.Millisecond,
		"worker_page=.5s":              500 * time.Millisecond,
		"worker_page=١٢":               12 * time.Millisecond,
		"worker_page=-0ms":             0,
		"worker_page=1e400ms":          math.MaxInt64,
		"worker_page=0x1p2000s":        math.MaxInt64,
		"worker_page=9223372036854775": math.MaxInt64, // milliseconds beyond the nanosecond range
	} {
		p := mustParse(t, spec, 0, 1)
		if got := p.Delay(WorkerPage, 0, 0); got != want {
			t.Errorf("%s: %v, want %v", spec, got, want)
		}
	}
	for _, spec := range []string{"worker_page=nanms", "worker_page=infinityms", "worker_page=0x1ms", "worker_page=1e5",
		"worker_page=-1", "worker_page=-0x1p0s"} {
		if _, err := Parse(spec, 0, 1); err == nil {
			t.Errorf("%s accepted", spec)
		}
	}
}

// TestHugeDelaysSaturate: scaling, the per-prefix term and jitter never
// turn a positive profile into a zero or negative delay (which the route
// reads as no injection).
func TestHugeDelaysSaturate(t *testing.T) {
	p := mustParse(t, "worker_page=1s", 0, 1e-12)
	if d := p.Delay(WorkerPage, 0, 0); d != math.MaxInt64 {
		t.Errorf("1s scaled by 1e-12: %v", d)
	}
	p = mustParse(t, "structure_probe=1s+100000000000ms/cp", 0, 1)
	if d := p.Delay(StructureProbe, 1_000_000, 0); d != math.MaxInt64 {
		t.Errorf("per-prefix overflow: %v", d)
	}
	p = mustParse(t, "structure_probe=9223372036s+1s/cp", 0, 1)
	if d := p.Delay(StructureProbe, 10, 0); d != math.MaxInt64 {
		t.Errorf("base plus per-prefix overflow: %v", d)
	}
	p = mustParse(t, "worker_page=9223372036854775807ms", 0.9, 1)
	for key := range int64(200) {
		if d := p.Delay(WorkerPage, 0, key); d <= 0 {
			t.Fatalf("jittered saturated delay %v for key %d", d, key)
		}
	}
}
