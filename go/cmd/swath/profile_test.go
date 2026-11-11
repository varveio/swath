// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/listing"
)

// profileSpan reads a pprof profile's start (time_nanos, field 9) and
// duration (duration_nanos, field 10) from its gzipped protobuf: the
// top-level fields only, without a pprof parser.
func profileSpan(t *testing.T, path string) (start time.Time, d time.Duration) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	zr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatalf("%s: %v", path, err)
	}
	b, err := io.ReadAll(zr)
	if err != nil {
		t.Fatalf("%s: %v", path, err)
	}
	var ns, dur int64
	for len(b) > 0 {
		key, n := binary.Uvarint(b)
		if n <= 0 {
			t.Fatalf("%s: malformed profile", path)
		}
		b = b[n:]
		switch field, wire := key>>3, key&7; wire {
		case 0:
			v, n := binary.Uvarint(b)
			if n <= 0 {
				t.Fatalf("%s: malformed varint", path)
			}
			b = b[n:]
			switch field {
			case 9:
				ns = int64(v)
			case 10:
				dur = int64(v)
			}
		case 2:
			l, n := binary.Uvarint(b)
			if n <= 0 || uint64(len(b)-n) < l {
				t.Fatalf("%s: malformed length", path)
			}
			b = b[n+int(l):]
		case 1:
			b = b[8:]
		case 5:
			b = b[4:]
		default:
			t.Fatalf("%s: wire type %d", path, wire)
		}
	}
	if ns == 0 {
		t.Fatalf("%s: no time_nanos", path)
	}
	return time.Unix(0, ns), time.Duration(dur)
}

// spin burns CPU for d, so a CPU profile has samples.
func spin(d time.Duration) {
	x := 0
	for end := time.Now().Add(d); time.Now().Before(end); {
		for i := range 10000 {
			x += i
		}
	}
	runtime.KeepAlive(x)
}

// TestProfilerPhases: the CPU profile is split at listingDone (FILE and
// FILE.listing before it, FILE.tail after it), the allocation profile is
// written at the boundary and at the end, and the account names them.
func TestProfilerPhases(t *testing.T) {
	dir := t.TempDir()
	cpu, mem := filepath.Join(dir, "cpu"), filepath.Join(dir, "mem")
	before := runtime.MemProfileRate
	defer func() { runtime.MemProfileRate = before }()
	p, err := startProfiles(cpu, mem, 4096, func(m string) { t.Error(m) })
	if err != nil {
		t.Fatal(err)
	}
	if runtime.MemProfileRate != 4096 {
		t.Fatalf("MemProfileRate %d, want 4096", runtime.MemProfileRate)
	}
	spin(50 * time.Millisecond)
	p.listingDone()
	p.listingDone() // once: a second call changes nothing
	a := p.account()
	boundary := a.PhaseBoundaryAt
	if boundary.IsZero() || a.CPUGapNs == nil || *a.CPUGapNs < 0 {
		t.Fatalf("account after the boundary: %+v", a)
	}
	spin(50 * time.Millisecond)
	p.stop()
	p.stop() // idempotent
	p.listingDone()

	ls, ld := profileSpan(t, cpu+phaseListing)
	ts, _ := profileSpan(t, cpu+phaseTail)
	if ls.After(boundary) || ls.Add(ld).After(ts) || ts.Before(boundary) {
		t.Fatalf("phases: listing %v+%v, boundary %v, tail from %v", ls, ld, boundary, ts)
	}
	if got := ts.Sub(boundary); got > time.Duration(*a.CPUGapNs)+time.Second {
		t.Fatalf("the tail started %v after the boundary, gap %dns", got, *a.CPUGapNs)
	}
	whole, _ := os.ReadFile(cpu)
	listing, _ := os.ReadFile(cpu + phaseListing)
	if len(whole) == 0 || !bytes.Equal(whole, listing) {
		t.Fatalf("FILE is not the listing phase: %d and %d bytes", len(whole), len(listing))
	}
	for _, path := range []string{mem + phaseListing, mem + phaseExit, mem} {
		profileSpan(t, path)
	}
	exit, _ := os.ReadFile(mem + phaseExit)
	if m, _ := os.ReadFile(mem); !bytes.Equal(m, exit) {
		t.Fatal("the memory FILE is not FILE.exit")
	}
	got := []string{}
	for _, f := range a.Files {
		got = append(got, f.Kind+" "+filepath.Base(f.Path))
	}
	want := []string{"cpu_listing cpu", "cpu_listing cpu.listing", "cpu_tail cpu.tail", "mem_listing mem.listing", "mem_exit mem.exit", "mem_exit mem"}
	if !slices.Equal(got, want) || a.MemProfileRate != 4096 {
		t.Fatalf("files %q rate %d, want %q 4096", got, a.MemProfileRate, want)
	}
}

// TestProfilerNoBoundary: a run that never finishes listing writes the
// listing-phase CPU files for the whole run, no tail and no allocation
// listing file, and reports no boundary; a nil profiler does nothing.
func TestProfilerNoBoundary(t *testing.T) {
	dir := t.TempDir()
	cpu, mem := filepath.Join(dir, "cpu"), filepath.Join(dir, "mem")
	before := runtime.MemProfileRate
	defer func() { runtime.MemProfileRate = before }()
	p, err := startProfiles(cpu, mem, defaultMemProfileRate, nil)
	if err != nil {
		t.Fatal(err)
	}
	spin(20 * time.Millisecond)
	a := p.account()
	p.stop()
	p.listingDone() // after stop: nothing starts
	if !a.PhaseBoundaryAt.IsZero() || a.CPUGapNs != nil || len(a.Files) != 4 {
		t.Fatalf("account: %+v", a)
	}
	profileSpan(t, cpu)
	profileSpan(t, cpu+phaseListing)
	for _, path := range []string{cpu + phaseTail, mem + phaseListing} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Errorf("%s: %v, want none", path, err)
		}
	}
	var none *profiler
	if q, err := startProfiles("", "", 0, nil); q != nil || err != nil {
		t.Fatalf("both off: %v %v", q, err)
	}
	none.stop()
	if none.account() != nil {
		t.Fatal("a nil profiler accounted profiles")
	}
}

// TestPhaseProfilesRun: a sorted run with both variables set writes the
// phase files, and its report's profiles name them, with the boundary at
// listing_done_at's event; unset, the run writes no profile and its report
// has no profiles.
func TestPhaseProfilesRun(t *testing.T) {
	captureStderr(t)
	before := runtime.MemProfileRate
	defer func() { runtime.MemProfileRate = before }()
	src := t.TempDir()
	for i := range 50 {
		if err := os.WriteFile(filepath.Join(src, fmt.Sprintf("k%03d", i)), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	readReport := func(path string) (listing.Report, map[string]any) {
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var r listing.Report
		var raw map[string]any
		if err := json.Unmarshal(b, &r); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(b, &raw); err != nil {
			t.Fatal(err)
		}
		return r, raw
	}
	var buf bytes.Buffer

	// Unset: nothing written beside the report, no profiles.
	t.Setenv("SWATH_CPUPROFILE", "")
	t.Setenv("SWATH_MEMPROFILE", "")
	t.Setenv("SWATH_MEMPROFILERATE", "")
	base := t.TempDir()
	report := filepath.Join(base, "report.json")
	if err := run(context.Background(), []string{src, "-o", filepath.Join(base, "OUT"), "--no-progress", "--no-stats", "--report", report}, &buf, false); err != nil {
		t.Fatal(err)
	}
	if r, raw := readReport(report); r.Profiles != nil || raw["profiles"] != nil || r.Timings.ListingDoneAt.IsZero() {
		t.Fatalf("unset: profiles %+v, listing_done_at %v", r.Profiles, r.Timings.ListingDoneAt)
	}
	if ents, _ := os.ReadDir(base); len(ents) != 2 {
		t.Fatalf("unset: %d entries beside the output, want the output and the report", len(ents))
	}

	base = t.TempDir()
	cpu, mem, report := filepath.Join(base, "cpu.pprof"), filepath.Join(base, "mem.pprof"), filepath.Join(base, "report.json")
	t.Setenv("SWATH_CPUPROFILE", cpu)
	t.Setenv("SWATH_MEMPROFILE", mem)
	t.Setenv("SWATH_MEMPROFILERATE", "512")
	out := filepath.Join(base, "OUT")
	if err := run(context.Background(), []string{src, "-o", out, "--no-progress", "--no-stats", "--report", report}, &buf, false); err != nil {
		t.Fatal(err)
	}
	r, _ := readReport(report)
	p := r.Profiles
	if p == nil || r.Timings.ListingDoneAt.IsZero() {
		t.Fatalf("profiles %+v, listing_done_at %v", p, r.Timings.ListingDoneAt)
	}
	// The boundary is listing_done_at's event, microseconds after it (the
	// report's times are at microsecond precision or finer).
	if d := p.PhaseBoundaryAt.Sub(r.Timings.ListingDoneAt); d < 0 || d > time.Second {
		t.Fatalf("boundary %v, listing_done_at %v", p.PhaseBoundaryAt, r.Timings.ListingDoneAt)
	}
	if p.MemProfileRate != 512 || p.CPUGapNs == nil {
		t.Fatalf("profiles %+v", p)
	}
	for _, f := range p.Files {
		if _, err := os.Stat(f.Path); err != nil {
			t.Errorf("%s %s: %v", f.Kind, f.Path, err)
		}
	}
	if len(p.Files) != 6 {
		t.Fatalf("files %+v", p.Files)
	}
	ls, _ := profileSpan(t, cpu+phaseListing)
	ts, _ := profileSpan(t, cpu+phaseTail)
	if ls.After(r.Timings.ListingDoneAt) || ts.Before(r.Timings.ListingDoneAt) {
		t.Fatalf("listing profile from %v, tail from %v, listing_done_at %v", ls, ts, r.Timings.ListingDoneAt)
	}
	// run.json, written at the commit, names the same files.
	runJSON, _ := readReport(filepath.Join(out, "run.json"))
	if runJSON.Profiles == nil || len(runJSON.Profiles.Files) != len(p.Files) || !runJSON.Profiles.PhaseBoundaryAt.Equal(p.PhaseBoundaryAt) {
		t.Fatalf("run.json profiles %+v", runJSON.Profiles)
	}
}

// TestPhaseProfileRefusals: a phase file is checked as its FILE is (here
// the output directory, and a symbolic link), and SWATH_MEMPROFILERATE must be
// a byte count: each is refused with exit 2 before the run.
func TestPhaseProfileRefusals(t *testing.T) {
	captureStderr(t)
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "a"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	base := t.TempDir()
	var buf bytes.Buffer
	t.Setenv("SWATH_MEMPROFILERATE", "")
	t.Setenv("SWATH_CPUPROFILE", filepath.Join(base, "rows"))
	err := run(context.Background(), []string{src, "-o", filepath.Join(base, "rows.tail"), "--no-progress", "--no-stats"}, &buf, false)
	if exitOf(err) != exitUsage || !strings.Contains(err.Error(), "rows.tail: is the output directory") {
		t.Fatalf("FILE.tail is the output: %v", err)
	}
	if _, err := os.Stat(filepath.Join(base, "rows")); !os.IsNotExist(err) {
		t.Fatalf("a profile was written: %v", err)
	}
	t.Setenv("SWATH_CPUPROFILE", "")

	target := filepath.Join(base, "target")
	if err := os.WriteFile(target, []byte("keep"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(base, "mem.exit")); err != nil {
		t.Skip("no symbolic links:", err)
	}
	t.Setenv("SWATH_MEMPROFILE", filepath.Join(base, "mem"))
	err = run(context.Background(), []string{src, "-o", filepath.Join(base, "OUT"), "--no-progress", "--no-stats"}, &buf, false)
	if exitOf(err) != exitUsage || !strings.Contains(err.Error(), "symbolic link") {
		t.Fatalf("FILE.exit a link: %v", err)
	}
	if b, _ := os.ReadFile(target); string(b) != "keep" {
		t.Fatal("the link's target changed")
	}

	t.Setenv("SWATH_MEMPROFILE", filepath.Join(base, "mem2"))
	for _, v := range []string{"-1", "x", "1.5"} {
		t.Setenv("SWATH_MEMPROFILERATE", v)
		err = run(context.Background(), []string{src, "-o", filepath.Join(base, "OUT2"), "--no-progress", "--no-stats"}, &buf, false)
		if exitOf(err) != exitUsage || !strings.Contains(err.Error(), "SWATH_MEMPROFILERATE") {
			t.Fatalf("SWATH_MEMPROFILERATE=%s: %v", v, err)
		}
	}
}
