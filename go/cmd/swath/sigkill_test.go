// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build !windows

package main

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"syscall"
	"testing"
	"time"

	pq "github.com/parquet-go/parquet-go"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/faults"
	"github.com/varveio/swath/go/internal/source/gcs/gcstest"
	"github.com/varveio/swath/go/internal/source/s3/s3test"
)

// TestSIGKILLResume kills a real swath process with SIGKILL once parts are
// published, resumes it with swath resume, and requires the dataset to be
// byte-identical to an uninterrupted run's.
func TestSIGKILLResume(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the binary")
	}
	bin := filepath.Join(t.TempDir(), "swath")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	// Slow enough to kill mid-run.
	url := s3test.Serve(t, s3test.Delayed(s3test.Handler(s3test.Backend(t, s3test.Keys(6000, 11))), 20*time.Millisecond))
	args := func(out string, extra ...string) []string {
		return append([]string{"list", "s3://bkt", "-endpoint", url, "-path-style", "-anonymous", "-workers", "8",
			"-part-size", "4KiB", "-row-budget", "512KiB", "-o", out}, extra...)
	}
	ref := filepath.Join(t.TempDir(), "ref")
	if out, err := exec.Command(bin, args(ref)...).CombinedOutput(); err != nil {
		t.Fatalf("reference run: %v\n%s", err, out)
	}
	out := filepath.Join(t.TempDir(), "ds")
	cmd := exec.Command(bin, args(out)...)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		if _, err := os.Stat(filepath.Join(out, "data", "part-00003.parquet")); err == nil {
			break
		}
		if time.Now().After(deadline) {
			cmd.Process.Kill()
			t.Fatal("no part published in time")
		}
		time.Sleep(2 * time.Millisecond)
	}
	if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	cmd.Wait()
	if _, err := os.Stat(filepath.Join(out, "_SUCCESS")); err == nil {
		t.Skip("the run finished before the kill")
	}
	// Only runtime flags may change on resume: a frozen one is a usage error.
	if o, err := exec.Command(bin, "resume", out, "--part-size", "8KiB").CombinedOutput(); err == nil || !bytes.Contains(o, []byte("only workers")) {
		t.Fatalf("resume with a frozen flag: %v\n%s", err, o)
	} else if ee, ok := err.(*exec.ExitError); !ok || ee.ExitCode() != 2 {
		t.Fatalf("resume with a frozen flag: exit %v, want 2", err)
	}
	if o, err := exec.Command(bin, "resume", "--workers", "3", out).CombinedOutput(); err != nil {
		t.Fatalf("resume: %v\n%s", err, o)
	}
	parts, _ := filepath.Glob(filepath.Join(ref, "data", "*.parquet"))
	got, _ := filepath.Glob(filepath.Join(out, "data", "*.parquet"))
	if len(parts) < 5 || len(got) != len(parts) {
		t.Fatalf("%d parts, reference %d", len(got), len(parts))
	}
	for _, p := range parts {
		a, _ := os.ReadFile(p)
		b, err := os.ReadFile(filepath.Join(out, "data", filepath.Base(p)))
		if err != nil || !bytes.Equal(a, b) {
			t.Fatalf("%s differs after resume (%v)", filepath.Base(p), err)
		}
	}
	insp, err := exec.Command(bin, "show", "--journal", out).Output()
	if err != nil || !bytes.Contains(insp, []byte(`"type":"header"`)) {
		t.Fatalf("show --journal: %v %s", err, insp)
	}
}

// TestSIGKILLAtEveryPoint makes the real process SIGKILL itself at each
// crash point (response read, encoding, journal append and sync, rename,
// publication, ...), resumes, and compares with an uninterrupted run, for
// each journaled provider. The byte-comparison arms run (and resume) with
// --part-interval=0: the default 30 s interval would close a part by time
// when a run under load is slow, and the part boundaries would then differ
// between runs whose rows agree. The "s3-interval" arm runs with a 1 ms
// part interval against a slower bucket, so parts are closed by time and
// boundaries depend on timing: it requires the clean run's rows, in order,
// and no refusal.
func TestSIGKILLAtEveryPoint(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the binary")
	}
	bin := filepath.Join(t.TempDir(), "swath")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	b := s3test.Backend(t, s3test.Keys(4000, 7))
	url := s3test.Serve(t, s3test.Handler(b))
	slowURL := s3test.Serve(t, s3test.Delayed(s3test.Handler(b), 3*time.Millisecond))
	gcs := gcstest.Bucket(t, gcstest.Keys(4000), 0)
	common := []string{"-workers", "8", "-part-size", "4KiB", "-row-budget", "512KiB"}
	for _, prov := range []struct {
		name    string
		src     []string
		runtime []string // also given to swath resume
		byTime  bool     // parts are closed by time: compare rows, not part bytes
	}{
		{"s3", []string{"s3://bkt", "-endpoint", url, "-path-style", "-anonymous"}, []string{"--part-interval=0"}, false},
		{"gcs", []string{"gs://bkt", "--gcs-endpoint", gcs, "--gcs-anonymous"}, []string{"--part-interval=0"}, false},
		{"s3-interval", []string{"s3://bkt", "-endpoint", slowURL, "-path-style", "-anonymous"}, []string{"--part-interval", "1ms"}, true},
	} {
		args := func(out string, extra ...string) []string {
			a := append(append(append(append([]string{"list"}, prov.src...), common...), prov.runtime...), "-o", out)
			return append(a, extra...)
		}
		ref := filepath.Join(t.TempDir(), "ref")
		if out, err := exec.Command(bin, args(ref)...).CombinedOutput(); err != nil {
			t.Fatalf("%s reference: %v\n%s", prov.name, err, out)
		}
		for _, point := range []string{"response-read", "split", "range-done", "encoding", "part-closed", "journal-append", "journal-sync",
			"recorded", "rename", "renamed-unsynced", "renamed", "published", "manifest"} {
			for _, nth := range []string{"1", "5"} {
				at := prov.name + " " + point + ":" + nth
				out := filepath.Join(t.TempDir(), "ds")
				cmd := exec.Command(bin, args(out)...)
				cmd.Env = append(os.Environ(), "SWATH_TEST_KILL_AT="+point+":"+nth)
				err := cmd.Run()
				if err == nil {
					t.Logf("%s not reached; run completed", at)
					continue
				}
				if ws, ok := cmd.ProcessState.Sys().(syscall.WaitStatus); !ok || !ws.Signaled() || ws.Signal() != syscall.SIGKILL {
					t.Fatalf("%s: not killed: %v", at, err)
				}
				if o, err := exec.Command(bin, append(append([]string{"resume"}, prov.runtime...), out)...).CombinedOutput(); err != nil {
					switch {
					case point == "manifest" && bytes.Contains(o, []byte("already complete")):
						// Killed after the commit: the dataset is complete,
						// which is what swath resume reports.
					case bytes.Contains(o, []byte("before recording its command")):
						// Killed before the journal header was durable:
						// nothing was published; the run starts over.
						if o, err := exec.Command(bin, args(out, "--overwrite")...).CombinedOutput(); err != nil {
							t.Fatalf("%s rerun: %v\n%s", at, err, o)
						}
					default:
						t.Fatalf("%s resume: %v\n%s", at, err, o)
					}
				}
				if prov.byTime {
					sameRows(t, at, ref, out)
				} else {
					sameParts(t, at, ref, out)
				}
			}
		}
	}
}

// TestSIGKILLInSegments makes the real process SIGKILL itself inside the
// segment-parallel tail (faults.SegmentPoints: the segments committed and
// none opened; a segment's part published with later ones open; every row
// handed on and the last part held before the router's Close), resumes,
// and requires the parts byte-identical, and the manifest but for its
// observation times, to an
// uninterrupted run's. The fixture is sized so the tail engages: listing
// against an undelayed bucket outruns four encoders, the rows wait, and
// part 0 freezes the size limit long before listing ends. The
// clean run must report segments engaged with more than one worker, and
// every kill must be reached, so a fixture that stops reaching the tail
// fails instead of passing vacuously. Two arms: a row budget that holds
// the rest in memory, and one that spills it, so the segments read spill
// frames on their own descriptors; both large enough for three workers
// (the budget sizes them: order.budgetWorkers). The child runs at GOMAXPROCS 4
// whatever the box (segments need two processors beside the drainer), and
// with --part-interval=0, so part boundaries are the size rule's alone.
func TestSIGKILLInSegments(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the binary")
	}
	bin := filepath.Join(t.TempDir(), "swath")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	url := s3test.Serve(t, s3test.Handler(s3test.Backend(t, s3test.Keys(200000, 13))))
	env := append(os.Environ(), "GOMAXPROCS=4")
	for _, arm := range []struct {
		name, budget string
		spilled      bool // the segments read spill frames
	}{{"memory", "64MiB", false}, {"spill", "24MiB", true}} {
		runtime := []string{"--part-interval=0", "--encoders", "4", "--row-budget", arm.budget}
		args := func(out string, extra ...string) []string {
			a := []string{"list", "s3://bkt", "-endpoint", url, "-path-style", "-anonymous", "-workers", "8",
				"-part-size", "16KiB", "-o", out}
			return append(append(a, runtime...), extra...)
		}
		ref := filepath.Join(t.TempDir(), "ref")
		report := filepath.Join(t.TempDir(), "report.json")
		cmd := exec.Command(bin, args(ref, "--report", report)...)
		cmd.Env = env
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("%s reference run: %v\n%s", arm.name, err, out)
		}
		var rep struct {
			Resources struct {
				Drain struct {
					Segments *struct {
						Reason     string `json:"reason"`
						Segments   int    `json:"segments"`
						Workers    int    `json:"workers"`
						MaxWorkers int    `json:"max_workers"`
						Rows       int64  `json:"rows"`
						ReadNs     int64  `json:"read_ns"`
					} `json:"segments"`
				} `json:"drain"`
			} `json:"resources"`
		}
		if b, err := os.ReadFile(report); err != nil || json.Unmarshal(b, &rep) != nil {
			t.Fatalf("%s report: %v", arm.name, err)
		}
		sg := rep.Resources.Drain.Segments
		if sg == nil || sg.Reason != "engaged" || sg.Workers < 2 || sg.MaxWorkers < 2 || sg.Segments < 4 || (sg.ReadNs > 0) != arm.spilled {
			t.Fatalf("%s: the fixture does not reach the segment tail it means to: %+v", arm.name, sg)
		}
		t.Logf("%s reference: %d segments, %d workers (cap %d), %d rows, %d ns reading spill", arm.name, sg.Segments, sg.Workers, sg.MaxWorkers, sg.Rows, sg.ReadNs)
		for _, point := range faults.SegmentPoints {
			nths := []string{"1"}
			if point == "segment-published" {
				nths = append(nths, "2")
			}
			for _, nth := range nths {
				at := arm.name + " " + point + ":" + nth
				out := filepath.Join(t.TempDir(), "ds")
				cmd := exec.Command(bin, args(out)...)
				cmd.Env = append(env, "SWATH_TEST_KILL_AT="+point+":"+nth)
				o, err := cmd.CombinedOutput()
				if err == nil {
					t.Fatalf("%s not reached: the run completed\n%s", at, o)
				}
				if ws, ok := cmd.ProcessState.Sys().(syscall.WaitStatus); !ok || !ws.Signaled() || ws.Signal() != syscall.SIGKILL {
					t.Fatalf("%s: not killed: %v\n%s", at, err, o)
				}
				res := exec.Command(bin, append(append([]string{"resume"}, runtime...), out)...)
				res.Env = env
				if o, err := res.CombinedOutput(); err != nil {
					t.Fatalf("%s resume: %v\n%s", at, err, o)
				}
				sameParts(t, at, ref, out)
				if a, b := manifestBytes(t, ref), manifestBytes(t, out); !bytes.Equal(a, b) {
					t.Fatalf("%s: the manifest differs:\n%s\n%s", at, a, b)
				}
			}
		}
	}
}

// manifestBytes is dir's manifest without its observation times
// (published_at, the listing's start and finish), which differ between
// runs whose datasets agree.
func manifestBytes(t *testing.T, dir string) []byte {
	t.Helper()
	var m dataset.Manifest
	b, err := os.ReadFile(filepath.Join(dir, dataset.ManifestName))
	if err != nil || json.Unmarshal(b, &m) != nil {
		t.Fatalf("manifest of %s: %v", dir, err)
	}
	m.PublishedAt, m.Coverage.ListingStartedAt, m.Coverage.ListingFinishedAt = time.Time{}, time.Time{}, time.Time{}
	b, err = json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// sameRows compares the keys of two datasets, in manifest order: parts
// closed by time differ between runs, their rows may not.
func sameRows(t *testing.T, at, ref, out string) {
	t.Helper()
	keys := func(dir string) []string {
		var m dataset.Manifest
		b, err := os.ReadFile(filepath.Join(dir, dataset.ManifestName))
		if err != nil || json.Unmarshal(b, &m) != nil {
			t.Fatalf("%s: manifest of %s: %v", at, dir, err)
		}
		var ks []string
		for _, p := range m.Parts {
			f, err := os.Open(filepath.Join(dir, p.Path))
			if err != nil {
				t.Fatal(err)
			}
			pf, err := pq.OpenFile(f, p.Bytes)
			if err != nil {
				t.Fatal(err)
			}
			rows := make([]pq.Row, pf.NumRows())
			r := pq.NewReader(pf)
			n, _ := r.ReadRows(rows)
			r.Close()
			f.Close()
			for _, row := range rows[:n] {
				ks = append(ks, string(row[0].ByteArray()))
			}
		}
		return ks
	}
	want, got := keys(ref), keys(out)
	if !slices.Equal(want, got) {
		t.Fatalf("%s: %d rows, want %d (or they differ)", at, len(got), len(want))
	}
}
