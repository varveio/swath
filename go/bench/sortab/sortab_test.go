// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package sortab

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/varveio/swath/go/internal/dataset/datasettest"
	"github.com/varveio/swath/go/internal/listing"
)

// small is a trace the unit tests list in well under a second per arm.
func small(name string) Shape {
	return Shape{Name: name, Keys: 40_000, Ranges: 24, PageKeys: 500, LatencyMs: 1, SlowLatencyMs: 8, SlowKeys: 4_000, Skew: 1.1, Jitter: 0.3, Seed: 7}
}

var smallLimits = Limits{Workers: 8, Memory: 1 << 20, Encoders: 2, PartSize: 64 << 10, SpillMax: -1}

// TestLayout: every shape tiles its keyspace, keys ascend across ranges,
// and a key's index parses back.
func TestLayout(t *testing.T) {
	for _, name := range []string{"slowfirst", "slowlast", "flat", "skewed"} {
		l, err := small(name).layout()
		if err != nil {
			t.Fatal(err)
		}
		if l.start[0] != 0 || l.start[len(l.start)-1] != l.Keys {
			t.Fatalf("%s: ranges cover %d..%d", name, l.start[0], l.start[len(l.start)-1])
		}
		prev := ""
		for i := int64(0); i < l.Keys; i += 97 {
			k := l.Key(i)
			if k <= prev {
				t.Fatalf("%s: key %d %q after %q", name, i, k, prev)
			}
			if j, err := indexOf(k); err != nil || j != i {
				t.Fatalf("%s: %q parses to %d, %v", name, k, j, err)
			}
			prev = k
		}
		for r := range l.Ranges {
			if l.start[r+1] <= l.start[r] {
				t.Fatalf("%s: range %d empty", name, r)
			}
			if got := l.rangeOf(l.start[r]); got != r {
				t.Fatalf("%s: rangeOf(start of %d) = %d", name, r, got)
			}
		}
	}
}

// TestArmsProduceTheDataset: both arms, uninterrupted and in process, list
// every shape into a dataset that verify accepts (exact rows, payloads,
// strict adjacency), with B spilling several runs.
func TestArmsProduceTheDataset(t *testing.T) {
	datasettest.RequireJournal(t)
	for _, name := range []string{"slowfirst", "flat", "skewed"} {
		l, _ := small(name).layout()
		for _, arm := range []string{"A", "B"} {
			dir := t.TempDir()
			out := filepath.Join(dir, "out")
			p, err := NewProvider(small(name), "")
			if err != nil {
				t.Fatal(err)
			}
			var runs int
			if arm == "A" {
				if _, err := runA(context.Background(), p, out, smallLimits, false, &listing.Progress{}); err != nil {
					t.Fatal(err)
				}
			} else {
				var enc, parts atomic.Int64
				st, err := runB(context.Background(), p, out, filepath.Join(dir, "staging"), smallLimits, false, &enc, &parts)
				if err != nil {
					t.Fatal(err)
				}
				runs = st.Runs
				if st.Runs < 2 || parts.Load() != int64(st.Parts) {
					t.Fatalf("B: %+v, %d parts published", st, parts.Load())
				}
			}
			v, err := verify(out, l)
			if err != nil {
				t.Fatalf("%s %s: %v", name, arm, err)
			}
			if v.parts < 2 {
				t.Fatalf("%s %s: %d parts", name, arm, v.parts)
			}
			t.Logf("%s %s: %d rows, %d parts, %d runs", name, arm, v.rows, v.parts, runs)
		}
	}
}

// TestKillAndRecover drives both arms through a SIGKILL at peak
// speculation (the slow first range half listed, its siblings done) and
// the resume, as child processes, and checks the recovered datasets and
// the resume accounting: B relists at most its in-memory tail plus a page
// per range; A relists everything past its last journaled part.
func TestKillAndRecover(t *testing.T) {
	datasettest.RequireJournal(t)
	if testing.Short() {
		t.Skip("subprocesses")
	}
	sh := small("slowfirst")
	sh.SlowLatencyMs = 100 // the frontier usually finishes long after its siblings
	for _, arm := range []string{"A", "B"} {
		c := Cell{Label: "test", Arm: arm, Shape: sh, Limits: smallLimits, KillFrac: 0.5, Dir: filepath.Join(t.TempDir(), "cell")}
		res := drive(c, os.Args[0])
		if res.Error != "" {
			t.Fatalf("%s: %s", arm, res.Error)
		}
		if len(res.Attempts) != 2 || !res.Attempts[0].Killed || !res.Verified || res.Rows != sh.Keys {
			t.Fatalf("%s: %+v", arm, res)
		}
		if res.RelistedKeys <= 0 || res.ResumeRequests <= 0 || res.ListedAtKill < sh.SlowKeys/2 {
			t.Fatalf("%s: relisted %d keys, %d resume requests, %d listed at the kill", arm, res.RelistedKeys, res.ResumeRequests, res.ListedAtKill)
		}
		if arm == "B" && res.RelistedKeys > smallLimits.Memory/2/100+int64(sh.Ranges*sh.PageKeys) {
			t.Fatalf("B relisted %d keys: more than its buffers and a page per range", res.RelistedKeys)
		}
		t.Logf("%s: listed at kill %d, durable %d, relisted %d keys in %d requests, restart %d ms, peak RSS %d KiB",
			arm, res.ListedAtKill, res.DurableAtKill, res.RelistedKeys, res.ResumeRequests, res.RestartMs, res.PeakRSS)
	}
}

// TestCascade: with the fan-in below the run count, B merges runs in
// stages and still produces the dataset.
func TestCascade(t *testing.T) {
	defer func(n int) { maxFanIn = n }(maxFanIn)
	maxFanIn = 4
	sh := small("flat")
	l, _ := sh.layout()
	dir := t.TempDir()
	p, err := NewProvider(sh, "")
	if err != nil {
		t.Fatal(err)
	}
	var enc, parts atomic.Int64
	st, err := runB(context.Background(), p, filepath.Join(dir, "out"), filepath.Join(dir, "staging"), smallLimits, false, &enc, &parts)
	if err != nil {
		t.Fatal(err)
	}
	if st.Cascades < 5 || st.PeakReaders > 4 || st.Runs > 3 {
		t.Fatalf("%+v", st)
	}
	if _, err := verify(filepath.Join(dir, "out"), l); err != nil {
		t.Fatal(err)
	}
}

// TestMergeRefusesDuplicates: B's merge never hides a key two runs share.
func TestMergeRefusesDuplicates(t *testing.T) {
	a := []*row{{Key: "a"}, {Key: "c"}}
	b := []*row{{Key: "b"}, {Key: "c"}}
	next := mergeSources([]mergeSource{&memSource{order: a}, &memSource{order: b}})
	for {
		r, err := next()
		if err != nil {
			return
		}
		if r == nil {
			t.Fatal("duplicate key merged")
		}
	}
}

// TestOverlap: the relisted-keys count.
func TestOverlap(t *testing.T) {
	a := []interval{{0, 10}, {10, 20}, {40, 50}}
	b := []interval{{15, 45}, {48, 60}}
	if got := overlap(a, b); got != 5+5+2 {
		t.Fatalf("overlap %d", got)
	}
}

// TestNotLinked: this package has no non-test files, so nothing imports
// it, and the command's dependencies do not include it.
func TestNotLinked(t *testing.T) {
	gobin := filepath.Join(runtime.GOROOT(), "bin", "go")
	out, err := exec.Command(gobin, "list", "-f", "{{.GoFiles}}", ".").CombinedOutput()
	if err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if s := strings.TrimSpace(string(out)); s != "[]" {
		t.Fatalf("non-test files in the harness: %s", s)
	}
	out, err = exec.Command(gobin, "list", "-deps", "../../cmd/swath").CombinedOutput()
	if err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	for _, dep := range strings.Fields(string(out)) {
		if strings.Contains(dep, "/bench/") {
			t.Fatalf("the command depends on %s", dep)
		}
	}
}
